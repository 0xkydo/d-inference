// Interactive disclosure and multi-selection. The result is download intent.
import Foundation
import ArgumentParser
import ProviderCore
import Darwin

extension Start {
    internal func runModelPicker(
        entries: [PickerEntry], memoryGb: Double, budgetGiB: Double,
        initialIDs: Set<String> = []
    ) throws -> [Int] {
        var selected = Set(entries.indices.filter { initialIDs.contains(entries[$0].id) })
        var expanded = selected.contains { entries[$0].fitReason != nil && !entries[$0].downloaded }
        var cursor = 0
        var note = ""
        let interactive = OnboardingUI.isTerminal && ProcessInfo.processInfo.environment["TERM"] != "dumb"
        let terminalMode = interactive ? PickerTerminalMode() : nil
        if interactive && terminalMode == nil { throw ExitCode.failure }
        defer { terminalMode?.restore() }
        var previousRows = 0
        var previousWidth = 0
        var previousHeight = 0
        while true {
            let visible: [Int] = Self.pickerGroups(entries: entries).prefix(expanded ? 3 : 2).flatMap { $0.1 }
            cursor = max(0, min(cursor, visible.count - 1))
            let focused: Int? = visible.isEmpty ? nil : visible[cursor]
            let lines = Self.pickerLines(entries: entries, memoryGb: memoryGb, budgetGiB: budgetGiB,
                                         selected: selected, expanded: expanded, focused: focused)
            var rows: [String] = lines.flatMap { OnboardingUI.wrapped($0) }
            rows.append(note)
            var size = winsize()
            _ = ioctl(STDOUT_FILENO, TIOCGWINSZ, &size)
            let height = Int(size.ws_row) > 0 ? Int(size.ws_row) : 24
            // Keep controls visible and scroll the body around the focused row.
            let limit = max(8, height - 2)
            if interactive && rows.count > limit {
                let focus = rows.firstIndex { $0.hasPrefix("›") } ?? 3
                let bodyCount = max(1, limit - 7)
                let first = min(max(3, focus - bodyCount / 2), max(3, rows.count - 4 - bodyCount))
                rows = Array(rows.prefix(3)) + Array(rows[first..<min(first + bodyCount, rows.count - 4)])
                    + Array(rows.suffix(4))
            }
            if interactive && previousRows > 0 && previousWidth == OnboardingUI.width && previousHeight == height {
                print("\u{1B}[\(previousRows)A\r\u{1B}[J", terminator: "")
            }
            for row in rows {
                let heading = ["Choose models to download", "Downloaded", "Available to download", "Additional models"].contains(row)
                let action = row.hasPrefix("›") || row.hasPrefix("▸") || row.hasPrefix("▾")
                let detail = row.contains("total RAM ·") || row.hasPrefix("Fit uses") || row.contains("show/hide")
                let code = heading || action ? "1" : detail ? "2" : nil
                print("  " + (code.map { OnboardingUI.style(row, $0) } ?? row))
            }
            fflush(stdout)
            previousRows = rows.count; previousWidth = OnboardingUI.width; previousHeight = height
            note = ""
            if !interactive {
                print("  Numbers separated by commas (or all), h to show/hide, q to quit: ", terminator: "")
                fflush(stdout)
                guard let input = readLine() else { throw CancellationError() }
                if input.lowercased() == "h" { expanded.toggle(); continue }
                switch Self.resolveFallbackSelection(input: input, entries: entries, memoryGb: memoryGb, expanded: expanded) {
                case .cancelled: throw CancellationError()
                case .rejected(let message): note = message
                case .selected(let ids): return entries.indices.filter { ids.contains(entries[$0].id) }
                }
                continue
            }
            switch TerminalPickerInput.readKey() {
            case .quit: throw CancellationError()
            case .up: cursor = max(0, cursor - 1)
            case .down: cursor = min(max(0, visible.count - 1), cursor + 1)
            case .expand: expanded.toggle()
            case .toggle:
                if let focused {
                    if selected.contains(focused) { selected.remove(focused) } else { selected.insert(focused) }
                }
            case .confirm:
                if !selected.isEmpty { return selected.sorted() }
                note = "Select a model with Space. Use h to show additional models."
            case .other: break
            }
        }
    }

    static func pickerLines(entries: [PickerEntry], memoryGb: Double, budgetGiB: Double,
                            selected: Set<Int>, expanded: Bool, focused: Int?) -> [String] {
        var lines = ["Choose models to download", String(format: "%.0f GiB total RAM · %.1f GiB model budget after system reserve", memoryGb, budgetGiB), ""]
        let groups = pickerGroups(entries: entries)
        for (title, indices) in groups where !indices.isEmpty {
            if title == "Additional models" && !expanded { continue }
            lines.append(title)
            if title == "Additional models" {
                lines.append("These models will most likely not fit on this Mac, or require different hardware. You can still download them.")
            }
            for index in indices {
                let entry = entries[index]
                lines.append("\(focused == index ? "›" : " ") [\(selected.contains(index) ? "x" : " ")] \(index + 1). \(OnboardingUI.clean(entry.displayName)) · \(String(format: "%.1f GB", entry.sizeGb))\(entry.resumable ? " · resume download" : "")")
                if let reason = entry.fitReason { lines.append("    " + OnboardingUI.clean(reason)) }
            }
            lines.append("")
        }
        let hidden = groups[2].1
        if !hidden.isEmpty {
            let count = selected.intersection(hidden).count
            lines.append("\(expanded ? "▾ Hide" : "▸ Show") \(hidden.count) additional \(hidden.count == 1 ? "model" : "models")\(count > 0 ? " · \(count) selected" : "") · h")
        }
        let size = selected.filter { !entries[$0].downloaded }.reduce(0.0) { $0 + entries[$1].sizeGb }
        lines.append(String(format: "\(selected.count) selected · %.1f GB to download", size))
        lines.append("Fit uses total RAM with padded weights, working memory and KV cache. Each model is estimated separately; running apps matter when loading.")
        lines.append("↑↓ move · Space toggle · h show/hide · Enter confirm · q quit")
        return lines
    }
}
