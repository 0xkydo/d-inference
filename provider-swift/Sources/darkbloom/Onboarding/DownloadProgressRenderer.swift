import Foundation
import ProviderCore
import Darwin

// MARK: - Renderer

/// Renders a multi-line progress display to the terminal using ANSI escape
/// codes. Falls back to simple per-file messages when stdout is not a TTY.
final class ProgressRenderer: @unchecked Sendable {

    private let isTTY: Bool
    private var linesPrinted: Int = 0
    private let lock = NSLock()
    /// Set of labels already printed in non-TTY mode.
    private var printedLabels: Set<String> = []

    init() {
        self.isTTY = isatty(STDOUT_FILENO) != 0
    }

    /// Render a frame given the current file progress snapshot.
    func render(_ files: [FileProgress]) {
        lock.lock()
        defer { lock.unlock() }

        if !isTTY {
            renderPlain(files)
            return
        }
        renderANSI(files)
    }

    /// Final render: clear the progress area and print completion summary.
    func finish(_ files: [FileProgress]) {
        lock.lock()
        defer { lock.unlock() }

        if isTTY {
            // Move up and clear all lines.
            if linesPrinted > 0 {
                print("\u{1B}[\(linesPrinted)A", terminator: "")
                for _ in 0..<linesPrinted {
                    print("\u{1B}[2K")
                }
                print("\u{1B}[\(linesPrinted)A", terminator: "")
                linesPrinted = 0
            }
        }

        // Print final summary lines.
        for f in files {
            let totalStr = Self.formatBytes(f.expectedBytes > 0 ? f.expectedBytes : f.downloadedBytes)
            let elapsed = (f.completionTime ?? Date()).timeIntervalSince(f.startTime)
            let avgSpeed = elapsed > 0.1 ? Double(max(0, f.downloadedBytes - f.baselineBytes)) / elapsed : 0
            let speedStr = Self.formatSpeed(avgSpeed)
            let timeStr = Self.formatDuration(elapsed)
            print("  \u{2713} \(f.label)  \(totalStr)  \(speedStr)  \(timeStr)")
        }
    }

    // MARK: - ANSI rendering

    private func renderANSI(_ files: [FileProgress]) {
        // Move cursor up to overwrite previous render.
        if linesPrinted > 0 {
            print("\u{1B}[\(linesPrinted)A", terminator: "")
        }

        let termWidth = Self.terminalWidth()
        var lines = 0
        for f in files {
            print("\u{1B}[2K", terminator: "")  // Clear the line
            let line = Self.formatLine(f, termWidth: termWidth)
            print(line)
            lines += 1
        }
        linesPrinted = lines
        fflush(stdout)
    }

    private func renderPlain(_ files: [FileProgress]) {
        for f in files where f.completed && !printedLabels.contains(f.label) {
            printedLabels.insert(f.label)
            let totalStr = Self.formatBytes(f.expectedBytes > 0 ? f.expectedBytes : f.downloadedBytes)
            print("  \u{2713} \(f.label)  \(totalStr)")
        }
    }

    // MARK: - Line formatting

    private static func formatLine(_ f: FileProgress, termWidth: Int) -> String {
        if f.completed {
            let totalStr = formatBytes(f.expectedBytes > 0 ? f.expectedBytes : f.downloadedBytes)
            let elapsed = (f.completionTime ?? Date()).timeIntervalSince(f.startTime)
            let avgSpeed = elapsed > 0.1 ? Double(max(0, f.downloadedBytes - f.baselineBytes)) / elapsed : 0
            return "  \u{2713} \(f.label)  \(totalStr)  \(formatSpeed(avgSpeed))  done"
        }

        let pct = Int(f.fraction * 100)
        let dlStr = formatBytes(f.downloadedBytes)
        let totStr = formatBytes(f.expectedBytes)
        let speedStr = formatSpeed(f.speed)
        let etaStr: String
        if let eta = f.eta {
            etaStr = "ETA \(formatDuration(eta))"
        } else {
            etaStr = "---"
        }

        // Assemble the suffix: "  62%  2.1/4.8 GB  113 MB/s  ETA 24s"
        let suffix = "  \(String(format: "%3d", pct))%  \(dlStr)/\(totStr)  \(speedStr)  \(etaStr)"

        // Calculate bar width: total - label - prefix - suffix - brackets - spaces
        let labelMaxWidth = min(f.label.count, 45)
        let label = f.label.count > labelMaxWidth
            ? String(f.label.suffix(labelMaxWidth - 1)).padding(toLength: labelMaxWidth, withPad: " ", startingAt: 0)
            : f.label
        let prefix = "  \(label)  ["
        let postfix = "]\(suffix)"
        let barWidth = max(10, termWidth - prefix.count - postfix.count)

        let filled = Int(f.fraction * Double(barWidth))
        let empty = barWidth - filled
        let bar = String(repeating: "\u{2588}", count: filled) + String(repeating: "\u{2591}", count: empty)

        return "\(prefix)\(bar)\(postfix)"
    }

    // MARK: - Formatting helpers

    static func formatBytes(_ bytes: Int64) -> String {
        let b = Double(bytes)
        if b < 1024 { return "\(bytes) B" }
        if b < 1_048_576 { return String(format: "%.1f KB", b / 1024) }
        if b < 1_073_741_824 { return String(format: "%.1f MB", b / 1_048_576) }
        return String(format: "%.1f GB", b / 1_073_741_824)
    }

    static func formatSpeed(_ bytesPerSec: Double) -> String {
        if bytesPerSec < 1024 { return String(format: "%.0f B/s", bytesPerSec) }
        if bytesPerSec < 1_048_576 { return String(format: "%.0f KB/s", bytesPerSec / 1024) }
        if bytesPerSec < 1_073_741_824 { return String(format: "%.0f MB/s", bytesPerSec / 1_048_576) }
        return String(format: "%.1f GB/s", bytesPerSec / 1_073_741_824)
    }

    static func formatDuration(_ seconds: Double) -> String {
        DurationFormatting.compact(
            seconds,
            secondsInMinuteRange: true,
            elideZeroMinutesInHourRange: false
        )
    }

    static func terminalWidth() -> Int {
        #if canImport(Darwin)
        var w = winsize()
        if ioctl(STDOUT_FILENO, TIOCGWINSZ, &w) == 0, w.ws_col > 0 {
            return Int(w.ws_col)
        }
        #endif
        return 80
    }
}
