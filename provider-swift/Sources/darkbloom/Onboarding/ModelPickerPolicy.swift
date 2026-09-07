import Foundation
import ProviderCore

extension Start {
    /// Entry shown in the interactive TUI model picker.
    ///
    /// `downloaded` is computed from an UNFILTERED on-disk check (not the
    /// available-memory-filtered scan) so a fully-downloaded model that exceeds
    /// available RAM still reads "downloaded (won't fit)" rather than "not
    /// downloaded". `resumable` flags a build whose foreground download was
    /// interrupted (staging on disk) so the picker can show "resuming".
    struct PickerEntry: Equatable {
        let id: String
        let catalogModel: CatalogModel
        let displayName: String
        let sizeGb: Double
        let minRamGb: Int?
        let downloaded: Bool
        var resumable: Bool = false
        var estimatedWeightsGiB: Double? = nil
        var fitReason: String? = nil
    }

    struct PickerCatalogRow {
        let model: CatalogModel
        let displayName: String
    }

    struct EligiblePickerCatalog {
        let models: [CatalogModel]
        let aliasDisplayByBuildID: [String: String]
        let hiddenBuildIDs: Set<String>
        let sourceHasAliases: Bool
    }

    /// Keep every public catalog row. Downloaded is based on the unfiltered
    /// scan; fitReason controls disclosure, never whether storage is allowed.
    /// Local memory estimates are already padded by the scanner.
    static func buildPickerEntries(
        rows: [PickerCatalogRow],
        downloadedIDs: Set<String>,
        localMemoryByID: [String: Double],
        resumableIDs: Set<String>,
        memoryGb: Double,
        reserveGb: UInt64 = 4,
        runtimeCapabilities: Set<ProviderRuntimeCapability> = []
    ) -> [PickerEntry] {
        var entries: [PickerEntry] = rows.map { row in
            let model = row.model
            let isDownloaded = downloadedIDs.contains(model.id)
            let diskGb = model.totalSizeBytes.map { Double($0) / 1_000_000_000 } ?? model.sizeGb
            let weights = localMemoryByID[model.id]
                ?? (model.totalSizeBytes.map { Double($0) / 1_073_741_824 } ?? model.sizeGb * 1_000_000_000 / 1_073_741_824) * 1.2
            let budget = pickerBudgetGiB(memoryGb: memoryGb, reserveGb: reserveGb)
            let required = ModelLoadAdmission.requiredToLoadGb(weightsGb: weights)
            var reasons: [String] = []
            if !diskGb.isFinite || diskGb <= 0 || !weights.isFinite || weights <= 0 {
                reasons.append("Memory estimate unavailable")
            }
            if required > budget { reasons.append(String(format: "Estimated %.1f GiB needed", required)) }
            if let minimum = model.minRamGb, Double(minimum) > memoryGb {
                reasons.append("Requires \(minimum) GiB RAM")
            }
            let eligibility = ModelRuntimeRequirements.evaluate(
                modelID: model.id, catalogRequirements: model.requiredProviderCapabilities,
                available: runtimeCapabilities)
            if !eligibility.isEligible {
                reasons.append("Requires " + eligibility.missing.sorted().map {
                    $0 == .appleM5 ? "Apple M5" : $0.rawValue
                }.joined(separator: ", "))
            }
            return PickerEntry(
                id: model.id,
                catalogModel: model,
                displayName: row.displayName,
                sizeGb: diskGb,
                minRamGb: model.minRamGb,
                downloaded: isDownloaded,
                resumable: !isDownloaded && resumableIDs.contains(model.id),
                estimatedWeightsGiB: weights,
                fitReason: reasons.isEmpty ? nil : reasons.joined(separator: "; ")
            )
        }
        // Downloaded first, then larger first.
        entries.sort { a, b in
            if a.downloaded != b.downloaded { return a.downloaded }
            return a.sizeGb > b.sizeGb
        }
        return entries
    }

    /// Total machine capacity, independent of current apps/MLX allocations.
    /// Use the shared OS/config reserve and conservative mixed-model headroom.
    static func pickerBudgetGiB(memoryGb: Double, reserveGb: UInt64 = 4) -> Double {
        let total = UInt64(max(0, memoryGb) * 1_073_741_824)
        let reserve = UnifiedMemoryCap.loadReserveBytes(
            physicalBytes: total, configReserveBytes: reserveGb > UInt64.max / 1_073_741_824 ? .max : reserveGb * 1_073_741_824)
        return Double(total > reserve ? total - reserve : 0) / 1_073_741_824
    }

    static func pickerGroups(entries: [PickerEntry]) -> [(String, [Int])] {
        [
            ("Downloaded", entries.indices.filter { entries[$0].downloaded }),
            ("Available to download", entries.indices.filter { !entries[$0].downloaded && entries[$0].fitReason == nil }),
            ("Additional models", entries.indices.filter { !entries[$0].downloaded && entries[$0].fitReason != nil })
        ]
    }

    /// Outcome of resolving a non-TTY fallback-picker input line.
    enum FallbackSelection: Equatable {
        case cancelled
        case selected([String])
        case rejected(String)
    }

    /// Select only visible rows. Expanding is explicit consent to include the
    /// additional models; this returns download intent, not permission to load.
    static func resolveFallbackSelection(
        input rawInput: String,
        entries: [PickerEntry],
        memoryGb: Double,
        expanded: Bool = false
    ) -> FallbackSelection {
        let input = rawInput.trimmingCharacters(in: .whitespaces)
        guard !input.isEmpty, input.lowercased() != "q" else { return .cancelled }
        let visible = pickerGroups(entries: entries).prefix(expanded ? 3 : 2).flatMap { $0.1 }
        if input.lowercased() == "all" {
            return visible.isEmpty ? .rejected("No visible models. Use h to show additional models.")
                : .selected(visible.map { entries[$0].id })
        }
        var picked: [String] = []
        for token in input.split(separator: ",", omittingEmptySubsequences: false) {
            guard let n = Int(token.trimmingCharacters(in: .whitespaces)), visible.contains(n - 1) else {
                return .rejected("Select a visible row number. Use h to show additional models.")
            }
            let id = entries[n - 1].id
            if !picked.contains(id) { picked.append(id) }
        }
        return .selected(picked)
    }
}
