import Foundation
import ProviderCore

extension Start {
    /// One Swift catalog/fit projection for both terminal frontends. Uses the
    /// unfiltered local scan and total physical RAM; selection is storage intent.
    static func loadPickerEntries(client: ModelCatalogClient, snapshot: RuntimeSnapshot,
                                  config: ProviderConfig, runtimeCapabilities: Set<ProviderRuntimeCapability>) async throws -> [PickerEntry] {
        let catalogSnapshot = try await client.fetchCatalogSnapshot(typeFilter: "text", includeAliases: true)
        let eligibleCatalog = Self.evaluateEligiblePickerCatalog(
            models: catalogSnapshot.models,
            aliases: catalogSnapshot.aliases,
            runtimeCapabilities: runtimeCapabilities, includeIneligible: true)
        let catalog = Self.pickerCatalogRows(catalog: eligibleCatalog)

        let memoryGb: Double = Double(snapshot.hardware?.memoryGb ?? 16)

        // "Downloaded" must be computed from an UNFILTERED on-disk scan: the
        // memory-filtered `snapshot.models` drops models too large for available
        // RAM, which would make a fully-downloaded-but-too-big model read "not
        // downloaded" forever on a marginal-RAM box. The filtered scan is only
        // used by the runtime; onboarding fit uses total physical RAM.
        let allLocal = snapshot.hardware.map { ModelScanner.scanAllModels(hardwareInfo: $0) } ?? []
        let downloadedIDs = Set(allLocal.map(\.id))
        let localMemoryByID = Dictionary(allLocal.map { ($0.id, $0.estimatedMemoryGb) }, uniquingKeysWith: { first, _ in first })
        // Builds with an interrupted foreground download staged on disk: show
        // "resuming" so re-selecting finishes rather than restarts.
        let resumableIDs = Set(catalog.compactMap { row -> String? in
            guard !downloadedIDs.contains(row.model.id), let prefix = row.model.r2Prefix else { return nil }
            return ModelDownloader.hasResumableStaging(modelID: row.model.id, r2Prefix: prefix) ? row.model.id : nil
        })

        let entries = Start.buildPickerEntries(
            rows: catalog,
            downloadedIDs: downloadedIDs,
            localMemoryByID: localMemoryByID,
            resumableIDs: resumableIDs,
            memoryGb: memoryGb,
            reserveGb: config.provider.memoryReserveGB,
            runtimeCapabilities: runtimeCapabilities
        )

        return entries
    }
}
