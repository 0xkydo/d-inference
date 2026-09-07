import Foundation
import ArgumentParser
import ProviderCore
import Darwin

extension Start {
    /// Fetches the model catalog from the coordinator, shows an interactive
    /// terminal picker, downloads any missing models, and returns the
    /// selected model IDs.
    internal func interactiveCatalogPicker(
        snapshot: RuntimeSnapshot,
        config: ProviderConfig,
        coordinatorURL: String,
        runtimeCapabilities: Set<ProviderRuntimeCapability>
    ) async throws -> [String] {
        let client = ModelCatalogClient(coordinatorURL: coordinatorURL)

        let catalogSnapshot: CatalogSnapshot
        do {
            catalogSnapshot = try await client.fetchCatalogSnapshot(typeFilter: "text", includeAliases: true)
        } catch {
            printError("Could not fetch model catalog from coordinator: \(error)")
            printError("hint: check your coordinator URL or use --model to specify models directly")
            throw ExitCode.failure
        }

        let eligibleCatalog = Self.evaluateEligiblePickerCatalog(
            models: catalogSnapshot.models,
            aliases: catalogSnapshot.aliases,
            runtimeCapabilities: runtimeCapabilities, includeIneligible: true)
        let catalog = Self.pickerCatalogRows(catalog: eligibleCatalog)

        guard !catalog.isEmpty else {
            printError("No models in the coordinator catalog.")
            throw ExitCode.failure
        }

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

        guard !entries.isEmpty else {
            printError("No public models are available.")
            throw ExitCode.failure
        }

        let saved = guidedOnboarding ? OnboardingState.load()?.selectedModelIDs ?? [] : []
        let selectedIndices = try runModelPicker(
            entries: entries, memoryGb: memoryGb,
            budgetGiB: Self.pickerBudgetGiB(memoryGb: memoryGb, reserveGb: config.provider.memoryReserveGB),
            initialIDs: Set(saved))
        guard !selectedIndices.isEmpty else { throw CancellationError() }
        let selected = selectedIndices.map { entries[$0] }
        if guidedOnboarding {
            var state = OnboardingState.load() ?? OnboardingState(coordinatorURL: coordinatorURL)
            state.selectedModelIDs = selected.map(\.id)
            try state.save()
        }
        try await downloadPickerModels(selected, client: client, runtimeCapabilities: runtimeCapabilities)
        let runnable = selected.filter { $0.fitReason == nil }
        if runnable.count != selected.count {
            OnboardingUI.line("Additional models are downloaded. Only models that fit are enabled by this selection.")
        }
        return runnable.map(\.id)
    }

}
