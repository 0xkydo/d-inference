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

        let entries: [PickerEntry]
        do {
            entries = try await Self.loadPickerEntries(client: client, snapshot: snapshot, config: config,
                                                      runtimeCapabilities: runtimeCapabilities)
        } catch {
            printError("Could not fetch model catalog from coordinator: \(error)")
            printError("hint: check your coordinator URL or use --model to specify models directly")
            throw ExitCode.failure
        }
        let memoryGb = Double(snapshot.hardware?.memoryGb ?? 16)

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
