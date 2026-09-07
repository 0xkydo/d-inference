import Foundation
import ProviderCore

extension Start {
    internal func downloadPickerModels(
        _ entries: [PickerEntry], client: ModelCatalogClient,
        runtimeCapabilities: Set<ProviderRuntimeCapability>
    ) async throws {
        let missing = entries.filter { !$0.downloaded }
        guard !missing.isEmpty else {
            OnboardingUI.line("Selected models are already downloaded.")
            return
        }
        OnboardingUI.heading("Download your models")
        let downloader = ModelDownloader(catalogClient: client, runtimeCapabilities: runtimeCapabilities)
        for (index, entry) in missing.enumerated() {
            OnboardingUI.line("[\(index + 1)/\(missing.count)] \(entry.resumable ? "Resuming" : "Downloading") \(OnboardingUI.clean(entry.displayName))")
            if let reason = entry.fitReason { OnboardingUI.line("Download only: " + OnboardingUI.clean(reason)) }
            do {
                // Explicit storage intent only. The normal downloader and all
                // serving gates continue enforcing runtime capabilities.
                let progress = TerminalDownloadProgress()
                try await downloader.downloadForStorage(model: entry.catalogModel, onProgress: { progress.update($0) })
                OnboardingUI.success("Downloaded \(OnboardingUI.clean(entry.displayName)).")
            } catch {
                OnboardingUI.failure("Download interrupted: \(OnboardingUI.clean(String(describing: error)))")
                OnboardingUI.line("Run darkbloom start to retry. Completed files are kept and partial downloads resume.")
                throw error
            }
        }
    }
}
