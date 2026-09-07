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
                let legacyProgress = LegacyModelDownloadProgress()
                let isManifest = entry.catalogModel.r2Prefix != nil && entry.catalogModel.aggregateSHA256 != nil
                let onProgress: (@Sendable (ModelDownloader.ProgressEvent) -> Void)?
                if isManifest { onProgress = nil }
                else { onProgress = { event in legacyProgress.update(event) } }
                try await downloader.downloadForStorage(model: entry.catalogModel, onProgress: onProgress)
                OnboardingUI.line("Downloaded \(OnboardingUI.clean(entry.displayName)).")
            } catch {
                OnboardingUI.line("Download interrupted: \(OnboardingUI.clean(String(describing: error)))")
                OnboardingUI.line("Run darkbloom start to retry. Completed files are kept and partial downloads resume.")
                throw error
            }
        }
    }
}

/// Manifest downloads already own their progress UI. Legacy catalog entries
/// report byte callbacks, throttled here so long downloads also show activity.
private final class LegacyModelDownloadProgress: @unchecked Sendable {
    private let lock = NSLock()
    private var lastUpdate = Date.distantPast
    private var lastFile = ""
    func update(_ progress: ModelDownloader.ProgressEvent) {
        lock.lock()
        defer { lock.unlock() }
        let now = Date()
        guard progress.file != lastFile || now.timeIntervalSince(lastUpdate) >= 2 else { return }
        lastFile = progress.file
        lastUpdate = now
        OnboardingUI.line("\(OnboardingUI.clean(progress.file)) · \(String(format: "%.1f MB", Double(progress.bytesDownloaded) / 1_000_000)) downloaded")
    }
}
