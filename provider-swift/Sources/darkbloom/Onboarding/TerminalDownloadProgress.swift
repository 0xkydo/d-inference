import Foundation
import ProviderCore

/// Callback-driven rendering, with no detached render task to outlive a download.
final class TerminalDownloadProgress: @unchecked Sendable {
    private let lock = NSLock()
    private let progress = ManifestDownloadProgress()
    private let renderer = ProgressRenderer()
    private var last = Date.distantPast
    private var registered = Set<String>()
    func update(_ event: ModelDownloader.ProgressEvent) {
        // Inventory snapshots are for the session dashboard; plain CLI rows
        // continue to be driven by the existing individual-file callbacks.
        guard event.files == nil else { return }
        lock.lock(); defer { lock.unlock() }
        if event.phase == .completed { renderer.finish(progress.allProgress); return }
        if event.phase == .publishing { return }
        if event.phase == .verifying {
            if registered.contains(event.file) { progress.complete(label: event.file) }
            return
        }
        if registered.insert(event.file).inserted {
            progress.register(label: event.file, expectedBytes: event.bytesTotal ?? 0, initialBytes: event.bytesDownloaded)
        }
        progress.update(label: event.file, downloadedBytes: event.bytesDownloaded)
        if Date().timeIntervalSince(last) >= 0.25 {
            renderer.render(progress.allProgress); last = Date()
        }
    }
}
