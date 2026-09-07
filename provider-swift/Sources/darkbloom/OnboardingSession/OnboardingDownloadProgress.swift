import Foundation
import ProviderCore

/// Accumulate concurrent file callbacks before the pipe coalesces events. Each
/// emitted frame is self-contained, including previously completed models.
final class OnboardingDownloadProgress: @unchecked Sendable {
    typealias C = OnboardingContract
    private let lock = NSLock()
    private var models: [C.DownloadItem]
    private var files: [String: C.DownloadItem] = [:]
    private var order: [String] = []
    private var current = ""
    private var networkBytes: Int64 = 0
    private var lastEmission: TimeInterval = 0
    private let emit: @Sendable (C.Progress) -> Void

    init(models: [C.DownloadItem], emit: @escaping @Sendable (C.Progress) -> Void) {
        self.models = models
        self.emit = emit
    }

    func receive(modelID: String, event: ModelDownloader.ProgressEvent) {
        lock.lock()
        defer { lock.unlock() }
        guard let index = models.firstIndex(where: { $0.id == modelID }) else { return }
        if current != modelID {
            current = modelID
            files = [:]
            order = []
        }
        let aggregate = event.file == modelID
        if let inventory = event.files {
            order = inventory.sorted { $0.total == $1.total ? $0.path < $1.path : $0.total > $1.total }.map(\.path)
            files = Dictionary(inventory.map { file in
                (file.path, C.DownloadItem(id: file.path, bytes: file.bytes, total: file.total,
                                          stage: file.verified ? "completed" : "queued"))
            }, uniquingKeysWith: { _, last in last })
        } else if !aggregate {
            let previous = files[event.file]
            if previous == nil { order.append(event.file) }
            let bytes = max(0, event.bytesDownloaded)
            // The inventory supplies the resume baseline. Legacy downloads lack
            // it; conservatively exclude their first callback from rate math.
            if event.phase == .transferring, let previous {
                networkBytes += max(0, bytes - previous.bytes)
            }
            files[event.file] = C.DownloadItem(id: event.file, bytes: bytes, total: event.bytesTotal,
                stage: event.phase == .verifying ? "completed" : event.phase.rawValue)
        }
        if aggregate {
            models[index].total = event.bytesTotal
            models[index].stage = event.phase.rawValue
            if event.phase == .completed { models[index].bytes = event.bytesDownloaded }
        } else {
            models[index].stage = "transferring"
        }
        if models[index].stage != "completed" {
            models[index].bytes = files.values.reduce(0) { $0 + $1.bytes }
        }
        let now = ProcessInfo.processInfo.systemUptime
        // Byte callbacks are frequent. Bound serialization work to 10 Hz,
        // while delivering inventory and phase changes immediately.
        guard aggregate || event.phase != .transferring || now - lastEmission >= 0.1 else { return }
        lastEmission = now
        var value = C.Progress(modelID: modelID, file: String(event.file.prefix(512)),
            bytes: models[index].bytes, total: models[index].total, stage: models[index].stage)
        value.models = models
        // Bound the frame independently of manifest file count/path length.
        value.files = order.prefix(128).compactMap { id in
            guard let file = files[id] else { return nil }
            return C.DownloadItem(id: String(file.id.prefix(128)), bytes: file.bytes, total: file.total, stage: file.stage)
        }
        value.fileCount = order.count
        value.networkBytes = networkBytes
        emit(value)
    }
}
