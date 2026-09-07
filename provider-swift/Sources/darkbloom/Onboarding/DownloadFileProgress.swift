/// Download progress tracking + terminal rendering for the foreground model
/// download path (`darkbloom models download` / the interactive picker).
///
/// Split out of `ModelCatalog.swift` so that file stays focused on catalog +
/// download orchestration. The tracker here is callback-driven (fed by the
/// streaming, byte-resumable downloader in `ModelCatalog.swift`) rather than a
/// `URLSessionDownloadDelegate`: downloads now stream straight to a `.part`
/// file for true byte-level resume, so there is no `URLSessionDownloadTask` to
/// observe — progress arrives as cumulative-bytes-on-disk callbacks instead.

import Foundation
import ProviderCore
#if canImport(Darwin)
import Darwin
#endif

// MARK: - Per-file progress

/// Per-file progress state rendered by `ProgressRenderer`.
struct FileProgress: Sendable {
    let label: String
    let expectedBytes: Int64
    var downloadedBytes: Int64 = 0
    /// Bytes already on disk when tracking started (a resumed `.part` prefix).
    /// Excluded from the speed/ETA math so a resume doesn't report an inflated
    /// instantaneous rate for bytes it never actually transferred this run.
    var baselineBytes: Int64 = 0
    var startTime: Date = Date()
    var completed: Bool = false
    var completionTime: Date?

    /// Bytes/second over this run's transfer (excludes the resumed prefix).
    var speed: Double {
        let elapsed = (completionTime ?? Date()).timeIntervalSince(startTime)
        guard elapsed > 0.1 else { return 0 }
        return Double(max(0, downloadedBytes - baselineBytes)) / elapsed
    }

    /// Estimated seconds remaining.
    var eta: Double? {
        guard speed > 0, expectedBytes > 0 else { return nil }
        let remaining = Double(expectedBytes - downloadedBytes)
        guard remaining > 0 else { return nil }
        return remaining / speed
    }

    var fraction: Double {
        guard expectedBytes > 0 else { return 0 }
        return min(1.0, Double(downloadedBytes) / Double(expectedBytes))
    }
}

// MARK: - Tracker

/// Thread-safe per-file download progress keyed by file label (manifest path).
///
/// Fed by byte-progress callbacks from the streaming downloader and read on a
/// timer by `ProgressRenderer`. Registration order is preserved so the rendered
/// rows stay stable across frames.
final class ManifestDownloadProgress: @unchecked Sendable {

    private let lock = NSLock()
    private var progress: [String: FileProgress] = [:]
    private var order: [String] = []

    /// Register a file to track. `initialBytes` seeds a resumed `.part` prefix
    /// so the bar starts where the previous run left off (and is excluded from
    /// the speed math via `baselineBytes`).
    func register(label: String, expectedBytes: Int64, initialBytes: Int64 = 0) {
        lock.lock()
        defer { lock.unlock() }
        if progress[label] == nil { order.append(label) }
        var p = FileProgress(label: label, expectedBytes: expectedBytes)
        p.downloadedBytes = max(0, initialBytes)
        p.baselineBytes = max(0, initialBytes)
        progress[label] = p
    }

    /// Update cumulative bytes-on-disk for a file.
    func update(label: String, downloadedBytes: Int64) {
        lock.lock()
        defer { lock.unlock() }
        guard var p = progress[label] else { return }
        p.downloadedBytes = downloadedBytes
        progress[label] = p
    }

    /// Mark a file complete (full size, stop the clock).
    func complete(label: String) {
        lock.lock()
        defer { lock.unlock() }
        guard var p = progress[label] else { return }
        p.downloadedBytes = p.expectedBytes > 0 ? p.expectedBytes : p.downloadedBytes
        p.completed = true
        p.completionTime = Date()
        progress[label] = p
    }

    /// Thread-safe snapshot of all tracked file progress, in registration order.
    var allProgress: [FileProgress] {
        lock.lock()
        defer { lock.unlock() }
        return order.compactMap { progress[$0] }
    }
}
