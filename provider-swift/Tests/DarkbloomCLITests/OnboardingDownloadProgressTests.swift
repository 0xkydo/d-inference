import Foundation
import Testing
@testable import darkbloom
@testable import ProviderCore

private final class DownloadFrames: @unchecked Sendable {
    private let lock = NSLock()
    private var values: [OnboardingContract.Progress] = []
    func append(_ value: OnboardingContract.Progress) { lock.withLock { values.append(value) } }
    var last: OnboardingContract.Progress? { lock.withLock { values.last } }
}

struct OnboardingDownloadProgressTests {
    typealias C = OnboardingContract
    typealias E = ModelDownloader.ProgressEvent

    @Test("coalesced frames retain concurrent files, resume baseline, and completed models")
    func completeSnapshots() throws {
        let frames = DownloadFrames()
        let tracker = OnboardingDownloadProgress(models: [
            C.DownloadItem(id: "one", bytes: 0, total: nil, stage: "queued"),
            C.DownloadItem(id: "two", bytes: 0, total: nil, stage: "queued")
        ], emit: { frames.append($0) })
        tracker.receive(modelID: "one", event: E(file: "one", bytesDownloaded: 0, bytesTotal: 400, files: [
            E.File(path: "a", bytes: 100, total: 200, verified: false),
            E.File(path: "b", bytes: 100, total: 200, verified: false)
        ]))
        #expect(frames.last?.bytes == 200)
        #expect(frames.last?.networkBytes == 0)
        DispatchQueue.concurrentPerform(iterations: 2) { i in
            let name = i == 0 ? "a" : "b"
            tracker.receive(modelID: "one", event: E(file: name, bytesDownloaded: 200, bytesTotal: 200))
            tracker.receive(modelID: "one", event: E(file: name, bytesDownloaded: 200, bytesTotal: 200, phase: .verifying))
        }
        #expect(frames.last?.files?.allSatisfy { $0.bytes == 200 && $0.stage == "completed" } == true)
        #expect(frames.last?.networkBytes == 200)
        tracker.receive(modelID: "one", event: E(file: "one", bytesDownloaded: 400, bytesTotal: 400, phase: .completed))
        tracker.receive(modelID: "two", event: E(file: "two", bytesDownloaded: 0, bytesTotal: 100, phase: .verifying))
        let frame = try #require(frames.last)
        #expect(frame.models?.first?.stage == "completed")
        #expect(frame.models?.first?.bytes == 400)
        #expect(frame.files?.isEmpty == true)
        #expect(frame.networkBytes == 200)
    }

    @Test("large manifests have bounded frames without truncating aggregate bytes")
    func boundedInventory() throws {
        let frames = DownloadFrames()
        let tracker = OnboardingDownloadProgress(models: [C.DownloadItem(id: "one", bytes: 0, total: nil, stage: "queued")], emit: { frames.append($0) })
        let files = (0..<200).map { E.File(path: "\($0)" + String(repeating: "🫖", count: 600), bytes: 1, total: 2, verified: false) }
        tracker.receive(modelID: "one", event: E(file: "one", bytesDownloaded: 0, bytesTotal: 400, files: files))
        let frame = try #require(frames.last)
        #expect(frame.bytes == 200)
        #expect(frame.fileCount == 200)
        #expect(frame.files?.count == 128)
        #expect(try JSONEncoder().encode(C.Event(kind: .progress, commandID: 1, progress: frame)).count < C.maxEventBytes)
    }
}
