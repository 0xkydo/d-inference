import Foundation
import Testing
import Darwin
@testable import ProviderCore

@Suite("Onboarding and shared model-cache locks")
struct OnboardingOperationLockTests {
    @Test func busyAndKernelRelease() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let path = root.appendingPathComponent("setup.lock")
        defer { try? FileManager.default.removeItem(at: root) }
        var owner: ProviderOperationLock? = try ProviderOperationLock.acquire(at: path)
        #expect(throws: ProviderOperationLock.Failure.self) { try ProviderOperationLock.acquire(at: path) }
        withExtendedLifetime(owner) {}; owner = nil
        let released = try ProviderOperationLock.acquire(at: path)
        withExtendedLifetime(released) {}
    }
    @Test func rejectsSymlinkLock() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let target = root.appendingPathComponent("target"), link = root.appendingPathComponent("link")
        try Data().write(to: target)
        try FileManager.default.createSymbolicLink(at: link, withDestinationURL: target)
        #expect(throws: ProviderOperationLock.Failure.self) { try ProviderOperationLock.acquire(at: link) }
    }
    @Test func downloadPrefetchAndRemovalShareGate() async throws {
        let id = "test-org/operation-lock-\(UUID().uuidString)"
        let lease = try ProviderOperationLock.model(id)
        defer { withExtendedLifetime(lease) {} }
        let model = CatalogModel(id: id, s3Name: id, displayName: "Test", sizeGb: 1)
        let downloader = ModelDownloader(r2CDNURL: "https://must-not-be-contacted.invalid")
        await #expect(throws: ProviderOperationLock.Failure.self) { try await downloader.downloadForStorage(model: model) }
        let manifest = ModelManifest(schemaVersion: 1, modelID: id, version: "1", r2Prefix: "test", aggregateSHA256: "test",
                                     totalSizeBytes: 0, fileCount: 0, files: [], createdAt: Date())
        await #expect(throws: ProviderOperationLock.Failure.self) { try await downloader.prefetch(model: model, manifest: manifest) }
        #expect(throws: ProviderOperationLock.Failure.self) { try ModelDownloader.remove(modelID: id) }
    }
    @Test func childProcessLockReleasedOnExit() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let path = root.appendingPathComponent("setup.lock")
        let process = Process(); process.executableURL = URL(fileURLWithPath: "/usr/bin/perl")
        process.arguments = ["-e", "use Fcntl qw(:flock); $|=1; open(my $f,'>>',$ARGV[0]) or die; flock($f,LOCK_EX) or die; print '1'; sleep 30;", path.path]
        let pipe = Pipe(); process.standardOutput = pipe; process.standardError = FileHandle.nullDevice
        try process.run()
        defer { if process.isRunning { kill(process.processIdentifier, SIGKILL); process.waitUntilExit() } }
        #expect(pipe.fileHandleForReading.readData(ofLength: 1) == Data("1".utf8))
        #expect(throws: ProviderOperationLock.Failure.self) { try ProviderOperationLock.acquire(at: path) }
        kill(process.processIdentifier, SIGKILL); process.waitUntilExit()
        let recovered = try ProviderOperationLock.acquire(at: path)
        withExtendedLifetime(recovered) {}
    }
}
