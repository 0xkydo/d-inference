import Foundation
import Testing
@testable import ProviderCoreFoundation

@Suite("Cancellable model verification")
struct WeightHasherCancellationTests {
    @Test func cancelledHashStopsStreamingAndLeavesFile() throws {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try Data(repeating: 0x42, count: 2_000_000).write(to: path)
        defer { try? FileManager.default.removeItem(at: path) }
        var checks = 0
        let result = WeightHasher.hashSingleFile(at: path, isCancelled: { checks += 1; return checks > 1 })
        #expect(result == nil)
        #expect(FileManager.default.fileExists(atPath: path.path))
        #expect(WeightHasher.hashSingleFile(at: path) != nil)
        #expect(WeightHasher.hashFilesWithRelativeKey([(path, "weights")], isCancelled: { true }) == nil)
    }
}
