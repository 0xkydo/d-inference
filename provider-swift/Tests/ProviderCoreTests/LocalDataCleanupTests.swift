import Foundation
import Testing
@testable import ProviderCore

@Suite("Local cleanup failure reporting")
struct LocalDataCleanupTests {
    @Test("a keychain failure is reported without skipping remaining cleanup")
    func reportsAndContinues() {
        var attempted: [String] = []
        let failures = LocalDataCleanup.perform([
            ("config", { attempted.append("config") }),
            ("keychain", {
                attempted.append("keychain")
                throw NSError(domain: "test-keychain", code: -34018)
            }),
            ("legacy key", { attempted.append("legacy key") }),
        ])
        #expect(attempted == ["config", "keychain", "legacy key"])
        #expect(failures.count == 1)
        #expect(failures[0].hasPrefix("keychain:"))
    }
}
