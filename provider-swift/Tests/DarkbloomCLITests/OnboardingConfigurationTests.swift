import Foundation
import ProviderCore
import Testing
@testable import darkbloom

@Suite("Onboarding coordinator persistence")
struct OnboardingConfigurationTests {
    private func fixture(_ body: (URL) throws -> Void) throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        try body(root)
    }

    @Test("dev and local endpoints survive subsequent command startup")
    func durableCoordinator() throws {
        try fixture { root in
            let path = root.appendingPathComponent(".config/darkbloom/provider.toml")
            var existing = ProviderConfig(provider: .init(name: "test"))
            existing.backend.idleTimeoutMins = 73
            try ConfigManager.save(existing, to: path)
            for origin in ["https://api.dev.darkbloom.xyz", "http://localhost:8080"] {
                try OnboardingConfiguration.saveCoordinator(origin, to: path, fallback: ProviderConfig(provider: .init(name: "test")))
                let loaded = try ConfigManager.load(from: path)
                let migrated = migrateConfigIfNeeded(configPath: path, config: loaded, home: root)
                #expect(try migrated.coordinator.url == OnboardingState.webSocketURL(origin))
                #expect(try ConfigManager.load(from: path).coordinator.url == migrated.coordinator.url)
                #expect(migrated.backend.idleTimeoutMins == 73)
            }
        }
    }

    @Test("explicit config stays isolated; discovered legacy config still migrates")
    func configIsolation() throws {
        try fixture { root in
            let source = root.appendingPathComponent(".config/eigeninference/provider.toml")
            let canonical = root.appendingPathComponent(".config/darkbloom/provider.toml")
            var config = ProviderConfig(provider: .init(name: "test"))
            config.coordinator.url = "wss://api.dev.darkbloom.xyz/ws/provider"
            try ConfigManager.save(config, to: source)
            _ = migrateConfigIfNeeded(configPath: source, config: config, migrateLegacyPath: false, home: root)
            #expect(!FileManager.default.fileExists(atPath: canonical.path))
            let custom = root.appendingPathComponent("experiment.toml")
            try ConfigManager.save(config, to: custom)
            _ = migrateConfigIfNeeded(configPath: custom, config: config, home: root)
            #expect(!FileManager.default.fileExists(atPath: canonical.path))
            _ = migrateConfigIfNeeded(configPath: source, config: config, home: root)
            #expect(try ConfigManager.load(from: canonical).coordinator.url == config.coordinator.url)
        }
    }

    @Test("new config uses supplied defaults and invalid TOML is never replaced")
    func createAndReject() throws {
        try fixture { root in
            let path = root.appendingPathComponent("provider.toml")
            var fallback = ProviderConfig(provider: .init(name: "test"))
            fallback.backend.idleTimeoutMins = 42
            try OnboardingConfiguration.saveCoordinator("https://example.test", to: path, fallback: fallback)
            #expect(try ConfigManager.load(from: path).backend.idleTimeoutMins == 42)
            try Data("[invalid".utf8).write(to: path)
            #expect(throws: (any Error).self) {
                try OnboardingConfiguration.saveCoordinator("https://example.test", to: path, fallback: fallback)
            }
            #expect(try String(contentsOf: path, encoding: .utf8) == "[invalid")
        }
    }
}
