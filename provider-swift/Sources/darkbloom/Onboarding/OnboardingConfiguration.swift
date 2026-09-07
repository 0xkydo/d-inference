import Foundation
import ProviderCore

enum OnboardingConfiguration {
    /// Use the existing config authority and lock. Reload under the lock so a
    /// concurrent settings command does not lose its changes during onboarding.
    static func saveCoordinator(_ coordinator: String, to path: URL, fallback: ProviderConfig) throws {
        let endpoint = try OnboardingState.webSocketURL(coordinator)
        try withExclusiveConfigLock(at: path) {
            let exists = FileManager.default.fileExists(atPath: path.path)
            var config = exists ? try ConfigManager.load(from: path) : fallback
            if exists, config.coordinator.url == endpoint { return }
            config.coordinator.url = endpoint
            try ConfigManager.save(config, to: path)
        }
    }
}
