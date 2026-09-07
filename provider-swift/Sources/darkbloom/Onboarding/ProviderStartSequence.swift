import Foundation
import ProviderCore

/// The existing daemon + watchdog sequence, shared by plain and session onboarding.
/// Hosted only inside darkbloom so LaunchAgent resolves the actual Swift executable.
enum ProviderStartSequence {
    static func start(coordinatorURL: String, models: [String], configPath: URL?,
                      watchdogConfigPath: URL, autoRestart: Bool,
                      localEndpoint: LaunchAgent.LocalEndpointOptions = .init()) throws -> Bool {
        try Task.checkCancellation()
        try LaunchAgent.installAndStart(coordinatorURL: coordinatorURL, models: models,
                                        configPath: configPath, localEndpoint: localEndpoint)
        switch WatchdogAgent.rearmAction(autoRestartEnabled: autoRestart, isLoaded: WatchdogAgent.isLoaded()) {
        case .arm:
            do { try WatchdogAgent.installAndStart(configPath: watchdogConfigPath) }
            catch { return false }
        case .disarm: try? WatchdogAgent.stop()
        case nil: break
        }
        return true
    }
}
