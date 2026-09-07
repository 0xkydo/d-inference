import Foundation
import ProviderCore

/// Compose existing services; persisted intent never substitutes for checking
/// actual state. Dependencies let tests exercise interruption without side effects.
enum GuidedOnboarding {
    static func prepare(
        coordinatorURL: String,
        enroll: () async throws -> Void,
        hasAccount: () -> Bool = { AuthTokenStore.load() != nil },
        login: () async throws -> Void
    ) async throws {
        OnboardingUI.heading("Set up Darkbloom")
        OnboardingUI.detail("Device enrollment → account linkage → models → start")
        try await enroll()
        if hasAccount() {
            OnboardingUI.line("Using your saved account link.")
        } else {
            try await login()
        }
    }

    static func retry(_ label: String, action: () async throws -> Void) async throws {
        while true {
            do { try await action(); return }
            catch is CancellationError { throw CancellationError() }
            catch {
                OnboardingUI.failure("\(label): \(OnboardingUI.clean(String(describing: error)))")
                try OnboardingUI.confirm("Press Enter to retry")
            }
        }
    }
}
