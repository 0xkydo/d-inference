import Foundation
import ProviderCore

/// Success comes from a fresh daemon snapshot, not merely launchctl accepting a
/// job. Never label enrollment alone as attestation or inference readiness.
enum OnboardingStartup {
    static func verified(_ state: DaemonState, launchedAt: Double, now: Double) -> Bool {
        state.startedAt >= launchedAt && !state.isStale(now: now)
            && (state.trust?.receivedAt ?? 0) >= launchedAt
            && ["hardware", "mda_verified"].contains(state.trust?.trustLevel ?? "")
            && state.trust?.status == "online"
            && !state.warmModels.isEmpty
    }

    static func report(launchedAt: Double) async {
        OnboardingUI.heading("Starting Darkbloom")
        OnboardingUI.line("Waiting for device verification and a model to become ready…")
        for _ in 0..<30 {
            if let state = DaemonStateFile.read(),
               state.processIdentity?.isCurrent() == true,
               verified(state, launchedAt: launchedAt, now: Date().timeIntervalSince1970) {
                OnboardingUI.line("Darkbloom is running and verified. Ready to serve requests.", style: "32")
                return
            }
            do { try await Task.sleep(for: .seconds(1)) } catch { return }
        }
        OnboardingUI.line("The background service was launched. Readiness is still pending.")
        if let state = DaemonStateFile.read(), state.startedAt >= launchedAt {
            if let error = state.lastModelLoadError {
                OnboardingUI.line("A model could not load: \(OnboardingUI.clean(String(describing: error)))")
            }
            if let trust = state.trust { OnboardingUI.line("Device verification: " + OnboardingUI.clean(trust.reason)) }
        }
        OnboardingUI.line("Check progress with darkbloom status. Run darkbloom doctor if it stays pending.", style: "36")
    }
}
