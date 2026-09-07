import Foundation
import ProviderCore

/// The same device-code login UI serves the standalone command and onboarding.
enum AccountLinkFlow {
    static func run(coordinatorURL: String) async throws {
        OnboardingUI.heading("Link your Darkbloom account")
        OnboardingUI.line("Link this Mac to receive earnings for serving inference.")
        try await performDeviceCodeLogin(
            coordinatorURL: coordinatorURL,
            onDisplayCode: { code, uri, expiresIn in
                print()
                OnboardingUI.line("Approve this Mac in your browser:")
                OnboardingUI.line(OnboardingUI.clean(uri), style: "36")
                OnboardingUI.line("Code: \(OnboardingUI.clean(code))", style: "1")
                OnboardingUI.line("Waiting for approval. This code expires in \(expiresIn / 60) minutes.")
                OnboardingUI.line("Return here after approval; setup continues automatically.")
            },
            onPollTick: {})
        OnboardingUI.line("Account linked.")
    }
}
