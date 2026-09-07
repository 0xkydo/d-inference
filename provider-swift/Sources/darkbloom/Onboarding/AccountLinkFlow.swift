import Foundation
import ProviderCore

/// The same device-code login UI serves the standalone command and onboarding.
enum AccountLinkFlow {
    static func run(coordinatorURL: String, relink: Bool = false, interactive: Bool = OnboardingUI.isTerminal,
                    confirm: (String) throws -> Void = { try OnboardingUI.confirm($0) },
                    login: ((String) async throws -> Void)? = nil) async throws {
        OnboardingUI.heading("Link your Darkbloom account")
        OnboardingUI.line("Link this Mac to receive earnings for serving inference.")
        if interactive {
            try confirm("Press Enter to open account linkage in your browser")
        }
        if let login { try await login(coordinatorURL) }
        else { try await loginInBrowser(coordinatorURL: coordinatorURL, relink: relink) }
        OnboardingUI.success("Account linked.")
    }

    private static func loginInBrowser(coordinatorURL: String, relink: Bool) async throws {
        try await performDeviceCodeLogin(
            coordinatorURL: coordinatorURL,
            onDisplayCode: { code, uri, expiresIn in
                print()
                OnboardingUI.line("Approve this Mac in your browser:")
                OnboardingUI.line(OnboardingUI.clean(uri), style: "4")
                OnboardingUI.line("Code: \(OnboardingUI.clean(code))", style: "1")
                OnboardingUI.detail("Waiting for approval. This code expires in \(expiresIn / 60) minutes.")
                OnboardingUI.line("Return here after approval; setup continues automatically.")
            },
            onPollTick: {}, relink: relink)
    }
}
