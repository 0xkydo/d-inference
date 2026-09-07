import Foundation
import ProviderCore

/// Shared by `enroll` and the guided start flow. Enrollment is complete only
/// when the OS reports Darkbloom's MDM, not when the profile was downloaded.
enum EnrollmentFlow {
    static func explain() {
        OnboardingUI.heading("Enroll this Mac for device verification")
        OnboardingUI.line("Device verification is a core Darkbloom security feature. It helps protect private requests by checking the identity and security settings of Macs serving them.")
        print()
        OnboardingUI.box(title: "This profile is read-only", paragraphs: [
            "Darkbloom can read device information, installed configuration profiles, and security settings such as SIP and Secure Boot.",
            "It cannot read your personal files, change settings, install apps, or lock, erase, or remotely control your Mac.",
            "You can remove it in System Settings → General → Device Management."
        ])
    }

    static func run(coordinatorURL: String, noOpen: Bool = false, waitForCompletion: Bool,
                    interactive: Bool = OnboardingUI.isTerminal,
                    checkEnrollment: (String) -> MDMEnrollmentState = { checkMDMEnrollment(coordinatorURL: $0) },
                    confirm: (String) throws -> Void = { try OnboardingUI.confirm($0) },
                    enroll: (String, Bool) async throws -> EnrollmentResult = {
                        try await EnrollmentService().enroll(coordinatorURL: $0, openSystemSettings: $1)
                    }) async throws {
        let state = checkEnrollment(coordinatorURL)
        if state.isDarkbloom {
            OnboardingUI.success("Device enrollment verified.")
            return
        }
        if case .enrolledOtherMDM(let serverURL) = state {
            throw EnrollmentError.managedByOtherMDM(serverURL: serverURL)
        }
        explain()
        if interactive && !noOpen {
            OnboardingUI.instruction("Next, Darkbloom will download the profile and open System Settings. You approve its installation there.")
            try confirm("Press Enter to open device enrollment")
        }
        let result = try await enroll(coordinatorURL, !noOpen)
        if result.alreadyEnrolled { return }
        OnboardingUI.detail("Profile saved: \(result.profilePath.path)")
        OnboardingUI.instruction("Open the profile, then go to System Settings → General → Device Management.")
        OnboardingUI.instruction("Select the Darkbloom profile, click Install or Enroll, and follow the macOS prompts.")
        guard waitForCompletion && !noOpen else {
            return
        }
        OnboardingUI.instruction("After clicking OK in macOS, return here. Account linkage is next.")
        while true {
            try confirm("Press Enter to check enrollment and continue")
            switch checkEnrollment(coordinatorURL) {
            case .enrolledDarkbloom:
                OnboardingUI.success("Device enrollment verified.")
                return
            case .enrolledOtherMDM(let serverURL):
                throw EnrollmentError.managedByOtherMDM(serverURL: serverURL)
            case .notEnrolled:
                OnboardingUI.line("Enrollment is still pending. Complete the Darkbloom profile in Device Management, then return here.")
            case .checkFailed:
                OnboardingUI.line("Could not check enrollment. Try again after System Settings finishes.")
            }
        }
    }
}
