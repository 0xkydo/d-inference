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

    static func run(coordinatorURL: String, noOpen: Bool = false, waitForCompletion: Bool) async throws {
        let state = checkMDMEnrollment(coordinatorURL: coordinatorURL)
        if state.isDarkbloom {
            OnboardingUI.line("Device enrollment verified.")
            return
        }
        if case .enrolledOtherMDM(let serverURL) = state {
            throw EnrollmentError.managedByOtherMDM(serverURL: serverURL)
        }
        explain()
        let result = try await EnrollmentService().enroll(
            coordinatorURL: coordinatorURL, openSystemSettings: !noOpen)
        if result.alreadyEnrolled { return }
        OnboardingUI.line("Profile saved: \(result.profilePath.path)")
        OnboardingUI.line("Open the profile, then go to System Settings → General → Device Management.")
        OnboardingUI.line("Select the Darkbloom profile, click Install or Enroll, and follow the macOS prompts.")
        guard waitForCompletion && !noOpen else {
            return
        }
        OnboardingUI.line("After clicking OK in macOS, return here. Account linkage is next.")
        while true {
            try OnboardingUI.confirm("Press Enter to check enrollment and continue")
            switch checkMDMEnrollment(coordinatorURL: coordinatorURL) {
            case .enrolledDarkbloom:
                OnboardingUI.line("Device enrollment verified.")
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
