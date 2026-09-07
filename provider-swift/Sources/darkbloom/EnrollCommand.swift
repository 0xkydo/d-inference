import ArgumentParser
import Foundation
import ProviderCore

struct Enroll: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Enroll this Mac in Darkbloom MDM (device-attestation profile).",
        discussion: """
        Requests a per-device .mobileconfig profile from the coordinator,
        opens it (registering with System Settings), then opens the
        Profiles pane so you can click Install. The profile lets the
        coordinator verify that SIP/Secure Boot are on and that the
        Secure Enclave is genuine Apple hardware.

        Darkbloom CANNOT erase, lock, or remotely control your Mac.
        Remove anytime in System Settings → Device Management.
        """
    )

    @OptionGroup var configOptions: ConfigOptions

    @Option(help: "Override coordinator URL (HTTPS).")
    var coordinator: String?

    @Flag(help: "Don't open System Settings; just download the profile.")
    var noOpen = false

    mutating func run() async throws {
        let snapshot = try loadRuntimeSnapshot(configOptions: configOptions)
        let coordinatorURL = coordinator
            ?? snapshot.config.coordinator.url
        do {
            try await EnrollmentFlow.run(coordinatorURL: coordinatorURL, noOpen: noOpen,
                                         waitForCompletion: OnboardingUI.isTerminal)
            OnboardingUI.line("Continue with: darkbloom start", style: "36")
        } catch is CancellationError {
            OnboardingUI.line("Enrollment is unfinished. Continue with: darkbloom enroll")
            throw ExitCode.failure
        } catch {
            printError("\(error)")
            throw ExitCode.failure
        }
    }
}
