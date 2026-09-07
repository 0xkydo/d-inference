import ArgumentParser
import Foundation
import ProviderCore

struct Login: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Link this machine to a Darkbloom account.",
        discussion: """
        Uses the RFC 8628 device code flow. The CLI requests a one-time code
        from the coordinator, displays it, and opens the verification URL in
        your browser. Once you authorize the code, the provider is linked to
        your account and earnings are credited to your account wallet.
        """
    )

    @OptionGroup var configOptions: ConfigOptions

    mutating func run() async throws {
        let snapshot = try loadRuntimeSnapshot(configOptions: configOptions)
        let coordinatorURL = snapshot.config.coordinator.url

        do {
            try await AccountLinkFlow.run(coordinatorURL: coordinatorURL)
            OnboardingUI.line("Start serving with: darkbloom start")
        } catch let error as DeviceAuthError {
            printError("\(error)")
            throw ExitCode.failure
        }
    }
}
