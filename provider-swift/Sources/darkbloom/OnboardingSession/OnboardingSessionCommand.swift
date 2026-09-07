import ArgumentParser
import Foundation
import ProviderCore
import Darwin

struct OnboardingSessionCommand: AsyncParsableCommand {
    static let configuration = CommandConfiguration(commandName: "onboarding-session", discussion: OnboardingCompanion.capability, shouldDisplay: false)
    @OptionGroup var configOptions: ConfigOptions
    @Option var coordinatorURL: String?

    mutating func run() async throws {
        // Only JSON events reach the private output. Existing service diagnostics
        // can contain remote bodies/token prefixes, so discard both prose streams
        // in this mode instead of forwarding them to the frontend or a log file.
        try disableCoreDumps()
        signal(SIGPIPE, SIG_IGN)
        let eventFD = dup(STDOUT_FILENO)
        let pipe = try OnboardingPipe(input: STDIN_FILENO, output: eventFD)
        let null = open("/dev/null", O_WRONLY)
        guard null >= 0 else { throw OnboardingContract.Failure.unavailable }
        dup2(null, STDOUT_FILENO); dup2(null, STDERR_FILENO); close(null)
        let signals = [SIGINT, SIGTERM, SIGHUP].map { number -> DispatchSourceSignal in
            signal(number, SIG_IGN)
            let source = DispatchSource.makeSignalSource(signal: number, queue: .global())
            source.setEventHandler { pipe.stop() }; source.resume()
            return source
        }
        defer { signals.forEach { $0.cancel() } }
        do {
            let lease = try ProviderOperationLock.setup()
            defer { withExtendedLifetime(lease) {} }
            let services = try LiveOnboardingServices(config: configOptions.config, coordinatorURL: coordinatorURL)
            await OnboardingSessionHost.run(pipe: pipe, services: services)
        } catch {
            let failure = error is ProviderOperationLock.Failure ? OnboardingContract.Failure.busy
                : (error as? OnboardingContract.Failure) ?? .unavailable
            pipe.send(.init(kind: .error, commandID: 0, error: failure))
            pipe.finish()
            await pipe.writeEvents()
        }
        // Never stop or signal the independently launched provider here.
    }
}
