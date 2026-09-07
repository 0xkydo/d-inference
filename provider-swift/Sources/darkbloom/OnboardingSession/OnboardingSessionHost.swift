import Foundation

/// Foreground operation lifetime. Reused by the real executable and the disposable
/// contract/PTY harness; the latter supplies services, never a second workflow.
enum OnboardingSessionHost {
    static func run(pipe: OnboardingPipe, services: any OnboardingServices) async {
        let writer = Task { await pipe.writeEvents() }
        let workflow = OnboardingWorkflow(services: services, emit: { pipe.send($0) })
        var operation: Task<Void, Never>?
        let completion = SessionOperationCompletion()
        do {
            while let line = try await pipe.nextLine() {
                let command = try OnboardingContract.Command.decode(line)
                if command.action == .cancel { break }
                if completion.isComplete { await operation?.value; operation = nil }
                guard operation == nil else {
                    pipe.send(.init(kind: .error, commandID: command.id, error: .busy)); continue
                }
                completion.begin()
                operation = Task {
                    await workflow.handle(command)
                    completion.complete()
                }
            }
        } catch {
            pipe.send(.init(kind: .error, commandID: 0, error: (error as? OnboardingContract.Failure) ?? .unavailable))
        }
        operation?.cancel()
        await operation?.value
        pipe.finish()
        await writer.value
    }
}

private final class SessionOperationCompletion: @unchecked Sendable {
    private let lock = NSLock()
    private var done = true
    var isComplete: Bool { lock.withLock { done } }
    func begin() { lock.withLock { done = false } }
    func complete() { lock.withLock { done = true } }
}
