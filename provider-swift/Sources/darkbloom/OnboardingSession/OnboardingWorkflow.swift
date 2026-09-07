import Foundation

/// Only the operations stage one uses. Live dependencies remain in this executable;
/// tests supply services without macOS prompts, files, network, or inference.
protocol OnboardingServices: Sendable {
    func observe() async throws -> OnboardingContract.Snapshot
    func enroll() async throws
    func link(display: @escaping @Sendable (OnboardingContract.LinkCode) -> Void) async throws
    func catalog() async throws -> OnboardingContract.Snapshot
    func remember(_ ids: [String]) async throws
    func download(_ ids: [String], progress: @escaping @Sendable (OnboardingContract.Progress) -> Void) async throws
    func start(_ ids: [String]) async throws
}

actor OnboardingWorkflow {
    typealias C = OnboardingContract
    private let services: any OnboardingServices
    private let emit: @Sendable (C.Event) -> Void
    private(set) var snapshot = C.Snapshot()
    private var connected = false
    private var busy = false
    private var lastID = 0

    init(services: any OnboardingServices, emit: @escaping @Sendable (C.Event) -> Void) {
        self.services = services; self.emit = emit
    }

    func handle(_ command: C.Command) async {
        do {
            try Task.checkCancellation()
            guard command.version == C.version else { throw C.Failure.versionMismatch }
            guard command.id > lastID else { throw C.Failure.staleCommand }
            lastID = command.id
            guard !busy else { throw C.Failure.busy }
            if command.action == .hello {
                guard !connected else { throw C.Failure.wrongPhase }
            } else {
                guard connected, command.revision == snapshot.revision else { throw C.Failure.staleCommand }
            }
            busy = true
            defer { busy = false }
            let failure: C.Failure
            switch command.action {
            case .hello, .refresh: failure = .unavailable
            case .enroll: failure = .enrollmentFailed
            case .link: failure = .linkFailed
            case .selectModels: failure = .downloadFailed
            case .start: failure = .startFailed
            case .cancel: throw CancellationError()
            }
            do {
                try await perform(command)
                try Task.checkCancellation()
            } catch is CancellationError { return }
            catch {
                try Task.checkCancellation()
                // Any partial operation is resumable through refresh. Start must
                // never be enabled after an operation fails or loses prerequisites.
                if snapshot.phase == .downloading || (snapshot.phase == .ready && command.action == .start) {
                    snapshot.phase = .models
                }
                throw (error as? C.Failure) ?? failure
            }
            connected = true
            publish(command.id)
        } catch is CancellationError { return }
        catch {
            emit(C.Event(kind: .error, commandID: command.id, error: (error as? C.Failure) ?? .unavailable))
            if connected { publish(command.id) }
        }
    }

    private func perform(_ command: C.Command) async throws {
        switch command.action {
        case .hello, .refresh:
            let observed = try await services.observe()
            snapshot.enrolled = observed.enrolled; snapshot.linked = observed.linked
            snapshot.providerRunning = observed.providerRunning; snapshot.observedAt = observed.observedAt
            snapshot.selectedModelIDs = observed.selectedModelIDs
            if !observed.enrolled {
                if snapshot.phase != .enrollmentPending { snapshot.phase = .enrollment }
            }
            else if !observed.linked { snapshot.phase = .account }
            else { try await loadCatalog() }
        case .enroll:
            guard snapshot.phase == .enrollment || snapshot.phase == .enrollmentPending else { throw C.Failure.wrongPhase }
            try await services.enroll()
            snapshot.phase = .enrollmentPending
        case .link:
            guard snapshot.phase == .account else { throw C.Failure.wrongPhase }
            try await checkPrerequisites(account: false)
            let emit = self.emit
            try await services.link { emit(C.Event(kind: .linkCode, commandID: command.id, linkCode: $0)) }
            try await checkPrerequisites(account: true)
            try await loadCatalog()
        case .selectModels:
            guard snapshot.phase == .models else { throw C.Failure.wrongPhase }
            try await checkPrerequisites(account: true)
            let ids = command.modelIDs ?? []
            let rows = snapshot.models.filter { ids.contains($0.id) }
            guard !ids.isEmpty, ids.count <= 128, Set(ids).count == ids.count,
                  rows.count == ids.count, rows.contains(where: { $0.fitReason == nil }) else {
                throw C.Failure.invalidSelection
            }
            try await services.remember(ids)
            snapshot.selectedModelIDs = ids
            snapshot.phase = .downloading
            publish(command.id)
            let emit = self.emit
            try await services.download(ids) { emit(C.Event(kind: .progress, commandID: command.id, progress: $0)) }
            try Task.checkCancellation()
            for i in snapshot.models.indices where ids.contains(snapshot.models[i].id) { snapshot.models[i].downloaded = true }
            snapshot.phase = .ready
        case .start:
            guard snapshot.phase == .ready else { throw C.Failure.wrongPhase }
            try await checkPrerequisites(account: true)
            let runnable = snapshot.models.filter {
                snapshot.selectedModelIDs.contains($0.id) && $0.fitReason == nil && $0.downloaded
            }.map(\.id)
            guard !runnable.isEmpty else { throw C.Failure.invalidSelection }
            try Task.checkCancellation()
            try await services.start(runnable)
            snapshot.phase = .started
            let observed = try? await services.observe()
            snapshot.providerRunning = observed?.providerRunning ?? false
            snapshot.notice = observed?.notice
            snapshot.observedAt = Date().timeIntervalSince1970
        case .cancel: throw CancellationError()
        }
    }

    private func checkPrerequisites(account: Bool) async throws {
        let observed = try await services.observe()
        snapshot.enrolled = observed.enrolled; snapshot.linked = observed.linked
        guard observed.enrolled, !account || observed.linked else { throw C.Failure.prerequisitesChanged }
    }

    private func loadCatalog() async throws {
        let catalog: C.Snapshot
        do { catalog = try await services.catalog() }
        catch is CancellationError { throw CancellationError() }
        catch { throw C.Failure.catalogFailed }
        snapshot.models = catalog.models
        snapshot.memoryGiB = catalog.memoryGiB; snapshot.budgetGiB = catalog.budgetGiB
        snapshot.phase = .models
    }

    private func publish(_ id: Int) {
        snapshot.revision += 1
        emit(C.Event(kind: .snapshot, commandID: id, snapshot: snapshot))
    }
}
