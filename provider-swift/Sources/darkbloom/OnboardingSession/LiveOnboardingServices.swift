import Foundation
import ProviderCore

/// Existing Swift services behind the narrow onboarding boundary. No provider
/// loop, enclave identity, APNs host, or inference/cache-reader is constructed.
actor LiveOnboardingServices: OnboardingServices {
    typealias C = OnboardingContract
    private let coordinator: String
    private let configPath: URL
    private let customConfig: Bool
    private var intent: OnboardingState
    private var entries: [Start.PickerEntry] = []
    private var notice: String?
    private var capabilities = Set<ProviderRuntimeCapability>()

    init(config: String?, coordinatorURL: String?) throws {
        let pending = OnboardingState.load()
        let path = config ?? pending?.configPath
        let runtime = try loadRuntimeSnapshot(configPath: path, migrateOnDisk: false)
        coordinator = try OnboardingState.webSocketURL(coordinatorURL
            ?? (config == nil ? pending?.coordinatorURL : nil) ?? runtime.config.coordinator.url)
        configPath = runtime.configPath
        customConfig = path != nil
        intent = pending?.coordinatorURL == coordinator ? pending! : OnboardingState(coordinatorURL: coordinator)
        intent.configPath = customConfig ? configPath.path : nil
        if (try? OnboardingState.webSocketURL(runtime.config.coordinator.url)) != coordinator {
            intent.requiresAccountLink = true
        }
    }

    func observe() throws -> C.Snapshot {
        var state = C.Snapshot()
        switch checkMDMEnrollment(coordinatorURL: coordinator) {
        case .enrolledDarkbloom: state.enrolled = true
        case .notEnrolled: break
        case .enrolledOtherMDM: throw C.Failure.otherMDM
        case .checkFailed: throw C.Failure.enrollmentUnknown
        }
        state.linked = intent.requiresAccountLink != true && AuthTokenStore.load(migrateLegacy: false) != nil
        if let daemon = DaemonStateFile.read(), let identity = daemon.processIdentity {
            state.providerRunning = ProcessIdentity.read(pid: daemon.pid) == identity
        }
        state.notice = notice
        state.observedAt = Date().timeIntervalSince1970
        state.selectedModelIDs = intent.selectedModelIDs
        return state
    }

    private func persistIntent() throws {
        try Task.checkCancellation()
        let runtime = try loadRuntimeSnapshot(configPath: configPath.path, migrateOnDisk: false)
        // Record the relink requirement before changing config, so an interrupted
        // environment switch cannot reuse the previous environment's account.
        try intent.save()
        try OnboardingConfiguration.saveCoordinator(coordinator, to: configPath, fallback: runtime.config)
    }

    func enroll() async throws {
        try persistIntent()
        _ = try await EnrollmentService().enroll(coordinatorURL: coordinator, openSystemSettings: true)
    }

    func link(display: @escaping @Sendable (C.LinkCode) -> Void) async throws {
        try persistIntent()
        _ = try await performDeviceCodeLogin(coordinatorURL: coordinator, onDisplayCode: { code, url, expires in
            display(C.LinkCode(code: code, url: url, expiresIn: expires))
        }, relink: intent.requiresAccountLink == true)
        try Task.checkCancellation()
        intent.requiresAccountLink = false
        try intent.save()
    }

    func catalog() async throws -> C.Snapshot {
        let runtime = try loadRuntimeSnapshot(configPath: configPath.path, migrateOnDisk: false)
        guard let hardware = runtime.hardware else { throw C.Failure.catalogFailed }
        // Keep the original preflight and bind-before-MLX ordering. This is the
        // catalog adapter, not a generic wrapper around Start.run().
        let start = try Start.parse([])
        try start.runPreflightChecks(snapshot: runtime)
        let hash = try Start.prepareServeRuntime(settings: runtime.config.gemmaOptimizations)
        capabilities = ProviderRuntimeCapabilityDetector.detectPrepared(hardware: hardware, boundMetallibHash: hash)
        entries = try await Start.loadPickerEntries(client: ModelCatalogClient(coordinatorURL: coordinator),
                                                     snapshot: runtime, config: runtime.config,
                                                     runtimeCapabilities: capabilities)
        guard !entries.isEmpty, entries.count <= 128 else { throw C.Failure.catalogFailed }
        var snapshot = C.Snapshot()
        snapshot.memoryGiB = Double(hardware.memoryGb)
        snapshot.budgetGiB = Start.pickerBudgetGiB(memoryGb: snapshot.memoryGiB, reserveGb: runtime.config.provider.memoryReserveGB)
        snapshot.models = entries.map {
            C.Model(id: $0.id, name: $0.displayName, sizeGB: $0.sizeGb, downloaded: $0.downloaded,
                    resumable: $0.resumable, fitReason: $0.fitReason)
        }
        return snapshot
    }

    func remember(_ ids: [String]) throws {
        intent.selectedModelIDs = ids
        try persistIntent()
    }

    func download(_ ids: [String], progress: @escaping @Sendable (C.Progress) -> Void) async throws {
        let downloader = ModelDownloader(catalogClient: ModelCatalogClient(coordinatorURL: coordinator), runtimeCapabilities: capabilities)
        let tracker = OnboardingDownloadProgress(models: ids.compactMap { id in
            entries.first { $0.id == id }.map {
                C.DownloadItem(id: id, bytes: 0, total: nil, stage: $0.downloaded ? "completed" : "queued")
            }
        }, emit: progress)
        for id in ids {
            try Task.checkCancellation()
            guard let entry = entries.first(where: { $0.id == id }) else { throw C.Failure.invalidSelection }
            guard !entry.downloaded else { continue }
            do {
                try await downloader.downloadForStorage(model: entry.catalogModel) { event in
                    tracker.receive(modelID: id, event: event)
                }
            } catch is ProviderOperationLock.Failure { throw C.Failure.busy }
        }
    }

    func start(_ ids: [String]) async throws {
        // Re-resolve the live catalog, disk state, and runtime gates after the
        // final action. Client flags or an old snapshot cannot grant eligibility.
        let current = try await catalog()
        guard ids.allSatisfy({ id in current.models.contains { $0.id == id && $0.downloaded && $0.fitReason == nil } }) else {
            throw C.Failure.prerequisitesChanged
        }
        let state = try observe()
        guard state.enrolled, state.linked else { throw C.Failure.prerequisitesChanged }
        try persistIntent()
        let runtime = try loadRuntimeSnapshot(configPath: configPath.path, migrateOnDisk: false)
        try Task.checkCancellation()
        let watchdogOK = try ProviderStartSequence.start(coordinatorURL: coordinator, models: ids,
            configPath: customConfig ? configPath : nil, watchdogConfigPath: configPath,
            autoRestart: runtime.config.provider.autoRestart)
        // The provider has started. Cleanup errors must not offer another Start
        // action or undo launch; the usual status/doctor commands expose recovery.
        try? OnboardingState.complete()
        notice = watchdogOK ? nil : "watchdog_unavailable"
    }
}
