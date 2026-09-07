// Start daemon install: resolve the model selection, then register + kick the
// launchd background service (the interactive-picker -> launchd path).
import Foundation
import ArgumentParser
import ProviderCore
#if canImport(Darwin)
import Darwin
#endif

extension Start {
    // MARK: - Daemon (interactive picker → launchd)

    internal mutating func launchDaemon(
        snapshot: RuntimeSnapshot,
        config: ProviderConfig,
        coordinatorURL: String,
        configPath: URL?,
        runtimeCapabilities: Set<ProviderRuntimeCapability>
    ) async throws {
        // Run critical checks before downloading models or prompting.
        try runPreflightChecks(snapshot: snapshot)

        guidedOnboarding = OnboardingState.shouldGuide(
            explicit: onboarding, pending: OnboardingState.isPending(),
            installedService: LaunchAgent.isInstalled(), interactive: OnboardingUI.isTerminal,
            explicitModels: !model.isEmpty || all || localEndpoint)
        if guidedOnboarding {
            var state = OnboardingState.load() ?? OnboardingState(coordinatorURL: coordinatorURL)
            if state.coordinatorURL != coordinatorURL { state = OnboardingState(coordinatorURL: coordinatorURL) }
            // Store absolute paths so resuming from another working directory
            // cannot accidentally open a different test config.
            state.configPath = configOptions.config == nil ? nil : snapshot.configPath.path
            if (try? OnboardingState.webSocketURL(config.coordinator.url)) != coordinatorURL {
                // Account tokens are shared local state. A deliberate environment
                // switch must link against the chosen coordinator before reusing
                // one. Persist this requirement across interrupted logins.
                state.requiresAccountLink = true
            }
            try state.save()
            // Keep later login/models/update/doctor commands on this environment,
            // including after the temporary resume marker has been removed.
            let savePath = configOptions.config == nil ? try ConfigManager.defaultConfigPath() : snapshot.configPath
            try OnboardingConfiguration.saveCoordinator(coordinatorURL, to: savePath, fallback: config)
            try await GuidedOnboarding.prepare(coordinatorURL: coordinatorURL, enroll: {
                try await GuidedOnboarding.retry("Enrollment could not finish") {
                    try await EnrollmentFlow.run(coordinatorURL: coordinatorURL, waitForCompletion: true)
                }
            }, hasAccount: {
                state.requiresAccountLink != true && AuthTokenStore.load() != nil
            }, login: {
                try await GuidedOnboarding.retry("Account linkage could not finish") {
                    try await AccountLinkFlow.run(coordinatorURL: coordinatorURL, relink: state.requiresAccountLink == true)
                    state.requiresAccountLink = false
                    try state.save()
                }
            })
        } else {
            await offerInlineLogin(coordinatorURL: coordinatorURL)
        }

        let selectedModelIDs: [String]

        if !model.isEmpty {
            let known = Set(snapshot.models.map(\.id))
            selectedModelIDs = model.filter {
                known.contains($0)
                    && ModelRuntimeRequirements.isEligible(
                        modelID: $0, available: runtimeCapabilities)
            }
        } else if all {
            selectedModelIDs = snapshot.models.compactMap {
                ModelRuntimeRequirements.isEligible(
                    modelID: $0.id, available: runtimeCapabilities) ? $0.id : nil
            }
        } else {
            selectedModelIDs = try await interactiveCatalogPicker(
                snapshot: snapshot,
                config: config,
                coordinatorURL: coordinatorURL,
                runtimeCapabilities: runtimeCapabilities
            )
        }

        guard !selectedModelIDs.isEmpty else {
            printError("No models were enabled for this Mac. Run darkbloom start and choose a model that fits; any downloads are kept.")
            throw ExitCode.failure
        }

        // Idle-memory policy: asked on the same interactive path as the model
        // picker (never for --model/--all/relaunch), with the CURRENT policy as
        // the Enter default. `--idle-timeout` already answered it in `run()`.
        var idleMinutes = config.backend.idleTimeoutMins
        if !guidedOnboarding, model.isEmpty, !all, idleTimeout == nil {
            idleMinutes = try promptIdleUnloadPolicy(
                current: idleMinutes,
                selectedModelIDs: selectedModelIDs,
                snapshot: snapshot)
        }

        if guidedOnboarding {
            OnboardingUI.heading("Ready to start Darkbloom")
            OnboardingUI.line("Device enrollment and account linkage are complete. Your selected models are downloaded.")
            OnboardingUI.line("Darkbloom will run in the background and start when you log in. Use darkbloom stop to stop it.")
            try OnboardingUI.confirm("Press Enter to start Darkbloom")
        }
        let launchedAt = Date().timeIntervalSince1970
        let autoRestartOn = config.provider.autoRestart
        let watchdogOK = try ProviderStartSequence.start(
            coordinatorURL: coordinatorURL, models: selectedModelIDs, configPath: configPath,
            watchdogConfigPath: snapshot.configPath, autoRestart: autoRestartOn,
            localEndpoint: LaunchAgent.LocalEndpointOptions(enabled: localEndpoint, port: port, bind: bind, noAuth: noAuth))
        if !watchdogOK { printError("note: could not install crash-recovery watchdog") }

        if guidedOnboarding {
            try OnboardingState.complete()
            await OnboardingStartup.report(launchedAt: launchedAt)
        }
        let logPath = LaunchAgent.logPath().path
        if !guidedOnboarding { print("Provider started as background service.") }
        print("  Models:  \(selectedModelIDs.count)")
        for id in selectedModelIDs {
            print("    \(id)")
        }
        print("  Memory:  \(IdleUnloadPolicy.describe(minutes: idleMinutes)) — `darkbloom idle` to change")
        if localEndpoint {
            let shownURL = "http://\(bind == "0.0.0.0" ? "127.0.0.1" : bind):\(port)/v1"
            print("  Local:   \(shownURL) (unified mode — run `darkbloom local` for the API key)")
        }
        print("  Logs:    \(logPath)")
        if autoRestartOn {
            print("  Recovery: auto-restart on (relaunches ~5 min after a crash)")
        }
        print()
        print("  darkbloom stop     Stop the provider")
        print("  darkbloom restart  Restart with the current selection")
        print("  darkbloom status   Check status")
    }

    // MARK: - Idle-memory policy prompt

    /// Three-way menu (always ready / free after 60 min / custom window). Enter
    /// keeps the current policy. Persists only when the answer differs from the
    /// pinned config value, then returns the effective minutes. Non-TTY stdin
    /// skips the prompt and keeps `current` (scripts never block here).
    internal func promptIdleUnloadPolicy(
        current: UInt64,
        selectedModelIDs: [String],
        snapshot: RuntimeSnapshot,
        readInput: () -> String? = { readLine() }
    ) throws -> UInt64 {
        guard isatty(STDIN_FILENO) != 0 else { return current }

        // Resident footprint of the selection (upper bound: max_model_slots may
        // keep fewer resident). Unfiltered scan so a too-big-for-RAM model still
        // has a size; nil when any selected model is unknown on disk.
        let allLocal = snapshot.hardware.map { ModelScanner.scanAllModels(hardwareInfo: $0) } ?? []
        let memoryByID = Dictionary(
            allLocal.map { ($0.id, $0.estimatedMemoryGb) }, uniquingKeysWith: { first, _ in first })
        let sizes = selectedModelIDs.compactMap { memoryByID[$0] }
        let holdsGb: Double? = sizes.count == selectedModelIDs.count && !sizes.isEmpty
            ? sizes.reduce(0, +) : nil

        print(IdleUnloadPolicy.menu(holdsGb: holdsGb, currentMinutes: current))

        let minutes = Self.resolveIdlePolicyAnswer(
            current: current,
            readInput: readInput,
            emit: { print($0, terminator: "") })

        let result = try setIdleUnloadMinutes(minutes, configPath: configOptions.config)
        print("  Memory when idle: \(IdleUnloadPolicy.describe(minutes: minutes))"
            + (result.changed ? " (saved)" : ""))
        print()
        return minutes
    }

    /// The menu dialogue after the menu text is on screen, as a pure function
    /// of the operator's keystrokes: `readInput` yields one line per prompt
    /// (nil = EOF), `emit` receives every prompt/complaint. Rules:
    ///   * blank answer keeps the current policy (Enter = the bracketed default)
    ///   * 1 → always ready (0), 2 → free after 60 min, 3 → ask for minutes
    ///   * the custom default is the current window, or 60 when the box is
    ///     currently always-ready (there is no "current window" to keep)
    ///   * three bad answers, or EOF, fall back to the default in force
    static func resolveIdlePolicyAnswer(
        current: UInt64,
        readInput: () -> String?,
        emit: (String) -> Void
    ) -> UInt64 {
        let fallback = IdleUnloadPolicy.defaultChoice(currentMinutes: current)
        var choice: IdleUnloadPolicy.MenuChoice = fallback
        for attempt in 0..<3 {
            emit("  Choice [\(IdleUnloadPolicy.menuNumber(fallback))]: ")
            guard let line = readInput() else { break } // EOF: keep current
            if let parsed = IdleUnloadPolicy.parseChoice(line, fallback: fallback) {
                choice = parsed
                break
            }
            if attempt < 2 { emit("  Please enter 1, 2 or 3.\n") }
        }

        switch choice {
        case .alwaysReady:
            return IdleUnloadPolicy.alwaysReadyMinutes
        case .freeWhenIdle:
            return IdleUnloadPolicy.defaultMinutes
        case .custom:
            let customDefault = current == IdleUnloadPolicy.alwaysReadyMinutes
                ? IdleUnloadPolicy.defaultMinutes : current
            for attempt in 0..<3 {
                emit("  Minutes without requests before unloading [\(customDefault)]: ")
                guard let line = readInput() else { break }
                if let parsed = IdleUnloadPolicy.parseMinutes(line, fallback: customDefault) {
                    return parsed
                }
                if attempt < 2 {
                    emit("  Please enter a whole number of minutes (1–\(IdleUnloadPolicy.maxMinutes)).\n")
                }
            }
            return customDefault
        }
    }

}
