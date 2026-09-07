import Foundation
import ArgumentParser
import ProviderCore
import Darwin

enum BubbleTeaLauncher {
    static func launch(config: String?, coordinatorURL: String?) throws -> Never {
        guard OnboardingUI.isTerminal, ProcessInfo.processInfo.environment["TERM"] != "dumb" else {
            throw ValidationError("--tui requires a terminal. Use darkbloom start for the plain CLI.")
        }
        // Resolve symlinks once: installed bin/darkbloom points inside the app.
        // Neither the companion nor the backend is searched through PATH.
        let backend = URL(fileURLWithPath: LaunchAgent.currentExecutablePath()).resolvingSymlinksInPath()
        let companion = backend.deletingLastPathComponent().appendingPathComponent("darkbloom-tui")
        guard FileManager.default.isExecutableFile(atPath: companion.path) else {
            throw ValidationError("Bubble Tea companion is missing. Install a complete candidate bundle, or build it beside darkbloom with make provider-tui-build. Plain darkbloom start is still available.")
        }
        var args = [companion.path, "--backend", backend.path]
        if let config { args += ["--config", config] }
        if let coordinatorURL { args += ["--coordinator-url", coordinatorURL] }
        let pointers = args.map { strdup($0) } + [nil]
        defer { pointers.forEach { free($0) } }
        pointers.withUnsafeBufferPointer { _ = execv(companion.path, $0.baseAddress!) }
        throw ValidationError("Could not launch the Bubble Tea companion.")
    }
}
