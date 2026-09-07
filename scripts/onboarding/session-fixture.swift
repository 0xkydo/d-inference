// Disposable test executable. Compiles the actual contract, workflow, and pipe
// host with injected services. It is never linked or packaged into darkbloom.
import Foundation
import Darwin

actor FixtureServices: OnboardingServices {
    typealias C = OnboardingContract
    let root: URL
    let delay: UInt64
    init(root: URL) {
        self.root = root
        delay = UInt64(ProcessInfo.processInfo.environment["FIXTURE_DOWNLOAD_MS"] ?? "80") ?? 80
    }
    func log(_ message: String) {
        let path = root.appendingPathComponent("actions")
        if !FileManager.default.fileExists(atPath: path.path) { FileManager.default.createFile(atPath: path.path, contents: nil) }
        let file = try! FileHandle(forWritingTo: path); defer { try? file.close() }
        try! file.seekToEnd(); try! file.write(contentsOf: Data((message+"\n").utf8))
    }
    func has(_ name: String) -> Bool { FileManager.default.fileExists(atPath: root.appendingPathComponent(name).path) }
    func mark(_ name: String) throws { try Data("fixture".utf8).write(to: root.appendingPathComponent(name)) }
    func observe() -> C.Snapshot {
        var s = C.Snapshot(); s.enrolled = has("enrolled"); s.linked = has("linked")
        s.providerRunning = has("provider-running")
        if let data = try? Data(contentsOf: root.appendingPathComponent("selection")) {
            s.selectedModelIDs = (try? JSONDecoder().decode([String].self, from: data)) ?? []
        }
        return s
    }
    func enroll() throws { log("enroll"); try mark("enrolled") }
    func link(display: @escaping @Sendable (C.LinkCode) -> Void) throws {
        log("link"); display(C.LinkCode(code: "ABCD-EFGH", url: "https://example.test/link", expiresIn: 300))
        try mark("linked")
    }
    func catalog() -> C.Snapshot {
        var s = C.Snapshot(); s.memoryGiB = 128; s.budgetGiB = 115.2
        s.models = [C.Model(id: "small", name: "Small model", sizeGB: 4, downloaded: has("small-complete"), resumable: has("small.part"), fitReason: nil),
                    C.Model(id: "large", name: "Large model", sizeGB: 400, downloaded: has("large-complete"), resumable: has("large.part"), fitReason: "Requires 512 GiB RAM")]
        return s
    }
    func remember(_ ids: [String]) throws { try JSONEncoder().encode(ids).write(to: root.appendingPathComponent("selection")) }
    func download(_ ids: [String], progress: @escaping @Sendable (C.Progress) -> Void) async throws {
        for id in ids {
            if has(id+"-complete") { log("reuse:"+id); continue }
            log((has(id+".part") ? "resume:" : "download:")+id); try mark(id+".part")
            do {
                for step in 0..<5 {
                    let bytes = Int64(1_000_000_000 + step * 200_000_000)
                    var frame = C.Progress(modelID: id, file: String(repeating: "w", count: Int(ProcessInfo.processInfo.environment["FIXTURE_PROGRESS_BYTES"] ?? "19") ?? 19), bytes: bytes, total: 4_000_000_000, stage: "transferring")
                    frame.models = [C.DownloadItem(id: id, bytes: bytes, total: 4_000_000_000, stage: "transferring")]
                    frame.files = [
                        C.DownloadItem(id: "model-00001-of-00002.safetensors", bytes: bytes / 2, total: 2_000_000_000, stage: "transferring"),
                        C.DownloadItem(id: "model-00002-of-00002.safetensors", bytes: bytes / 2, total: 2_000_000_000, stage: "transferring"),
                        C.DownloadItem(id: "config.json", bytes: 1024, total: 1024, stage: "completed")
                    ]
                    frame.fileCount = 3
                    frame.networkBytes = Int64(step * 200_000_000)
                    progress(frame)
                    try await Task.sleep(nanoseconds: delay*1_000_000 / 5)
                }
                progress(C.Progress(modelID: id, file: "weights.safetensors", bytes: 1024, total: 1024, stage: "verifying"))
                try await Task.sleep(nanoseconds: 10_000_000)
                try mark(id+"-complete")
                progress(C.Progress(modelID: id, file: "weights.safetensors", bytes: 1024, total: 1024, stage: "completed"))
            } catch { log("cancelled:"+id); throw error }
        }
    }
    func start(_ ids: [String]) throws {
        log("start:"+ids.joined(separator: ",")); try mark("provider-running")
        // A harmless detached subprocess represents an already-started provider.
        // It has no relationship to launchd, real config, keys, or inference.
        let child = Process(); child.executableURL = URL(fileURLWithPath: "/bin/sleep"); child.arguments = ["60"]
        child.standardInput = FileHandle.nullDevice; child.standardOutput = FileHandle.nullDevice; child.standardError = FileHandle.nullDevice
        try child.run()
        try Data(String(child.processIdentifier).utf8).write(to: root.appendingPathComponent("provider-pid"))
    }
}

@main enum SessionFixture {
    static func main() async throws {
        guard let directory = ProcessInfo.processInfo.environment["DARKBLOOM_SESSION_FIXTURE_ROOT"],
              FileManager.default.fileExists(atPath: directory) else { fatalError("disposable test directory required") }
        signal(SIGPIPE, SIG_IGN)
        let services = FixtureServices(root: URL(fileURLWithPath: directory))
        await services.log("session-pid:\(getpid())")
        let pipe = try OnboardingPipe(input: STDIN_FILENO, output: dup(STDOUT_FILENO))
        await OnboardingSessionHost.run(pipe: pipe, services: services)
        await services.log("session-exit")
    }
}
