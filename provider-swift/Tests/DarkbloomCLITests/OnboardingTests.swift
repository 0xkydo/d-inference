import Foundation
import ProviderCore
import Testing
@testable import darkbloom

@Suite("Onboarding composition and resume")
struct OnboardingTests {
    @Test("new interactive installs and pending setup guide, updates and scripts do not")
    func entryModes() {
        func guide(_ pending: Bool, _ installed: Bool, _ interactive: Bool = true, _ models: Bool = false) -> Bool {
            OnboardingState.shouldGuide(explicit: false, pending: pending, installedService: installed,
                                        interactive: interactive, explicitModels: models)
        }
        #expect(guide(false, false))
        #expect(guide(true, true))
        #expect(!guide(false, true))
        #expect(!guide(true, false, false))
        #expect(!guide(true, false, true, true))
    }

    @Test("installer origins become daemon WebSocket endpoints")
    func coordinatorAddress() throws {
        #expect(try OnboardingState.webSocketURL("https://example.test") == "wss://example.test/ws/provider")
        #expect(try OnboardingState.webSocketURL("http://localhost:8080/") == "ws://localhost:8080/ws/provider")
        #expect(try OnboardingState.webSocketURL("wss://example.test/ws/provider") == "wss://example.test/ws/provider")
        #expect(try OnboardingState.webSocketURL("https://example.test/ws/provider") == "wss://example.test/ws/provider")
    }

    @Test("resume intent survives interruption and is scoped to the coordinator")
    func statePersistence() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("pending")
        let state = OnboardingState(coordinatorURL: "https://example.test", selectedModelIDs: ["one", "two"],
                                    configPath: "/tmp/custom/provider.toml", requiresAccountLink: true)
        try state.save(at: path)
        #expect(OnboardingState.load(at: path) == state)
        #expect(OnboardingState.isPending(at: path))
        try OnboardingState.complete(at: path)
        #expect(!OnboardingState.isPending(at: path))
        try Data().write(to: path)
        #expect(OnboardingState.isPending(at: path))
        #expect(OnboardingState.load(at: path) == nil)
        try Data("https://custom.test\n".utf8).write(to: path)
        #expect(OnboardingState.load(at: path)?.coordinatorURL == "wss://custom.test/ws/provider")
        try Data(#"{"coordinatorURL":"wss://old.test/ws/provider","selectedModelIDs":[]}"#.utf8).write(to: path)
        #expect(OnboardingState.load(at: path)?.requiresAccountLink == nil)
    }

    @Test("enrollment precedes login and a failed enrollment cannot proceed")
    func ordering() async throws {
        var calls: [String] = []
        try await GuidedOnboarding.prepare(coordinatorURL: "https://example.test", enroll: {
            calls.append("enroll")
        }, hasAccount: { false }, login: { calls.append("login") })
        #expect(calls == ["enroll", "login"])
        calls = []
        do {
            try await GuidedOnboarding.prepare(coordinatorURL: "https://example.test", enroll: {
                calls.append("enroll")
                throw CancellationError()
            }, hasAccount: { false }, login: { calls.append("login") })
            Issue.record("enrollment cancellation must stop the flow")
        } catch is CancellationError {}
        #expect(calls == ["enroll"])
        calls = []
        try await GuidedOnboarding.prepare(coordinatorURL: "https://example.test", enroll: {
            calls.append("recheck enrollment")
        }, hasAccount: { true }, login: { calls.append("unexpected login") })
        #expect(calls == ["recheck enrollment"])
    }

    @Test("Enter is required to start; EOF and quit never count as confirmation")
    func confirmation() throws {
        try OnboardingUI.confirm("Start", readInput: { "" })
        #expect(throws: CancellationError.self) { try OnboardingUI.confirm("Start", readInput: { nil }) }
        #expect(throws: CancellationError.self) { try OnboardingUI.confirm("Start", readInput: { "q" }) }
        var input = ["no", ""]
        try OnboardingUI.confirm("Start", readInput: { input.removeFirst() })
        #expect(input.isEmpty)
    }

    @Test("total RAM estimates include padded weights, configured reserve, working memory and KV")
    func totalCapacity() {
        let small = CatalogModel(id: "small", s3Name: "small", displayName: "Small", sizeGb: 6,
                                 totalSizeBytes: 6 * 1_073_741_824)
        let large = CatalogModel(id: "large", s3Name: "large", displayName: "Large", sizeGb: 200,
                                 minRamGb: 256, totalSizeBytes: 200 * 1_073_741_824)
        let rows = [small, large].map { Start.PickerCatalogRow(model: $0, displayName: $0.displayName) }
        let entries = Start.buildPickerEntries(rows: rows, downloadedIDs: [], localMemoryByID: [:],
                                               resumableIDs: [], memoryGb: 48)
        #expect(abs(Start.pickerBudgetGiB(memoryGb: 48) - 43.2) < 0.001)
        #expect(entries.first { $0.id == "small" }?.fitReason == nil)
        #expect(abs((entries.first { $0.id == "small" }?.estimatedWeightsGiB ?? 0) - 7.2) < 0.001)
        #expect(entries.first { $0.id == "large" }?.fitReason != nil)
        let constrained = Start.buildPickerEntries(rows: rows, downloadedIDs: [], localMemoryByID: [:],
                                                   resumableIDs: [], memoryGb: 48, reserveGb: 40)
        #expect(constrained.allSatisfy { $0.fitReason != nil })
        let hiddenIndex = entries.firstIndex { $0.id == "large" }!
        #expect(Start.resolveFallbackSelection(input: "\(hiddenIndex + 1)", entries: entries, memoryGb: 48, expanded: true)
                == .selected(["large"]))
        let collapsed = Start.pickerLines(entries: entries, memoryGb: 48, budgetGiB: 43.2,
                                           selected: [], expanded: false, focused: nil).joined(separator: "\n")
        #expect(!collapsed.contains("Downloaded"))
        #expect(!collapsed.contains("Large"))
        #expect(collapsed.contains("Available to download"))
        #expect(collapsed.contains("Show 1 additional model"))
    }

    @Test("readiness must belong to this launch, be verified, and have a warm model")
    func readiness() {
        var state = DaemonState(pid: 123, version: "test", writtenAt: 101, startedAt: 100,
                                trust: .init(trustLevel: "hardware", status: "online", reason: "", receivedAt: 101),
                                warmModels: ["small"], inferenceActive: false, stats: .init())
        #expect(OnboardingStartup.verified(state, launchedAt: 99, now: 102))
        #expect(!OnboardingStartup.verified(state, launchedAt: 101, now: 102))
        #expect(!OnboardingStartup.verified(state, launchedAt: 99, now: 300))
        state.warmModels = []
        #expect(!OnboardingStartup.verified(state, launchedAt: 99, now: 102))
        state.warmModels = ["small"]
        state.trust?.status = "untrusted"
        #expect(!OnboardingStartup.verified(state, launchedAt: 99, now: 102))
    }
}
