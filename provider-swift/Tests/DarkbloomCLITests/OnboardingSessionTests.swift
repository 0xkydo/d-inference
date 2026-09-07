import Foundation
import Testing
@testable import darkbloom

private final class SessionEvents: @unchecked Sendable {
    private let lock = NSLock()
    private var events: [OnboardingContract.Event] = []
    func append(_ event: OnboardingContract.Event) { lock.withLock { events.append(event) } }
    var all: [OnboardingContract.Event] { lock.withLock { events } }
}

private actor SessionServices: OnboardingServices {
    typealias C = OnboardingContract
    var enrolled = false
    var linked = false
    var calls: [String] = []
    var blockDownload = false
    var failLink = false
    func set(enrolled: Bool, linked: Bool) { self.enrolled = enrolled; self.linked = linked }
    func block() { blockDownload = true }
    func failAccount() { failLink = true }
    func observe() -> C.Snapshot { var s = C.Snapshot(); s.enrolled = enrolled; s.linked = linked; return s }
    func enroll() { calls.append("enroll"); enrolled = true }
    func link(display: @escaping @Sendable (C.LinkCode) -> Void) throws {
        calls.append("link")
        if failLink { throw NSError(domain: "secret-token-or-http-body", code: 1) }
        linked = true
    }
    func catalog() -> C.Snapshot {
        var s = C.Snapshot()
        s.models = [C.Model(id: "small", name: "Small", sizeGB: 4, downloaded: false, resumable: false, fitReason: nil),
                    C.Model(id: "large", name: "Large", sizeGB: 400, downloaded: false, resumable: false, fitReason: "Too large")]
        return s
    }
    func remember(_ ids: [String]) { calls.append("remember") }
    func download(_ ids: [String], progress: @escaping @Sendable (C.Progress) -> Void) async throws {
        calls.append("download:" + ids.joined(separator: ","))
        if blockDownload {
            do { try await Task.sleep(nanoseconds: 60_000_000_000) }
            catch { calls.append("cancelled"); throw error }
        }
    }
    func start(_ ids: [String]) { calls.append("start:" + ids.joined(separator: ",")) }
}

@Suite("Onboarding session contract and workflow")
struct OnboardingSessionTests {
    typealias C = OnboardingContract
    @Test func sharedFixtures() throws {
        let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent()
            .deletingLastPathComponent().deletingLastPathComponent()
        let object = try #require(JSONSerialization.jsonObject(with: Data(contentsOf: root.appendingPathComponent("provider-tui/testdata/contract.json"))) as? [String: [[String: Any]]])
        for row in object["commands"]! {
            let data = try JSONSerialization.data(withJSONObject: row)
            let command = try C.Command.decode(data)
            #expect(try JSONDecoder().decode(C.Command.self, from: JSONEncoder().encode(command)) == command)
        }
        for row in object["events"]! {
            let data = try JSONSerialization.data(withJSONObject: row)
            let event = try JSONDecoder().decode(C.Event.self, from: data)
            #expect(try JSONDecoder().decode(C.Event.self, from: JSONEncoder().encode(event)) == event)
        }
    }
    @Test func rejectUntrustedFrames() throws {
        for frame in ["{}", "[]", "null", #"{"version":1,"id":1,"action":"sign"}"#,
                      #"{"version":1,"id":1,"action":"start","shell":"secret"}"#,
                      #"{"version":1,"id":1,"action":"start","modelIDs":["x"]}"#,
                      #"{"version":1,"id":0,"action":"hello"}"#,
                      #"{"version":2,"id":1,"action":"hello"}"#,
                      String(repeating: "x", count: C.maxCommandBytes+1)] {
            #expect(throws: C.Failure.self) { try C.Command.decode(Data(frame.utf8)) }
        }
    }
    @Test func explicitActionsAndRunnableSelection() async {
        let services = SessionServices(); let events = SessionEvents()
        let workflow = OnboardingWorkflow(services: services, emit: { events.append($0) })
        await workflow.handle(.init(version: 1, id: 1, action: .hello))
        #expect(await services.calls.isEmpty)
        await workflow.handle(.init(version: 1, id: 2, action: .start, revision: 1))
        #expect(await services.calls.isEmpty)
        var id = 3
        func send(_ action: C.Action, ids: [String]? = nil) async {
            let revision = await workflow.snapshot.revision
            await workflow.handle(.init(version: 1, id: id, action: action, revision: revision, modelIDs: ids)); id += 1
        }
        await send(.enroll)
        #expect(await services.calls == ["enroll"])
        await send(.refresh)
        #expect(await workflow.snapshot.phase == .account)
        await send(.link)
        await send(.selectModels, ids: ["large"])
        #expect(events.all.contains { $0.error == .invalidSelection })
        await send(.selectModels, ids: ["small", "large"])
        #expect(await workflow.snapshot.phase == .ready)
        #expect(await services.calls == ["enroll", "link", "remember", "download:small,large"])
        await workflow.handle(.init(version: 1, id: id, action: .start, revision: 0)); id += 1
        #expect(!events.all.filter { $0.error == .staleCommand }.isEmpty)
        await send(.start)
        #expect(await services.calls.last == "start:small")
        #expect(await workflow.snapshot.phase == .started)
    }
    @Test func prerequisitesRecheckedAndErrorsSanitized() async {
        let services = SessionServices(); await services.set(enrolled: true, linked: false); await services.failAccount()
        let events = SessionEvents(); let workflow = OnboardingWorkflow(services: services, emit: { events.append($0) })
        await workflow.handle(.init(version: 1, id: 1, action: .hello))
        await workflow.handle(.init(version: 1, id: 2, action: .link, revision: 1))
        #expect(events.all.contains { $0.error == .linkFailed })
        #expect(!String(describing: events.all).contains("secret-token"))
        await services.set(enrolled: false, linked: false)
        await workflow.handle(.init(version: 1, id: 3, action: .link, revision: 2))
        #expect(events.all.contains { $0.error == .prerequisitesChanged })
        #expect(await services.calls == ["link"])
    }
    @Test func cancellationCannotReachReadyOrStart() async throws {
        let services = SessionServices(); await services.set(enrolled: true, linked: true); await services.block()
        let events = SessionEvents(); let workflow = OnboardingWorkflow(services: services, emit: { events.append($0) })
        await workflow.handle(.init(version: 1, id: 1, action: .hello))
        let task = Task { await workflow.handle(.init(version: 1, id: 2, action: .selectModels, revision: 1, modelIDs: ["small"])) }
        for _ in 0..<100 {
            if await services.calls.contains("download:small") { break }
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        task.cancel(); await task.value
        #expect(await services.calls.contains("cancelled"))
        #expect(!events.all.contains { $0.snapshot?.phase == .ready || $0.snapshot?.phase == .started })
    }
}
