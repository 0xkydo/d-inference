import Foundation
import ProviderCore
import Testing
@testable import darkbloom

@Suite("Onboarding interaction gates and styling")
struct OnboardingInteractionTests {
    @Test("enrollment waits for confirmation; quitting never requests or opens a profile")
    func enrollmentConfirmation() async throws {
        struct StopAfterRequest: Error {}
        var calls: [String] = []
        do {
            try await EnrollmentFlow.run(coordinatorURL: "https://example.test", waitForCompletion: false,
                interactive: true, checkEnrollment: { _ in .notEnrolled },
                confirm: { _ in calls.append("confirm"); throw CancellationError() },
                enroll: { _, _ in calls.append("request"); throw StopAfterRequest() })
            Issue.record("quit must cancel enrollment")
        } catch is CancellationError {}
        #expect(calls == ["confirm"])
        calls = []
        do {
            try await EnrollmentFlow.run(coordinatorURL: "https://example.test", waitForCompletion: false,
                interactive: true, checkEnrollment: { _ in .notEnrolled },
                confirm: { _ in calls.append("confirm") },
                enroll: { _, open in
                    #expect(open)
                    calls.append("request")
                    throw StopAfterRequest()
                })
        } catch is StopAfterRequest {}
        #expect(calls == ["confirm", "request"])
        calls = []
        try await EnrollmentFlow.run(coordinatorURL: "https://example.test", waitForCompletion: false,
            interactive: true, checkEnrollment: { _ in .enrolledDarkbloom(serverURL: "https://example.test/mdm/connect") },
            confirm: { _ in calls.append("unexpected confirm") },
            enroll: { _, _ in calls.append("unexpected request"); throw StopAfterRequest() })
        #expect(calls.isEmpty)
    }

    @Test("login waits for confirmation and quit never requests a code or opens a browser")
    func loginConfirmation() async throws {
        var calls: [String] = []
        do {
            try await AccountLinkFlow.run(coordinatorURL: "https://example.test", interactive: true,
                confirm: { _ in calls.append("confirm"); throw CancellationError() },
                login: { _ in calls.append("login") })
            Issue.record("quit must cancel login")
        } catch is CancellationError {}
        #expect(calls == ["confirm"])
        calls = []
        try await AccountLinkFlow.run(coordinatorURL: "https://example.test", interactive: true,
            confirm: { _ in calls.append("confirm") }, login: { _ in calls.append("login") })
        #expect(calls == ["confirm", "login"])
    }

    @Test("terminal styling respects explicit opt-out and leaves redirected output plain")
    func terminalStyling() {
        #expect(OnboardingUI.supportsColor(terminal: true, environment: ["TERM": "xterm-256color"]))
        #expect(OnboardingUI.supportsColor(terminal: true, environment: ["NO_COLOR": ""]))
        #expect(!OnboardingUI.supportsColor(terminal: true, environment: ["NO_COLOR": "1"]))
        #expect(!OnboardingUI.supportsColor(terminal: true, environment: ["TERM": "dumb"]))
        #expect(!OnboardingUI.supportsColor(terminal: false, environment: [:]))
    }

    @Test("download-only and noninteractive enrollment do not add a terminal prompt",
          arguments: [(true, true), (false, false)])
    func enrollmentWithoutPrompt(mode: (Bool, Bool)) async throws {
        struct StopAfterRequest: Error {}
        let (noOpen, interactive) = mode
        var requested = false
        do {
            try await EnrollmentFlow.run(coordinatorURL: "https://example.test", noOpen: noOpen,
                waitForCompletion: false, interactive: interactive,
                checkEnrollment: { _ in .notEnrolled },
                confirm: { _ in Issue.record("unexpected prompt") },
                enroll: { _, open in
                    #expect(open == !noOpen)
                    requested = true
                    throw StopAfterRequest()
                })
        } catch is StopAfterRequest {}
        #expect(requested)
    }
}
