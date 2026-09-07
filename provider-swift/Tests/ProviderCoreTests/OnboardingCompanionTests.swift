import Foundation
import Testing
@testable import ProviderCore

@Suite("Onboarding companion packaging contract")
struct OnboardingCompanionTests {
    @Test func capabilityMarkerAndCompanionMustAgree() throws {
        let fm = FileManager.default
        let app = fm.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let binary = app.appendingPathComponent("Contents/MacOS/darkbloom")
        let marker = app.appendingPathComponent(OnboardingCompanionVerifier.markerPath)
        let helper = app.appendingPathComponent(OnboardingCompanionVerifier.helperPath)
        try fm.createDirectory(at: binary.deletingLastPathComponent(), withIntermediateDirectories: true)
        try fm.createDirectory(at: marker.deletingLastPathComponent(), withIntermediateDirectories: true)
        defer { try? fm.removeItem(at: app) }
        try Data("old release".utf8).write(to: binary)
        try OnboardingCompanionVerifier.verify(app: app, executable: binary, signaturePolicy: nil)
        try Data(OnboardingCompanion.capability.utf8).write(to: binary)
        #expect(throws: (any Error).self) { try OnboardingCompanionVerifier.verify(app: app, executable: binary, signaturePolicy: nil) }
        try Data("1\n".utf8).write(to: marker)
        try Data("disposable helper".utf8).write(to: helper)
        try fm.setAttributes([.posixPermissions: 0o755], ofItemAtPath: helper.path)
        try OnboardingCompanionVerifier.verify(app: app, executable: binary, signaturePolicy: nil)
        #expect(throws: (any Error).self) { try OnboardingCompanionVerifier.rejectFlat(binary) }
        for mode in [0o777, 0o4755, 0o2755] {
            try fm.setAttributes([.posixPermissions: mode], ofItemAtPath: helper.path)
            #expect(throws: (any Error).self) { try OnboardingCompanionVerifier.verify(app: app, executable: binary, signaturePolicy: nil) }
        }
        try fm.removeItem(at: helper)
        try fm.createSymbolicLink(at: helper, withDestinationURL: binary)
        #expect(throws: (any Error).self) { try OnboardingCompanionVerifier.verify(app: app, executable: binary, signaturePolicy: nil) }
    }
}
