import Foundation
import Darwin

/// One sealed companion in the provider release, with no inference entitlements.
public enum OnboardingCompanion {
    public static let capability = "darkbloom-onboarding-session-v1"
}

enum OnboardingCompanionVerifier {
    static let markerPath = "Contents/Resources/darkbloom-runtime-capabilities/onboarding-session-v1"
    static let helperPath = "Contents/MacOS/darkbloom-tui"

    static func containsCapability(_ executable: URL) throws -> Bool {
        try Data(contentsOf: executable, options: .mappedIfSafe).range(of: Data(OnboardingCompanion.capability.utf8)) != nil
    }
    static func verify(app: URL, executable: URL, signaturePolicy: DarkbloomCodeSignature.Policy?) throws {
        let marker = app.appendingPathComponent(markerPath), helper = app.appendingPathComponent(helperPath)
        let code = try containsCapability(executable)
        var markerStat = stat(), helperStat = stat()
        let hasMarker = lstat(marker.path, &markerStat) == 0, hasHelper = lstat(helper.path, &helperStat) == 0
        if !code && !hasMarker && !hasHelper { return } // older releases / rollback
        guard code, hasMarker, hasHelper,
              markerStat.st_mode & S_IFMT == S_IFREG,
              helperStat.st_mode & S_IFMT == S_IFREG,
              helperStat.st_mode & 0o7777 == 0o755,
              (try? String(contentsOf: marker, encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines)) == "1" else {
            throw UpdateError.replaceFailed("onboarding requires matching CLI code, signed marker, and regular companion")
        }
        if let signaturePolicy {
            try DarkbloomCodeSignature.verify(helper, deep: false,
                policy: signaturePolicy == .structuralForIsolatedTest ? .structuralForIsolatedTest : .darkbloomOnboarding)
        }
    }
    static func rejectFlat(_ executable: URL) throws {
        if try containsCapability(executable) {
            throw UpdateError.replaceFailed("onboarding-capable releases require the signed Darkbloom.app layout")
        }
    }
}
