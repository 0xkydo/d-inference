import Foundation
import ArgumentParser

/// This records intent, never proof of enrollment, account linkage or readiness.
/// Those are checked again on resume. The shell records its coordinator origin;
/// zero-byte markers from earlier installers are also accepted as pending intent.
struct OnboardingState: Codable, Equatable {
    var coordinatorURL: String
    var selectedModelIDs: [String] = []
    var configPath: String? = nil
    // Optional for markers written before coordinator persistence existed.
    var requiresAccountLink: Bool? = nil

    static var path: URL {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".darkbloom/onboarding-pending")
    }
    static func isPending(at path: URL = path) -> Bool {
        FileManager.default.fileExists(atPath: path.path)
    }
    static func load(at path: URL = path) -> Self? {
        guard let data = try? Data(contentsOf: path) else { return nil }
        if let state = try? JSONDecoder().decode(Self.self, from: data) { return state }
        // The shell records just the origin, before it knows which CLI release
        // is available. Preserve that environment on an unattended-install resume.
        guard let origin = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines),
              let coordinator = try? webSocketURL(origin) else { return nil }
        return Self(coordinatorURL: coordinator)
    }
    func save(at path: URL = path) throws {
        try FileManager.default.createDirectory(at: path.deletingLastPathComponent(), withIntermediateDirectories: true)
        try JSONEncoder().encode(self).write(to: path, options: .atomic)
    }
    static func complete(at path: URL = path) throws {
        if isPending(at: path) { try FileManager.default.removeItem(at: path) }
    }
    /// Installers receive an HTTP origin; the daemon must connect to its WS
    /// provider endpoint. Already-configured WS endpoints retain their path.
    static func webSocketURL(_ raw: String) throws -> String {
        guard var url = URLComponents(string: raw), url.host != nil else {
            throw ValidationError("Invalid coordinator URL.")
        }
        switch url.scheme {
        case "https", "http":
            url.scheme = url.scheme == "https" ? "wss" : "ws"
            url.path = url.path.trimmingCharacters(in: CharacterSet(charactersIn: "/"))
            if !url.path.hasSuffix("ws/provider") {
                url.path = "/" + (url.path.isEmpty ? "" : url.path + "/") + "ws/provider"
            } else {
                url.path = "/" + url.path
            }
        case "wss", "ws": break
        default: throw ValidationError("Coordinator URL must use HTTPS, HTTP, WSS, or WS.")
        }
        guard let result = url.string else { throw ValidationError("Invalid coordinator URL.") }
        return result
    }
    static func shouldGuide(explicit: Bool, pending: Bool, installedService: Bool,
                            interactive: Bool, explicitModels: Bool) -> Bool {
        explicit || (interactive && !explicitModels && (pending || !installedService))
    }
}
