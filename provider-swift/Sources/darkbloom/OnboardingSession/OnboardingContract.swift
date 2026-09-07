import Foundation

/// Private, bounded JSON-lines contract. No terminal, inference, or credential types.
/// Version changes require the companion and Swift backend from the same release.
enum OnboardingContract {
    static let version = 1
    static let maxCommandBytes = 16_384
    static let maxEventBytes = 262_144

    enum Action: String, Codable, Sendable {
        case hello, refresh, enroll, link, selectModels = "select_models", start, cancel
    }
    enum Phase: String, Codable, Sendable {
        case enrollment, enrollmentPending = "enrollment_pending", account, models, downloading, ready, started
    }
    struct Command: Codable, Sendable, Equatable {
        let version: Int
        let id: Int
        let action: Action
        var revision: Int? = nil
        var modelIDs: [String]? = nil

        static func decode(_ data: Data) throws -> Self {
            guard data.count <= maxCommandBytes,
                  let fields = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
                  Set(fields.keys).isSubset(of: ["version", "id", "action", "revision", "modelIDs"]) else {
                throw Failure.invalidCommand
            }
            let command: Self
            do { command = try JSONDecoder().decode(Self.self, from: data) }
            catch { throw Failure.invalidCommand }
            guard command.version == OnboardingContract.version else { throw Failure.versionMismatch }
            guard command.id > 0, command.id <= 2_147_483_647,
                  command.modelIDs == nil || command.action == .selectModels,
                  command.modelIDs?.count ?? 0 <= 128,
                  command.modelIDs?.allSatisfy({ !$0.isEmpty && $0.utf8.count <= 512 }) ?? true else {
                throw Failure.invalidCommand
            }
            return command
        }
    }
    struct Model: Codable, Sendable, Equatable {
        let id: String
        let name: String
        let sizeGB: Double
        var downloaded: Bool
        let resumable: Bool
        let fitReason: String?
    }
    struct Snapshot: Codable, Sendable, Equatable {
        var phase: Phase = .enrollment
        var revision = 0
        var enrolled = false
        var linked = false
        // Observation, not readiness or the coordinator's private-routing verdict.
        var providerRunning = false
        var notice: String? = nil
        var observedAt: Double = 0
        var memoryGiB: Double = 0
        var budgetGiB: Double = 0
        var models: [Model] = []
        var selectedModelIDs: [String] = []
    }
    struct Progress: Codable, Sendable, Equatable {
        let modelID: String
        let file: String
        let bytes: Int64
        let total: Int64?
        let stage: String
    }
    struct LinkCode: Codable, Sendable, Equatable {
        let code: String
        let url: String
        let expiresIn: Int
    }
    struct Event: Codable, Sendable, Equatable {
        enum Kind: String, Codable, Sendable { case snapshot, progress, linkCode = "link_code", error }
        var version = OnboardingContract.version
        let kind: Kind
        let commandID: Int
        var snapshot: Snapshot? = nil
        var progress: Progress? = nil
        var linkCode: LinkCode? = nil
        var error: Failure? = nil
    }
    /// Deliberately closed, safe errors. Never serialize arbitrary service errors,
    /// HTTP bodies, diagnostics or tokens (including the existing token-prefix error).
    enum Failure: String, Error, Codable, Sendable {
        case invalidCommand = "invalid_command", versionMismatch = "version_mismatch"
        case staleCommand = "stale_command", wrongPhase = "wrong_phase", busy
        case enrollmentFailed = "enrollment_failed", enrollmentUnknown = "enrollment_unknown", otherMDM = "other_mdm"
        case linkFailed = "link_failed", catalogFailed = "catalog_failed", invalidSelection = "invalid_selection"
        case downloadFailed = "download_failed", startFailed = "start_failed", prerequisitesChanged = "prerequisites_changed"
        case unavailable, disconnected
    }
}
