import Foundation

public enum LocalDataCleanup: Sendable {
    /// Attempt every requested removal, reporting failures instead of claiming
    /// success after a locked keychain or missing signing entitlement. Missing
    /// files/keys are success. The caller owns confirmation and service shutdown.
    @discardableResult
    public static func purge(
        configDirectory: Bool = true,
        legacyKeyFiles: Bool = true,
        authToken: Bool = true,
        secureEnclaveKey: Bool = true
    ) -> [String] {
        let home = FileManager.default.homeDirectoryForCurrentUser
        var operations: [(String, () throws -> Void)] = []
        func removeFile(_ relative: String) {
            operations.append((relative, {
                do { try FileManager.default.removeItem(at: home.appendingPathComponent(relative)) }
                catch CocoaError.fileNoSuchFile { /* Already removed. */ }
            }))
        }
        if configDirectory {
            for relative in [".config/darkbloom", ".config/eigeninference"] { removeFile(relative) }
        }
        if legacyKeyFiles {
            for name in ["wallet_key", "enclave_key.data", "node_key", "secret_key"] {
                removeFile(".darkbloom/" + name)
            }
        }
        if authToken { operations.append(("account token", { try AuthTokenStore.delete() })) }
        if secureEnclaveKey {
            operations.append(("wrapped cache key", { try KeychainWrappedKEKStorage().delete() }))
            operations.append(("Secure Enclave key (v2)", { try PersistentEnclaveKey.delete() }))
            operations.append(("Secure Enclave key (v1)", {
                try PersistentEnclaveKey.delete(label: PersistentEnclaveKey.legacyLabelV1)
            }))
        }
        return perform(operations)
    }

    static func perform(_ operations: [(String, () throws -> Void)]) -> [String] {
        operations.compactMap { name, operation in
            do { try operation(); return nil }
            catch { return "\(name): \(error)" }
        }
    }
}
