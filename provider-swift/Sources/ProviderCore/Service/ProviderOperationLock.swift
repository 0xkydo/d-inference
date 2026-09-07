import Foundation
import CryptoKit
import Darwin

/// Short-lived, nonblocking cross-process exclusion for setup and cache writers.
/// Lock files are outside the protected model directory: removal must never
/// unlink a held lock and let a second writer acquire a different inode.
public final class ProviderOperationLock: @unchecked Sendable {
    public enum Failure: Error, CustomStringConvertible {
        case busy, unavailable
        public var description: String {
            switch self {
            case .busy: "Another setup or model operation is active. Close it and retry."
            case .unavailable: "Could not acquire the local operation lock."
            }
        }
    }

    private let descriptor: Int32
    private init(_ descriptor: Int32) { self.descriptor = descriptor }
    deinit { flock(descriptor, LOCK_UN); close(descriptor) }

    public static func acquire(at path: URL) throws -> ProviderOperationLock {
        try FileManager.default.createDirectory(at: path.deletingLastPathComponent(), withIntermediateDirectories: true)
        let fd = open(path.path, O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0o600)
        guard fd >= 0 else { throw Failure.unavailable }
        var info = stat()
        guard fstat(fd, &info) == 0, info.st_uid == getuid(), (info.st_mode & S_IFMT) == S_IFREG else {
            close(fd); throw Failure.unavailable
        }
        guard flock(fd, LOCK_EX | LOCK_NB) == 0 else {
            let code = errno
            close(fd)
            throw code == EWOULDBLOCK ? Failure.busy : Failure.unavailable
        }
        return ProviderOperationLock(fd)
    }

    public static func setup() throws -> ProviderOperationLock {
        try acquire(at: FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".darkbloom/onboarding.lock"))
    }

    public static func model(_ modelID: String) throws -> ProviderOperationLock {
        // Validate before allowing catalog IDs to form cache paths.
        guard !modelID.isEmpty, modelID.utf8.count <= 512,
              !modelID.contains("\\"), !modelID.hasPrefix("/"),
              modelID.split(separator: "/", omittingEmptySubsequences: false).allSatisfy({
                  !$0.isEmpty && $0 != "." && $0 != ".."
              }) else { throw Failure.unavailable }
        let directory = ModelDownloader.cacheModelDirectory(for: modelID).standardizedFileURL
        let key = SHA256.hash(data: Data(directory.path.utf8)).map { String(format: "%02x", $0) }.joined()
        return try acquire(at: directory.deletingLastPathComponent()
            .appendingPathComponent(".darkbloom-locks/\(key).lock"))
    }
}
