import Foundation
import Darwin

/// The reader and writer run independently. A slow or vanished reader can never
/// prevent command EOF / cancellation from being noticed. Progress is coalesced;
/// control events have a bounded queue and a two-second write deadline.
final class OnboardingPipe: @unchecked Sendable {
    typealias C = OnboardingContract
    private let input: Int32
    private let output: Int32
    private let lock = NSLock()
    private var queue: [Data] = []
    private var progress: Data?
    private var stopped = false
    private var finished = false
    private var buffer = Data()

    init(input: Int32, output: Int32) throws {
        self.input = input; self.output = output
        for fd in [input, output] {
            var info = stat()
            guard fstat(fd, &info) == 0, (info.st_mode & S_IFMT) == S_IFIFO,
                  fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK) == 0 else {
                throw C.Failure.unavailable
            }
            _ = fcntl(fd, F_SETFD, FD_CLOEXEC)
        }
    }
    deinit { close(output) }
    var isStopped: Bool { lock.withLock { stopped } }
    func stop() { lock.withLock { stopped = true } }
    func finish() { lock.withLock { finished = true } }

    func send(_ event: C.Event) {
        guard var data = try? JSONEncoder().encode(event), data.count < C.maxEventBytes else { stop(); return }
        data.append(10)
        lock.withLock {
            if event.kind == .progress { progress = data }
            else if queue.count < 32 {
                // Old progress must not follow a ready/error snapshot.
                progress = nil
                queue.append(data)
            } else { stopped = true }
        }
    }

    func nextLine() async throws -> Data? {
        while !isStopped {
            if let end = buffer.firstIndex(of: 10) {
                let line = Data(buffer[..<end]); buffer.removeSubrange(...end)
                guard line.count <= C.maxCommandBytes else { throw C.Failure.invalidCommand }
                return line
            }
            guard buffer.count <= C.maxCommandBytes else { throw C.Failure.invalidCommand }
            var bytes = [UInt8](repeating: 0, count: 4096)
            let count = read(input, &bytes, bytes.count)
            if count > 0 { buffer.append(contentsOf: bytes.prefix(count)); continue }
            // EOF discards any incomplete frame; it is never consent.
            if count == 0 { return nil }
            if errno != EAGAIN && errno != EINTR { return nil }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        return nil
    }

    func writeEvents() async {
        while !isStopped && !Task.isCancelled {
            let (data, done): (Data?, Bool) = lock.withLock {
                if !queue.isEmpty { return (queue.removeFirst(), false) }
                if let data = progress { progress = nil; return (data, false) }
                return (nil, finished)
            }
            if done { return }
            guard let data else { try? await Task.sleep(nanoseconds: 10_000_000); continue }
            var offset = 0
            let deadline = Date().addingTimeInterval(2)
            while offset < data.count && !isStopped && !Task.isCancelled {
                let count = data.withUnsafeBytes { write(output, $0.baseAddress!.advanced(by: offset), data.count - offset) }
                if count > 0 { offset += count; continue }
                if count < 0 && (errno == EAGAIN || errno == EINTR) && Date() < deadline {
                    try? await Task.sleep(nanoseconds: 10_000_000); continue
                }
                stop()
            }
        }
    }
}
