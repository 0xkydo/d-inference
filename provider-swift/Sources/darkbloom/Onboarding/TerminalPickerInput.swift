import Darwin

/// Read a key at a time: pipe/PTY reads can split escape sequences or combine
/// multiple keystrokes. EOF and Ctrl-C/Ctrl-D always leave through the TTY defer.
enum TerminalPickerInput {
    enum Key { case up, down, toggle, expand, confirm, quit, other }
    static func readKey(fd: Int32 = STDIN_FILENO) -> Key {
        var byte: UInt8 = 0
        guard read(fd, &byte, 1) == 1 else { return .quit }
        switch byte {
        case 3, 4, 113: return .quit
        case 32: return .toggle
        case 104: return .expand
        case 10, 13: return .confirm
        case 27:
            var descriptor = pollfd(fd: fd, events: Int16(POLLIN), revents: 0)
            guard poll(&descriptor, 1, 100) > 0, read(fd, &byte, 1) == 1 else { return .quit }
            guard byte == 91 || byte == 79 else { return .other }
            guard poll(&descriptor, 1, 100) > 0, read(fd, &byte, 1) == 1 else { return .other }
            return byte == 65 ? .up : byte == 66 ? .down : .other
        default: return .other
        }
    }
}

/// Own only the terminal mode we change. All cancellation/error paths restore
/// it, and tests can exercise this against a disposable PTY.
final class PickerTerminalMode {
    private let fd: Int32
    private var original = termios()
    init?(fd: Int32 = STDIN_FILENO) {
        self.fd = fd
        guard tcgetattr(fd, &original) == 0 else { return nil }
        var raw = original
        raw.c_lflag &= ~UInt(ECHO | ICANON | ISIG)
        raw.c_cc.16 = 1
        raw.c_cc.17 = 0
        guard tcsetattr(fd, TCSANOW, &raw) == 0 else { return nil }
    }
    func restore() { tcsetattr(fd, TCSANOW, &original) }
}
