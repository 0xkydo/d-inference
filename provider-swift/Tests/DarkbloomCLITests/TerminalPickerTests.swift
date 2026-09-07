import Foundation
import Darwin
import Testing
@testable import darkbloom

@Suite("Model picker terminal interaction")
struct TerminalPickerTests {
    @Test("combined keystrokes are read independently, EOF quits")
    func combinedKeys() throws {
        var descriptors: [Int32] = [0, 0]
        #expect(pipe(&descriptors) == 0)
        defer { close(descriptors[0]) }
        let input = Array(" \u{1B}[Bh\r".utf8)
        input.withUnsafeBytes { _ = write(descriptors[1], $0.baseAddress, input.count) }
        close(descriptors[1])
        #expect(TerminalPickerInput.readKey(fd: descriptors[0]) == .toggle)
        #expect(TerminalPickerInput.readKey(fd: descriptors[0]) == .down)
        #expect(TerminalPickerInput.readKey(fd: descriptors[0]) == .expand)
        #expect(TerminalPickerInput.readKey(fd: descriptors[0]) == .confirm)
        #expect(TerminalPickerInput.readKey(fd: descriptors[0]) == .quit)
    }

    @Test("split escape sequences are collected within the timeout")
    func splitArrow() throws {
        var descriptors: [Int32] = [0, 0]
        #expect(pipe(&descriptors) == 0)
        let inputFD = descriptors[0], outputFD = descriptors[1]
        defer { close(inputFD) }
        var escape: UInt8 = 27
        _ = write(outputFD, &escape, 1)
        DispatchQueue.global().asyncAfter(deadline: .now() + 0.02) {
            let suffix = Array("[A".utf8)
            suffix.withUnsafeBytes { _ = write(outputFD, $0.baseAddress, suffix.count) }
            close(outputFD)
        }
        #expect(TerminalPickerInput.readKey(fd: inputFD) == .up)
    }

    @Test("Ctrl-C, Ctrl-D and Escape quit and the terminal restores")
    func cancellationRestoresMode() throws {
        for key: UInt8 in [3, 4, 27, 113] {
            var master: Int32 = 0, slave: Int32 = 0
            #expect(openpty(&master, &slave, nil, nil, nil) == 0)
            defer { close(master); close(slave) }
            var before = termios()
            #expect(tcgetattr(slave, &before) == 0)
            do {
                let mode = try #require(PickerTerminalMode(fd: slave))
                defer { mode.restore() }
                var byte = key
                _ = write(master, &byte, 1)
                #expect(TerminalPickerInput.readKey(fd: slave) == .quit)
            }
            var after = termios()
            #expect(tcgetattr(slave, &after) == 0)
            #expect(before.c_lflag & ~UInt(PENDIN) == after.c_lflag & ~UInt(PENDIN))
            #expect(before.c_cc.16 == after.c_cc.16)
            #expect(before.c_cc.17 == after.c_cc.17)
        }
    }
}
