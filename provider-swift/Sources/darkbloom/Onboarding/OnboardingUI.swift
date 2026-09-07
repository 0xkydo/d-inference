import Foundation
import Darwin
import ArgumentParser

/// Small shared terminal vocabulary. No color or cursor controls in logs.
enum OnboardingUI {
    static var isTerminal: Bool { isatty(STDIN_FILENO) != 0 && isatty(STDOUT_FILENO) != 0 }
    static var usesColor: Bool {
        isatty(STDOUT_FILENO) != 0 && ProcessInfo.processInfo.environment["NO_COLOR"] == nil
            && ProcessInfo.processInfo.environment["TERM"] != "dumb"
    }
    static var width: Int {
        var size = winsize()
        return ioctl(STDOUT_FILENO, TIOCGWINSZ, &size) == 0 && size.ws_col > 0
            ? max(20, Int(size.ws_col) - 4) : 76
    }
    static func clean(_ text: String) -> String {
        String(String.UnicodeScalarView(text.unicodeScalars.filter { !CharacterSet.controlCharacters.contains($0) }))
    }
    static func style(_ text: String, _ code: String) -> String {
        usesColor ? "\u{1B}[\(code)m\(text)\u{1B}[0m" : text
    }
    static func wrapped(_ text: String, width: Int = width) -> [String] {
        var lines = [String](), current = ""
        for word in text.split(separator: " ") {
            if !current.isEmpty && current.count + word.count + 1 > width {
                lines.append(current); current = ""
            }
            var remainder = String(word)
            while remainder.count > width {
                if !current.isEmpty { lines.append(current); current = "" }
                lines.append(String(remainder.prefix(width)))
                remainder = String(remainder.dropFirst(width))
            }
            current += (current.isEmpty ? "" : " ") + remainder
        }
        lines.append(current)
        return lines
    }
    static func line(_ text: String = "", style code: String? = nil) {
        for row in wrapped(text) { print("  " + (code.map { style(row, $0) } ?? row)) }
    }
    static func heading(_ text: String) { print(); line(text, style: "1") }
    static func box(title: String, paragraphs: [String]) {
        let inner = min(68, width - 4)
        print("  ┌" + String(repeating: "─", count: inner + 2) + "┐")
        for text in [title, ""] + paragraphs.flatMap({ [$0, ""] }) {
            for row in wrapped(text, width: inner) {
                print("  │ " + row + String(repeating: " ", count: max(0, inner - row.count)) + " │")
            }
        }
        print("  └" + String(repeating: "─", count: inner + 2) + "┘")
    }
    static func confirm(_ prompt: String, readInput: () -> String? = { readLine() }) throws {
        line(prompt + " (q to quit)", style: "36")
        fflush(stdout)
        while let input = readInput() {
            let answer = input.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            if answer.isEmpty { return }
            if answer == "q" { throw CancellationError() }
            line("Press Enter to continue, or q to quit.")
        }
        throw CancellationError()
    }
}
