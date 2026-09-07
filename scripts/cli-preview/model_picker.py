"""Mock-only multi-select picker with the existing CLI's keyboard conventions."""

import os
import select
import shutil
import sys
import termios
import textwrap
import tty


MODELS = (
    ("Everyday text", 4),
    ("Larger text", 8),
    ("Reasoning text", 12),
)


def summary(selected):
    return ", ".join(MODELS[i][0] for i in selected)


def needs_download(selected, downloaded):
    return any(i not in downloaded for i in selected)


def pick(term, selected, downloaded):
    """Return navigation action and selection, restoring terminal settings always."""
    chosen = set(selected)
    cursor = 0
    previous_lines = 0
    interactive = sys.stdin.isatty() and term.tty and os.environ.get("TERM") != "dumb"
    original = termios.tcgetattr(sys.stdin.fileno()) if interactive else None
    notice = ""
    previous_size = None

    def draw():
        nonlocal previous_lines, previous_size
        size = shutil.get_terminal_size()
        width = max(12, size.columns - 4)
        lines = ["Select one or more models · 32 GB RAM (sample)", ""]
        for heading, indices in (("Downloaded", [i for i in range(len(MODELS)) if i in downloaded]),
                                 ("Available to download", [i for i in range(len(MODELS)) if i not in downloaded])):
            if not indices:
                continue
            lines.extend([heading])
            for i in indices:
                name, gb = MODELS[i]
                arrow = "›" if cursor == i else " "
                check = "x" if i in chosen else " "
                lines.append(f"{arrow} [{check}] {i + 1}. {name} · {gb} GB")
            lines.append("")
        gb = sum(MODELS[i][1] for i in chosen if i not in downloaded)
        lines.extend(["", f"{len(chosen)} selected · {gb} GB to download",
                      "Models may be loaded on demand.", "",
                      "↑↓ move · Space toggle · Enter confirm" if interactive
                      else "Type 1, 2, or 3 to toggle; Enter confirms.",
                      "Mock: b back · m scenarios · q quit",
                      "Mock: n simulates no compatible models",
                      notice])
        # Keep the redraw region bounded to the terminal width.
        rows = [row for line in lines for row in (textwrap.wrap(line, width) or [""])]
        if interactive and previous_lines and previous_size == size and previous_lines < size.lines:
            print(f"\033[{previous_lines}A\r\033[J", end="")
        for row in rows:
            code = "36" if row.startswith("›") else None
            term.line("  " + row, code)
        previous_lines = len(rows)
        previous_size = size

    try:
        if interactive:
            tty.setcbreak(sys.stdin.fileno())
        while True:
            draw()
            if interactive:
                key = os.read(sys.stdin.fileno(), 1).decode(errors="replace") or "q"
                if key == "\x1b":
                    # Arrow sequences can arrive in separate reads.
                    while len(key) < 3 and select.select([sys.stdin], [], [], 0.08)[0]:
                        key += os.read(sys.stdin.fileno(), 1).decode(errors="replace")
            else:
                value = sys.stdin.readline()
                key = value.strip() if value else "q"
            notice = ""
            if key in ("q", "m", "b", "\x04", "\x1b"):
                return ("q" if key in ("\x04", "\x1b") else key), tuple(sorted(chosen))
            if key == "n":
                return "no-models", tuple(sorted(chosen))
            if key in ("\x1b[A", "\x1b[B", "\x1bOA", "\x1bOB"):
                order = sorted(range(len(MODELS)), key=lambda i: (i not in downloaded, i))
                pos = order.index(cursor)
                cursor = order[max(0, min(len(order) - 1, pos + (-1 if key.endswith("A") else 1)))]
            elif key == " " or key in ("1", "2", "3"):
                index = int(key) - 1 if key.isdigit() else cursor
                chosen.symmetric_difference_update({index})
            elif key in ("", "\r", "\n"):
                if chosen:
                    return "confirm", tuple(sorted(chosen))
                notice = "Select at least one model to continue."
    finally:
        if original is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, original)
