#!/usr/bin/env python3
"""Output-only installer design study. Never executes the installer or uses network IO."""

import argparse
import os
from pathlib import Path
import shutil
import sys
import textwrap
import time

PROFILE_EXPLANATION = [
    "Device enrollment is a core part of Darkbloom's security. It lets "
    "us verify your Mac's identity and security settings to help protect "
    "the privacy of requests it serves.",
    "The MDM profile allows read-only checks of device information, "
    "installed configuration profiles, and security status such as "
    "Secure Boot and System Integrity Protection.",
    "It does not grant access to your personal files or control of your "
    "Mac. It cannot change your settings, install apps, lock, or erase "
    "your Mac.",
]

class Terminal:
    def __init__(self, instant=False):
        self.delay = 0 if instant else 0.7
        self.tty = sys.stdout.isatty()
        self.color = self.tty and "NO_COLOR" not in os.environ and os.environ.get("TERM") != "dumb"

    def style(self, value, code):
        return f"\033[{code}m{value}\033[0m" if self.color else value

    def line(self, value="", code=None):
        print(self.style(value, code) if code else value, flush=True)

    def stage(self, number, title):
        self.line()
        self.line(f"  {number}/4  {title}", "1")
        time.sleep(self.delay)

    def box(self, title, paragraphs):
        width = max(12, min(66, shutil.get_terminal_size().columns - 4))
        inner = width - 4
        self.line("  ┌" + "─" * (width - 2) + "┐")
        for index, paragraph in enumerate([title, *paragraphs]):
            if index:
                self.line("  │ " + " " * inner + " │")
            for line in textwrap.wrap(paragraph, width=inner):
                content = line.ljust(inner)
                if index == 0:
                    content = self.style(content, "1")
                self.line("  │ " + content + " │")
        self.line("  └" + "─" * (width - 2) + "┘")

    def progress(self, start=0, end=100):
        if not self.tty or os.environ.get("TERM") == "dumb":
            self.line(f"       Downloading... {end}%")
            return
        for percent in range(start, end + 1, 10):
            filled = percent // 5
            bar = "=" * filled + " " * (20 - filled)
            print(f"\r       [{bar}] {percent:3}%", end="", flush=True)
            time.sleep(self.delay / 3)
        print(flush=True)


def proposed(term, scenario):
    term.line("  Darkbloom", "1;36")
    term.line("  Install the CLI to share your Mac's compute.")
    term.line()
    term.line("  Apple M2 Pro · 32 GB memory · macOS 15.6", "2")
    term.stage(1, "Find the latest release")
    if scenario == "offline":
        term.line("       Could not fetch the latest release.", "1")
        term.line("       Check your connection, then run the install command again.")
        term.line("       Nothing was installed.")
        return
    term.line("       Version 0.8.10")
    term.stage(2, "Download and install")
    term.progress()
    term.line("       Checking download integrity and Apple code signature...")
    time.sleep(term.delay)
    if scenario == "verification-failed":
        term.line("       Download integrity check failed. Installation stopped.", "1")
        term.line("       Run the install command again to download a fresh copy.")
        term.line("       Your existing installation was not replaced.")
        return
    term.line("       Download and signature verified.")
    term.line("       Runtime checks passed.")
    term.line("       Installed in ~/.darkbloom")
    term.stage(3, "Check hardware identity")
    term.line("       Secure Enclave available.")
    term.stage(4, "Set up device verification")
    if scenario == "enrolled":
        term.line("       An MDM enrollment profile is already installed.")
        term.line("       Darkbloom enrollment still needs to be confirmed.")
    else:
        term.line("       The device enrollment profile is ready to install.")
    enrollment_handoff(term, scenario)


def enrollment_handoff(term, scenario):
    term.line()
    term.line("  CLI installation complete", "1")
    term.line()
    term.box("This profile is read-only", PROFILE_EXPLANATION)
    term.line()
    if scenario == "enrolled":
        # The current installer checks for ANY MDM enrollment, not Darkbloom's.
        term.line("  Check that the installed profile belongs to Darkbloom.", "1")
        term.line("    darkbloom doctor", "36")
        term.line('    Look for: mdm enrollment ... Darkbloom profile installed')
        term.line("    An enrollment with another MDM does not complete this step.")
    else:
        term.line("  Next: enroll this Mac in System Settings", "1")
        term.line("    1. Open System Settings > General > Device Management.")
        term.line("       If it is not there, search Settings for 'Profiles'.")
        term.line("    2. Open the Darkbloom profile and review its details.")
        term.line("    3. Click Install and follow the password/confirmation prompts.")
        term.line("       Wait until the profile appears as installed.")
        term.line()
        term.line("    If the profile is missing, reopen enrollment:")
        term.line("      darkbloom enroll")
    term.line()
    term.line("  After the Darkbloom profile is installed, return here and run:", "1")
    term.line("    darkbloom login", "36")
    term.line("    This links your Mac to your account.")
    term.line()
    term.line("  If the command is not found, open a new terminal window.", "2")


def current(term):
    # Curated happy-path transcript, not a dry run of the installer. The marker
    # replaces curl's dynamic progress bar; the other lines preserve current copy.
    transcript = Path(__file__).with_name("install-current.txt").read_text()
    for line in transcript.splitlines():
        if line == "[DOWNLOAD]":
            term.progress()
        else:
            term.line(line)
            if line.startswith("→"):
                time.sleep(term.delay)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("design", choices=["current", "proposed"], nargs="?", default="proposed")
    parser.add_argument("--scenario", choices=["pending", "enrolled", "offline", "verification-failed"], default="pending")
    parser.add_argument("--instant", action="store_true", help="Disable simulated delays")
    args = parser.parse_args()
    if args.design == "current" and args.scenario != "pending":
        parser.error("the current transcript only covers enrollment pending")
    term = Terminal(args.instant)
    term.line()
    term.line(f"  PREVIEW ONLY · {args.design} · {args.scenario}", "2")
    term.line("  Sample hardware/version and timing. No installation or system changes.", "2")
    term.line()
    if args.design == "current":
        current(term)
    else:
        proposed(term, args.scenario)
    term.line()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nPreview stopped.")
        sys.exit(130)
