"""Behavior checks for the design mock, never the real installer."""

import os
from pathlib import Path
import pty
import select
import signal
import subprocess
import sys
import termios
import unittest

from scenes import SCENARIOS, SCENES

RUNNER = str(Path(__file__).with_name("playground.py"))


def run(scene, commands, width=80):
    result = subprocess.run(
        [sys.executable, RUNNER, "--instant", "--scene", scene],
        input=commands, text=True, capture_output=True, timeout=5,
        env=dict(os.environ, COLUMNS=str(width)), check=True,
    )
    return result.stdout


class PlaygroundTests(unittest.TestCase):
    def test_every_declared_transition(self):
        for key, scene in SCENES.items():
            transitions = [(f"e{i}\n", destination) for i, (_, destination) in enumerate(scene.events, 1)]
            if scene.next:
                transitions.append(("\n", scene.next))
            if key == "models":
                transitions = [("n\n", "no-models"), ("\n", "start"), ("2\n\n", "model-download")]
            for command, destination in transitions:
                with self.subTest(source=key, destination=destination):
                    self.assertIn(destination, SCENES)
                    output = run(key, command + "q\n")
                    self.assertIn(SCENES[destination].title, output)

    def test_fresh_install_multiselect_and_start_gate(self):
        # Enroll, authorize, select all models, interrupt/resume, then stop at
        # the explicit startup prompt. A fresh install has no downloaded model.
        commands = "\n\n\ne1\n\n\ne1\n\n2\n3\n\ne1\n\n\nq\n"
        output = run("install", commands)
        self.assertIn("3 selected · 24 GB to download", output)
        self.assertIn("Download paused", output)
        self.assertIn("Ready to start Darkbloom", output)
        self.assertNotIn("Connected · verifying", output)
        output = run("install", commands[:-2] + "\ne1\ne1\nq\n")
        self.assertIn("Ready to receive requests", output)

    def test_retry_code_and_download_fixture(self):
        output = run("authorize", "e2\n\nq\n")
        self.assertIn("DEMO-0001", output)
        self.assertIn("DEMO-0002", output)
        output = run("download-paused", "\n\nq\n")
        self.assertIn("To download: Larger text", output)
        self.assertIn("Ready to start Darkbloom", output)

    def test_empty_selection_and_back(self):
        output = run("models", "1\n\n2\n\nb\nq\n")
        self.assertIn("Select at least one model", output)
        self.assertGreaterEqual(output.count("1 selected · 8 GB to download"), 2)

    def test_update_lifecycle(self):
        output = run("update", "\n\nq\n")
        self.assertIn("Your provider is still stopped", output)
        self.assertNotIn("Link your account", output)
        output = run("update-running", "\n\ne1\ne2\ne1\nq\n")
        self.assertIn("Finishing active requests", output)
        self.assertIn("previous version was restored", output)
        self.assertIn("Ready to receive requests", output)

    def test_all_scenarios_exit_and_all_screens_wrap(self):
        for _, key in SCENARIOS:
            self.assertIn("Preview closed", run(key, "q\n"))
        for width in (40, 80, 120):
            output = subprocess.check_output(
                [sys.executable, RUNNER, "--render-all"], text=True,
                env=dict(os.environ, COLUMNS=str(width)), timeout=5)
            self.assertNotIn("\x1b", output)
            self.assertTrue(all(len(line) <= width for line in output.splitlines()))

    def test_pty_arrows_and_terminal_restoration(self):
        for cancel in (False, True):
            master, slave = pty.openpty()
            original = termios.tcgetattr(slave)
            process = subprocess.Popen(
                [sys.executable, RUNNER, "--scene", "models", "--instant"],
                stdin=slave, stdout=slave, stderr=slave,
                preexec_fn=lambda: signal.signal(signal.SIGINT, signal.SIG_DFL),
                env=dict(os.environ, TERM="xterm-256color"))
            output = b""
            def receive(marker):
                nonlocal output
                while marker not in output:
                    if not select.select([master], [], [], 5)[0]:
                        self.fail("Timed out waiting for picker output")
                    chunk = os.read(master, 65536)
                    if not chunk:
                        self.fail("PTY closed before expected screen")
                    output += chunk
            try:
                receive(b"Mock: n")
                if cancel:
                    process.send_signal(signal.SIGINT)
                else:
                    os.write(master, b"\x1b[B \x1b[B \n")
                    receive(b"Connection lost at 40%")
                    os.write(master, b"q\n")
                receive(b"Preview closed")
                process.wait(timeout=5)
                restored = termios.tcgetattr(slave)
                # macOS may set PENDIN after queued input; it is not a mode
                # configured by the picker. Compare all configurable settings.
                restored[3] &= ~getattr(termios, "PENDIN", 0)
                original[3] &= ~getattr(termios, "PENDIN", 0)
                self.assertEqual(restored, original)
                if not cancel:
                    self.assertIn(b"3 selected", output)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                os.close(master)
                os.close(slave)


if __name__ == "__main__":
    unittest.main()
