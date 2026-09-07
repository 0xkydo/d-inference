#!/usr/bin/env python3
"""Exercise the installer's real handoff with a fake CLI and disposable paths."""
import fcntl
import os
from pathlib import Path
import pty
import select
import shlex
import subprocess
import tempfile
import termios
import time
import unittest

SOURCE = (Path(__file__).resolve().parent / 'install.sh').read_text()
FUNCTION = SOURCE[SOURCE.index('finish_installation() {'):SOURCE.index('if [ "${1:-}" = "--verify-staged-app-signature-test" ]')]


class InstallerHandoffTests(unittest.TestCase):
    def run_handoff(self, *, terminal=False, pending=True, install_only=False, supported=True, fail=False, tui=False):
        with tempfile.TemporaryDirectory(prefix='darkbloom-handoff-') as root:
            directory = Path(root)
            cli = directory / 'darkbloom'
            log = directory / 'calls'
            marker = directory / 'pending'
            if pending:
                marker.touch()
            cli.write_text('#!/bin/bash\n' +
                'if [ "${2:-}" = "--help" ]; then\n' +
                ('  echo "--onboarding --tui"\n' if supported else '  echo "legacy CLI"\n') +
                '  exit 0\nfi\n' +
                'printf "%s\\n" "$*" >> ' + shlex.quote(str(log)) + '\n' +
                '[ -t 0 ] || exit 95\n' +
                'echo "fake CLI awaiting Enter"\nread -r answer\n' +
                ('exit 7\n' if fail else 'exit 0\n'))
            cli.chmod(0o755)
            script = 'set -eu\n' + FUNCTION
            for key, value in dict(BIN_DIR=root, ONBOARDING_PENDING=str(marker),
                                   COORD_URL='https://example.test',
                                   ONBOARDING_TUI='true' if tui else 'false',
                                   INSTALL_ONLY='true' if install_only else 'false').items():
                script += key + '=' + shlex.quote(value) + '\n'
            script += 'finish_installation\n'
            if not terminal:
                result = subprocess.run(['bash'], input=script, text=True, capture_output=True, timeout=5)
                output, code = result.stdout, result.returncode
            else:
                master, slave = pty.openpty()
                def control_terminal():
                    os.setsid()
                    fcntl.ioctl(1, termios.TIOCSCTTY, 0)
                child = subprocess.Popen(['bash'], stdin=subprocess.PIPE, stdout=slave, stderr=slave,
                                         preexec_fn=control_terminal)
                os.close(slave)
                child.stdin.write(script.encode())
                child.stdin.close()
                output_bytes = b''
                deadline = time.monotonic() + 5
                answered = False
                try:
                    while time.monotonic() < deadline:
                        if select.select([master], [], [], .1)[0]:
                            try:
                                data = os.read(master, 65536)
                            except OSError:
                                break
                            if not data:
                                break
                            output_bytes += data
                            if b'fake CLI awaiting Enter' in output_bytes and not answered:
                                os.write(master, b'\n')
                                answered = True
                        if child.poll() is not None:
                            break
                    code = child.wait(timeout=1)
                    output = output_bytes.decode()
                finally:
                    if child.poll() is None:
                        child.kill()
                        child.wait()
                    os.close(master)
            return output, code, log.read_text() if log.exists() else '', marker.exists()

    def test_curl_pipe_hands_terminal_to_cli(self):
        output, code, calls, marker = self.run_handoff(terminal=True)
        self.assertEqual(code, 0, output)
        self.assertIn('fake CLI awaiting Enter', output)
        self.assertEqual(calls.strip(), 'start --onboarding --coordinator-url https://example.test')

    def test_opt_in_tui_handoff(self):
        output, code, calls, _ = self.run_handoff(terminal=True, tui=True)
        self.assertEqual(code, 0, output)
        self.assertEqual(calls.strip(), 'start --onboarding --tui --coordinator-url https://example.test')

    def test_updates_and_install_only_never_start_onboarding(self):
        for arguments in [dict(terminal=True, pending=False), dict(terminal=True, install_only=True)]:
            output, code, calls, _ = self.run_handoff(**arguments)
            self.assertEqual(code, 0, output)
            self.assertEqual(calls, '')

    def test_unattended_install_prints_resume_without_reading_stdin(self):
        output, code, calls, marker = self.run_handoff()
        self.assertEqual(code, 0)
        self.assertEqual(calls, '')
        self.assertTrue(marker)
        self.assertIn('darkbloom start --coordinator-url https://example.test', output)

    def test_interrupted_onboarding_keeps_pending_and_reports_failure(self):
        output, code, calls, marker = self.run_handoff(terminal=True, fail=True)
        self.assertEqual(code, 1, output)
        self.assertTrue(marker)
        self.assertIn('Setup is unfinished', output)

    def test_older_cli_receives_manual_instructions(self):
        output, code, calls, marker = self.run_handoff(terminal=True, supported=False)
        self.assertEqual(code, 0, output)
        self.assertEqual(calls, '')
        self.assertTrue(marker)
        self.assertIn('This CLI release uses manual setup', output)


if __name__ == '__main__':
    unittest.main()
