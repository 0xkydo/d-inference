#!/usr/bin/env python3
"""Real Swift workflow/private-pipe and Bubble Tea PTY tests with injected services.

All state is disposable. Never invokes the real installer, enrollment/login,
launchd, provider start, or trust operations. No reset of host state is needed.
"""
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import signal
import struct
import subprocess
import tempfile
import termios
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
BUILD = None
BACKEND = None
FRONTEND = None


def read_line(pipe, timeout=5):
    data = b""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if select.select([pipe], [], [], .05)[0]:
            char = os.read(pipe.fileno(), 1)
            if not char:
                raise AssertionError("session exited before event: " + data.decode(errors="replace"))
            data += char
            if char == b"\n":
                return json.loads(data)
    raise AssertionError("timed out waiting for session event")


class ContractClient:
    """A non-Bubble-Tea client using the same production Swift session host."""
    def __init__(self, root, delay=80, progress_bytes=19):
        self.child = subprocess.Popen([str(BACKEND), "onboarding-session"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=dict(os.environ, DARKBLOOM_SESSION_FIXTURE_ROOT=str(root), FIXTURE_DOWNLOAD_MS=str(delay), FIXTURE_PROGRESS_BYTES=str(progress_bytes)))
        self.revision, self.next_id = None, 0
        self.root = root

    def send(self, action, ids=None, revision=None):
        self.next_id += 1
        command = dict(version=1, id=self.next_id, action=action)
        if self.revision is not None:
            command["revision"] = self.revision if revision is None else revision
        if ids is not None:
            command["modelIDs"] = ids
        self.child.stdin.write(json.dumps(command).encode() + b"\n")
        self.child.stdin.flush()

    def event(self):
        event = read_line(self.child.stdout)
        if event["kind"] == "snapshot":
            self.revision = event["snapshot"]["revision"]
        return event

    def phase(self, phase):
        for _ in range(30):
            event = self.event()
            if event["kind"] == "error":
                raise AssertionError(event)
            if event.get("snapshot", {}).get("phase") == phase:
                return event["snapshot"]
        raise AssertionError("phase not reached: " + phase)

    def models(self):
        self.send("hello")
        phase = self.event()["snapshot"]["phase"]
        if phase == "enrollment":
            self.send("enroll"); self.phase("enrollment_pending")
            self.send("refresh"); self.phase("account")
            self.send("link"); self.phase("models")
        else:
            assert phase == "models"

    def close(self):
        if not self.child.stdin.closed:
            self.child.stdin.close()
        self.child.wait(timeout=5)
        self.child.stdout.close(); self.child.stderr.close()


class FixtureTestCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="darkbloom-session-test-")
        self.root = Path(self.temp.name)
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            if client.child.poll() is None:
                client.child.kill()
            client.child.wait()
            for stream in [client.child.stdin, client.child.stdout, client.child.stderr]:
                stream.close()
        pid_file = self.root / "provider-pid"
        if pid_file.exists():
            try: os.kill(int(pid_file.read_text()), signal.SIGTERM)
            except ProcessLookupError: pass
        self.temp.cleanup()

    def client(self, delay=80, progress_bytes=19):
        client = ContractClient(self.root, delay, progress_bytes)
        self.clients.append(client)
        return client

    def actions(self):
        path = self.root / "actions"
        return path.read_text() if path.exists() else ""

class SessionTests(FixtureTestCase):
    def test_real_workflow_explicit_start_and_independent_lifetime(self):
        c = self.client(); c.models()
        c.send("select_models", ["small", "large"]); c.phase("ready")
        self.assertNotIn("start:", self.actions())
        c.send("start", revision=0)
        self.assertEqual(c.event()["error"], "stale_command")
        c.phase("ready")
        c.send("start"); c.phase("started")
        self.assertIn("start:small\n", self.actions())
        self.assertNotIn("start:small,large", self.actions())
        c.close()
        os.kill(int((self.root / "provider-pid").read_text()), 0)

    def test_eof_cancels_awaits_and_reuses_partial_files(self):
        c = self.client(delay=60000); c.models()
        c.send("select_models", ["small"]); c.phase("downloading")
        while "download:small" not in self.actions(): time.sleep(.01)
        start = time.monotonic(); c.close()
        self.assertLess(time.monotonic()-start, 3)
        self.assertIn("cancelled:small", self.actions())
        self.assertNotIn("start:", self.actions())
        self.assertTrue((self.root / "small.part").exists())
        c = self.client(); c.models()
        c.send("select_models", ["small"]); c.phase("ready"); c.close()
        self.assertIn("resume:small", self.actions())
        c = self.client(); c.models()
        c.send("select_models", ["small"]); c.phase("ready"); c.close()
        self.assertIn("reuse:small", self.actions())

    def test_saturated_event_reader_does_not_block_cancellation(self):
        c = self.client(delay=60000, progress_bytes=200000)
        c.models(); c.send("select_models", ["small"]); c.phase("downloading")
        time.sleep(.1)  # writer is blocked partway through a large event
        started = time.monotonic(); c.close()
        self.assertLess(time.monotonic()-started, 3)
        self.assertIn("cancelled:small", self.actions())
        self.assertNotIn("start:", self.actions())

    def test_partial_frame_and_cancel_do_not_start(self):
        c = self.client(); c.models(); c.send("select_models", ["small"]); c.phase("ready")
        c.child.stdin.write(b'{"version":1,"id":99,"action":"start"'); c.child.stdin.flush()
        c.close(); self.assertNotIn("start:", self.actions())
        c = self.client(); c.models(); c.send("cancel"); c.close()
        self.assertNotIn("start:", self.actions())

    def test_malformed_unknown_oversized_and_version_mismatch(self):
        for frame, code in [(b'{}', 'invalid_command'),
            (b'{"version":1,"id":1,"action":"decrypt"}', 'invalid_command'),
            (b'{"version":2,"id":1,"action":"hello"}', 'version_mismatch'),
            (b'x'*17000, 'invalid_command')]:
            c = self.client()
            try:
                c.child.stdin.write(frame+b'\n'); c.child.stdin.flush()
            except BrokenPipeError:
                pass
            self.assertEqual(c.event()["error"], code)
            c.close()
        self.assertNotIn("enroll\n", self.actions())


class TerminalTests(FixtureTestCase):
    def run_terminal(self, mode):
        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 28, 88, 0, 0))
        before = termios.tcgetattr(slave)
        def controlling_terminal():
            os.setsid(); fcntl.ioctl(0, termios.TIOCSCTTY, 0)
        # Keep the controlling terminal's session leader alive until terminal
        # modes have been sampled. macOS revokes slave ioctls on leader exit.
        supervisor = (
            "import json,os,subprocess,sys,termios; "
            "before=termios.tcgetattr(0); result=subprocess.run(sys.argv[1:]); "
            "after=termios.tcgetattr(0); "
            "before[3] &= ~termios.PENDIN; after[3] &= ~termios.PENDIN; "
            "open(os.environ['TERMINAL_RESULT'],'w').write(json.dumps(dict(restored=before==after,before=repr(before),after=repr(after)))); "
            "sys.exit(result.returncode)"
        )
        child = subprocess.Popen([os.sys.executable, '-c', supervisor, str(FRONTEND), '--backend', str(BACKEND)], stdin=slave, stdout=slave, stderr=slave,
            preexec_fn=controlling_terminal,
            env=dict(os.environ, TERMINAL_RESULT=str(self.root/'terminal-restored'), TERM='xterm-256color', NO_COLOR='1', DARKBLOOM_SESSION_FIXTURE_ROOT=str(self.root),
                     FIXTURE_DOWNLOAD_MS='60000' if mode == 'cancel' else '80'))
        transcript = bytearray()
        def wait(text):
            deadline = time.monotonic()+8
            while time.monotonic() < deadline:
                if text.encode() in transcript:
                    transcript.clear(); return
                if select.select([master], [], [], .05)[0]:
                    transcript.extend(os.read(master, 65536))
            raise AssertionError(f'missing {text!r}: {transcript.decode(errors="replace")}')
        try:
            wait('1. Enroll this Mac')
            self.assertNotIn('enroll\n', self.actions())
            os.write(master, b'\r'); wait('Complete enrollment')
            if mode == 'auto':
                # Fixture enrollment is now approved. No terminal key may be
                # required to notice it, and login must remain behind Enter.
                wait('Link your account')
            else:
                os.write(master, b'\r'); wait('Link your account')
            self.assertNotIn('link\n', self.actions())
            os.write(master, b'\r'); wait('Choose models')
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 16, 42, 0, 0))
            os.killpg(child.pid, signal.SIGWINCH)
            os.write(master, b' '); os.write(master, b'\r')
            if mode == 'cancel':
                wait('Download and verify'); os.write(master, b'\x03')
            else:
                wait('Ready to start')
                self.assertNotIn('start:', self.actions())
                if mode == 'eof':
                    os.close(master); master = None
                else:
                    os.write(master, b'q')
            deadline = time.monotonic()+8
            while child.poll() is None and time.monotonic() < deadline:
                if master is not None and select.select([master], [], [], .05)[0]:
                    os.read(master, 65536)  # drain terminal output during restoration
                else:
                    time.sleep(.01)
            child.wait(timeout=1)
            if master is not None:
                self.assertEqual(child.returncode, 0)
                restored = json.loads((self.root/'terminal-restored').read_text())
                self.assertTrue(restored['restored'], restored)
            else:
                self.assertIn(child.returncode, [0, -signal.SIGHUP])
                deadline = time.monotonic()+5
                while 'session-exit' not in self.actions() and time.monotonic() < deadline: time.sleep(.01)
            self.assertNotIn('start:', self.actions())
            self.assertIn('session-exit', self.actions())
            if mode == 'cancel': self.assertIn('cancelled:small', self.actions())
        finally:
            if child.poll() is None: child.kill()
            if master is not None: os.close(master)
            os.close(slave)
            child.wait(timeout=3)

    def test_pty_resize_and_quit_restore_terminal(self): self.run_terminal('quit')
    def test_pty_ctrl_c_cancels_download(self): self.run_terminal('cancel')
    def test_pty_hangup_closes_session(self): self.run_terminal('eof')
    def test_pty_approval_advances_without_opening_login(self): self.run_terminal('auto')


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='darkbloom-session-build-') as build:
        BUILD = Path(build); BACKEND = BUILD / 'fixture'; FRONTEND = BUILD / 'darkbloom-tui'
        subprocess.run(['bash', str(ROOT/'scripts/onboarding/build-session-fixture.sh'), str(BACKEND)], check=True)
        go = os.environ.get('GO_BIN', 'go')
        subprocess.run([go, '-C', str(ROOT/'provider-tui'), 'build', '-o', str(FRONTEND), '.'], check=True)
        unittest.main()
