#!/usr/bin/env python3
"""Exercise the installer's companion contract using disposable files only."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SOURCE = (ROOT / 'scripts/install.sh').read_text()
FUNCTION = SOURCE[SOURCE.index('verify_onboarding_companion() {'):SOURCE.index('verify_staged_app() {')]


class CompanionPackageTests(unittest.TestCase):
    def check(self, variant):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory)
            binary = app / 'Contents/MacOS/darkbloom'
            helper = app / 'Contents/MacOS/darkbloom-tui'
            marker = app / 'Contents/Resources/darkbloom-runtime-capabilities/onboarding-session-v1'
            binary.parent.mkdir(parents=True); marker.parent.mkdir(parents=True)
            binary.write_text('old' if variant in ['old', 'missing-code'] else 'darkbloom-onboarding-session-v1')
            if variant != 'old':
                marker.write_text('2' if variant == 'wrong-marker' else '1\n')
                helper.write_text('fixture companion'); helper.chmod(0o755)
            if variant == 'missing-helper': helper.unlink()
            if variant == 'missing-marker': marker.unlink()
            if variant == 'mode': helper.chmod(0o777)
            if variant == 'symlink': helper.unlink(); helper.symlink_to(binary)
            # Stub only the expensive signing syscall in this isolated harness.
            # The production verifier's pinned requirement is asserted exactly.
            script = '''set -eu
fail_install() { echo "$*" >&2; return 1; }
verify_code_requirement() {
  [ "$2" = 0 ] && [ "$3" = 'anchor apple generic and identifier "io.darkbloom.onboarding" and certificate leaf[subject.OU] = "SLDQ2GJ6TL"' ] || return 1
  [ "$REJECT_SIGNATURE" = 0 ]
}
''' + FUNCTION + '\nverify_onboarding_companion "$1"\n'
            return subprocess.run(['bash', '-c', script, 'fixture', str(app)], capture_output=True,
                env=dict(os.environ, REJECT_SIGNATURE='1' if variant == 'signature' else '0'))

    def test_complete_and_older_bundles(self):
        for variant in ['complete', 'old']:
            result = self.check(variant)
            self.assertEqual(result.returncode, 0, result.stderr.decode())

    def test_missing_malformed_unsafe_or_untrusted_companion_rejected(self):
        for variant in ['missing-code', 'missing-helper', 'missing-marker', 'wrong-marker', 'mode', 'symlink', 'signature']:
            self.assertNotEqual(self.check(variant).returncode, 0, variant)


if __name__ == '__main__': unittest.main()
