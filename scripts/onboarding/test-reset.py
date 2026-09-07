#!/usr/bin/env python3
"""Exercise reset file operations only inside isolated fixtures, never on host state."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("reset.sh").resolve()


class ResetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="darkbloom-reset-test-")
        self.root = Path(self.temp.name).resolve()

    def tearDown(self):
        self.temp.cleanup()

    def run_shell(self, code, *arguments):
        # HOME is untouched. Sourcing exposes helpers without running main.
        env = dict(os.environ, RESET_TEST_ROOT=str(self.root), RESET_TEST_SCRIPT=str(SCRIPT))
        return subprocess.run(
            ["/bin/bash", "-c", 'source "$RESET_TEST_SCRIPT"; reset_home=$RESET_TEST_ROOT; ' + code,
             "reset-test", *arguments], env=env, text=True, capture_output=True)

    def test_exact_model_directory_only(self):
        wanted = self.root / ".cache/huggingface/hub/models--org--model"
        unrelated = wanted.with_name("models--someone--else")
        for path in [wanted, unrelated]:
            path.mkdir(parents=True)
            (path / "weights").write_text("fixture")
        result = self.run_shell('reset_remove_path "$(reset_model_path "$1")"', "org/model")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(wanted.exists())
        self.assertTrue(unrelated.exists())

    def test_model_ids_cannot_escape(self):
        for model in ["../model", "org/../../victim", "/tmp/model", "org/model;rm -rf ~", "org/\nmodel"]:
            result = self.run_shell('reset_model_path "$1"', model)
            self.assertNotEqual(result.returncode, 0, model)

    def test_full_reset_inventory_then_removal_in_disposable_home(self):
        remove = [".cache/huggingface/hub/models--org--model/snapshots/v1/weights",
                  ".cache/huggingface/hub/models--short-id/snapshots/.staging/weights.part",
                  ".cache/huggingface/hub/.locks/models--org--model/file.lock",
                  ".cache/huggingface/hub/.darkbloom-locks/file.lock",
                  "Downloads/darkbloom-cli-candidate-123/bundle.tar.gz",
                  "Downloads/Darkbloom-Monitor-v1.dmg"]
        keep = [".cache/huggingface/token", ".cache/huggingface/hub/datasets--org--data/data",
                "Downloads/personal.txt", "source/provider-swift/.build/debug/darkbloom",
                ".local/share/darkbloom-onboarding-reset/Darkbloom.app/helper"]
        for name in remove + keep:
            path = self.root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text("fixture")
        plan = self.run_shell('reset_full_paths')
        self.assertEqual(plan.returncode, 0, plan.stderr)
        self.assertTrue(all((self.root / name).exists() for name in remove + keep))
        result = self.run_shell('plan=$(reset_full_paths); while IFS= read -r path; do reset_remove_path "$path"; done <<< "$plan"')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(all(not (self.root / name).exists() for name in remove))
        self.assertTrue(all((self.root / name).exists() for name in keep))

    def test_full_reset_rejects_linked_cache_parent(self):
        outside = self.root / "unrelated"; outside.mkdir()
        (outside / "models--keep").mkdir()
        cache = self.root / ".cache/huggingface"; cache.mkdir(parents=True)
        (cache / "hub").symlink_to(outside)
        result = self.run_shell('reset_full_paths')
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue((outside / "models--keep").exists())

    def test_symlink_parent_refused_and_leaf_only_unlinked(self):
        outside = self.root / "unrelated"
        outside.mkdir()
        sentinel = outside / "model"
        sentinel.write_text("keep")
        cache = self.root / ".cache"
        cache.symlink_to(outside)
        result = self.run_shell('reset_remove_path "$reset_home/.cache/model"')
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(sentinel.exists())
        result = self.run_shell('reset_remove_path "$reset_home/.cache"')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(cache.is_symlink())
        self.assertTrue(sentinel.exists())

    def test_shell_edits_preserve_custom_lines_and_backup(self):
        rc = self.root / ".zshrc"
        custom = '# My Darkbloom tools\nexport PATH="$HOME/custom:$PATH"\nalias db="darkbloom status"\n'
        original = '# Darkbloom\nexport PATH="$HOME/.darkbloom/bin:$PATH"\n' + custom
        rc.write_text(original)
        result = self.run_shell('reset_shell_file "$reset_home/.zshrc"')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(rc.read_text(), custom)
        backups = list(self.root.glob(".zshrc.before-darkbloom-reset.*"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_text(), original)
        again = self.run_shell('reset_shell_file "$reset_home/.zshrc"')
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(len(list(self.root.glob(".zshrc.before-darkbloom-reset.*"))), 1)

    def test_shell_symlink_refused(self):
        target = self.root / "shared-rc"
        target.write_text('# Darkbloom\n')
        (self.root / ".zshrc").symlink_to(target)
        result = self.run_shell('reset_shell_file "$reset_home/.zshrc"')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(target.read_text(), '# Darkbloom\n')


if __name__ == "__main__":
    unittest.main()
