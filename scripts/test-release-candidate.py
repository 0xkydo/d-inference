#!/usr/bin/env python3
"""Regression checks for the nonpublishing signed-candidate workflow."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("manifest", ROOT / "scripts/release-qualification-manifest.py")
manifest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(manifest)
prepare_spec = importlib.util.spec_from_file_location("prepare", ROOT / "scripts/onboarding/prepare-candidate.py")
prepare_module = importlib.util.module_from_spec(prepare_spec)
prepare_spec.loader.exec_module(prepare_module)
VERSION = re.search(r'public static let version = "([^"]+)"',
                    (ROOT / "provider-swift/Sources/ProviderCore/ProviderCore.swift").read_text())[1]


class ReleaseCandidateTests(unittest.TestCase):
    def resolve(self, **overrides):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            env = dict(os.environ, EVENT_NAME="workflow_dispatch", INPUT_ENVIRONMENT="dev",
                       INPUT_PUBLISH_RELEASE="false", INPUT_VERSION_OVERRIDE="",
                       REF_NAME="codex/candidate", REF_TYPE="branch", GITHUB_OUTPUT=str(output))
            env.update(overrides)
            result = subprocess.run(["bash", "scripts/resolve-provider-release.sh"], cwd=ROOT,
                                    env=env, capture_output=True, text=True)
            return result, output.read_text() if output.exists() else ""

    def test_nonpublishing_mode_and_historical_publication(self):
        result, output = self.resolve()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("publish_release=false\n", output)
        for inputs in [dict(INPUT_PUBLISH_RELEASE="true"), dict(INPUT_PUBLISH_RELEASE=""),
                       dict(EVENT_NAME="push", REF_NAME=f"v{VERSION}", REF_TYPE="tag")]:
            result, output = self.resolve(**inputs)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("publish_release=true\n", output)

    def test_invalid_mode_or_version_emits_no_job_outputs(self):
        for inputs in [dict(INPUT_ENVIRONMENT="prod"), dict(INPUT_ENVIRONMENT="unexpected"),
                       dict(INPUT_PUBLISH_RELEASE="maybe"), dict(INPUT_VERSION_OVERRIDE="9.9.99999"),
                       dict(INPUT_VERSION_OVERRIDE=VERSION + "\nenvironment=prod"),
                       dict(INPUT_ENVIRONMENT="prod", INPUT_PUBLISH_RELEASE="true")]:
            result, output = self.resolve(**inputs)
            self.assertNotEqual(result.returncode, 0, inputs)
            self.assertEqual(output, "", inputs)

    def test_all_remote_release_writes_are_guarded(self):
        workflow = (ROOT / ".github/workflows/release-swift.yml").read_text()
        for step in ["Resolve env-specific secrets", "Install awscli (R2)",
                     "Upload bundle to R2", "Register release with coordinator", "Create GitHub Release"]:
            block = workflow.split("      - name: " + step + "\n", 1)[1].split("      - name:", 1)[0]
            self.assertIn("if: needs.resolve-env.outputs.publish_release == 'true'", block)
        upload = workflow.split("      - name: Retain qualified dev artifacts\n", 1)[1].split("      - name:", 1)[0]
        self.assertIn("needs.resolve-env.outputs.publish_release == 'false'", upload)
        self.assertIn("if-no-files-found: error", upload)
        paths = upload.split("          path: |\n", 1)[1].split("          if-no-files-found", 1)[0]
        self.assertEqual(paths.split(), ["/tmp/darkbloom-bundle-macos-arm64.tar.gz",
                                       "/tmp/darkbloom-qualification-manifest.json"])

    def fixture(self, root):
        archive = root / "darkbloom-bundle-macos-arm64.tar.gz"
        archive.write_bytes(b"signed archive fixture")
        env = dict(GITHUB_EVENT_NAME="workflow_dispatch", ENV_PREFIX="dev", PUBLISH_RELEASE="false",
                   VERSION=VERSION, BUNDLE_HASH=hashlib.sha256(archive.read_bytes()).hexdigest(),
                   BINARY_HASH="a" * 64, METALLIB_HASH="b" * 64,
                   APPLE_APP_PASSWORD="must-not-be-exported")
        for name in ["REPOSITORY", "SHA", "REF", "WORKFLOW_REF", "WORKFLOW_SHA", "RUN_ID", "RUN_ATTEMPT"]:
            env["GITHUB_" + name] = "fixture"
        notary = dict(status="Accepted", id="11111111-1111-1111-1111-111111111111",
                      account="must-not-be-exported")
        return archive, env, notary

    def test_manifest_contains_only_public_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, env, notary = self.fixture(root)
            result = manifest.qualification_manifest(root, notary, env)
            self.assertFalse(result["publish_release"])
            self.assertEqual(result["archives"][0]["sha256"], env["BUNDLE_HASH"])
            self.assertNotIn("must-not-be-exported", json.dumps(result))

    def test_manifest_rejects_changed_bytes_rejected_notary_and_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, env, notary = self.fixture(root)
            for bad_notary, bad_env in [(dict(notary, status="Invalid"), env),
                                        (notary, dict(env, PUBLISH_RELEASE="true"))]:
                with self.assertRaises(ValueError):
                    manifest.qualification_manifest(root, bad_notary, bad_env)
            archive.write_bytes(b"changed after qualification")
            with self.assertRaises(ValueError):
                manifest.qualification_manifest(root, notary, env)

    def test_preparation_pins_source_and_archive_before_writing_install_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive, env, notary = self.fixture(root)
            env.update(GITHUB_SHA="c" * 40, GITHUB_REPOSITORY="Layr-Labs/d-inference")
            record = manifest.qualification_manifest(root, notary, env)
            (root / "darkbloom-qualification-manifest.json").write_text(json.dumps(record))
            result = prepare_module.prepare(root, "c" * 40)
            self.assertEqual(result["url"], archive.resolve().as_uri())
            self.assertEqual(result["binary_hash"], env["BINARY_HASH"])
            with self.assertRaises(ValueError):
                prepare_module.prepare(root, "d" * 40)
            archive.write_bytes(b"corrupted")
            with self.assertRaises(ValueError):
                prepare_module.prepare(root, "c" * 40)

    def test_fork_candidate_requires_explicit_repository_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, env, notary = self.fixture(root)
            env.update(GITHUB_SHA="c" * 40, GITHUB_REPOSITORY="0xkydo/d-inference")
            record = manifest.qualification_manifest(root, notary, env)
            (root / "darkbloom-qualification-manifest.json").write_text(json.dumps(record))
            with self.assertRaises(ValueError):
                prepare_module.prepare(root, "c" * 40)
            result = prepare_module.prepare(root, "c" * 40, "0xkydo/d-inference")
            self.assertEqual(result["binary_hash"], env["BINARY_HASH"])

    def test_local_metadata_avoids_release_server_and_preserves_install_only(self):
        installer = (ROOT / "scripts/install.sh").read_text()
        functions = installer[installer.index("parse_install_options() {"):installer.index("verify_file_hash() {")]
        script = ('set -eu\n' + functions + '\n'
                  'curl() { echo "unexpected network"; return 9; }\n'
                  'parse_install_options "$@"\nprintf "%s\\n" "$INSTALL_ONLY"\nread_release_metadata')
        with tempfile.TemporaryDirectory() as directory:
            metadata = Path(directory) / "release with spaces.json"
            metadata.write_text('{"version":"fixture"}')
            for arguments in [["--release-file", str(metadata)],
                              ["--install-only", "--release-file", str(metadata)]]:
                result = subprocess.run(["bash", "-c", script, "test"] + arguments,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertNotIn("unexpected network", result.stdout)
                self.assertIn(metadata.read_text(), result.stdout)
                self.assertTrue(result.stdout.startswith("true\n" if "--install-only" in arguments else "false\n"))
            for arguments in [["--release-file"], ["--release-file", str(metadata) + "missing"],
                              ["--install-only", "typo"]]:
                result = subprocess.run(["bash", "-c", script, "test"] + arguments,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 64, result.stderr)


if __name__ == "__main__":
    unittest.main()
