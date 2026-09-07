#!/usr/bin/env python3
"""Verify a downloaded workflow candidate and write local installer metadata."""
import argparse
import hashlib
import json
from pathlib import Path
import re


def prepare(directory, expected_commit):
    record = json.loads((directory / "darkbloom-qualification-manifest.json").read_text())
    if record.get("schema_version") != 1 or record.get("publish_release") is not False:
        raise ValueError("Expected an unpublished workflow candidate")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_commit) or record["source"]["sha"] != expected_commit:
        raise ValueError("Candidate source commit does not match the requested checkout")
    if record["source"]["repository"] != "Layr-Labs/d-inference":
        raise ValueError("Unexpected candidate repository")
    if record["notarization"]["status"] != "Accepted":
        raise ValueError("Candidate did not pass notarization")
    archive_name = "darkbloom-bundle-macos-arm64.tar.gz"
    entry = next(item for item in record["archives"] if item["name"] == archive_name)
    archive = directory / archive_name
    if archive.is_symlink() or not archive.is_file() or archive.stat().st_size != entry["size_bytes"]:
        raise ValueError("Missing regular candidate archive or size mismatch")
    digest = hashlib.sha256()
    with archive.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != entry["sha256"]:
        raise ValueError("Candidate archive hash mismatch")
    for key in ["binary_sha256", "metallib_sha256"]:
        if not re.fullmatch(r"[0-9a-f]{64}", record[key]):
            raise ValueError(f"Invalid {key}")
    return dict(version=record["version"], backend="mlx-swift", url=archive.resolve().as_uri(),
                bundle_hash=digest.hexdigest(), binary_hash=record["binary_sha256"],
                metallib_hash=record["metallib_sha256"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    metadata = prepare(args.directory, args.commit)
    target = args.directory.resolve() / "local-release.json"
    target.write_text(json.dumps(metadata, separators=(",", ":")) + "\n")
    print(target)


if __name__ == "__main__":
    main()
