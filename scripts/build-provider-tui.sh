#!/bin/bash
# Build the opt-in companion beside a Swift build; no installation or signing.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GO_BIN=${GO_BIN:-go}
DEST=${1:-$(swift build --package-path "$ROOT/provider-swift" --show-bin-path)}
mkdir -p "$DEST"
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 GOTOOLCHAIN=local \
  "$GO_BIN" -C "$ROOT/provider-tui" build -trimpath -ldflags='-s -w' -o "$DEST/darkbloom-tui" .
printf 'Built %s/darkbloom-tui\n' "$DEST"
