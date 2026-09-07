#!/bin/bash
# Build only a disposable fixture executable. Never install/sign it as a provider.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUTPUT=${1:?Usage: build-session-fixture.sh /temporary/output/path}
SOURCE="$ROOT/provider-swift/Sources/darkbloom/OnboardingSession"
swiftc -parse-as-library \
  "$SOURCE/OnboardingContract.swift" "$SOURCE/OnboardingWorkflow.swift" \
  "$SOURCE/OnboardingPipe.swift" "$SOURCE/OnboardingSessionHost.swift" \
  "$ROOT/scripts/onboarding/session-fixture.swift" -o "$OUTPUT"
