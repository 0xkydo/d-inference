#!/bin/bash
# Resolve the release mode before exposing any job outputs.
set -euo pipefail
# Push tags always publish to prod. An absent manual input retains
# the historical publishing default; an explicit false must survive.
PUBLISH_RELEASE=true
case "$EVENT_NAME" in
  push) ENV=prod ;;
  workflow_dispatch)
    ENV="${INPUT_ENVIRONMENT:-prod}"
    PUBLISH_RELEASE="${INPUT_PUBLISH_RELEASE:-true}"
    ;;
  *) echo "::error::Unsupported release event"; exit 1 ;;
esac
case "$ENV" in
  dev|prod) ;;
  *) echo "::error::Unsupported release environment"; exit 1 ;;
esac
case "$PUBLISH_RELEASE" in
  true|false) ;;
  *) echo "::error::publish_release must be true or false"; exit 1 ;;
esac
if [ "$PUBLISH_RELEASE" = "false" ] && [ "$ENV" != "dev" ]; then
  echo "::error::Qualification-only runs require environment=dev"
  exit 1
fi
if [ "$REF_TYPE" = "tag" ] && [[ "$REF_NAME" == *-dev.* ]]; then
  echo "::error::-dev tags are unsupported by the exact-version release contract; use workflow_dispatch with environment=dev"
  exit 1
fi
if [ "$ENV" = "prod" ] && [ "$REF_TYPE" != "tag" ]; then
  echo "::error::Production publication requires a source-matching release tag"
  exit 1
fi
if [ -n "$INPUT_VERSION_OVERRIDE" ]; then
  VERSION="$INPUT_VERSION_OVERRIDE"
elif [ "$REF_TYPE" != "tag" ]; then
  VERSION=$(awk -F'\"' '/public static let version =/ { print $2 }' \
    provider-swift/Sources/ProviderCore/ProviderCore.swift)
else
  REF="${REF_NAME#v}"
  VERSION="${REF%-swift*}"
fi
# Validate before writing ANY output. A newline in an untrusted
# version must not inject a different environment or publication mode.
case "$VERSION" in
  *$'\n'*|*$'\r'*) echo "::error::Release version must be a single line"; exit 1 ;;
esac
bash scripts/check-release-version.sh "$VERSION" >/dev/null
{
  echo "environment=$ENV"
  echo "publish_release=$PUBLISH_RELEASE"
  echo "version=$VERSION"
} >> "$GITHUB_OUTPUT"
echo "Resolved env=$ENV version=$VERSION publish_release=$PUBLISH_RELEASE"
