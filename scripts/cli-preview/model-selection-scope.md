# Model-selection design scope

Reviewed against `bbf6f83d4` on 2026-09-06. This narrows the earlier research;
runtime memory and auth changes are outside this visual-design task.

## Existing ownership

`Start.launchDaemon` runs preflight and offers account linking, then calls
`interactiveCatalogPicker`. The picker fetches the catalog with aliases, filters
runtime requirements and hidden builds, combines it with unfiltered local-model
and resumable-download discovery, and runs the multi-select UI. It downloads
missing selections through `ModelDownloader.download`, the same entry point used
by `darkbloom models download`, and returns selected concrete model IDs.

The daemon launcher then asks for the idle-memory policy and passes the selected
IDs to `LaunchAgent.installAndStart`. Runtime model loading, eviction, request
admission, and coordinator-directed build migration belong to their existing
subsystems. Alias display names are already used where available: users should
not have to pick between hidden rollout/rollback artifacts.

## What the selection mock should explore

- A fresh catalog with no downloads.
- A catalog with some models already downloaded.
- An interrupted download shown as resumable in the list.
- Empty selection and cancel/back behavior.
- Catalog loading/unavailable/empty states as small boundaries around the picker.

Each row needs a friendly model name and an unambiguous download state/size.
Show supplied eligibility reasons when relevant, without implementing new fit
calculations or asking users to diagnose runtime memory. Keep the existing
Up/Down, Space, Enter conventions and permit multiple selections. The summary
states selection count and what must be downloaded, not guaranteed concurrency.
Do not introduce recommendation rankings without an existing source for them.

After confirmation, hand off to the existing downloader. Its progress can be
presented consistently without changing source fallback, verification, or retry
semantics. Selection should return IDs and cancellation, not certify runtime
readiness.

## Adjacent user choice we missed

The current CLI asks **Memory when idle** after the picker/download:
Always ready, Free when idle (60 minutes), or Custom. Enter preserves the current
policy. This belongs between model preparation and the final start confirmation,
as a separate small screen; it is not another memory-fit scenario. Mocking it is
still pending. Keep memory-cap configuration out: the documented `darkbloom memory`
command is proposed, not implemented.

## Sources

- [Provider quickstart](../../docs/provider/quickstart.md)
- [Model registry](../../docs/architecture/model-registry.md)
- [Hardware policy](../../docs/architecture/hardware-support.md)
- [CLI reference](../../docs/provider/cli-reference.md)
- [Picker](../../provider-swift/Sources/darkbloom/StartCommand+Picker.swift)
- [Daemon launcher](../../provider-swift/Sources/darkbloom/StartCommand+Daemon.swift)

These links are relative to this file under `scripts/cli-preview`; use the
repository root's `docs/` and `provider-swift/` when navigating manually.
