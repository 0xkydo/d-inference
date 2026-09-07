# Onboarding design playground

These are retained design fixtures. The real implementation now lives in
`provider-swift/Sources/darkbloom/Onboarding/`; use
[the Mac test guide](../../docs/developer/onboarding-test.md) for end-to-end testing.

## Live model picker (narrow selection/download preview)

```sh
python3 scripts/cli-preview/live_models.py
```

Fetches the public production catalog, uses public alias names, scans the same
default HF cache as the provider for exact catalog IDs, and reads this Mac's chip,
total RAM. Fit does not depend on currently running apps. Empty Downloaded sections are omitted. Models
likely to fit appear under Available to download. `h` reveals additional models;
they remain selectable for download despite the displayed memory/hardware caveat.
Downloaded models remain visible, with a caveat if needed. Use arrows, Space,
and Enter. Download progress is **simulated**; no weights are downloaded or loaded.

Requires Python 3.11+ and the repo's Swift toolchain. It compiles a tiny temporary
helper with the actual `UnifiedMemoryCap` and `ModelLoadAdmission` sources. The estimate uses the conservative default working-memory floor (rather
than a changing per-selection floor), minimum KV headroom, disk × 1.2 loading
padding, configured `memory_reserve_gb`, and inherited cap/reserve env overrides.
It uses catalog total bytes, including ancillary files. It does not inspect live
MLX allocations/reservations or daemon-specific env overrides: this is a
conservative total-capacity estimate. Runtime admission still checks memory
available when a model actually loads.

The normal run always fetches fresh data; `--catalog-file FILE` explicitly uses a
saved response for debugging. `--list` prints without prompting; `--show-hidden`
expands additional models immediately. Unknown required runtime capabilities are
shown as needing a compatibility check, never presented as a RAM-only failure.
Local discovery checks config/weights and indexed shard existence but does not
hash weights or certify that local bytes match the current release manifest.

## Full onboarding fixtures

Start the interactive mock from the repository root:

```sh
python3 scripts/cli-preview/playground.py
```

Select a scenario, then follow the proposed customer prompts with Enter. Mock
controls below each screen trigger external events: for example, `e1` can simulate
installing the profile in Settings or approving login in the browser. Use `b` to
go back, `r` to replay, `m` to switch scenarios, and `q` to quit. The model picker
uses Up/Down to move, Space to toggle multiple selections, and Enter to confirm.
It separates downloaded models from available downloads and shows the total new
download size. At least one selection is required. On that screen, controls act
immediately; `n` triggers the no-compatible-models scenario. Outside the picker,
press Enter after each control. Piped input uses numbered toggles followed by Enter.
The last setup screen says "Ready to start Darkbloom" and waits for Enter before
simulating startup. The later green readiness screen represents network verification.

There are 20 scenario entry points and 48 screens spanning first installation,
enrollment, resuming setup, login expiry, interrupted downloads, verification
delays/rejection, stopped/running updates, rollback, unattended installation, and
failures. Individual screens are also directly accessible:

```sh
python3 scripts/cli-preview/playground.py --scene enroll
python3 scripts/cli-preview/playground.py --scene models
python3 scripts/cli-preview/playground.py --scene update
python3 scripts/cli-preview/playground.py --render-all
```

This is a simulation, not an implementation of installer behavior. Browser and
Settings actions never open windows. Resume and update state exists only in the
scenario fixtures; no state is saved to disk. Model labels, sizes, and hardware
are illustrative. Mock controls are deliberately separated from customer copy.
The model download and resume screens pause at 40% so you can trigger an interruption; Enter
simulates finishing the remaining download. Animated timing is artificial.

Try narrow/wide windows, light/dark terminal themes, `NO_COLOR=1`, and `--instant`.
No full-screen mode is used, so each screen remains in scrollback. The mock uses
Python 3 for local design iteration; this adds no prerequisite to the real installer.

## Earlier installer comparisons

Run from the repository root in a terminal with Python 3:

```sh
python3 scripts/cli-preview/install.py current
python3 scripts/cli-preview/install.py proposed
python3 scripts/cli-preview/install.py proposed --scenario enrolled
python3 scripts/cli-preview/install.py proposed --scenario offline
python3 scripts/cli-preview/install.py proposed --scenario verification-failed
```

This is an output-only design prototype. It never executes `scripts/install.sh`,
downloads software, changes configuration, opens System Settings, or enrolls a Mac.
Hardware, version, progress, and timing are illustrative fixtures. The current
transcript reproduces the normal piped install with enrollment still pending,
excluding optional migration messages; it is manually maintained against
`scripts/install.sh`. Its download bar is illustrative rather than curl output.

Use `--instant` to skip delays, `NO_COLOR=1` to inspect uncolored output, and resize
the terminal to check wrapping. Redirected output has no ANSI escapes or carriage
return animation. Ctrl-C exits the preview without changing terminal modes.

The proposed copy is not wired into the production installer. The earlier
`install.py proposed` study still shows the manual command handoff; `playground.py`
explores the continuous experience. These fixtures preserve the design-review history; they are not the authority
for the implemented flow. Use the production CLI to verify current behavior.

## Flow audit

Run the mock's regression checks:

```sh
python3 -m unittest discover -s scripts/cli-preview -p 'test_*.py' -v
```

Checks cover every declared transition, all scenario exits, a fresh install with
multiple selections and interrupted/resumed downloads, the explicit startup gate,
new codes on login retry, update/rollback state, empty selection, back navigation,
wrapping at 40/80/120 columns, and real PTY arrows/Space/Enter and cancellation with
terminal-mode restoration. Back is a design-review control: it restores a prior
in-memory snapshot, not a proposed way to undo an actual enrollment or update.

Remaining design cases, not covered by this mock:

- Live catalog/model identities, per-model progress, and memory-fit restrictions.
- Partial startup when only some selected models can load.
- Detailed device-verification reasons and precise recovery instructions.
- Existing credentials that are revoked or belong to a different account.
- Updater lock contention, request-drain timeout, and failed rollback recovery.
- Actual persistent resume across processes, browser/Settings permissions, and
  live readiness detection. These require implementation and integration testing.

The combined model progress bar and `.invalid` browser URL are illustrative.
The first-install fixture has no models downloaded; interrupted-download entry
points select an undownloaded model. Update entries explicitly distinguish a
stopped provider from a running provider and never repeat enrollment/login.
