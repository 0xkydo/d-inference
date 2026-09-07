# Test provider onboarding on a Mac

> Last updated: 2026-09-07 · commit `4314668d6`

Test the real installer, device enrollment, account linkage, model downloads,
and background startup on a physical Apple Silicon Mac. Use the reset script
to repeat first-time setup without redownloading models on every pass.

## Local UI iteration without release signing

For interactive UI iteration, run the local debug executable with its sibling
Bubble Tea companion. Swift and Go local builds carry ad-hoc signatures; this
does not require a Developer ID certificate, notarization, or a GitHub workflow.
Build with `make provider-build provider-tui-build` when needed, then run from
the repository root:

```bash
# Optional full reset: bash scripts/onboarding/reset.sh --all --apply --cli /absolute/path/to/signed/darkbloom
# Full reset removes local state, ALL default shared model caches and downloaded Darkbloom installers.
# Skip reset to retain enrollment/login and test resume.
provider-swift/.build/debug/darkbloom start --tui \
  --coordinator-url https://api.darkbloom.dev
```

The human operates enrollment, browser account linkage and downloads. These
use the existing services; already-completed steps may be skipped. Quit with
`q` at the final Start screen. This exercises the actual onboarding UI and
Swift services, but does not test the installer, privileged identity access,
APNs/MDA acceptance or verified serving. It does not replace the signed install.
Any reset must use a working signed cleanup CLI; do not substitute the ad-hoc
build to bypass a keychain cleanup failure. Remove the Darkbloom profile manually
only when intentionally repeating fresh enrollment.

After you approve the profile, Bubble Tea checks enrollment every two seconds
and advances to account linkage automatically. Press Enter on that next screen
to open the browser. A persistent header shows the five steps, marks completed
steps, and highlights your current position. The primary action stays at the
bottom; use Page Up/Down to read long text in a small terminal.

## Signed installer prerequisites

- Use an Apple Silicon Mac with a logged-in desktop session and the supported
  [macOS/security configuration](../provider/hardware-requirements.md). The
  M3 Max with 128 GB is the physical test target.
- Fetch `codex/bubbletea-stage-one` from `0xkydo/d-inference`, based on
  `codex/cli-onboarding-m3-local`. Use the exact candidate commit when testing.
  This branch includes the opt-in Bubble Tea frontend and the existing plain CLI.
- A **signed, notarized candidate from this branch**, downloaded from GitHub
  Actions. A local debug build cannot substitute for the signed bundle's
  entitlement and provisioning contract.
- Python 3 and `gh` for downloading/preparing the test artifact. They are test
  tooling; the customer installer continues to require neither.

No running dev coordinator is required to build or install this candidate.
`environment=dev` below selects the GitHub signing environment; with
`publish_release=false`, the workflow skips R2 uploads and release registration.
It does not deploy infrastructure or update the public release.

The interactive test uses the existing production account/catalog/enrollment
services on your own test Mac. These are real account/device actions and model
downloads. For UI testing, quit at the final Enter-to-start gate. A complete
network-readiness test additionally requires the coordinator to accept the
candidate's exact signed binary identity; local code signing alone does not
satisfy that gate. Registering a production candidate is a separate operation.

## Steps

### 1. Build and prepare the signed candidate

First confirm that GitHub Actions has registered the release workflow in the
fork. Having the YAML file in a branch does not establish that registration:

```bash
gh api repos/0xkydo/d-inference/actions/workflows \
  --jq '.workflows[] | select(.path == ".github/workflows/release-swift.yml") | {id, state}'
```

If this prints nothing, or dispatch reports `workflow not found on the default
branch`, stop before resetting. The repository owner must make the release
workflow available to Actions, with its `workflow_dispatch` declaration on the
default branch. The fork also needs authorized Apple signing/provisioning/
notarization credentials and access to the workflow's runner labels. Enabling
Actions alone does not supply those dependencies. Do not create a release tag,
drop `publish_release=false`, or use an unsigned build as a substitute for this
signed installation test.

```bash
gh workflow run release-swift.yml --repo 0xkydo/d-inference \
  --ref codex/bubbletea-stage-one \
  -f environment=dev -f publish_release=false
```

The fork needs its authorized signing/provisioning/notarization secrets in the
selected GitHub environment. Do not copy secrets from another repository or
publish/register a release to make this UI test pass. This guide prepares
commands; the implementation's automated tests do not dispatch this workflow.

After that run succeeds, download its `darkbloom-dev-qualification-<run-id>-<attempt>`
artifact. Substitute the actual run ID and attempt below. Use the exact checkout
commit built by the run when preparing the artifact.

```bash
gh run download <run-id> --repo 0xkydo/d-inference \
  --name darkbloom-dev-qualification-<run-id>-<attempt> \
  --dir "$HOME/Downloads/darkbloom-cli-candidate"
python3 scripts/onboarding/prepare-candidate.py \
  "$HOME/Downloads/darkbloom-cli-candidate" --commit "$(git rev-parse HEAD)" \
  --repo 0xkydo/d-inference
```

Preparation checks the repository, source SHA, accepted notarization record,
archive size/hash, and binary/metallib hash fields, then writes `local-release.json`.
The installer subsequently verifies the signed archive contents and pinned
Developer ID. Fetching the ordinary public installer without this local metadata
still installs the currently registered release, not the candidate.

### 2. Reset the test installation

Close other onboarding sessions before resetting. The reset is for a fresh
setup pass; skip it when checking resume or completed-install updates.

```bash
bash scripts/onboarding/reset.sh --all
bash scripts/onboarding/reset.sh --all --apply
```

The first command is read-only. The second performs a **full local test reset**:
it removes every `models--*` folder in the default shared Hugging Face cache,
including all revisions, partial downloads and model locks, plus downloaded
Darkbloom candidates, bundles and installers. Other apps using those model
files will need to download them again. Source, local builds, the preserved
cleanup tool, unrelated Downloads files and Hugging Face datasets stay.

For the signed installer pass, this also deletes the candidate downloaded in
step 1. After resetting, repeat its artifact download and metadata preparation
before step 3; the successful workflow does not need to be rebuilt. Local UI
iteration uses the preserved worktree build and needs no artifact download.

It also stops the provider and watchdog,
uninstalls an optional fan helper through its normal restore procedure, asks
the installed CLI to remove identity keys, removes known local install/config/
log/cache paths, and backs up shell files before removing exact installer PATH
lines. Run as the provider user, without `sudo`; an installed fan helper or
root-owned CLI shortcut can require an administrator prompt for that step.
The reset refuses to delete files while a provider process remains running.

Remove the **Darkbloom** profile in System Settings → General → Device Management
before reinstalling. This is a macOS user action. Keep other management profiles.
Open a new terminal after the reset.

Omit `--all` to keep shared models and downloaded installers. To remove only
specific models instead of all models, pass exact catalog IDs:

```bash
bash scripts/onboarding/reset.sh --remove-model 'org/model'
bash scripts/onboarding/reset.sh --apply --remove-model 'org/model'
```

Replace `org/model` with an actual selected model ID. This deletes that model's
whole shared cache folder, including other revisions and partial downloads;
other apps using it will need to download it again. There is no blanket delete
of the Hugging Face cache for this targeted option. Custom config/cache locations, browser sessions,
cloud account/history, shell backups, and macOS system logs remain. Clearing
browser login is unnecessary to repeat the device-code account-link step.

If the installed CLI is missing, supply `--cli /absolute/path/to/darkbloom`
from a working signed bundle. The script keeps the executable if cleanup fails.
With this candidate, `unenroll` reports keychain failures; older releases may
silently ignore them, so use the candidate CLI for a verified identity reset.

### 3. Run the real installer with the candidate archive

```bash
cat scripts/install.sh | COORD_URL=https://api.darkbloom.dev bash -s -- \
  --tui --release-file "$HOME/Downloads/darkbloom-cli-candidate/local-release.json"
```

`--release-file` replaces release discovery only. Bundle hashes, pinned signing
requirements, runtime-resource checks, and the atomic install path still run.
The pipe hands the real terminal to the new CLI, which continues through
profile approval, browser account linkage, model selection, and downloads.

For the first UI pass, enter `q` at **Press Enter to start Darkbloom**. The
candidate remains installed and setup can resume with `darkbloom start --tui`.
The opt-in switch can be omitted to exercise the existing plain CLI.
No verification success should be inferred from completing these screens.
Starting the background service is a separate test once its binary identity
is accepted by the coordinator. A not-yet-registered candidate can be rejected
or remain unverified after launch; do not report that as a passing trust test.

Do not pipe the installer's output to `tee`: onboarding requires a terminal
for both input and output. Use the terminal emulator's transcript feature.

To resume without removing any state:

```bash
# Fresh-setup reset alternative only: bash scripts/onboarding/reset.sh --all --apply
# Scope: local state, all default shared model caches and downloaded Darkbloom installers.
# Do not reset when testing resume or completed-install updates.
darkbloom start --tui
```

Closing Bubble Tea cancels and awaits its foreground download work. It retains
completed and partial model files, and never stops an independently running
provider. At Ready, Start is still a separate Enter action. If a provider is
already running, explicitly starting with a new selection follows the existing
start/reinstall sequence. Device enrollment shown by the frontend is a local
macOS observation, not a coordinator trust verdict.

### 4. Exercise the important transitions

| Pass | Action | Expected result |
|---|---|---|
| Fresh setup | Remove the profile and local state, then install | Enrollment is an explicit next step; account linkage follows confirmed enrollment; no background service starts before the final Enter |
| Models | Select multiple models with Space, then Enter | Downloaded section appears only when populated; available models use total physical RAM with load safeguards; selected downloads reuse the verified downloader |
| Additional models | Expand the hidden section when the live catalog contains models beyond the estimate | A fit caveat is visible; they can be downloaded, but this selection does not enable them for serving; choose at least one fitting model to finish |
| Interruption | Quit before enrollment approval or final start; interrupt a download separately; run `darkbloom start --tui` | The same coordinator/config and model intent resume; completed/partial downloads are reused; final Enter is still required |
| Update | Complete setup, rerun the same installer while running, then repeat after `darkbloom stop` | No enrollment/account/model prompts; running/stopped service state is preserved; the installer prints the applicable restart/start instruction |
| Other entry points | Try `darkbloom enroll`, `darkbloom login`, `darkbloom models catalog`, `darkbloom restart`, and `darkbloom doctor` | The standalone commands remain usable; the configured coordinator is retained after setup |

Repeat the model-picker pass with ordinary apps open and closed. Its estimate
must use **128 GB total RAM** on this machine, not currently free RAM. It
estimates whether each model fits individually, not whether all selected
models can reside simultaneously. The live catalog may have no oversized
entries on a large Mac; do not expect a hidden section when there are none.

For unattended/update automation, the shell-only path is:

```bash
COORD_URL=https://api.darkbloom.dev bash scripts/install.sh --install-only \
  --release-file "$HOME/Downloads/darkbloom-cli-candidate/local-release.json"
```

## Verify

```bash
darkbloom --version
shasum -a 256 "$HOME/.darkbloom/bin/darkbloom"
darkbloom status
darkbloom doctor
darkbloom models list
```

The binary hash must match the candidate. Record which UI transitions were
exercised. For a separately authorized full-network test with a recognized
candidate, confirm current device verification and a ready model, then send an
actual [self-route request](../provider/self-route.md) using the same account.
Self-route relaxes the hardware-trust floor, so a successful response alone does
not prove attestation; check both results. Pending readiness is not a pass.

Local regression checks, without enrolling or removing host state:

```bash
python3 scripts/test-install-onboarding.py
bash scripts/test-install-atomic.sh
python3 scripts/onboarding/test-reset.py
python3 scripts/test-release-candidate.py
make provider-tui-test
swift test --package-path provider-swift --filter 'Onboarding|TerminalPicker|PickerEntry|LocalDataCleanup'
```

These fixture tests do not need a reset: they use disposable state and injected
services. They establish workflow/terminal behavior, not signed identity,
APNs/MDA acceptance, or successful network inference.

Captured terminal previews use those disposable services and fixture catalog
data: [enrollment](../assets/bubbletea-stage-one/enrollment.png),
[account linkage](../assets/bubbletea-stage-one/account.png),
[model selection](../assets/bubbletea-stage-one/models.png), and
[explicit Start](../assets/bubbletea-stage-one/ready.png).

Stage the source-matched metallib first as described in [test.md](test.md).
These automated checks cover composition, terminal input, fit estimates,
update guards, verified downloads, and cleanup failure handling. Real macOS
profile approval, browser login, APNs/MDA and inference still need the Mac pass.

## Related

- [Provider installation](../provider/installation.md)
- [Provider release](../operations/provider-release.md)
- [Dev environment](../operations/dev-environment.md)
- [CLI reference](../provider/cli-reference.md)
