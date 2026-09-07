# Test provider onboarding on a Mac

> Last updated: 2026-09-07 · commit `bbf6f83d4`

Test the real installer, device enrollment, account linkage, model downloads,
and background startup on a physical Apple Silicon Mac. Use the reset script
to repeat first-time setup without redownloading models on every pass.

## Prerequisites

- Use the M3 Max with 128 GB, a logged-in desktop session, and the supported
  macOS/security configuration in [hardware requirements](../provider/hardware-requirements.md).
  Use UTM for supplementary UI/recovery checks; record hardware attestation
  and inference results from the physical Mac.
- Copy this checkout's `scripts/` directory to the test Mac. Run the commands
  below from its parent directory. No Swift build is needed on that Mac when
  testing a signed release.
- **Publish a signed candidate containing these changes to dev first.** Follow
  [provider release](../operations/provider-release.md) using
  `workflow_dispatch`, `environment=dev`, and a reviewed source ref. Record
  the source SHA, version, and signed binary hash from that run. Publication
  changes the shared dev release; coordinate it with other dev users. A local
  `.build/debug/darkbloom` is useful for UI tests but cannot substitute for the
  signed, provisioned bundle in the full device-verification test.
- Use the dev account/console and coordinator together. Dev is configured to
  exercise MDM and MDA in `deploy/environments/dev.env`; check its current
  health and release metadata before resetting the Mac. A missing release
  (`404`) or unavailable endpoint must be fixed before testing.

## Steps

### 1. Confirm the candidate is available

```bash
curl -fsS https://api.dev.darkbloom.xyz/health
curl -fsS https://api.dev.darkbloom.xyz/v1/releases/latest
```

Compare `version` and `binary_hash` with the candidate release run. The public
production install URL and an unmodified dev release do not test local changes.
The repository installer still fetches the registered release from the selected
coordinator, even when you run the script from this checkout.

### 2. Reset the test installation

```bash
bash scripts/onboarding/reset.sh
bash scripts/onboarding/reset.sh --apply
```

The first command is read-only. The second stops the provider and watchdog,
uninstalls an optional fan helper through its normal restore procedure, asks
the installed CLI to remove identity keys, removes known local install/config/
log/cache paths, and backs up shell files before removing exact installer PATH
lines. Run as the provider user, without `sudo`; an installed fan helper or
root-owned CLI shortcut can require an administrator prompt for that step.
The reset refuses to delete files while a provider process remains running.

Remove the **Darkbloom** profile in System Settings → General → Device Management
before reinstalling. This is a macOS user action. Keep other management profiles.
Open a new terminal after the reset.

Model files stay in the shared Hugging Face cache by default. To repeat an
actual model download, pass exact catalog IDs, repeating the option as needed:

```bash
bash scripts/onboarding/reset.sh --remove-model 'org/model'
bash scripts/onboarding/reset.sh --apply --remove-model 'org/model'
```

Replace `org/model` with an actual selected model ID. This deletes that model's
whole shared cache folder, including other revisions and partial downloads;
other apps using it will need to download it again. There is no blanket delete
of the Hugging Face cache. Custom config/cache locations, browser sessions,
cloud account/history, shell backups, and macOS system logs remain. Clearing
browser login is unnecessary to repeat the device-code account-link step.

If the installed CLI is missing, supply `--cli /absolute/path/to/darkbloom`
from a working signed bundle. The script keeps the executable if cleanup fails.
With this candidate, `unenroll` reports keychain failures; older releases may
silently ignore them, so use the candidate CLI for a verified identity reset.

### 3. Run the installer from this checkout

```bash
cat scripts/install.sh | COORD_URL=https://api.dev.darkbloom.xyz bash
```

This exercises the pipe-to-Bash handoff with the new shell source and the dev
signed bundle. It continues through enrollment, browser account linkage, the
model picker and downloads, then waits at **Press Enter to start Darkbloom**.
Do not pipe its output to `tee`: onboarding requires a terminal for both input
and output. Record a transcript through the terminal emulator if needed.

After the coordinator's embedded installer is also deployed to dev, test the
literal customer command against dev:

```bash
curl -fsSL https://api.dev.darkbloom.xyz/install.sh | bash
```

Deploying the installer alone cannot add the new flow to an older CLI; the
shell detects older CLIs and prints their manual setup commands instead.

### 4. Exercise the important transitions

| Pass | Action | Expected result |
|---|---|---|
| Fresh setup | Remove the profile and local state, then install | Enrollment is an explicit next step; account linkage follows confirmed enrollment; no background service starts before the final Enter |
| Models | Select multiple models with Space, then Enter | Downloaded section appears only when populated; available models use total physical RAM with load safeguards; selected downloads reuse the verified downloader |
| Additional models | Expand the hidden section when the live catalog contains models beyond the estimate | A fit caveat is visible; they can be downloaded, but this selection does not enable them for serving; choose at least one fitting model to finish |
| Interruption | Quit before enrollment approval or final start; interrupt a download separately; run `darkbloom start` | The same coordinator/config and model intent resume; completed/partial downloads are reused; final Enter is still required |
| Update | Complete setup, rerun the same installer while running, then repeat after `darkbloom stop` | No enrollment/account/model prompts; running/stopped service state is preserved; the installer prints the applicable restart/start instruction |
| Other entry points | Try `darkbloom enroll`, `darkbloom login`, `darkbloom models catalog`, `darkbloom restart`, and `darkbloom doctor` | The standalone commands remain usable; the saved dev coordinator is retained after setup |

Repeat the model-picker pass with ordinary apps open and closed. Its estimate
must use **128 GB total RAM** on this machine, not currently free RAM. It
estimates whether each model fits individually, not whether all selected
models can reside simultaneously. The live catalog may have no oversized
entries on a large Mac; do not expect a hidden section when there are none.

For unattended/update automation, the shell-only path is:

```bash
COORD_URL=https://api.dev.darkbloom.xyz bash scripts/install.sh --install-only
```

## Verify

```bash
darkbloom --version
shasum -a 256 "$HOME/.darkbloom/bin/darkbloom"
darkbloom status
darkbloom doctor
darkbloom models list
```

The binary hash must match the candidate. Confirm current device verification
and a ready model; a pending-readiness message is not a pass. Then send an
actual [self-route request](../provider/self-route.md), replacing the example's
production URL with `https://api.dev.darkbloom.xyz` and using an API key from
the same dev account. Self-route relaxes the hardware-trust floor, so a
successful response alone does not prove attestation; check both results.

Local regression checks, without enrolling or removing host state:

```bash
python3 scripts/test-install-onboarding.py
bash scripts/test-install-atomic.sh
python3 scripts/onboarding/test-reset.py
swift test --package-path provider-swift --filter 'Onboarding|TerminalPicker|PickerEntry|LocalDataCleanup'
```

Stage the source-matched metallib first as described in [test.md](test.md).
These automated checks cover composition, terminal input, fit estimates,
update guards, verified downloads, and cleanup failure handling. Real macOS
profile approval, browser login, APNs/MDA and inference still need the Mac pass.

## Related

- [Provider installation](../provider/installation.md)
- [Provider release](../operations/provider-release.md)
- [Dev environment](../operations/dev-environment.md)
- [CLI reference](../provider/cli-reference.md)
