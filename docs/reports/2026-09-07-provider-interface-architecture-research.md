# Provider interfaces: architecture research and review record

> Last updated: 2026-09-07 · commit `fdb2edd28`

This report preserves the source findings, product inputs and technical reviews
behind the [provider interface design](../design/provider-ui-shared-swift.md).
It is for future agents and people who need the starting point and reasoning,
including why larger changes were deferred. It is a consolidated research
record, not a verbatim conversation transcript or a claim of implementation.
External source notes are paraphrases with links, not archived article copies.

## Provenance and limits

- Code baseline: `0xkydo/d-inference`, `codex/cli-onboarding-m3-local`,
  `fdb2edd2839af0a66208563dd67971a1122ba217`. The requested fork and branch,
  rather than an unrelated worktree's current state, anchor the design.
- Another active CLI worktree was inspected read-only for context. Its work
  was not edited or merged. Reference-only inspection of newer main branches
  did not change the implementation base.
- Repository docs are authoritative for existing architecture. In particular,
  read [provider components](../architecture/components/provider.md),
  [attestation](../architecture/security/attestation.md),
  [identity binding](../architecture/security/identity-binding.md) and
  [encryption](../architecture/security/encryption.md). Do not infer a different
  privacy model from diagrams of the management interface.
- Evidence consists of code/doc inspection, primary external sources and three
  technical reviews. No live provider behavior, production policy values,
  signed App Attest integration or performance improvement was measured.
- The original framework-discovery input was
  [awesome-terminal-aesthetics](https://github.com/kud/awesome-terminal-aesthetics#tui-frameworks).
  This record investigates the selected Bubble Tea direction; it does not
  claim a benchmark or exhaustive comparison of terminal frameworks.
- The conversational reference to an "Airness agent" was not reliably
  identified. No architecture claim here relies on guessing that product.

## Product inputs and how the scope evolved

| Owner input | Consequence |
|---|---|
| First version must prove real onboarding through Bubble Tea using Swift services. | Rendering fixtures alone are insufficient; enrollment, account linking, model selection/download and explicit start must use actual Swift operations. |
| The future macOS app will be the primary interface for most providers. | Keep business decisions in Swift and decouple them from terminal presentation. |
| Initially emphasized a strong shared foundation for CLI and GUI. Later clarified that the first change should remain minimal. | Reuse existing services through a narrow session now; defer a broad shared application module until the native client needs it. |
| Closing the terminal should stop downloads cleanly and resume completed/partial files on reopening. | Management work can be client-owned; no permanent download manager is required in stage one. |
| Provider is not App Sandboxed. | Do not invent App Sandbox constraints or shared-container requirements. Process lifetime and access boundaries still need deliberate ownership. |
| Future app intends to use App Attest on macOS 27, but should not be fully future-compatible now. | Isolate verification and platform integration without implementing a speculative migration framework. |
| Use docs inside the repo. | Keep the direction and evidence under version control and indexed. Scratch notes and chat history are not the handoff source. |
| Implementation will be a separate command in another worktree. | This task produces documentation only; branch names and diagrams must not imply stage one has shipped. |

The early architecture exploration considered a shared always-on Swift manager
with GUI and CLI as equal socket clients. That offered clear ownership of
long-lived jobs but added service startup, reconnection, upgrade and recovery
requirements. The selected stop/resume behavior removed the immediate need for
that process. Subsequent macOS research confirmed that the existing persistent
provider already supplies the essential independent runtime.

The phrase "shared Swift application services" initially described a desired
logical layer too abstractly. Source inspection and owner feedback clarified
that it does not exist as a unified module. The final stage-one scope is focused
files in the current executable, not a wholesale decomposition of ProviderCore.

## Source findings: onboarding and downloads

| Source and symbol | Observation | Architectural consequence |
|---|---|---|
| `provider-swift/Package.swift` (`ProviderCore` target) | The core links inference, crypto, server and model dependencies. There is no management-session target or Bubble Tea implementation on the baseline. | Reuse the implementations without making the GUI import the inference stack. Keep new contract/workflow files dependency-light. |
| `Sources/darkbloom/StartCommand.swift` (`run`) | Runtime/Metal preparation precedes parts of onboarding. | Do not expose the entire command as a generic UI RPC. Route the new session to focused operations. |
| `Sources/darkbloom/StartCommand+Daemon.swift` (`launchDaemon`) | Preflight, persisted setup intent, enrollment/login, selection, download, final consent and service startup are coordinated here. | Preserve order and consent while separating terminal presentation. |
| `Sources/darkbloom/Onboarding/GuidedOnboarding.swift` | Injected operations already provide test seams, but headings, retry messages and confirmation remain direct terminal calls. | Adapt existing decision logic instead of implementing a second onboarding policy in Go. |
| `Onboarding/EnrollmentFlow.swift`, `Onboarding/AccountLinkFlow.swift` | Browser/Settings confirmations rely on interactive/TTY behavior; enrollment checks actual OS state. | Pipes require explicit actions and backend prerequisite checks. A status read must never open a browser or profile. |
| `Onboarding/ModelPickerCatalog.swift`, `ModelPickerPolicy.swift` | Catalog and eligibility policy are Swift code tied to the Start extension. | Expose results to the frontend. Preserve total-physical-RAM policy and download-versus-serve distinctions. |
| `Onboarding/OnboardingState.swift`, `OnboardingConfiguration.swift` | Setup intent is persisted; config changes already use exclusive locking. | Reuse those stores. Persisted intent is not proof that enrollment, linkage or download succeeded. |
| `Sources/ProviderCore/Models/ModelDownloader+Download.swift` (`downloadManifestModel`) | Stable staging and `.part` files support resume, integrity checks and publication. The function also creates a terminal renderer and detached progress task. | Preserve the file pipeline; separate progress/cancellation from rendering. Bytes transferred, verification and published completion are distinct phases. |
| `Sources/ProviderCore/Server/ModelPrefetchCoordinator.swift` | In-process coordination does not serialize a different process writing the same model destination. | Place cross-process coordination below UI entry points; actors alone are insufficient. |

Paths abbreviated to `Sources/...` and `Onboarding/...` above are relative to
`provider-swift/` and `provider-swift/Sources/darkbloom/`, respectively.

## Source findings: service lifetime and operation authority

All source paths in this section are relative to `provider-swift/Sources/`.

- `ProviderCore/Service/LaunchAgent.swift`: provider is a per-user GUI-session
  launch agent. Explicit start enables it; login can restore it. Explicit stop
  unloads and persistently disables it. `KeepAlive` is deliberately false;
  crash recovery belongs to the existing watchdog.
- `LaunchAgent.installAndStart` and watchdog installation derive the current
  executable path. Calling them unchanged inside SwiftUI could register the
  GUI executable as the provider. Stage one keeps the adapter hosted in the
  existing executable; a future split needs explicit installed-path resolution.
- `darkbloom/StopCommand.swift`: disarms watchdog recovery before stopping the
  provider. `RestartCommand.swift` also coordinates recovery configuration.
  Sharing a low-level launch function is insufficient; preserve the sequence.
- `ProviderCore/ProviderLoop+AutoUpdate.swift`, `Update/UpdateProcessLock.swift`,
  `Service/WatchdogRecoveryService.swift`: update staging, drain, replacement,
  rollback and recovery already coordinate through existing state/leases.
  The GUI must not add a competing restart timer or bundle updater.
- `ProviderCore/Service/DaemonStateFile.swift`: atomically written observations
  include process identity, version, timestamps, models, connectivity and
  diagnostic trust facts. They remain useful after a crash but can be stale.
  Reading them must not create a second provider identity.
- `darkbloom/StatusCommand.swift`: stale observations and a wedged process are
  different conditions; idle-unloaded models are not necessarily failures.
  A management disconnect must not automatically restart inference.
- `ProviderCore/Coordinator/CoordinatorClient+Connection.swift`: serving owns
  reconnect/backoff and suspension detection. `Service/ProcessLifecycle.swift`
  and `System/InferencePowerAssertion.swift` contain existing power behavior.
  A new UI should not own those leases or imply guaranteed serving during sleep.
- `darkbloom/FanActivityLease.swift`, `DarkbloomFanHelper/FanXPCService.swift`:
  the optional root helper is narrowly scoped, authenticates its peers, and
  restores automatic fan behavior on lease/session loss. It is not an ordinary
  management daemon, and its absence must not block inference.
- Existing watchdog and provider share a replaceable executable. This is not
  an independent rescue system capable of repairing every unlaunchable binary
  after reboot. The design does not claim to solve that separate limitation.

Runtime counters do not establish credited earnings. The native interface will
need coordinator accounting data with its own freshness, separate from local
serving statistics. No earnings UI is in stage one.

## Security review evidence and implications

These findings explain which existing boundary the management work must
preserve; the canonical security documents remain the authority.

| Inspected source | Finding and implication |
|---|---|
| `ProviderCore/ProviderLoop.swift` | The serving process creates its ephemeral X25519 key. UI management does not need access to that key or inference data. |
| `ProviderCore/Security/PersistentEnclaveKey.swift`, `KVCache/SecureEnclaveKeyWrappingService.swift` | The persistent identity is involved in attestation and cache-key unwrapping. A generic sign/decrypt/unwrap bridge would expand authority far beyond onboarding. |
| `ProviderCore/ProviderLoop+AttestationChallenge.swift` (`answerCodeChallenge`) | An internal challenge path decrypts supplied payload and signs returned bytes. It must not become a frontend-callable crypto oracle. |
| `darkbloom/ProviderAppKitHost.swift`, `ProviderCore/Apns/APNsBridge.swift` | Current APNs handling is part of the serving identity/process. The host explicitly avoids ordinary notification authorization because alert-mode behavior can retain challenge payloads. Preserve this current constraint without making it a permanent GUI architecture rule. |
| `darkbloom/Onboarding/OnboardingStartup.swift`, `ProviderCore/Service/DaemonStateFile.swift`, `coordinator/protocol/messages.go` | Hardware trust and warm models are not the complete private-routing verdict. UI readiness must distinguish observed facts, unknown/stale state and server acceptance. |
| `.github/workflows/release-swift.yml`, `scripts/install.sh` | Signing, provisioning, nested helpers, post-sign hashes and installation/update behavior are coupled. A Go companion should be part of one release, with no new provider keychain/APNs privileges. |

Provider source paths in the table are relative to `provider-swift/Sources/`.
The existing signed executable still carries its entitlements in session mode.
Separating source files is not OS privilege isolation, and serving-mode
hardening must not be assumed to run automatically in another entry path.
Pipes also do not make operator-controlled input trustworthy.

Coordinator routing gates and release-policy settings need source and runtime
context. This review did not read live production values, and should not be
used to assert which optional policy gates were active on a particular fleet.

## External research source notebook

Sources were read on 2026-09-07. These notes record the specific inference drawn
from each source and the limit of the analogy.

| Primary sources | Finding | Limit |
|---|---|---|
| [Apple background processes](https://developer.apple.com/documentation/appkit/managing-ongoing-background-processes-in-your-mac) | Work surviving app exit should have explicit management and visible user controls; test user-disabled background activity. | Background permission is not process health, network readiness or trust acceptance. |
| [SMAppService](https://developer.apple.com/documentation/servicemanagement/smappservice), [helper migration](https://developer.apple.com/documentation/servicemanagement/updating-helper-executables-from-earlier-versions-of-macos) | Modern native packaging can register a bundled per-user agent through a lifecycle adapter. | Do not bundle that installation migration into the initial terminal UI change. |
| [Apple XPC](https://developer.apple.com/documentation/xpc) | XPC transport and a bundled client-associated XPC service are different choices; launch agents can expose IPC. | XPC alone does not grant an independently persistent lifecycle. Do not implement both XPC and socket transports preemptively. |
| [SwiftUI model data](https://developer.apple.com/documentation/swiftui/managing-model-data-in-your-app), [MenuBarExtra](https://developer.apple.com/documentation/swiftui/menubarextra) | Views can observe a UI data model and offer menu-bar controls for background work. | UI observation is not cross-process state ownership. |
| [Ollama supervisor](https://github.com/ollama/ollama/blob/main/app/server/server.go), [app lifecycle](https://github.com/ollama/ollama/blob/main/app/cmd/app/app.go), [API client](https://github.com/ollama/ollama/blob/main/api/client.go) | Desktop code supervises a bundled serving subprocess; CLI/API code talks to a backend. | Ollama's app-owned supervision is not a reason to replace Darkbloom's existing launchd/watchdog arrangement. |
| [LM Studio 0.4.0](https://lmstudio.ai/blog/0.4.0), [headless modes](https://lmstudio.ai/docs/developer/core/headless), [CLI client](https://github.com/lmstudio-ai/lms/blob/main/src/createClient.ts) | A reusable runtime can be packaged as llmster independently of the GUI; daemon and HTTP-server startup are distinct. CLI code discovers/connects to the backend. | Do not claim every GUI and headless invocation necessarily shares one identical running instance. |
| [Tailscale process variants](https://tailscale.com/docs/reference/tailscaled) | Shared code can run in distinct GUI, extension and CLI contexts, including a common binary. | Its network extension exists for networking requirements, not generic background work. |
| [Docker architecture](https://docs.docker.com/get-started/docker-overview/#docker-architecture), [Mac backend/permissions](https://docs.docker.com/desktop/setup/install/mac-permission-requirements/) | Client/server APIs and scoped privilege helpers are established patterns. | Its VM and networking needs do not justify equivalent process complexity for Darkbloom. |

The resulting recommendation is project-specific: preserve the independent
provider and use foreground management sessions for the selected lifetime.
The examples establish no universal requirement for JSON pipes or another
manager. Local inference app APIs also do not establish which data Darkbloom
should expose to a provider operator.

For the future verification migration, Apple's
[WWDC26 session](https://developer.apple.com/videos/play/wwdc2026/201/) and
[server validation documentation](https://developer.apple.com/documentation/devicecheck/validating-apps-that-connect-to-your-server)
establish the App Attest mechanism. They do not prove a completed Darkbloom
integration. GUI attestation alone does not bind a separate inference process
and its receiving key. Proof generation, server policy, identity/cache
migration, packaging and older-provider coexistence need a dedicated design.
No speculative implementation of those concerns belongs in this first stage.

## Technical review record

Three bounded reviews covered service architecture/reliability, Swift/macOS
integration, and delivery/UX/testing. Follow-up reviews covered the security
boundary and external macOS precedents. Findings below preserve the substance
of the reviews rather than conversational tool logs.

| Perspective | Challenge | Resolution or deferred work |
|---|---|---|
| Service reliability | An extra manager introduces startup, recovery and upgrade coordination before persistent jobs are required. | Session-owned onboarding; retain existing serving lifetime. Revisit on a concrete job-lifetime requirement. |
| Service reliability | Multiple process-local actors cannot protect one shared cache/config. | Reuse config locks and coordinate competing model writers below frontend code. Conflicting setup returns busy. |
| Swift/macOS | Calling current launch helpers from a GUI may register the GUI binary. | Host adapters in the existing executable now; resolve the actual provider path explicitly before a native split. |
| Swift/macOS | Importing ProviderCore for UI state drags in inference dependencies. | Keep new types and workflow free of those imports; extract a lightweight shared module when it has a native consumer. |
| Swift/macOS | A bundled XPC helper is not synonymous with a persistent service. | Pick lifetime first, transport second; defer extra IPC implementations. |
| UX | TTY-dependent consent can disappear when stdin becomes a pipe. | Explicit confirm commands and backend prerequisite checks, including the final separate Start. |
| UX | Quit, window close, Stop and login have different effects. | Preserve explicit serving intent; GUI visibility never becomes the recovery authority. |
| UX | A stale status or idle model can look like a crash; token counts can look like money. | Preserve state source/freshness and separate lifecycle, model, trust and accounting facts. |
| Testing | Independent preview scenes can pass while the real flow is wrong. | Test actual Swift workflow and Go components with shared fixtures, plus PTY cancellation/terminal behavior. |
| Delivery | A new binary can break signing, installation, rollback or unattended use. | One packaged companion/release; verify installer handoff and plain command/update paths. |
| Security | A generic command bridge can accidentally expose signing, decryption, arbitrary execution or raw diagnostics. | Small allowlisted product actions; never expose crypto operations, keys or inference contents. |
| Security | Same-binary session mode is not privilege separation. | Keep initialization narrow and review protocol/logging bounds; do not claim the UI refactor strengthens OS isolation. |

The final source/evidence consistency review found no consequential
contradiction in the session-first design. That is architectural review,
not runtime verification or approval of a future implementation.

## Alternatives and revisit conditions

| Alternative | Why it was considered | Current disposition |
|---|---|---|
| Rewrite provider logic in Go | One language for the TUI and its backend. | Declined: duplicates Swift services and works against the native-app direction. |
| Direct Go/Swift FFI | Avoid a process protocol. | Not selected: introduces language ABI/build/lifetime coupling while still needing a clear product boundary. No comparative performance benchmark was performed. |
| Parse CLI output or forward keystrokes | Appears to minimize Swift edits. | Not the real-flow design: terminal prose and TTY consent are unsuitable as a typed operation contract. |
| Full shared application framework first | Gives the future GUI a broad library immediately. | Deferred after scope clarification. Stage one adds only the session and targeted refactors it needs. |
| Persistent Swift manager and Unix socket now | Central ownership for simultaneous clients and long-lived jobs. | Deferred until jobs must survive all clients or shared ongoing operations are committed requirements. |
| XPC plus a native shared agent now | Fits native OS integration. | Deferred; use existing privileged XPC only for its existing fan scope. Native packaging can reassess transport. |
| Let the GUI own provider restarts | Common in some desktop apps. | Not selected: duplicates existing launchd/watchdog/updater authority and ties serving to an interface. |
| Fully design App Attest now | Might avoid later rework. | Deferred explicitly by the owner; retain a narrow replaceable verification dependency. |

## Open implementation choices and verification limits

The implementation agent still needs to select the exact Bubble Tea version,
opt-in entry flag, message schema, frame bounds, cancellation protocol, process
launch arrangement and narrow shared-file locking strategy. These are not
established by illustrative names or diagrams in the design. Validate a small
real workflow before expanding the contract. Do not expose a general RPC API.

The native-app stage still needs product choices about Quit wording, background
permission UX, settings coverage, earnings display and whether any job requires
continuous management without a frontend. Those choices should drive service
hosting rather than follow from it automatically.

Repository onboarding requirements remain binding, including human-operated
installer/reset/enrollment/login/start prompts and separate signed qualification.
Candidate preparation tooling had fork-specific expectations that need checking
before preparing a signed artifact. No release registration, production changes
or attestation bypass is authorized merely by this record.

Completed for this investigation: source review, external primary-source
research, technical challenge/review, diagrams and documentation checks.
Not completed: runtime changes, Go/Swift protocol tests, UI/PTY tests, signed
onboarding validation, native GUI work or App Attest integration. The linked
design contains the stage-one acceptance cases and separate-task handoff.
