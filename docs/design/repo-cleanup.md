# d-inference cleanup — specification and shard plan

> Last updated: 2026-09-10 · commit `4742dc9ae`

Status: In progress — 2026-09-10

This design record defines the dependency-ordered cleanup shards and their
review gates. The supporting inventory is retained outside the repository.

## 0. What "cleanup, not rewrite" means here

The repo already has strong governance (AGENTS, docs/AGENTS.md freshness
stamps, the archived migration journal's "RETRACTED DELETIONS", CI guards against silently-skipped
Swift suites). The cleanup's job is to make the codebase *match* that governance
everywhere, remove drift, and reduce the cost of understanding it — while every
externally observable behavior (HTTP/WS protocol, billing math, attestation,
routing decisions, telemetry shapes, installer contract) stays byte-for-byte
identical unless a defect is explicitly named and covered by a regression test.

## 1. Principles (derived from the two posts, adapted)

From *Bun in Rust*:
1. **Spec before edits.** A mapping document (this file + per-shard design
   record) is written before workers touch code. Workers execute; they don't
   redesign.
2. **Tests are the contract; verify they *run*.** 3,265 Go test funcs, 682
   console vitest, 252 Swift test files. Before trusting green, confirm the
   test executed (no accidental skips, no `exclude`d dirs, no build-tag gating
   on Linux hiding a failure). Baseline is recorded per shard.
3. **Failures are a work queue, not a judgment call.** Baseline red items
   (§3) are fixed first, individually, with a root cause each — not bulk
   suppressed.
4. **Adversarial review that assumes the change is wrong.** Every shard gets a
   reviewer with a different lens than the implementer, looking specifically
   for compile-clean semantic drift (default values, error-path ordering, nil
   vs empty, retired-field decodability).
5. **Fix the process, not the symptom.** If a class of drift recurs (stale
   counts in docs, missing admin-ui coverage, or missing TypeScript coverage),
   add the check that makes it impossible, not just the one-time fix. PR #3
   added the missing UI jobs.

From *Agent-swarm model economics*:
6. **Tree of bounded shards; one owner per file.** No two shards touch the same
   file concurrently. Megafiles (§4) are explicitly *blocked* for workers until
   a decomposition design exists.
7. **Design decisions are central.** Naming, package boundaries, helper
   locations, "delete vs. keep" verdicts are made in this doc / design records,
   never independently by a worker → no split-brain.
8. **Field guide over folklore.** Institutional knowledge discovered during
   cleanup lands in AGENTS.md (single home; CLAUDE.md becomes a pointer) so it
   compounds instead of being rediscovered.
9. **Measure by behavior, not activity.** Success = same tests pass + new
   guards in CI + fewer lines/files/concepts to understand; not commit count.

## 2. Hard rules (non-negotiable for every shard)

R1 **Deletion gate** (the migration journal's RETRACTED DELETIONS, made mandatory). Nothing
   is deleted unless the PR records evidence for *all* of: symbol grep incl.
   `Tests/` and `*_test.go`; accessor/method/inferred-type use; protocol/
   interface/Codable/JSON-tag requirements; config defaults *and* shipped
   manifests (`deploy/`); scripts/docs/workflows/Makefile; string-form
   references (reflection, route strings, env var names). Compiler tools
   (`deadcode`, knip) are leads, not proof.
R2 **Wire mirrors are frozen** unless the shard is explicitly a protocol
   shard: `coordinator/protocol` ↔ `ProviderCore/Protocol`; telemetry types
   (Go/Swift/`telemetry-types.ts`); `toolschema.go` ↔ `ToolSchemaNormalization`;
   `encryption.ts`. Retired fields stay decodable.
R3 **Behavior invariants listed in AGENTS.md are test-protected, not
   "simplified"**: Slots-authoritative/WarmModels-fallback, 1h idle unload,
   250 ms chunk-overflow grace, 120 s queue timeout, pre-content failover,
   one-image-at-a-time vision, MLX fault check after eval, cooldown 2-in-60s.
R4 **No test is weakened to pass.** A failing test is either a real bug (fix
   code + keep test) or a provably wrong expectation (fix test, cite the
   source-of-truth line in the PR). Never both in one commit without saying so.
R5 **Megafiles are decomposed by move-only commits.** Extractions are
   `git mv`-style cut/paste into sibling files in the *same package* with zero
   logic edits; `gofmt`/`go vet`/`go test` unchanged. Renames or logic changes
   go in separate commits so review can diff them independently.
R6 **Docs follow code in the same PR** (docs/AGENTS.md): stamps, build.md/
   test.md for any CI/script/Make change, Mermaid before/after in PR body.
R7 **Linux-verifiable only, except grep-verifiable Swift.** Swift/MLX cannot
   build here; Swift changes are limited to things a reviewer can verify by
   reading (dead imports, doc drift, duplicated pure helpers) and must be
   labelled "needs macOS CI" in the PR.
R8 **No production, release, deploy, or secrets changes.** Ever.

## 3. Baseline (measured on this Linux box; see 00-environment.md)

| Check | Result |
|---|---|
| `go build`, `go vet` | pass |
| `go test ./...` | **FAIL** before PR #3: `coordinator/api TestInstallScriptTemplating/enrollment_excludes_hardware_identity` ("install.sh must delegate enrollment to the CLI onboarding flow") |
| Rust sidecar | pass |
| console-ui eslint | pass, 76 warnings |
| console-ui `tsc` | **FAIL** before PR #3 (~20 errors: dashboard fixtures vs `MyProvider`/`MyReputation`, DatadogRUM user type, Privy `createOnLogin`, `HeadersInit`, vitest.config) |
| console-ui vitest | **679/682** before PR #3, 3 failed in `__tests__/payouts.test.tsx` (withdraw copy/analytics) |
| admin-ui eslint/tsc/vitest | pass before PR #3 |
| docs-check | pass (137 files) |
| golangci-lint (unused/ineffassign/govet) | not runnable here; CI-only |
| staticcheck | 1 real hit before PR #3: redundant `sessEnd` initialization in `store/memory_base_rewards.go`; rest is nhooyr/websocket deprecation noise |
| Fork CI | last runs on `master` **cancelled after 24h** (runner never picked up) → CI is currently not a safety net on this fork |

PR #3 addressed the stale installer and UI expectations, corrected the store
initialization, and added the missing UI CI coverage. Its fork checks remained
queued without starting, so local gates remain the merge gate until the
`blacksmith-*` runner infrastructure is fixed.

## 4. Structural findings that drive the shard plan

- `coordinator/api` is a 396-file single package with 252 test files
  (1,514 test funcs). It's the megapackage: `consumer.go` 4.7k, `dispatch.go`
  3.9k, `provider.go` 3.8k, `server.go` 3.8k lines. Test files are heavily
  fragmented by incident (`*_w5fix2_test.go`, `*_followups_test.go`,
  `edge_*_test.go`) — naming records history, not the behavior protected.
- `coordinator/registry/registry.go` 6.5k lines, `scheduler.go` 3.4k;
  `store/postgres.go` 5.9k.
- `CLAUDE.md` (332 lines) and `AGENTS.md` (300 lines) overlapped heavily; the
  former is now a pointer and the latter is canonical.
- Root/journal: the paged-KV migration journal was a working log with
  load-bearing deletion guidance buried inside it; it is now an archived report.
- Scripts: `scripts/cli-preview/`, `scripts/gemma_contbatch/`,
  `scripts/gptoss_profile/`, `benchmark-light.py`, MTP scripts — no literal
  references from Makefile/CI/docs. Owner confirmation needed (R1).
- 26 jscpd clones: Next.js API-route proxy scaffolding, provider pages,
  earnings/payout formatters (console + admin), MTP python scripts.
- `docs/reports/` 111 files, 2026-06-15 → 09-07, plus undated JSON/CSV/PNG
  evidence; immutable by docs rules, but the *index* and freshness should be
  checked.
- Duplicated small Go helpers (`envDuration/envOr/envTrue`, clamp/min/max)
  across registry/datadog/api despite an existing `coordinator/env` package.

## 5. Shard plan (dependency order; each = one PR, one reviewer lens)

**Shard 0 — Make the safety net real** (blocks everything)
- Fix `TestInstallScriptTemplating` (decide: install.sh drifted from contract,
  or contract stale after Bubble Tea onboarding merge — read both; R4).
- Fix console `tsc` errors and 3 payouts tests at root cause.
- Add to `ci.yml`: console `npx tsc --noEmit` + `npm test`; admin-ui
  lint/tsc/test job. Document in `docs/developer/test.md`.
- Fix SA4006 in `memory_base_rewards.go` (inspect first — may be a real bug).
- Investigate why fork CI runs get cancelled (macOS runner availability?);
  if Swift lanes can't run on this fork, make that explicit rather than
  silently red.
- Review lens: "does every test that should run, run?"

**Shard A — Root, guidance, docs hygiene**
- Make `AGENTS.md` the single source and `CLAUDE.md` a pointer. Fix stale
  facts (E2E wording, package paths, and 120-second queue versus one-hour idle
  terminology).
- Promote the migration journal's RETRACTED DELETIONS into AGENTS.md as the
  deletion gate and archive the journal under `docs/reports/`.
- Deduplicate `.gitignore`, update indexes, and remove genuine docs path
  false positives.
- Review lens: "is every claim in the guidance grep-verifiable in code?"

**Shard B1 — Coordinator low-risk**
- Consolidate env helpers onto `coordinator/env`; clamp/min/max → Go 1.21
  builtins `min`/`max` where types allow (move-only + mechanical).
- Triage `deadcode` 8 candidates with R1 evidence each; delete only proven.
- Websocket deprecation: *do not* migrate `nhooyr.io/websocket` → `coder/
  websocket` in this effort (behavior risk on the relay path); record as a
  follow-up design decision instead.
- Review lens: adversarial "semantic drift in helpers" (int vs float
  parsing, default on parse error, empty vs unset).

**Shard B2 — Coordinator megafile decomposition (move-only, R5)**
- Design record first (`docs/design/`): target file map for `api/consumer.go`,
  `dispatch.go`, `provider.go`, `server.go`, `registry/registry.go`,
  `store/postgres.go` — grouped by the concern seams already visible
  (routing / streaming relay / normalization / billing-commit; registration /
  heartbeat / attestation / relay; migrations / queries per aggregate).
- One file per PR-commit; `git diff --color-moved` must show pure moves.
- Test-file rationalization: group incident-named tests under behavior-named
  files *without changing test bodies* (move-only), so the protected
  behavior is discoverable. No test deleted.
- Review lens: "is every hunk a pure move?" (mechanical, tool-assisted).

**Shard C — console-ui / admin-ui / landing**
- Shared Next API-route proxy helper for the cloned routes; shared
  money/format helpers (console `lib/format/*` vs admin `lib/format.ts`
  — decide one home; admin may import via path alias or copy stays with a
  sync comment—decision recorded).
- Knip candidates (`WorkspacePreview.tsx`, `earn/calc.ts` exports) with R1
  evidence; wire-contract files (`telemetry-types.ts`, `encryption.ts`) are
  frozen (R2).
- Burn down the 76 eslint warnings or justify each rule.
- Review lens: UI behavior unchanged — vitest + build + manual smoke of chat,
  billing, providers dashboard.

**Shard D — scripts/**
- Owner decision per unreferenced cluster (cli-preview, gemma_contbatch,
  gptoss_profile, MTP, benchmark-light): keep-and-document (README + Make
  target or docs/developer link) vs. archive. Nothing deleted without an
  explicit answer from the user.
- Dedupe MTP python and shell `fail/require_cmd/cleanup` helpers.

**Shard E — provider-swift (grep-verifiable only, R7)**
- Doc/comment drift vs. coordinator, dead imports, duplicated pure helpers
  visible by reading. No logic, MLX, cache, or attestation edits. Everything
  labelled "needs macOS CI".

**Shard F — process guards (Bun principle 5)**
- docs-check extension: a `counts` stamp check or removal of hard-coded
  counts from guidance; CI job that fails if a `*_test.go` package has zero
  executed tests on Linux (guard against build-tag hiding); golangci-lint
  config gains `staticcheck` with the websocket deprecation excluded
  explicitly (so the one real SA4006-class hit can't hide again).

## 6. Execution model

- Each shard is one PR, merged in dependency order. The lead writes the shard's
  design record and done-criteria; the implementer executes; a separate
  adversarial pass reviews with the shard's stated lens. Shards B1/C/D can run
  in parallel when their files are disjoint; B2 is serialized per megafile.
- Megafiles remain frozen for other shards while their decomposition PR is
  open. Unreferenced scripts require PR archaeology; remove them only when a
  replacement superseded them.
- Per-PR gate: `gofmt`, `go vet`, `go test ./...` (race in CI), sidecar
  `cargo test`, `npm run lint && tsc && npm test` for touched UIs, `make
  docs-check`, plus the shard's own behavioral check. Mermaid before/after in
  the PR body.
- Trial run first: Shard 0 doubles as the calibration shard — if the process
  (spec → implement → adversarial review) surfaces issues there, rules get
  amended before wider shards start.
