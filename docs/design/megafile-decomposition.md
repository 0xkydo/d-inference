# Megafile decomposition — target file map and move-only rules

> Last updated: 2026-09-10 · commit `c4d8b7b9c`

Status: In progress — 2026-09-10 (Shard B2 of [repo-cleanup.md](repo-cleanup.md); sub-shards land as separate PRs)

## 1. Why

Seven coordinator files hold ~32k lines (`api/consumer.go` 4743, `api/dispatch.go`
3905, `api/provider.go` 3786, `api/server.go` 3765, `registry/registry.go` 6513,
`registry/scheduler.go` 3362, `store/postgres.go` 5852). They are the files most
often edited concurrently, the files where a reviewer cannot see a whole concern
at once, and the files where the inventory found unrelated clusters (billing
refunds next to SSE normalization next to HTTP handlers). Splitting them along
concern boundaries — *without changing a single declaration* — lowers review
cost and lets later shards own one concern each.

## 2. Rules (frozen for the whole shard)

1. **Move-only.** A decomposition commit moves top-level declarations between
   files in the same package. It never edits a declaration body, doc comment,
   name, signature, receiver, or order of statements. Import blocks are the only
   permitted textual change. `go/ast`-level verification: the multiset of
   printed top-level declarations (doc comment included, imports excluded) is
   identical before and after (`coordinator/cmd/movecheck`).
2. **Same package, same visibility.** No new packages, no export changes, no
   interface extraction. Cross-package restructuring is a different design.
3. **One megafile per commit.** Each commit is `refactor(<pkg>): split <file>
   into <n> files` and is verifiable in isolation with movecheck against its
   parent.
4. **Logic edits are separate commits, never mixed.** If a move reveals a
   defect, record it in the PR "Follow-ups" section; do not fix it in the
   decomposition PR.
5. **Tests stay put in this shard.** `*_test.go` files are not renamed or split
   here; grouping incident-named tests is a later, separate step (see §5).
6. **Naming.** New files take the megafile's stem as prefix:
   `consumer_stream.go`, `registry_capacity.go`, `postgres_billing.go`. Existing
   sibling conventions (`provider_*.go`, `admin_*.go`) are reused where a
   sibling already owns the concern. The original file keeps the entry-point
   handlers/constructors and anything that does not clearly belong to one
   concern.
7. **File header.** Each new file starts with a 1–3 line comment naming the
   concern it holds; the comment is the only text that is not a moved
   declaration.
8. **Freeze.** While a decomposition PR for a file is open, no other PR edits
   that file. Rebases of the decomposition PR are re-verified with movecheck.
9. **Gate.** `movecheck`, `gofmt -l`, `go build ./...`, `go vet ./...`,
   `go test ./coordinator/<pkg>/...`, `git diff --color-moved=dimmed-zebra
   --stat`.

## 3. Sub-shards (one PR each, in this order)

| Sub-shard | Files | PR |
|---|---|---|
| B2a | `api/consumer.go` | #6 |
| B2b | `api/dispatch.go`, `api/provider.go` | #7 |
| B2c | `api/server.go` | — |
| B2d | `registry/registry.go`, `registry/scheduler.go` | — |
| B2e | `store/postgres.go` | — |

## 4. Target file maps

Assignment rule for every declaration not named below: it goes to the target
file whose concern its *callers* belong to (single-caller helpers follow their
caller); if callers span two targets, it stays in the original file. Constants
and error sentinels travel with the code that reads them.

### 4.1 `api/consumer.go` → 8 files

| Target | Concern | Anchors (line ranges at `4742dc9a`) |
|---|---|---|
| `consumer.go` | Entry-point handlers and request/model resolution | `handleChatCompletions` 1828–2447, `handleCompletions`, `handleAnthropicMessages`, `handleGenericInference` 4386–4743, `resolveRequestedModel`, `aliasFallback*`, `maybeFallbackAlias`, `consumerModel`, `ensureMaxTokensBound`, `explicitMaxTokens`, `defaultMaxOutputTokens` |
| `consumer_dispatch.go` | Provider dispatch, reservation hand-off, cancel, provider-body preparation | consts 50–130 (timeouts, attempt ceilings, `speculativeTimerRatio`, `cancelWriteTimeout`), `sendProviderCancel`, `writeProviderInferenceRequestDeferred`, `cancelDispatch*`, `err*` sentinels 665–692, `attempt0RouteAnchor`, `routeDecisionRecorder`, `dispatchReserver`, `dispatchOneProvider`, `dispatchWithReserver`, `releaseUnsentDispatch`, `penaltySafeProviderVersion`, `visionPenaltyFields`, `bodyForProvider` … `bodyForCacheAttempt` 1408–1556 |
| `consumer_failover.go` | Circuit breakers, TTFT gate, Retry-After estimation, warm-pool nudges | `FirstContentDeadline`, `shedIfModelRejected`, `writeGenericProviderError`, `noteInferenceError`, `metricCapacityCooldownTripped`, `isRequestShapeBatchBudgetReject`, `noteInferenceSuccess`, `noteDispatchProviderError`, `failedProviderVersion`, `ttftTooSlow` … `rejectionSamplingParams` 819–934, `routeLatencyEWMAAlpha` … `writeServiceUnavailable` 1630–1708 |
| `consumer_billing.go` | Reservation cost, refunds, service-consumer checks | `refundProviderExtra`, `reservationCost`, `refundReservedBalance`, `providerHasPayoutDestination`, `providerPricingKeys`, `providerReservationCost`, `isServiceConsumer`, `reserveAdditionalForProvider`, `usdToMicro`, `microToUSD` |
| `consumer_stream.go` | SSE relay to the consumer, boilerplate hold, terminal-chunk finalization | `handleStreamingResponseWithFirstChunk*`, `writeChatStreamProviderError`, `handleResponsesStreamingResponseWithFirstChunk`, `handleNonStreamingResponseWithFirstChunk*`, `maxHeldBoilerplate`, `isSSEDoneEventGroup`, `stripSSEDoneEvents`, `isResponsesAPIEventChunk`, `isBoilerplateChunk`, `parseUsageOnlyStreamChunk`, `finalizeUsageChunk`, `parseFinishStreamChunk`, `finalizeFinishChunk`, `truncatedByMaxTokens`, `effectiveFinishReason`, `buildNonStreamingResponse` |
| `consumer_normalize.go` | Provider→OpenAI response normalization, reasoning/think handling, tool-call reassembly | `thinkBlockPattern`, `rewriteChunkModel`, `rewriteRawFinishReason`, `normalizeCompleteChatResponse` … `normalizedRawChoiceIndex` 3044–3315, `maxLogicalToolCalls`, `toolCallWireIndex`, `toolCallAccumulator` + methods, `extractedMessage`, `extractMessage*`, `injectReasoningDetailIntoRawUsage`, `resolveReasoningTokens` |
| `consumer_responses.go` | Chat-completion → Responses API envelope conversion | `buildResponsesUsage` … `chatCompletionToResponses` 3907–4085 |
| `consumer_account.go` | Non-inference consumer endpoints | `createAPIKeyRequest`, `handleHealth`, `handleVersion`, `handleBalance`, `handleUsage`, `handleProviderEarnings` |

### 4.2 `api/dispatch.go` → 6 files

| Target | Concern | Anchors (line ranges at `4742dc9a`) |
|---|---|---|
| `dispatch.go` | Per-request state, outcome enum, orchestrator | `dispatchOutcome` + `outcome*` consts 52–76, `dispatchTerminalFailure`, `dispatchState` 91–288 and its small accessors `traits`, `configurePending`, `excludedProviderIDs`, `shouldQueueCompatibleProvider`, `run` 3454–3734 |
| `dispatch_policy.go` | Kill switches and queue TTFT ceiling | `envTTFTTerminalReject`, `ttftTerminalRejectEnabled`, `envJinjaTerminalReject`, `jinjaTerminalRejectEnabled`, `jinjaTerminalRejectMessage`, `queueMaxTTFTMs` 340–392 |
| `dispatch_routing_outcome.go` | Routing-decision ledger writes and outcome updates | `routingOutcomeKey`, `recordRoutingDecision*`, `timingMsBetween`, `applyTimingDecomposition`, `commitFirstContent`, `successRoutingOutcomeFor`, `errorRoutingOutcome*`, `recordProviderBodyTooLargeRoute`, `routeOutcomeUsesProviderErrorText`, `providerReportedBudget`, `providerFailedRoutingOutcome*`, `queuedExitOutcome`, `closeQueuedAttempt`, `rejectionInfo*`, `dispatchRoutingAttempt`, `routingAttempt`, `currentOrCapturedRoutingAttempt`, `updateRoutingOutcome*`, `markSpeculativeLoser`, `updateSpeculative*`, `emitClientGone`, `rejectionReason*` consts 1812–1843, `errQueueDeadlineExpired` |
| `dispatch_failure.go` | Error latching, terminal-failure classification, failover stop rule | `setLastError`, `isGenuinePreContentFault`, `terminalFailureFromMessage`, `captureGenuineFault`, `currentTerminalFailure`, `terminalFailureForExhaustion`, `classifyExhaustedStatus`, `exhaustedDominance` + consts, `resolveDominantExhaustedStatus`, `noteProviderBodyTooLarge*`, `preflightLegacyCacheBust`, `latchProviderBodyTooLarge`, `setLastInferenceError`, `isTerminalClientErrorCode`, `dispatchErrorClass`, `noteDispatchRetry`, `noteProviderError`, `shouldStopFailover`, `latchJinjaTerminalReject`, `latchDeterministicLoser` |
| `dispatch_primary.go` | Provider selection + reservation for one attempt | `dispatchPrimary` 1224–1764 |
| `dispatch_wait.go` | Speculative TTFT-aware first-chunk wait, race arms, post-accept wait, committed write | section `---- Speculative TTFT-aware first-chunk wait ----`, `waitFirstChunk`, `runSpeculative`, `waitNoBackup`, `emptyCompletionPrecedesChunk`, `awaitPrimaryEmptyCompletion`, `awaitBackupEmptyCompletion`, `runRace`, `race*` 2949–3271, `waitAccepted`, `contentLatency`, `adjustLatencyForPrefill`, `shouldRecordReputationLatency`, `writeCommittedResponse` |

### 4.3 `api/provider.go` → 5 files

| Target | Concern | Anchors (line ranges at `4742dc9a`) |
|---|---|---|
| `provider.go` | WebSocket upgrade, session lifecycle, read loop, models update | `handleProviderWS`, `maxProviderVersionLength`, `sessionDisconnectReason`, `readErrorReason*`, `readErrorDisconnectReason`, `closeSessionWithReason`, `providerReadLoop` 234–842, `validLoadModelStatus`, `handleModelsUpdate`, `attachProviderLocation` |
| `provider_challenge.go` | Periodic attestation challenges and response verification | `DefaultChallengeInterval`, `ChallengeResponseTimeout`, `RegistrationAttestationMaxAge`, `RegistrationAttestationMaxFutureSkew`, `minProviderVersionForReconnectAttestation`, `MaxConsecutiveChallengeTimeoutsBeforeReconnect`, `pendingChallenge`, `challengeTracker` + methods, `CodeAttestResponseTimeout`, `challengeLoop`, `generateNonce`, `sendChallenge`, `handleAttestationResponse`, `verifyChallengeResponse` 1138–1659, `verificationSubmitPriority`, `applyChallengeRuntimePolicy`, `applyChallengeMinVersionPolicy`, `handleTransientChallengeFailure`, `handleChallengeFailure` |
| `provider_inference_msgs.go` | Inference-side provider messages: chunk, accepted, complete, inference_error | `cacheSelectionTerminalTags`, `emitCacheSelectionTerminal`, `cacheSelectionTTFTSample`, `emitCacheSelectionTTFT`, `handleChunk`, `chunkOverflowGrace`, `sendChunkWithGrace`, `decryptTextResponseChunk`, `errTextChunkViolation`, `textChunkViolationError` + method, `handleInferenceAccepted`, `maxPlausibleDecodeTPS`, `handleComplete`, `handleCompleteAt` 2031–2736, `handleInferenceError`, `handleInferenceErrorOwned` |
| `provider_attestation.go` | Secure Enclave / MDM / Apple device attestation and trust status | `verifyProviderAttestation` 3003–3221, `mdmVerifyOutcome` + consts, `verifyProviderViaMDM`, `ApplyLateSecurityInfo`, `stageDurableMDAChain`, `attachCachedMDAProof`, `verifyAppleDeviceAttestation`, `sendTrustStatus` |
| `provider_attestation_status.go` | Redacted trust-status HTTP endpoint and its cache | `providerAttestationCacheTTL`, `providerAttestationCacheKey`, `handleProviderAttestation` |

### 4.4 `api/server.go` → 7 files

| Target | Concern | Anchors (line ranges at `4742dc9a`) |
|---|---|---|
| `server.go` | `Server` struct, constructor, lifecycle, dependency setters, routes | `LatestProviderVersion`, `minProviderVersionForDesiredModels`, `latestReleasedVersion`, `approvedReleasePolicy`, `releaseTrustPolicySnapshot`, `Server` 199–541, `NewServer`, `handleRuntimeCapabilitiesPromoted`, `Close`, every `Set*`/getter that only assigns or reads a field (`SetAdminKey` … `SetMDMWebhookSecret` 990–1204, `SetProfileSigner`, `SetBilling`, `Billing`, `SetBaseRewards`, `BaseRewards`, `SetChallengeInterval`, `SetSkipChallenge`, `SetAllowDuplicateProviderSerialsForTesting`, `SetPrivyAuth`, `SetAdminEmails`, `SetMDMClient`, `StartMDMScheduler`, `SetCodeAttestor`, `SetCodeAttestationDeadline`, `SetTTFTHardReject`, `SetRejectModels`, `modelShed`, `SetMinDecodeTPS`, `SetServabilityGate`, `SetDisableClientErrorStop`, `SetLongPromptThreshold`, `SetLongPromptPrefillWeight`, `SetReleaseKey`, `SetCoordinatorKey`), `SyncModelCatalog`, `syncModelAliases`, `invalidateCatalogCache`, `resolveBaseURL`, `installScript`, `installScriptPlaceholder`, `routes` 2732–3015, `Handler`, `handleUnimplementedEndpoint`, `handleAdminMetrics` |
| `server_context.go` | Request-context keys and accessors | `contextKey`, `ctxKey*` consts, `requestIDFromContext`, `cryptoRand`, `consumerKeyFromContext`, `apiKeyFromContext`, `keyIDFromContext`, `keyLimitMicroFromContext`, `keyLimitResetFromContext`, `newRequestID`, `extractBearerToken` |
| `server_auth.go` | API-key cache and auth middlewares | `apiKeyCacheEntry`, `apiKeyCacheTTL`, `apiKeyCacheMaxSize`, `lookupAPIKeyCache`, `storeAPIKeyCache`, `invalidateAPIKeyCache`, `invalidateAllAPIKeyCache`, `requireAuth`, `requirePrivyAuth`, `readCacheJanitorInterval`, `StartReadCacheJanitor`, `runReadCacheJanitor` |
| `server_ratelimit.go` | Rate limiter wiring, token/key limits, 429 writers, limiter middlewares | `SetRateLimiter`, `SetFinancialRateLimiter`, `SetServiceRateLimiter`, `SetTokenLimiters`, `SetOutputAdmissionEstimator`, `SetKeyLimiters`, `applyTokenRateLimit*`, `outputAdmissionTags`, `reconcileOutputAdmission`, `writeTokenRateLimited`, `setTokenRateLimitHeaders`, `applyKeyRPMLimit`, `keyTokenParams`, `setRequestRateLimitHeaders`, `rateLimitConsumer`, `rateLimitFinancial`, `rateLimiterFn`, `financialRateLimiterFn`, `rateLimitWith`, `rateLimitWithTier`, `DefaultRoutingConcurrency`, `SetRoutingConcurrency`, `scanSlotResult` + consts, `acquireRoutingScanSlot`, `releaseRoutingScanSlot` |
| `server_middleware.go` | Body limits, recover, CORS, logging, status writer | `maxMDMWebhookBodyBytes`, `maxRequestBodyBytes`, `maxControlPlaneBodyBytes`, `HandleMDMWebhook`, `mdmWebhookTokenValid`, `bodyLimitMiddleware`, `decodeCappedJSON`, `recoverMiddleware`, `publicCORSPaths`, `corsMiddleware`, `loggingMiddleware`, `httpPathLabel`, `strconvItoa`, `statusWriter` + methods |
| `server_telemetry.go` | Emitter/Datadog wiring, emit helpers, gauge loop | `submitTelemetry`, `SetEmitter`, `SetDatadog`, `Datadog`, `Metrics`, `emit`, `emitRequest`, `ddIncr`, `ddCount`, `ddHistogram`, `ddGauge`, `emitPanic`, `registerDefaultGauges`, `StartDDGaugeLoop` |
| `server_release_policy.go` | Binary-hash policy, release evidence, runtime manifest | `SetBinaryHashEnforcement`, `SetKnownBinaryHashes`, `normalizeKnownBinaryHashes`, `AddKnownBinaryHashes`, `hasConfiguredHashInput`, `SyncBinaryHashes`, `convergeReleasePolicy*`, `releaseEvidenceStillApproved`, `evidence*` consts 1907–1917, `recordReleaseEvidenceOutcome`, `evidenceRejected`, `deriveApprovedReleaseTransition`, `releaseMetallibMatches`, `rebuildBinaryHashPolicyLocked`, `binaryHashPolicySnapshot`, `SyncRuntimeManifest`, `convergeRuntimeManifest*`, `revalidateConnectedProvidersAgainstRuntimePolicy`, `runtimeManifestApprovesMetallib`, `RuntimeManifest` + methods 2364–2448, `templateHashAccepted`, `sortedTemplateHashes`, `semverGreater`, `semverLess`, `SetRuntimeManifest`, `verifyRuntimeHashesForBackend`, `verifyRuntimeHashesAgainstManifest`, `handleRuntimeManifest` |

Note: `HandleMDMWebhook` stays with the body-limit constants it reads only if
its two callers are the router and tests; if `admin_*.go` siblings already
own MDM webhook code, place it there instead and record the deviation.

### 4.5–4.6

Maps for `registry.go`, `scheduler.go`, and `postgres.go` are
added to this record by the PR that opens each sub-shard, using the outline in
the cleanup inventory and the same assignment rule. A sub-shard PR may not
start until its map is in this file.

## 5. Deferred (explicitly out of this shard)

- Grouping incident-named tests (`*_regression_test.go`, ticket-named tests)
  by protected behavior. Requires a separate map and reviewer sign-off; test
  bodies are never deleted.
- Any package split (e.g. moving SSE normalization out of `api`).
- Fixing defects noticed during moves — recorded per PR as follow-ups.

## 6. Verification tool

`coordinator/cmd/movecheck` (added in B2a) parses two directories with
`go/parser`, prints every non-import top-level declaration with its doc
comment via `go/printer`, and diffs the multisets. Usage in review:

```bash
git worktree add /tmp/before <parent-sha>
go run ./coordinator/cmd/movecheck /tmp/before/coordinator/api coordinator/api
# expect: decls before=N after=N problems=0
```
