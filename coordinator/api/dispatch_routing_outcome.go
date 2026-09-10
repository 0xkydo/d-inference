package api

// Routing-decision ledger writes and dispatch outcome updates.

import (
	"net/http"
	"strings"
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/saferun"
	"github.com/eigeninference/d-inference/coordinator/store"
)

// routingOutcomeKey returns a stable requestID + attempt identifier used for
// telemetry updates. It prefers the explicit dispatch requestID, falling back
// to the pending request's ID when the dispatch requestID has not been set yet.
func (d *dispatchState) routingOutcomeKey() string {
	if d.requestID != "" {
		return d.requestID
	}
	if d.pr != nil {
		return d.pr.RequestID
	}
	return ""
}

// recordRoutingDecision writes a best-effort snapshot of the scheduler decision
// for the current attempt. It never blocks inference.
func (d *dispatchState) recordRoutingDecision(decision registry.RoutingDecision, dispatchErr, outcomeOverride string) {
	d.recordRoutingDecisionFor(d.provider, d.pr, d.routingOutcomeKey(), d.attempt, decision, dispatchErr, outcomeOverride)
}

func (d *dispatchState) recordRoutingDecisionFor(provider *registry.Provider, pr *registry.PendingRequest, requestID string, attempt int, decision registry.RoutingDecision, dispatchErr, outcomeOverride string) {
	s := d.s
	if requestID == "" && pr != nil {
		requestID = pr.RequestID
	}

	providerID := ""
	if provider != nil {
		providerID = provider.ID
	} else if decision.ProviderID != "" {
		providerID = decision.ProviderID
	}

	outcome := outcomeOverride
	if outcome == "" {
		switch {
		case providerID != "":
			outcome = "selected"
		case dispatchErr == errModelTooLarge:
			outcome = "model_too_large"
		case dispatchErr == errTTFTTooSlow:
			outcome = "ttft_429"
		case dispatchErr == "no provider available":
			outcome = "no_provider"
		default:
			outcome = "error"
		}
	}

	keyID := ""
	if pr != nil {
		keyID = pr.KeyID
	}

	// Scans per attempt (rescans included). Plan-based retries reuse the
	// previous scan and report zero, which is not emitted.
	if decision.ScanCount > 0 {
		s.ddCount("routing.scans", int64(decision.ScanCount), []string{"model:" + d.model, "outcome:" + outcome})
	}

	record := &store.InferenceRouteRecord{
		RequestID:               requestID,
		Attempt:                 attempt,
		ProviderID:              providerID,
		Model:                   d.model,
		PublicModel:             d.publicModel,
		ConsumerKeyHash:         store.HashKey(d.consumerKey),
		KeyID:                   keyID,
		Outcome:                 outcome,
		CostMs:                  decision.CostMs,
		StateMs:                 decision.StateMs,
		QueueMs:                 decision.QueueMs,
		PendingMs:               decision.PendingMs,
		BacklogMs:               decision.BacklogMs,
		ThisReqMs:               decision.ThisReqMs,
		HealthMs:                decision.HealthMs,
		TTFTMs:                  decision.TTFTMs,
		BestTTFTMs:              decision.BestTTFTMs,
		EffectiveQueue:          decision.EffectiveQueue,
		CandidateCount:          decision.CandidateCount,
		CapacityRejections:      decision.CapacityRejections,
		ModelTooLargeRejections: decision.ModelTooLargeRejections,
		VisionRejections:        decision.VisionRejections,
		TTFTRejections:          decision.TTFTRejections,
		EffectiveTPS:            decision.EffectiveTPS,
		StaticTPS:               decision.StaticTPS,
		EstimatedPromptTokens:   d.estimatedPromptTokens,
		RequestedMaxTokens:      d.requestedMaxTokens,
		RequiresVision:          d.requiresVision,
		HasTools:                d.hasTools,
		SelfRouteOnly:           d.policy.enabled,
		PreferOwner:             d.policy.prefer,
		CreatedAt:               time.Now(),
		UpdatedAt:               time.Now(),
	}

	if provider != nil {
		provider.Mu().Lock()
		record.ProviderStatus = string(provider.Status)
		record.ProviderTrustLevel = string(provider.TrustLevel)
		record.ProviderVersion = provider.Version
		record.HardwareChip = provider.Hardware.ChipName
		record.HardwareChipFamily = provider.Hardware.ChipFamily
		record.HardwareTier = provider.Hardware.ChipTier
		record.MemoryGB = provider.Hardware.MemoryGB
		record.GPUCores = provider.Hardware.GPUCores
		record.CPUCores = provider.Hardware.CPUCores.Total
		record.SystemMemoryPressure = provider.SystemMetrics.MemoryPressure
		record.SystemCPUUsage = provider.SystemMetrics.CPUUsage
		record.SystemThermalState = provider.SystemMetrics.ThermalState
		if cap := provider.BackendCapacity; cap != nil {
			record.GPUMemoryActiveGB = cap.GPUMemoryActiveGB
			record.GPUMemoryPeakGB = cap.GPUMemoryPeakGB
			record.GPUMemoryCacheGB = cap.GPUMemoryCacheGB
			for _, slot := range cap.Slots {
				if slot.Model == d.model {
					record.SlotState = slot.State
					record.BackendRunning = slot.NumRunning
					record.BackendWaiting = slot.NumWaiting
					record.ActiveTokenBudgetUsed = slot.ActiveTokenBudgetUsed
					record.ActiveTokenBudgetMax = slot.ActiveTokenBudgetMax
					record.QueuedTokenBudget = slot.QueuedTokenBudget
					break
				}
			}
		}
		provider.Mu().Unlock()
	}

	// Phase-0 shadow TTFT admission/spread metrics. No-op unless the request was
	// evaluated (admission mode != off AND a provider was selected). Emitted on
	// the synchronous path (cheap counter incr), not inside the async store write.
	s.emitTTFTShadowMetrics(d.model, decision)
	if decision.CacheDiscountMs > 0 {
		s.ddIncr("routing.cache_evaluation", []string{
			"mode:active",
			"tier:" + lowCardinalityCacheTier(decision.CacheTier),
		})
	}

	// Off the request path: the batching sink coalesces this snapshot with its
	// neighbours into one multi-row write (route_telemetry_submit.go).
	s.submitRouteRecord(record)
}

// timingMsBetween returns the elapsed milliseconds between two request-lifecycle
// timestamps, or 0 when either endpoint is unset or the interval is non-positive.
// It keeps the latency-decomposition fields defensive: never a negative value,
// never a panic on a zero timestamp.
func timingMsBetween(a, b time.Time) float64 {
	if a.IsZero() || b.IsZero() || !b.After(a) {
		return 0
	}
	return float64(b.Sub(a).Milliseconds())
}

// applyTimingDecomposition fills the coordinator-side latency-decomposition
// fields (ParseMs..DispatchMs) on a routing outcome from the per-request timing
// stamps. Each segment is populated only when both of its endpoints are set
// (timingMsBetween returns 0 otherwise), so a partially-instrumented request
// never records a negative or bogus segment. QueueWaitMs is 0 for requests that
// were dispatched without queueing (QueuedAt unset).
//
// firstChunk is passed in (not read from t.FirstChunkAt) so this can also be
// called from the provider read-loop goroutine (handleComplete) with a value
// obtained via PendingRequest.FirstChunkAtSafe; t.FirstChunkAt itself must only
// be read directly by the dispatch goroutine that owns the request.
func applyTimingDecomposition(out *store.InferenceRouteOutcome, t *registry.RequestTiming, firstChunk time.Time) {
	if out == nil || t == nil {
		return
	}
	out.ParseMs = timingMsBetween(t.ReceivedAt, t.ParsedAt)
	out.ReserveMs = timingMsBetween(t.ParsedAt, t.ReservedAt)
	// Remote-media fetch (when it happened) sits between ReservedAt and
	// RoutedAt; anchor the route segment past it so a multi-second download
	// doesn't masquerade as routing latency. The fetch duration itself is
	// reported via the X-Timing header and DD histogram (no outcome column).
	routeAnchor := t.ReservedAt
	if !t.MediaFetchedAt.IsZero() {
		routeAnchor = t.MediaFetchedAt
	}
	out.RouteMs = timingMsBetween(routeAnchor, t.RoutedAt)
	out.EncryptMs = timingMsBetween(t.RoutedAt, t.EncryptedAt)
	out.QueueWaitMs = timingMsBetween(t.QueuedAt, t.DispatchedAt)
	out.DispatchMs = timingMsBetween(t.DispatchedAt, firstChunk)
}

// commitFirstContent records the first CONTENT chunk on the committed attempt and
// stamps FirstContentAt (the actual_ttft_ms anchor) in the SAME instant, on the
// dispatch goroutine that reads the chunk. Stamping HERE — rather than later in
// writeCommittedResponse — guarantees FirstContentAt is set before ANY route
// outcome is built for this attempt: the committed/success outcome written by
// this goroutine (e.g. waitFirstChunk / waitAccepted's defer) AND the terminal
// completeRouteOutcome written concurrently by handleComplete on the provider
// read-loop. Without it a fast single-chunk completion could persist
// actual_ttft_ms as 0/NULL (applyPendingRouteTelemetry derives it solely from
// FirstContentAt). pr is the COMMITTED attempt — the backup on a speculative
// backup win, the primary otherwise. MarkFirstChunkArrived is kept (idempotent:
// it preserves an earlier preamble's first-byte time for dispatch_to_first_chunk_ms).
func (d *dispatchState) commitFirstContent(pr *registry.PendingRequest, chunk string) {
	d.firstChunk = chunk
	pr.MarkFirstChunkArrived()
	pr.MarkFirstContentArrived()
	d.stampFirstContent(pr)
	// Mark THIS attempt as the committed one so handleComplete's fallback only
	// ever stamps FirstContentAt for the attempt that actually delivered content —
	// never a late-completing abandoned/retried attempt sharing the same Timing.
	pr.MarkContentCommitted()
	d.s.observeTTFTCalibration(pr)
	// First CONTENT chunk == the provider ACCEPTED and is serving: clear the
	// pair's capacity-reject streak NOW rather than at completion. A long
	// generation on a busy box must keep vouching for the pair while the box
	// legitimately sheds concurrent dispatches — waiting for the completion
	// accept (noteInferenceSuccess) would let transient fullness masquerade as
	// the zero-accepts black-hole signature. See registry/capacity_cooldown.go.
	//
	// The recorder takes the registry WRITE lock, which in production waits
	// behind every queued writer (~190 ms at the median, seconds at the tail),
	// and this runs BEFORE the chunk is written to the client. It is pure
	// bookkeeping, so it runs off this goroutine and the first byte no longer
	// waits for it. Exactly-once for the capacity-503 RATE window is kept by
	// stamping the request BEFORE the recorder runs: the completion-time
	// re-offer (noteInferenceSuccess) fires only for an unstamped request, and
	// the recorder declines to store an offered accept only when rate tracking
	// is disabled (PenaltyMs <= 0) — in which case the completion re-offer
	// would store nothing either. So the unconditional stamp never loses an
	// outcome and never double counts.
	//
	// The accept carries the instant it was OBSERVED — the first content
	// chunk, stamped above by MarkFirstContentArrived — not the instant the
	// goroutine finally holds the lock: a capacity reject for the same pair
	// recorded in between happened AFTER this accept and must survive it
	// (registry.RecordCapacityAcceptObserved).
	pr.MarkRateOutcomeCounted()
	providerID, model := pr.ProviderID, pr.Model
	observedAt := pr.FirstContentAtSafe()
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	saferun.Go(d.s.logger, "api.recordCapacityAccept", func() {
		d.s.registry.RecordCapacityAcceptObserved(providerID, model, observedAt, true)
	})
}

func (d *dispatchState) successRoutingOutcomeFor(pr *registry.PendingRequest) *store.InferenceRouteOutcome {
	return committedRouteOutcome(pr)
}

// errorRoutingOutcome builds an error / timeout / cancelled outcome.
func (d *dispatchState) errorRoutingOutcome(status, class string, code int) *store.InferenceRouteOutcome {
	return d.errorRoutingOutcomeFor(d.pr, status, class, code)
}

func (d *dispatchState) errorRoutingOutcomeFor(pr *registry.PendingRequest, status, class string, code int) *store.InferenceRouteOutcome {
	providerReason, errorText := "", ""
	if routeOutcomeUsesProviderErrorText(class) {
		providerReason = d.lastErrReason
		errorText = d.lastErr
	}
	out := routeOutcomeWithReason(status, class, code, providerReason, errorText)
	applyPendingRouteTelemetry(out, pr)
	return out
}

func (d *dispatchState) recordProviderBodyTooLargeRoute(
	provider *registry.Provider,
	pr *registry.PendingRequest,
	decision registry.RoutingDecision,
) {
	if provider == nil || pr == nil {
		return
	}
	d.recordRoutingDecisionFor(
		provider, pr, pr.RequestID, pr.Attempt, decision, "", "")
	d.s.updateInferenceRouteOutcomeForPending(pr, dispatchFailedPendingRouteOutcome(
		pr, errorClassClientError, http.StatusRequestEntityTooLarge))
}

func routeOutcomeUsesProviderErrorText(class string) bool {
	class = strings.ToLower(strings.TrimSpace(class))
	return class == errorReasonProviderError ||
		class == errorClassDeadlineUnreachable ||
		// client_error rows keep the provider-supplied reason too: a jinja_*
		// template-render failure is recorded as class client_error (not a
		// provider fault) but its reason must stay jinja_* on the row, so the
		// inference.error{reason:jinja_*} series measures real render failures
		// instead of being silenced by the reclassification. The reason is
		// still whitelisted downstream (normalizeInferenceErrorReason).
		class == errorClassClientError ||
		strings.HasPrefix(class, "provider_error") ||
		strings.HasPrefix(class, "provider_disconnect") ||
		strings.Contains(class, "provider_incomplete")
}

// providerReportedBudget reads a provider's reported token budget for a model,
// tolerating a nil provider (returns 0 = unknown).
func providerReportedBudget(provider *registry.Provider, model string) int64 {
	if provider == nil {
		return 0
	}
	return provider.ReportedTokenBudgetMaxForModel(model)
}

// providerFailedRoutingOutcome builds the outcome for a POST-DISPATCH provider
// failure: the request had already been admitted to a specific provider (passed
// the admission gate and was dispatched over the WebSocket) and that provider
// then reported an error — including provider-reported OOM / model-load failures
// that surface on pr.ErrorCh. It flags AdmittedButFailed to expose the
// admission-gate mismatch (coordinator said "this provider can serve" but it
// could not). It is intentionally only used from the post-dispatch wait loops;
// pre-dispatch failures (queue reservation DB error, invalid key, keygen, send
// failure) and coordinator-side timeouts are NOT flagged.
func (d *dispatchState) providerFailedRoutingOutcome() *store.InferenceRouteOutcome {
	return d.providerFailedRoutingOutcomeFor(d.pr)
}

func (d *dispatchState) providerFailedRoutingOutcomeFor(pr *registry.PendingRequest) *store.InferenceRouteOutcome {
	if isDeadlineUnreachableErrorReason(d.lastErrReason) {
		// The provider declined work before execution because the coordinator's
		// remaining absolute budget could not be met. Preserve the typed reason
		// without marking the provider as admitted-but-failed.
		out := d.errorRoutingOutcomeFor(
			pr, "error", errorClassDeadlineUnreachable, d.lastErrCode)
		applyAttemptUsage(out, d.lastErrAttemptUsage)
		return out
	}
	if isTerminalClientErrorCode(d.lastErrCode) || isNonProviderFaultErrorReason(d.lastErrReason) {
		// Deterministic non-provider fault: a 4xx status the provider maps for
		// malformed bodies, OR a structured non-provider-fault reason (jinja_*
		// template-render failures, tool_noncompliance model-output 422s).
		// Record as client_error WITHOUT AdmittedButFailed so neither pollutes
		// the admission-mismatch gauge — keyed on the SAME vocabulary as the
		// reputation and breaker exemptions (isNonProviderFaultErrorReason).
		// The structured reason survives on the row (see
		// routeOutcomeUsesProviderErrorText). Typed partial usage (if any)
		// still lands on the row — observability only, no billing effect.
		out := d.errorRoutingOutcomeFor(pr, "error", errorClassClientError, d.lastErrCode)
		applyAttemptUsage(out, d.lastErrAttemptUsage)
		return out
	}
	class := "provider_error"
	if d.lastErrCoordinatorCause.IsProviderDisconnect() {
		class = "provider_disconnect_pre_commit"
	}
	out := d.errorRoutingOutcomeFor(pr, "error", class, d.lastErrCode)
	out.AdmittedButFailed = true
	// Pre-content typed failures on the ordinary dispatch path flow through
	// the deferred route update via this builder (not the standalone
	// preResponse/postCommit constructors), so the typed attempt_usage
	// retained by setLastInferenceError must be applied here too or the row
	// records null token counts for the most common failure path.
	applyAttemptUsage(out, d.lastErrAttemptUsage)
	return out
}

// queuedExitOutcome records the terminal route outcome of a queue-wait exit
// and mirrors its status/reason onto the placeholder attempt profile. While
// the request waits, d.pr is nil, so updateRoutingOutcome takes the request-id
// path and never reaches the attempt profile; the pair is written here, in one
// place, so the row and the profile cannot drift. It is deliberately NOT
// routed through updateInferenceRouteOutcomeForPending, which would also fire
// the cache-selection terminal for a request that never had a provider.
func (d *dispatchState) queuedExitOutcome(ap *registry.AttemptProfile, status, reason string, code int) {
	outcome := d.errorRoutingOutcome(status, reason, code)
	// No provider attempt was dispatched: the funnel counts this exit on
	// inference.queue_outcome, never on inference.attempt_outcome.
	outcome.QueueExit = true
	d.updateRoutingOutcome(outcome)
	ap.SetOutcome(status, reason, "", "", "")
}

// closeQueuedAttempt closes the queue-path placeholder attempt when it never
// reached the wire (closeUndispatchedAttempt is a no-op for a dispatched or
// winning attempt), recording the error the failing branch left on d.
//
// AttemptProfile.SetOutcome is first-write-wins, and every queue-path exit
// has already written its final_status/error_reason on the placeholder by the
// time this runs: the pre-assignment exits (queue full, client gone, deadline,
// ttft_too_slow, tool constraint, queue timeout) write it explicitly
// (queuedExitOutcome / the queue-full SetOutcome), and the post-assignment
// exits (top-up, key, encrypt, writer timeout, write error) write it through
// the pending route-outcome funnel because d.pr is set by then. This close
// therefore contributes provider_outcome=not_dispatched, and its own
// status/class only as a fallback for an exit that recorded nothing. A status
// code of 0 means the branch had no HTTP status: the code is defaulted by how
// the wait ended, but the text only when nothing was recorded, so a real error
// text with no code (e.g. "no provider with E2E encryption") keeps its own
// class instead of collapsing to queue_rejected.
func (d *dispatchState) closeQueuedAttempt(ap *registry.AttemptProfile) {
	errText, code := d.lastErr, d.lastErrCode
	if code == 0 {
		clientGone := d.r != nil && d.r.Context().Err() != nil
		if clientGone {
			code = 499
		} else {
			code = http.StatusTooManyRequests
		}
		if errText == "" {
			if clientGone {
				errText = "client_gone"
			} else {
				errText = "queue_rejected"
			}
		}
	}
	closeUndispatchedAttempt(ap, errText, code)
}

func (d *dispatchState) rejectionInfo(stage, reason string, status, retryAfterMs int) rejectionInfo {
	info := rejectionInfo{
		r:                     d.r,
		stage:                 stage,
		reasonCode:            reason,
		httpStatus:            status,
		keyID:                 keyIDFromContext(d.r.Context()),
		consumerKeyHash:       store.HashKey(d.consumerKey),
		requestedModel:        d.publicModel,
		resolvedModel:         d.model,
		stream:                d.stream,
		estimatedPromptTokens: d.estimatedPromptTokens,
		requestedMaxTokens:    d.requestedMaxTokens,
		requiresVision:        d.requiresVision,
		hasTools:              d.hasTools,
		selfRouteOnly:         d.policy.enabled,
		preferOwner:           d.policy.prefer,
		retryAfterMs:          retryAfterMs,
	}
	if reason == "payload_too_large" {
		info.servabilityComputed = true
		if d.providerBodyTooLargeBytes > 0 {
			info.requestBodyBytes = d.providerBodyTooLargeBytes
		}
	}
	return info
}

func (d *dispatchState) rejectionInfoWithDecision(stage, reason string, status, retryAfterMs int, decision registry.RoutingDecision) rejectionInfo {
	info := d.rejectionInfo(stage, reason, status, retryAfterMs)
	info.servabilityComputed = true
	info.candidateCount = decision.CandidateCount
	info.capacityRejections = decision.CapacityRejections
	info.modelTooLargeRejections = decision.ModelTooLargeRejections
	info.visionRejections = decision.VisionRejections
	info.bestTTFTMs = decision.BestTTFTMs
	return info
}

// dispatchRoutingAttempt is immutable identity captured before a wait path can
// clear or promote mutable dispatchState provider/request fields.
type dispatchRoutingAttempt struct {
	provider  *registry.Provider
	pending   *registry.PendingRequest
	requestID string
	attempt   int
}

func routingAttempt(provider *registry.Provider, pr *registry.PendingRequest, requestID string, attempt int) dispatchRoutingAttempt {
	return dispatchRoutingAttempt{provider: provider, pending: pr, requestID: requestID, attempt: attempt}
}

func (d *dispatchState) currentOrCapturedRoutingAttempt(captured dispatchRoutingAttempt) dispatchRoutingAttempt {
	if d.pr == nil {
		// A cleared request ID is an intentional no-op sentinel: speculative
		// sub-waits clear all three fields after recording each racer's terminal
		// outcome themselves. Restoring captured here would attribute the
		// surviving racer's later failure or timeout to the already-finalized
		// primary. Ordinary single-attempt fallbacks retain requestID and still
		// use captured below.
		if d.requestID == "" {
			return dispatchRoutingAttempt{}
		}
		return captured
	}
	return routingAttempt(d.provider, d.pr, d.routingOutcomeKey(), d.attempt)
}

func (d *dispatchState) updateRoutingOutcomeForAttempt(target dispatchRoutingAttempt, outcome *store.InferenceRouteOutcome) {
	requestID, attempt := target.requestID, target.attempt
	if requestID == "" {
		return
	}
	providerMatches := target.provider == nil ||
		(target.pending != nil && target.pending.ProviderID != "" && target.pending.ProviderID == target.provider.ID)
	if target.pending != nil && target.pending.RequestID == requestID && target.pending.Attempt == attempt && providerMatches {
		d.s.updateInferenceRouteOutcomeForPending(target.pending, outcome)
		return
	}
	d.s.updateInferenceRouteOutcomeWithModel(requestID, attempt, d.model, outcome)
}

// updateRoutingOutcome writes an outcome update for the current attempt. It is
// a no-op when there is no request ID to correlate.
func (d *dispatchState) updateRoutingOutcome(outcome *store.InferenceRouteOutcome) {
	requestID := d.routingOutcomeKey()
	if requestID == "" {
		return
	}
	// Capture attempt on the dispatch goroutine: the closure runs on a telemetry
	// sink worker, while run()'s retry loop concurrently advances d.attempt.
	attempt := d.attempt
	d.updateRoutingOutcomeForAttempt(routingAttempt(d.provider, d.pr, requestID, attempt), outcome)
}

func (d *dispatchState) markSpeculativeLoser(pr *registry.PendingRequest) {
	if pr == nil {
		return
	}
	pr.UsedBackup = true
	d.s.updateInferenceRouteOutcomeForPending(pr, speculativeLoserOutcome(pr))
}

func (d *dispatchState) updateSpeculativeFailure(pr *registry.PendingRequest, msg protocol.InferenceErrorMessage) {
	if pr == nil {
		return
	}
	pr.UsedBackup = true
	d.s.updateInferenceRouteOutcomeForPending(pr, preCommitProviderErrorOutcome(pr, msg))
}

func (d *dispatchState) updateSpeculativeTimeout(pr *registry.PendingRequest, class string) {
	if pr == nil {
		return
	}
	pr.UsedBackup = true
	d.s.updateInferenceRouteOutcomeForPending(pr, pendingRouteOutcome(pr, "timeout", class, http.StatusGatewayTimeout))
}

func (d *dispatchState) updateSpeculativeClientGone(pr *registry.PendingRequest) {
	if pr == nil {
		return
	}
	pr.UsedBackup = true
	d.s.updateInferenceRouteOutcomeForPending(pr, pendingRouteOutcome(pr, "cancelled", "client_gone", 0))
}

// emitClientGone records a before-first-token cancellation on the
// d_inference.routing.client_gone counter for this attempt. It reads
// the current candidate's chip family (or "unknown" when no provider is selected
// yet, e.g. a queue-wait cancel) and the estimated prompt-token bucket. Called
// once per logical client_gone at the central classification sites so speculative
// backup bookkeeping (updateSpeculativeClientGone) never double-counts.
func (d *dispatchState) emitClientGone(phase string) {
	d.stampClientGone(phase)
	// deadline_bucket: elapsed on the request clock vs the first-content
	// budget. At/past ~the budget the upstream timed out on us (its 504), so
	// the OR-view outcome is `timeout`; earlier it is an excluded client abort.
	bucket := d.clientGoneDeadlineBucket()
	d.s.emitClientGoneBucketed(d.model, d.estimatedPromptTokens, providerChipFamily(d.provider), phase, bucket)
	d.recordRequestOutcomeORView(orViewClassForClientGone(bucket))
}
