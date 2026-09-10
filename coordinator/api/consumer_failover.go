package api

// Circuit-breaker handling, TTFT gates, retry-after estimation, and warm-pool nudges.

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/eigeninference/d-inference/coordinator/modelpolicy"
	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/store"
)

// FirstContentDeadline returns this server's request-absolute first-content
// budget for a concrete model. The ordinary base is instance-owned so
// production-like E2E servers can use the production value without mutating
// concurrent unit tests. Exact-model overrides and the fixed 1ms/token slope
// are centralized in modelpolicy.
func (s *Server) FirstContentDeadline(model string, estimatedPromptTokens int) time.Duration {
	base := s.firstContentDeadlineBase
	if base <= 0 {
		base = defaultFirstContentDeadlineBase
	}
	return modelpolicy.CoordinatorFirstContentDeadline(model, estimatedPromptTokens, base)
}

// shedIfModelRejected answers a public/prefer-owner request with 429 +
// Retry-After when its requested alias or resolved build is in the operator
// reject set (EIGENINFERENCE_REJECT_MODELS). This is a deterministic
// per-model circuit breaker: it takes an unhealthy model out of rotation before
// rate-limit, reservation, or routing work, so aggregators see rate limiting
// rather than dropped/cancelled streams. Exclusive self-route bypasses the shed
// because it never falls back to the public fleet.
func (s *Server) shedIfModelRejected(w http.ResponseWriter, r *http.Request, parsed map[string]any, policy selfRoutePolicy, publicModel, model string, stream bool, estimatedPromptTokens, requestedMaxTokens int, requiresVision, hasTools bool) bool {
	if policy.enabled || !s.modelShed(model, publicModel) {
		return false
	}
	retryAfter := s.estimateRetryAfter(model)
	if retryAfter <= 0 {
		retryAfter = 30
	}
	s.ddIncr("routing.decisions", []string{"model:" + model, "model_type:" + s.registry.ModelType(model), "outcome:model_shed"})
	s.recordRejection(rejectionInfo{
		r:                     r,
		stage:                 "model_shed",
		reasonCode:            "model_shed",
		httpStatus:            http.StatusTooManyRequests,
		keyID:                 keyIDFromContext(r.Context()),
		consumerKeyHash:       store.HashKey(consumerKeyFromContext(r.Context())),
		requestedModel:        publicModel,
		resolvedModel:         model,
		stream:                stream,
		estimatedPromptTokens: estimatedPromptTokens,
		requestedMaxTokens:    requestedMaxTokens,
		requiresVision:        requiresVision,
		hasTools:              hasTools,
		selfRouteOnly:         policy.enabled,
		preferOwner:           policy.prefer,
		retryAfterMs:          retryAfter * 1000,
		params:                rejectionSamplingParams(parsed),
	})
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	writeJSON(w, http.StatusTooManyRequests, errorResponse("rate_limit_exceeded",
		fmt.Sprintf("model %q is temporarily rate-limited — retry after %ds", publicModel, retryAfter),
		withCode("rate_limit_exceeded")))
	return true
}

// writeGenericProviderError writes the terminal HTTP body for a provider error
// on paths WITHOUT a failover ladder or in-band SSE error framing: the generic
// inference handlers (/v1/messages, /v1/completions) and the non-streaming
// chat response assembly. Deterministic non-provider-fault reasons surface the
// SAME curated bodies as the chat dispatch ladder — a jinja_* template-render
// failure becomes the 422 model_capability invalid_request_error (the raw
// template backtrace never reaches a client), gated by the ladder's
// EIGENINFERENCE_JINJA_TERMINAL_REJECT kill switch; tool_noncompliance keeps
// its provider-typed 422 message (already curated and content-free) but in the
// invalid_request_error/model_capability envelope instead of provider_error.
// Every other error is mapped from the closed failure_code vocabulary. Raw
// provider prose is never passed through.
func (s *Server) writeGenericProviderError(w http.ResponseWriter, errMsg protocol.InferenceErrorMessage) {
	errMsg = normalizeInferenceErrorForInternalUse(errMsg)
	if jinjaTerminalRejectEnabled() && isJinjaTemplateErrorReason(errMsg.ErrorReason) {
		writeJSON(w, http.StatusUnprocessableEntity,
			errorResponse("invalid_request_error", jinjaTerminalRejectMessage, withCode("model_capability")))
		return
	}
	if normalizeInferenceErrorReason(errMsg.ErrorReason) == errorReasonToolNoncompliance {
		writeJSON(w, http.StatusUnprocessableEntity,
			errorResponse("invalid_request_error", clientSafeInferenceErrorMessage(errMsg), withCode("model_capability")))
		return
	}
	statusCode := errMsg.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusBadGateway
	}
	writeJSON(w, statusCode, errorResponse("provider_error", clientSafeInferenceErrorMessage(errMsg)))
}

// noteInferenceError feeds the circuit breakers for a provider-side error
// received on a pending request's ErrorCh (any phase, pre- or post-commit):
//   - the shape-keyed inference-error breaker (counts only sickness-shaped
//     500/502/504 for the (provider, model, shape) triple),
//   - the per-provider node-health breaker, which also counts fault-shaped
//     503s (errStr classifies capacity-503 vs fault-503),
//   - the stable-identity ejection breaker (survives reconnect churn), and
//   - the capacity-reject cooldown (the ONLY consumer of capacity-class
//     rejections, which every breaker above deliberately ignores).
//
// It emits the cool-down metric on the inference-error transition and the
// provider_breaker_open metric on the node-health transition into quarantine.
// errStr is the provider's error message and errReason its structured
// InferenceErrorMessage.ErrorReason ("" for synthetic timeouts and legacy
// providers) — the reason feeds the gray-box request-shape classification the
// same way the dispatch failover trusts it (classifyRejection P1).
// terminalCause is the provider's typed InferenceErrorMessage.TerminalCause
// ("" for synthetic terminals and legacy providers): a typed NEUTRAL cause
// (safety_deadline / backpressure_timeout / cancelled — platform policy or
// consumer behavior) feeds NOTHING here, strike or clear; a typed CAPACITY
// cause (admission_timeout — healthy but busy) feeds only the black-hole
// capacity cooldown. Absent/engine_error/unknown causes keep the legacy
// status/string funnels bit-for-bit (see api/terminal_cause.go).
func (s *Server) noteInferenceError(providerID string, pr *registry.PendingRequest, statusCode int, errStr, errReason, terminalCause string, causes ...protocol.CoordinatorInferenceErrorCause) {
	if providerID == "" || pr == nil {
		return
	}
	// Structured health-neutral outcomes (isProviderHealthNeutralErrorReason:
	// jinja_* template-render failures, tool_noncompliance, and the
	// request-clock-specific deadline_unreachable refusal) never feed provider
	// health or capacity trackers. Gating HERE (the single breaker chokepoint)
	// mirrors the dispatch-funnel gate
	// (dispatchState.noteProviderError) and the reputation exemption
	// (handleInferenceError), and closes the generic-inference path
	// (/v1/messages, /v1/completions), which calls noteInferenceError directly on
	// pre-commit provider errors. Capacity-class rejections never carry these
	// reasons except deadline_unreachable, whose exclusion is intentional.
	if isProviderHealthNeutralErrorReason(errReason) {
		return
	}
	// Typed drain refusal (R2, registry/drain_state.go): the provider is
	// restarting, not sick and not dishonest about capacity. It feeds NO
	// breaker and NO gray-box capacity state (no cooldown strike, no rate
	// derate, no budget clamp). Ingress marks draining before releasing the
	// pending slot, so its queue drain already skips this provider. Do not
	// repeat that mutation here: an idle/serving heartbeat may have cleared
	// the mark while this consumer was waiting to process its error channel.
	if isDrainingErrorReason(errReason) {
		return
	}
	// Typed terminal-cause gate (the deadline-incident fix): the provider told
	// us WHY the attempt died, so the status/string heuristics below must not
	// misread a platform-policy terminal as sickness. Neutral causes touch no
	// tracker at all — strictly neutral, never a success/clear either.
	// admission_timeout records exactly one capacity-signal strike (the
	// black-hole cooldown, whose zero-interleaved-accepts discriminator keeps
	// serving boxes safe) and skips every fault breaker. All other causes —
	// absent (legacy/synthetic), engine_error, the fault causes
	// (prefill_stall / decode_stall / watchdog), and unknown drift values —
	// fall through to the unchanged legacy funnels.
	switch class, _ := classifyTerminalCause(terminalCause); class {
	case causeClassNeutral:
		return
	case causeClassCapacity:
		if s.registry.RecordCapacityRejectBusy(providerID, pr.Model) {
			s.ddIncr(metricCapacityCooldownTripped, []string{"provider_id:" + providerID, "model:" + pr.Model})
			s.logger.Warn("capacity-reject cooldown tripped: provider+model admission-timing-out with zero interleaved accepts — routing will skip the pair until the cooldown expires",
				"provider_id", providerID,
				"model", pr.Model,
				"status_code", statusCode,
				"terminal_cause", terminalCause,
			)
		}
		return
	}
	// Late disconnect-flush strike (registry/version_reset.go): the session this
	// 502 was flushed from was dropped at or before its identity's version-
	// changed reset, so the reset already accounted for it. The flush is
	// recorded HERE, by the request goroutine, not by Disconnect — and
	// registration evicts a same-serial predecessor and stores the new version
	// on one goroutine, ahead of these consumers — so without the check the
	// new binary would be quarantined for the old one's death.
	if s.registry.IsSupersededDisconnectFlush(providerID, statusCode, causes...) {
		return
	}
	if s.registry.RecordInferenceError(providerID, pr.Model, statusCode, pr.Traits.CooldownShape(), causes...) {
		s.ddIncr("routing.cooldown_entered", []string{"model:" + pr.Model})
	}
	// Feed EVERY provider terminal into the per-provider node-health breaker (not
	// just the shape-keyed 5xx the inference-error breaker counts) so a node
	// fault-503ing ~all of its requests gets quarantined fleet-wide. errStr lets
	// the breaker tell a capacity-503 (ignored) from a fault-503 (counted). Both
	// breakers coexist.
	if opened, _ := s.registry.RecordProviderOutcome(providerID, false, statusCode, errStr, causes...); opened {
		s.ddIncr("routing.provider_breaker_open", []string{"model:" + pr.Model})
	}
	// Feed the STABLE-IDENTITY ejection breaker too (survives reconnect churn, so a
	// zombie that fault-loops while constantly disconnecting still accumulates).
	if ejected, _ := s.registry.RecordProviderSessionServeOutcome(providerID, false, statusCode, errStr, causes...); ejected {
		s.ddIncr("routing.provider_ejected", []string{"model:" + pr.Model})
	}
	// Feed the capacity-reject cooldown. Capacity-class rejections are
	// DELIBERATELY invisible to reputation and to ALL the breakers above (a
	// busy box must never be punished for shedding) — which turns a box that
	// capacity-rejects EVERYTHING into a routing black hole: its idle-looking
	// heartbeats keep winning the cost scheduler while every dispatch bounces
	// (2026-07 incident: 7 boxes, ~9k "token_budget_exhausted" rejections in
	// 30 min, zero successes). Strikes accumulate per (provider, model); any
	// accept (first content chunk or clean completion) resets the streak, so
	// transient fullness on a serving box can never trip. Gated to 429/404/5xx
	// so a client-shape 4xx that happens to carry a capacity-looking string
	// never strikes; explicit context-overflow rejections are excluded by
	// isCapacityRejectStrike (they indict the request, not the provider).
	//
	// 404 is included WITH CARE for the cold "model not loaded" miss: a lazy
	// load on first touch makes a 404-then-load-then-serve sequence NORMAL
	// lifecycle, so the zero-interleaved-accepts discriminator remains the
	// safety — the first accept after the load clears the streak, and only a
	// box that 404s FOREVER (never loads, zero accepts) trips. A 404 whose
	// message is not capacity-class (e.g. "model not found" for an unknown
	// model id — a request-shape error) never strikes, because
	// isCapacityRejectStrike only matches the capacity vocabulary
	// ("not loaded" / "no model loaded").
	if (statusCode == http.StatusTooManyRequests || statusCode == http.StatusNotFound ||
		statusCode >= http.StatusInternalServerError) &&
		isCapacityRejectStrike(errStr) {
		// A cold "model not loaded" miss is benign warm-up lifecycle, not
		// capacity dishonesty. It still feeds the black-hole cooldown (a box
		// that 404s forever with zero accepts is a black hole), but it must NOT
		// derate the pair's gray-box capacity-503 RATE (capacity_rate.go) — that
		// window has no accept-reset, so counting a healthy box's normal reloads
		// would penalize it. A "batch token budget" reject that classifyRejection
		// proves REQUEST-deterministic (provider budget not below the model
		// context ⇒ the binding term was the fleet-wide context) indicts the
		// request, not the provider: it counts a cooldown strike only — arming
		// the one-shot clamp or the no-reset rate window off a single oversized
		// prompt would gate/derate a healthy pair. Genuine capacity/token-budget
		// 503s feed everything.
		var tripped bool
		switch {
		case isColdModelMissRejection(errStr):
			tripped = s.registry.RecordCapacityRejectLifecycle(providerID, pr.Model)
		case s.isRequestShapeBatchBudgetReject(providerID, pr.Model, errStr, errReason):
			tripped = s.registry.RecordCapacityRejectRequestShape(providerID, pr.Model)
		default:
			tripped = s.registry.RecordCapacityReject(providerID, pr.Model)
		}
		if tripped {
			s.ddIncr(metricCapacityCooldownTripped, []string{"provider_id:" + providerID, "model:" + pr.Model})
			s.logger.Warn("capacity-reject cooldown tripped: provider+model capacity-rejecting with zero interleaved accepts — routing will skip the pair until the cooldown expires",
				"provider_id", providerID,
				"model", pr.Model,
				"status_code", statusCode,
			)
		}
	}
}

// metricCapacityCooldownTripped counts transitions of a (provider, model) pair
// into the capacity-reject routing cooldown (registry/capacity_cooldown.go),
// tagged provider_id + model. Distinct from routing.cooldown_entered (the 5xx
// inference-error breaker) and routing.provider_breaker_open (node health) so
// black-hole trips are independently alertable.
const metricCapacityCooldownTripped = "routing.capacity_cooldown_tripped"

// isRequestShapeBatchBudgetReject reports whether a capacity-class rejection
// is PROVEN request-deterministic by classifyRejection: a "batch token budget"
// reject from a provider whose reported token budget is not below the model's
// context window (the admission cap min(context, budget) was the CONTEXT — the
// prompt is too big fleet-wide), or an explicit request_exceeds_context
// structured reason. Such a reject must arm neither the one-shot budget clamp
// nor the no-reset capacity-503 rate window
// (RecordCapacityRejectRequestShape). When the reported budget IS below the
// context, the binding term may have been this node's memory-pressured KV
// budget — a genuine provider-specific capacity signal — and the reject feeds
// the full gray-box state (same discrimination the dispatch failover uses:
// classifyRejection in inference_failure_class.go, DAR-347).
//
// Inputs mirror the dispatch path exactly: the structured errReason
// (InferenceErrorMessage.ErrorReason — a provider that says
// request_exceeds_node_budget / capacity_busy is TRUSTED over the stale
// heartbeat-budget heuristic, so a stale snapshot that still reads >= context
// cannot misroute a genuine node-capacity failure away from the gray-box
// trackers), providerBudget from the provider's last heartbeat
// (ReportedTokenBudgetMaxForModel), and modelContext from the model registry
// record. Called only inside the isCapacityRejectStrike branch, so explicit
// context-overflow STRINGS never reach it (they never strike at all). The
// cheap gate keeps the two lookups off every other rejection.
func (s *Server) isRequestShapeBatchBudgetReject(providerID, model, errStr, errReason string) bool {
	e := strings.ToLower(strings.TrimSpace(errStr))
	e = strings.ReplaceAll(e, "’", "'")
	reason := strings.ToLower(strings.TrimSpace(errReason))
	if !strings.Contains(e, "batch token budget") && reason != "request_exceeds_context" {
		return false
	}
	var providerBudget int64
	if p := s.registry.GetProvider(providerID); p != nil {
		providerBudget = p.ReportedTokenBudgetMaxForModel(model)
	}
	modelContext := 0
	if rec, err := s.store.GetModelRegistryRecord(model); err == nil && rec != nil {
		modelContext = rec.MaxContextLength
	}
	// No typed CapacityRejectionReason threads into the strike funnel
	// (noteInferenceError carries only the string vocabulary), so this stays
	// the legacy string+heartbeat heuristic — enriched typed reasons already
	// reach it mapped onto error_reason by the sanitizer.
	return classifyRejection(errReason, errStr, providerBudget, modelContext, "") == rejectionDeterministicUnservable
}

// noteInferenceSuccess clears the inference-error strike state for the serving
// provider-model pair on a clean completion (streaming relay ended without a
// provider error; non-streaming response assembled OK).
func (s *Server) noteInferenceSuccess(pr *registry.PendingRequest) {
	if pr == nil || pr.ProviderID == "" {
		return
	}
	s.registry.RecordInferenceSuccess(pr.ProviderID, pr.Model, pr.Traits.CooldownShape())
	// A clean completion is an ACCEPT for the capacity-reject cooldown: clear
	// the pair's reject streak, any active capacity cooldown, and the re-trip
	// backoff. Belt-and-braces with the commit-time accept (commitFirstContent)
	// and the only accept signal on paths that never stream content. For the
	// capacity-503 RATE window (capacity_rate.go) one served request must
	// count exactly ONE outcome, so this completion-time accept re-offers the
	// outcome only when the commit-time accept did not actually RECORD one
	// (RateOutcomeCountedSafe — stamped from RecordCapacityAccept's return at
	// every commit site). With rate tracking enabled, commit-time accepts are
	// retained even before the first reject; paths that never commit content
	// record their sole outcome here instead.
	s.registry.RecordCapacityAcceptOutcome(pr.ProviderID, pr.Model, !pr.RateOutcomeCountedSafe())
	// A clean completion proves the node is healthy — close its node-health
	// breaker (and reset the exponential backoff) if it had tripped.
	if _, closed := s.registry.RecordProviderOutcome(pr.ProviderID, true, 200, ""); closed {
		s.ddIncr("routing.provider_breaker_closed", []string{"model:" + pr.Model})
	}
	// A clean completion is a success for the stable-identity ejection breaker too
	// — closes it (half-open recovery) if this identity had been ejected.
	if sid := s.registry.GetProviderStableIdentity(pr.ProviderID); sid != "" {
		if _, recovered := s.registry.RecordProviderServeOutcome(sid, true, 200, ""); recovered {
			s.ddIncr("routing.provider_ejection_recovered", []string{"model:" + pr.Model})
		}
	}
}

// noteDispatchProviderError records a provider error received while the
// dispatch loop had NOT yet committed to that provider: it feeds the
// inference-error breaker, refunds the failed attempt's provider-specific
// reservation top-up, and, when boilerplate chunks from that provider were
// being held (deferred commit), discards them and emits the pre-content
// failover counter — the invisible-retry signal that replaces what used to be
// an in-band SSE error after a premature commit. Returns true when held
// chunks were discarded so callers skip their generic retry counter.
//
// The refund lives here because both ErrorCh senders (handleInferenceError and
// registry.Disconnect's pending flush) remove the pending request BEFORE
// pushing the error, so the arm's cancelDispatch sees RemovePending()==nil and
// skips its own refund — without this the custom-price surcharge reserved by
// reserveAdditionalForProvider would be stranded for the failed attempt.
// refundProviderExtra is idempotent (it resets ReservedMicroUSD to the base),
// so arms where cancelDispatch did refund are safe, and a failed pre-commit
// attempt never reaches settlement (its channels are closed and it is neither
// pending nor parked), so this can never double-credit against a settle.
func (s *Server) noteDispatchProviderError(provider *registry.Provider, pr *registry.PendingRequest, statusCode int, errStr, errReason, terminalCause string, held *[]string, causes ...protocol.CoordinatorInferenceErrorCause) (discardedHeld bool) {
	if provider != nil {
		s.noteInferenceError(provider.ID, pr, statusCode, errStr, errReason, terminalCause, causes...)
	}
	s.refundProviderExtra(pr)
	if held == nil || len(*held) == 0 {
		return false
	}
	*held = nil
	s.ddIncr("inference.dispatches", []string{"status:retry_precontent"})
	return true
}

// failedProviderVersion reads a provider's reported binary version under its
// lock (mirroring the policy.prefer owner reads). Captured when an attempt
// fails so the next attempt's Traits.AvoidVersion can steer the retry to a
// different build — a deterministic per-version bug must not burn every retry
// on identical binaries.
func failedProviderVersion(p *registry.Provider) string {
	if p == nil {
		return ""
	}
	p.Mu().Lock()
	defer p.Mu().Unlock()
	return p.Version
}

func ttftTooSlow(bestTTFT time.Duration, hasTTFT bool, threshold time.Duration) bool {
	return hasTTFT && bestTTFT > threshold
}

// hardTTFTGateApplies reports whether the scheduler's token-prefill estimate is
// authoritative enough to reject this request before dispatch. Media requests
// run CPU decode plus a separate vision tower before text prefill; neither cost
// exists in estimatedTTFTFromSnapshot, so treating that partial estimate as a
// hard ceiling rejects healthy video/image requests on a number that cannot
// predict their TTFT. They still use the best-available provider and remain
// bounded by the same request-absolute first-content deadline.
func (s *Server) hardTTFTGateApplies(requiresVision bool) bool {
	return s.ttftHardReject && !requiresVision
}

func fasterTTFTEstimate(primaryModel string, primary time.Duration, alternateModel string, alternate time.Duration, alternateOK bool) (string, time.Duration) {
	if alternateOK && alternate < primary {
		return alternateModel, alternate
	}
	return primaryModel, primary
}

func (s *Server) estimateTTFTRetryAfter(model string, bestTTFT, threshold time.Duration) int {
	overage := bestTTFT - threshold
	seconds := int(math.Ceil(overage.Seconds()))
	if base := s.estimateRetryAfter(model); seconds < base {
		seconds = base
	}
	if seconds < 2 {
		seconds = 2
	}
	if seconds > 30 {
		seconds = 30
	}
	return seconds
}

// writeFirstTokenTimeout writes the OpenRouter-compatible retryable 429 used
// when a request-absolute first-token clock expires after dispatch. Chat
// exhausted already uses this shape (Retry-After + rate_limit_exceeded);
// /v1/completions and /v1/messages must match so aggregators retry instead
// of treating the timeout as a provider 504.
func (s *Server) writeFirstTokenTimeout(w http.ResponseWriter, model, message string) {
	retryAfter := s.estimateRetryAfter(model)
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	writeJSON(w, http.StatusTooManyRequests, errorResponse("rate_limit_exceeded",
		message, withCode("rate_limit_exceeded")))
}

func (s *Server) writeTTFTTooSlow(w http.ResponseWriter, model, publicModel string, bestTTFT, threshold time.Duration) {
	retryAfter := s.estimateTTFTRetryAfter(model, bestTTFT, threshold)
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	s.ddIncr("routing.decisions", []string{"model:" + model, "model_type:" + s.registry.ModelType(model), "outcome:ttft_429"})
	writeJSON(w, http.StatusTooManyRequests, errorResponse("rate_limit_exceeded",
		ttftTooSlowMessage(publicModel, bestTTFT, threshold, retryAfter),
		withCode("rate_limit_exceeded")))
}

// ttftTooSlowMessage is the single wording for a fleet-wide TTFT rejection.
func ttftTooSlowMessage(publicModel string, bestTTFT, threshold time.Duration, retryAfter int) string {
	return fmt.Sprintf(
		"all providers for model %q are above the %ds TTFT target (best estimate %.1fs); retry after %ds",
		publicModel, int(math.Ceil(threshold.Seconds())), bestTTFT.Seconds(), retryAfter)
}

func (s *Server) triggerWarmPool() {
	if s == nil || s.registry == nil {
		return
	}
	s.registry.RequestWarmPoolTrigger()
}

func (s *Server) recordWarmPoolQueueState(model string) {
	if s == nil || s.registry == nil || s.registry.Queue() == nil {
		return
	}
	depth, oldest := s.registry.Queue().QueueStats(model)
	if depth <= 0 {
		s.registry.RecordWarmPoolQueueCleared(model)
		return
	}
	s.registry.RecordWarmPoolQueueEnqueued(model, depth, oldest)
	s.triggerWarmPool()
}

// ttftMsForRejection converts a pre-flight TTFT estimate to milliseconds for the
// rejection ledger, returning 0 when the pre-flight produced no estimate.
func ttftMsForRejection(bestTTFT time.Duration, hasTTFT bool) float64 {
	if !hasTTFT {
		return 0
	}
	return float64(bestTTFT.Milliseconds())
}

// rejectionSamplingParams captures only the non-content sampling knobs already
// parsed from an inbound request body for the rejection ledger. It never
// includes prompt/message/input content. Returns nil when none are present.
func rejectionSamplingParams(parsed map[string]any) json.RawMessage {
	if parsed == nil {
		return nil
	}
	knobs := make(map[string]any, 4)
	for _, k := range []string{"temperature", "top_p", "presence_penalty", "frequency_penalty"} {
		if v, ok := parsed[k]; ok {
			knobs[k] = v
		}
	}
	if len(knobs) == 0 {
		return nil
	}
	b, err := json.Marshal(knobs)
	if err != nil {
		return nil
	}
	return b
}

// routeLatencyEWMAAlpha weights the newest attempt-0 route-latency sample in
// the distress EWMA: ~10 healthy requests pull a degraded average back under
// the threshold once the collapse clears.
const routeLatencyEWMAAlpha = 0.2

// degradedRouteEWMAThresholdMs is the attempt-0 route-latency EWMA above which
// estimateRetryAfter switches from the queue-depth heuristic to distress
// scaling. Healthy routing runs ~40ms; anything over a second means the
// coordinator itself is the bottleneck.
const degradedRouteEWMAThresholdMs = 1000.0

// maxDistressRetryAfter caps the distress-scaled Retry-After (seconds).
const maxDistressRetryAfter = 60

// noteAttempt0RouteLatency folds one attempt-0 route latency (ReceivedAt →
// RoutedAt) into the distress EWMA. Called from dispatchWithReserver where
// RoutedAt is stamped; negative samples (clock skew) are dropped.
func (s *Server) noteAttempt0RouteLatency(d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	if ms < 0 {
		return
	}
	s.routeLatencyMu.Lock()
	if s.routeLatencyEWMAMs == 0 {
		s.routeLatencyEWMAMs = ms
	} else {
		s.routeLatencyEWMAMs = routeLatencyEWMAAlpha*ms +
			(1-routeLatencyEWMAAlpha)*s.routeLatencyEWMAMs
	}
	s.routeLatencyMu.Unlock()
}

// attempt0RouteEWMAMs reads the current attempt-0 route-latency EWMA (ms).
func (s *Server) attempt0RouteEWMAMs() float64 {
	s.routeLatencyMu.Lock()
	defer s.routeLatencyMu.Unlock()
	return s.routeLatencyEWMAMs
}

// estimateRetryAfter returns a suggested wait time in seconds before retrying
// a request for the given model. Based on queue depth as a rough proxy for
// fleet backlog. OpenRouter uses the Retry-After header to schedule retries.
//
// Distress scaling (2026-09-01 congestion collapse): queue depth alone was a
// LIAR under CPU saturation — the queue was empty (nothing could even reach
// it), so every 429 carried "Retry-After: 2" and upstream obligingly hammered
// the coordinator every 2s, sustaining the death loop. When the attempt-0
// route-latency EWMA shows routing itself is degraded (> 1s), the answer
// scales with the observed degradation — max(base, ceil(EWMA seconds)×5),
// capped at 60s — so upstream backoff actually relieves pressure. Queue-depth
// behavior is unchanged while routing is healthy.
func (s *Server) estimateRetryAfter(model string) int {
	estimate := 2 // Light load, retry soon
	if queueDepth := s.registry.Queue().QueueSize(model); queueDepth > 0 {
		// Rough estimate: each queued request takes ~3 seconds to drain.
		estimate = queueDepth * 3
		if estimate < 2 {
			estimate = 2
		}
		if estimate > 30 {
			estimate = 30
		}
	}
	if ewmaMs := s.attempt0RouteEWMAMs(); ewmaMs > degradedRouteEWMAThresholdMs {
		scaled := int(math.Ceil(ewmaMs/1000)) * 5
		if scaled > maxDistressRetryAfter {
			scaled = maxDistressRetryAfter
		}
		if scaled > estimate {
			estimate = scaled
		}
	}
	return estimate
}

// writeServiceUnavailable writes a retryable 503 with a Retry-After header so
// clients (and OpenRouter) can schedule the retry instead of blind backoff.
func (s *Server) writeServiceUnavailable(w http.ResponseWriter, model string) {
	w.Header().Set("Retry-After", strconv.Itoa(s.estimateRetryAfter(model)))
	writeJSON(w, http.StatusServiceUnavailable, errorResponse("service_unavailable",
		"service temporarily unavailable — please retry"))
}
