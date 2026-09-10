package api

// Per-request dispatch state machine for the consumer inference path.
//
// This file holds the speculative TTFT-aware dispatch loop that handleChatCompletions
// drives: it picks a provider (or queues), waits for the first CONTENT chunk with a
// speculative backup race, fails over invisibly on provider error/timeout up to
// maxDispatchAttempts, and commits exactly once. It is a PURELY STRUCTURAL extraction
// of what previously lived inline in consumer.go — every select arm, timer Stop/Reset,
// channel-close+ErrorCh grace window, heldChunks cap, liveness extension, speculative
// race (backup dispatch / cancel-loser / skipBackup), refund-exactly-once, breaker
// call, DD metric, and status code is preserved exactly.
//
// Control-flow mapping (former labeled blocks → methods):
//
//	for attempt := range maxDispatchAttempts   → dispatchState.run (the orchestrator)
//	dispatch-primary block (incl. queue path)  → dispatchState.dispatchPrimary
//	firstChunkWait + speculative race          → dispatchState.waitFirstChunk
//	  noBackupWait                             →   dispatchState.waitNoBackup
//	  race + sub-waits                         →   dispatchState.runRace
//	    backupFailedPrimaryWait                →     dispatchState.raceBackupFailedWaitPrimary
//	    primaryFailedBackupWait                →     dispatchState.racePrimaryFailedWaitBackup
//	    backupFailedWaitPrimary                →     dispatchState.raceBackupErrWaitPrimary
//	acceptedWait                               → dispatchState.waitAccepted
//
// The former labeled jumps become method returns: `continue dispatch` → outcomeRetry,
// `break`/commit → outcomeCommitted, `break <label>` into the accepted wait →
// outcomeAccepted, `return` (client gone, after refund) → outcomeClientGone, and the
// queue-rejection `writeJSON; return` paths → outcomeResponseWritten. The orchestrator
// switches on the outcome, exactly reproducing the original flow.

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/store"
)

// dispatchOutcome is the result of a per-attempt dispatch phase (provider
// selection, first-chunk wait, accepted wait). The orchestrator (dispatchState.run)
// switches on it to reproduce the original loop's continue/break/return flow.
type dispatchOutcome int

const (
	// outcomeCommitted: a content chunk (or a clean close) committed the attempt.
	// The orchestrator stops the loop and streams the response.
	outcomeCommitted dispatchOutcome = iota
	// outcomeAccepted: legacy/unstamped preamble liveness earned a bounded
	// content wait. AcceptedCh itself never produces this outcome.
	outcomeAccepted
	// outcomeRetry: the attempt failed (provider error / timeout). Equivalent to
	// the original `continue dispatch` — the orchestrator advances to the next attempt.
	outcomeRetry
	// outcomeFailFast: the loop must stop without a committed provider (e.g.
	// model-too-large, or no-provider on a retry attempt). Equivalent to `break`.
	outcomeFailFast
	// outcomeClientGone: the request context was cancelled; the reservation was
	// already refunded and the handler must return with no response body.
	outcomeClientGone
	// outcomeResponseWritten: a terminal HTTP response was already written
	// (queue rejection / queue timeout / queue insufficient funds 402 etc.) and
	// the handler must return immediately.
	outcomeResponseWritten
	// outcomeProceed: provider selection succeeded; the orchestrator continues
	// to the first-chunk wait for this attempt.
	outcomeProceed
)

type dispatchTerminalFailure struct {
	errText       string
	statusCode    int
	terminalCause string
	deadline      bool
	attribution   dispatchSlotAttribution
}

// dispatchState carries everything the per-request dispatch loop needs. The
// immutable inputs are set once by runDispatch; the mutable fields track the
// in-flight attempt (selected provider, held preamble, commit/accept flags,
// last error for the exhaustion ladder, and the version to steer retries away from).
type dispatchState struct {
	s *Server

	// ---- immutable inputs (set once) ----
	w                      http.ResponseWriter
	r                      *http.Request
	model                  string
	publicModel            string
	rawBody                []byte
	consumerKey            string
	consumerLocation       *store.ProviderLocation
	reservedMicroUSD       int64
	serviceReservation     bool
	estimatedPromptTokens  int
	requestedMaxTokens     int
	tokenAdmission         registry.TokenAdmission
	requiresVision         bool
	hasTools               bool
	requiresToolConstraint bool
	toolChoiceMode         string
	toolChoiceName         string
	parallelToolCalls      bool
	isResponsesAPI         bool
	consumerEndpoint       string
	requestedStopSequences []string
	stream                 bool
	metadataDetails        bool
	policy                 selfRoutePolicy
	allowedProviderSerials []string
	cachePlan              registry.CachePlan
	timing                 *registry.RequestTiming
	profile                *registry.RequestProfile
	deadline               time.Duration
	speculativeAt          time.Duration
	// Deterministic test seams for speculative timer/ingress arbitration.
	// Production requests leave both nil.
	onSpeculativeDispatch func()
	onSpeculativeDeferral func()
	// modelMaxContext is the model's context window (0 = unknown), used by
	// shouldStopFailover/classifyRejection to tell a fleet-wide context overflow
	// apart from a memory-pressured provider's shrunk KV budget when a "batch token
	// budget" rejection arrives.
	modelMaxContext int
	// refundReservation refunds the shared base reservation (the caller's closure).
	refundReservation func()

	// ---- mutable per-request state ----
	provider      *registry.Provider
	pr            *registry.PendingRequest
	requestID     string
	firstChunk    string
	heldChunks    []string
	initialError  *protocol.InferenceErrorMessage
	lastErr       string
	lastErrCode   int
	lastErrReason string
	// lastErrProviderBudget is the rejecting provider's reported token budget
	// (ActiveTokenBudgetMax) for d.model at the time lastErr was set, or 0 when the
	// error is not a provider rejection / the provider reported no budget. Captured
	// by setLastInferenceError so shouldStopFailover can classify a "batch token
	// budget" rejection as deterministic (budget >= context) vs transient
	// (budget < context — this node was memory-pressured).
	lastErrProviderBudget int64
	// lastErrRejectionReason is the typed CapacityRejectionReason from the
	// last provider error ("" for legacy providers). classifyRejection
	// treats a typed token_budget as AUTHORITATIVE transient: the provider's
	// live gate named the shortage, so a deterministic-unservable verdict
	// must never be re-derived from the stale heartbeat budget fallback.
	lastErrRejectionReason protocol.CapacityRejectionReason
	// lastErrTerminalCause is the typed terminal_cause from the last provider
	// error ("" for legacy providers). shouldStopFailover trusts a typed
	// admission_timeout as transient capacity directly — the provider's engine
	// TOLD us it was busy — instead of inferring from error-string substrings
	// that the fixed "admission_timeout: …" text would never match.
	lastErrTerminalCause string
	// lastErrCoordinatorCause is a non-wire marker for coordinator-synthetic
	// terminals such as a provider disconnect. A provider cannot set it.
	lastErrCoordinatorCause protocol.CoordinatorInferenceErrorCause
	// lastErrAttemptUsage is the typed partial usage from the last provider
	// error (nil for legacy providers), applied to the failed attempt's route
	// row by providerFailedRoutingOutcomeFor so pre-content typed failures on
	// the ordinary dispatch path keep their observability data.
	lastErrAttemptUsage *protocol.UsageInfo
	// genuineFault is request-wide terminal precedence, separate from the
	// lastErr* per-attempt scratch used to persist each attempt's route outcome.
	// Capacity/lifecycle refusals, deadline refusals, neutral typed causes, and
	// deterministic client/model errors never enter this slot.
	genuineFault      *dispatchTerminalFailure
	committed         bool
	lastFailedVersion string
	excludeProviders  map[string]struct{}
	// capacityRetries counts pre-content TRANSIENT-capacity failovers (this
	// node's live KV budget, a full queue, a drain). Bounded by
	// maxCapacityClassRetries so a fleet-wide transient cannot storm; a
	// DETERMINISTIC-context rejection (prompt > model context) stops on the first
	// attempt regardless (see classifyRejection / failoverOutcome).
	capacityRetries int
	// firstChunkTimeoutRetries counts attempts that ended in a
	// coordinator-synthesized first-chunk TIMEOUT (untyped 504 → the
	// "first_chunk_timeout" 429 on exhaustion). Bounded by
	// maxFirstChunkTimeoutRetries so a slow-provider storm cannot burn a
	// fresh fleet scan per attempt across the ladder (the 2026-09-01
	// congestion collapse; see the constant). Each counted attempt was on a
	// distinct provider — the timed-out provider joins excludeProviders.
	firstChunkTimeoutRetries int
	// lastFailureDeadline is scoped to the most recent terminal attempt. A
	// deadline refusal remains eligible for deadline_unreachable only while no
	// later genuine provider fault has replaced it.
	lastFailureDeadline bool
	// unservable is set when the dispatch loop stops because the request cannot
	// be served (deterministic-context rejection, or a transient that exhausted
	// maxCapacityClassRetries). The exhausted ladder then emits a single
	// uptime-neutral 429 with unservableReason instead of retrying/5xx'ing.
	unservable       bool
	unservableReason string
	// terminalClientError is set when a dispatched provider returned a DETERMINISTIC
	// client-shape 4xx (400/413/422/415 — invalid tool payload / role / response_format
	// / unsupported media). That rejection is identical on every provider (the bad
	// request body is forwarded unchanged), so the loop stops immediately and the
	// exhausted ladder surfaces terminalClientErrorCode ONCE — instead of failing over
	// up to maxDispatchAttempts (the prod 29×/max-63 storm). String-blind: the status
	// code is ground truth; the human-readable provider string drifts across versions.
	terminalClientError     bool
	terminalClientErrorCode int
	// terminalClientErrorReason, when non-empty, overrides the exhausted
	// ladder's rejection-ledger reason_code for a latched terminal client
	// error ("template_render_failed" for the jinja_* stop — distinguishable
	// from the StatusCode-driven stop's generic "client_error").
	terminalClientErrorReason string
	// terminalClientErrorMessage, when non-empty, overrides the surfaced
	// error-body message (the jinja_* stop surfaces the curated
	// model_capability text, not the provider's raw template backtrace).
	terminalClientErrorMessage string
	// servedKVSlot latches the KV-cache backend attribution of the SLOT the
	// most recent attempt was dispatched to (v0.8.0 paged rollout, Gate G5) —
	// the resolved kind AND whether that kind was a silent degrade. It is NOT
	// per-attempt scratch: the failure tails run after a retry has cleared
	// d.provider/d.pr, and a 5xx from a paged slot that just fell over is
	// exactly the sample the rollout dashboard must not lose. Zero value until
	// the request reaches a slot, which tags unknown on both dimensions.
	servedKVSlot dispatchSlotAttribution

	// ---- Routing v2 wave-2 plan/hedge state ----
	// plan is the bounded dispatch plan retained by the FIRST full-scan
	// reservation (registry.ReserveProviderWithPlan): up to eight provisional
	// alternates from the same scan that chose the primary. Retries and the
	// speculative backup consume it (ReserveNextFromPlan, then one refresh)
	// before any rescan. nil for queue-path and no-reservation flows —
	// selection behavior is then exactly legacy.
	plan *registry.DispatchPlan
	// planRefreshUsed latches the request's single RefreshDispatchPlan across
	// BOTH consumers (failover retries and the speculative backup). The plan
	// object enforces once-per-plan-chain; this enforces once-per-request.
	planRefreshUsed bool
	// probesLaunched: the one parallel capacity-probe round has started
	// (maybeProbePlanCandidates). One round per request, launched only after
	// the primary frame handoff so probes never add primary latency.
	probesLaunched bool
	// hedgeAdvanceCh delivers the probe round's refined ABSOLUTE speculative
	// launch instant (hedgeLaunchAt) when a confirmed backup's quoted q90
	// proves the 50% point too late to be useful. Buffered 1, written at most
	// once by the quote collector; nil until probes launch. waitFirstChunk
	// consumes at most one value under only-earlier / only-once /
	// never-after-fire guards; without a value the 50% default stands.
	hedgeAdvanceCh chan time.Time
	// hedgeGovernorVerdict is the governor's decision for this request's
	// speculative launch ("" = the governor never ran: no speculative point
	// reached, or an owner-served prefer request). Telemetry/log only.
	hedgeGovernorVerdict string
	// providerDispatches counts inference frames actually handed to a
	// provider — primary, queued, plan-retry, and speculative-backup sends
	// alike, incremented in the write handoff callback that stamps
	// Timing.DispatchedAt. Client-visible exhaustion messages report this
	// machine count; route rows keep the loop index d.attempt untouched.
	providerDispatches int
	// visionImageCount is the number of media parts in the request (0 for
	// text-only), carried into capacity probes as count-only shape metadata.
	visionImageCount int
	// lastErrFeasibleAfterMS is the enriched rejection's forecast of when a
	// request of this shape could next be admitted (0 = absent/legacy),
	// captured by setLastInferenceError and surfaced into the exhaustion
	// 429's Retry-After.
	lastErrFeasibleAfterMS int64

	// ---- per-attempt scratch (reset each attempt) ----
	attempt          int
	preambleLiveness bool
	// dispatchErr captures the non-empty error string from dispatchOneProvider
	// for this attempt so outcome telemetry can classify the routing decision.
	dispatchErr string
	// dispatchErrCode captures the HTTP status code associated with dispatchErr.
	dispatchErrCode int
	// providerBodyTooLargeErr preserves a protocol-0 cache-buster overflow
	// while failover tries providers whose newer protocol does not add it.
	providerBodyTooLargeErr   string
	providerBodyTooLargeBytes int
	minPrefixCacheProtocol    int
}

// traits builds the routing traits for the current attempt, steering away from
// the most recently failed provider's binary version.
func (d *dispatchState) traits() registry.RequestTraits {
	return registry.RequestTraits{
		HasTools:               d.hasTools,
		RequiresToolConstraint: d.requiresToolConstraint,
		ToolChoiceMode:         d.toolChoiceMode,
		ToolChoiceName:         d.toolChoiceName,
		ParallelToolCalls:      d.parallelToolCalls,
		AvoidVersion:           d.lastFailedVersion,
		MinPrefixCacheProtocol: d.minPrefixCacheProtocol,
	}
}

func (d *dispatchState) configurePending(pr *registry.PendingRequest) {
	if pr == nil {
		return
	}
	pr.ConsumerEndpoint = d.consumerEndpoint
	pr.RequestedStopSequences = append(
		pr.RequestedStopSequences[:0], d.requestedStopSequences...)
	pr.MetadataDetails = d.metadataDetails
}

func (d *dispatchState) excludedProviderIDs() []string {
	ids := make([]string, 0, len(d.excludeProviders))
	for id := range d.excludeProviders {
		ids = append(ids, id)
	}
	return ids
}

func (d *dispatchState) shouldQueueCompatibleProvider(decision registry.RoutingDecision) bool {
	return d.providerBodyTooLargeErr != "" &&
		d.lastErrCode == http.StatusRequestEntityTooLarge &&
		decision.CapacityRejections > 0
}

// run is the dispatch orchestrator. It replaces the giant inline `for attempt :=
// range maxDispatchAttempts { ... }` block plus the post-loop !committed ladder,
// attestation headers, timing header, settlement defer, and final response handoff.
func (d *dispatchState) run() {
	s := d.s
	defer d.finalizeProfile()
	w, r := d.w, d.r
	d.preflightLegacyCacheBust()

	for attempt := range maxDispatchAttempts {
		d.attempt = attempt
		// Deadline-bounded failover: after the first attempt, stop failing over
		// once the request's deadline/context has fired (client gone or a request
		// timeout). We keep trying fresh healthy providers only while there is
		// time budget left. Candidate exhaustion is handled inside dispatchPrimary
		// (it returns outcomeFailFast as soon as no eligible provider remains), so
		// in practice the loop ends at exhaustion or success; maxDispatchAttempts
		// is only a hot-loop ceiling and this is the wall-clock bound.
		if attempt > 0 && r.Context().Err() != nil {
			// The client left between attempts (D2). There is no in-flight
			// provider (the previous attempt already cleaned up and wrote its
			// own route outcome) and nobody to write a 429/5xx to, so record it
			// as client_gone like every other pre-content cancel arm — not as
			// the exhausted ladder's rate_limited / provider_5xx outcome.
			d.refundReservation()
			d.emitClientGone(phaseBeforeFirstToken)
			return
		}
		if attempt > 0 && d.firstTokenExpired() {
			// The request-absolute first-token budget is gone: the client must
			// see the retryable 429 (synthetic 504 -> first_chunk_timeout), not
			// whatever the last provider attempt happened to fail with.
			d.setLastError("timeout waiting for first response", http.StatusGatewayTimeout)
			goto exhausted
		}
		// Each attempt holds preamble chunks from its own provider only.
		d.heldChunks = nil

		switch d.dispatchPrimary() {
		case outcomeRetry:
			continue
		case outcomeFailFast:
			goto exhausted
		case outcomeResponseWritten, outcomeClientGone:
			return
		case outcomeProceed:
			// fall through to the first-chunk wait below
		}

		d.requestID = d.pr.RequestID
		// d.pr.Attempt is already stamped at PendingRequest construction in
		// dispatchOneProvider (and on the queued path), before the provider send —
		// so it is never written here, where it would race handleComplete.
		if d.timing.RoutedAt.IsZero() {
			d.timing.RoutedAt = time.Now()
		}
		d.emitRouteLatency()

		s.ddIncr("routing.decisions", []string{"model:" + d.model, "outcome:selected"})
		s.ddIncr("routing.provider_selected", []string{"provider_id:" + d.provider.ID, "model:" + d.model})

		s.logger.Info("inference request dispatched",
			"trace_id", requestIDFromContext(r.Context()),
			"request_id", d.requestID,
			"model", d.model,
			"provider_id", d.provider.ID,
			"stream", d.stream,
			"attempt", attempt+1,
		)

		s.logger.Info("dispatch_pool",
			"model", d.model,
			"ttft_deadline_ms", d.deadline.Milliseconds(),
			"speculative_at_ms", d.speculativeAt.Milliseconds(),
		)

		if d.firstTokenExpired() {
			// A token that is already buffered beats the clock: deliver it
			// instead of 429ing a request the provider answered on time.
			if chunk, ok := drainReadyFirstContent(d.pr, &d.heldChunks); ok {
				d.commitFirstContent(d.pr, chunk.Data)
				d.committed = true
				break
			}
			if d.abandonInflightForFirstTokenTimeout() {
				goto exhausted
			}
		}

		// ---- Speculative TTFT-aware first-chunk wait ----
		switch d.waitFirstChunk() {
		case outcomeRetry:
			// Post-dispatch provider failure. Stop failing over when the request is
			// unservable (deterministic context overflow, or a capacity transient
			// past maxCapacityClassRetries) so we don't storm all 64 providers; the
			// exhausted ladder then emits one uptime-neutral 429. Faults/timeouts
			// return false and keep failing over as before.
			if d.shouldStopFailover() {
				goto exhausted
			}
			continue
		case outcomeClientGone:
			return
		case outcomeAccepted:
			// Provider accepted or held preamble but hasn't produced content.
			switch d.waitAccepted() {
			case outcomeRetry:
				if d.shouldStopFailover() {
					goto exhausted
				}
				continue
			case outcomeClientGone:
				return
			}
		}

		break
	}

exhausted:
	if !d.committed {
		d.refundReservation()
		if d.providerBodyTooLargeErr != "" &&
			d.lastErrCode == http.StatusRequestEntityTooLarge {
			d.latchProviderBodyTooLarge(d.providerBodyTooLargeErr)
		}
		failure, stickyFault := d.terminalFailureForExhaustion()
		statusCode, reason, timeoutReclassified, dominance :=
			d.resolveDominantExhaustedStatus(failure, stickyFault)
		if timeoutReclassified {
			s.ddIncr("routing.first_chunk_timeout_reclassified", []string{"model:" + d.model, "reason:" + reason})
		}
		switch dominance {
		case exhaustedClientError:
			// Deterministic provider client 4xx (identical fleet-wide): pass the real
			// code through ONCE. Checked BEFORE d.unservable / statusCode==0 so it can
			// never be reclassified to 429/503 — this is a client fault, not capacity.
			s.ddIncr("routing.client_error_passthrough", []string{"model:" + d.model, "code:" + strconv.Itoa(statusCode)})
		case exhaustedGenuineFault:
			// A genuine provider fault observed on any pre-content attempt is
			// request-terminal precedence. Later neutral deadline/capacity
			// refusals still own their own route rows but cannot hide the fault.
		case exhaustedUnservable:
			// The loop stopped early because no provider can serve this request
			// (deterministic context overflow, or a capacity transient that
			// exhausted maxCapacityClassRetries). We already know the verdict, so
			// skip the quick-capacity probe and the 5xx→429 reclassification below:
			// emit a single uptime-neutral 429. This is the proactive complement to
			// the always-on backstop — it converts the request BEFORE storming the
			// fleet, not after 64 attempts.
			s.ddIncr("routing.oversized_request_rejected", []string{"model:" + d.model, "stage:dispatch"})
		case exhaustedDeadline:
			// Every refusal was health-neutral and did not consume the generic
			// capacity retry cap. Once no untried candidate remains, expose one
			// uptime-neutral 429 with its own closed reason.
			s.ddIncr("routing.deadline_unreachable_rejected", []string{"model:" + d.model, "stage:dispatch"})
		case exhaustedUndecided:
			if statusCode == 0 {
				// Distinguish capacity exhaustion (429) from genuine unavailability (503).
				// A quick capacity check tells us if providers exist but are full.
				_, capRej, _ := s.registry.QuickCapacityCheckForRequest(
					d.model, d.estimatedPromptTokens, d.requestedMaxTokens,
					d.traits(), d.requiresVision, d.allowedProviderSerials...)
				if capRej > 0 {
					statusCode = http.StatusTooManyRequests
				} else {
					statusCode = http.StatusServiceUnavailable
				}
			} else if statusCode >= 500 && isCapacityClassProviderError(failure.errText) {
				// Backstop (always on): the provider admitted the request then
				// rejected it because (prompt+max_tokens) overflowed its token budget /
				// KV / context — a capacity condition, not a server fault. Return an
				// uptime-neutral 429 (OpenRouter fails over) instead of the raw 5xx,
				// which would count against our uptime. Fires only on a real provider
				// rejection, so it cannot over-reject servable traffic.
				statusCode = http.StatusTooManyRequests
				reason = "unservable_token_budget"
				s.ddIncr("routing.unservable_reclassified", []string{"model:" + d.model})
			}
		}
		// Resolved once: the telemetry event and the OR-uptime counter must agree
		// on which slot's backend this failure belongs to, and on whether that
		// backend was chosen or degraded into (v0.8.0 paged rollout).
		kvBackend := d.exhaustedKVBackendAttribution(failure, stickyFault)
		s.emitRequest(r.Context(), protocol.SeverityError, d.requestID,
			fmt.Sprintf("inference failed after %d attempt(s)", d.exhaustionAttemptCount()),
			map[string]any{
				"reason":      "dispatch_exhausted",
				"attempt":     d.exhaustionAttemptCount(),
				"status_code": statusCode,
				"last_error":  failure.errText,
				"kv_backend":  kvBackend.Backend,
			})
		if s.metrics != nil {
			s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "failure"})
		}
		s.ddIncr("inference.dispatches", []string{"status:failure"})
		// OR-uptime outcome for a dispatched-but-failed request (exactly once;
		// pre-dispatch rejections emit from recordRejection instead).
		d.recordDispatchedRequestOutcome(kvBackend, classifyOutcomeByCode(statusCode))
		d.recordRequestOutcomeORView(classifyOutcomeByCode(statusCode))
		if statusCode == http.StatusTooManyRequests || statusCode == http.StatusServiceUnavailable {
			retryAfter := s.estimateRetryAfter(d.model)
			if d.lastErrFeasibleAfterMS > 0 {
				// Enriched rejection (routing v2): the rejecting provider
				// forecast when a request of this shape could next be admitted
				// — an honest Retry-After beats the queue-depth heuristic.
				// Clamped to the heuristic's own [2,30]s band so a
				// provider-authored value can neither hammer nor park clients.
				hinted := int((d.lastErrFeasibleAfterMS + 999) / 1000)
				if hinted < 2 {
					hinted = 2
				}
				if hinted > 30 {
					hinted = 30
				}
				retryAfter = hinted
			}
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
			info := d.rejectionInfo("dispatch", reason, statusCode, retryAfter*1000)
			if !stickyFault && (d.unservable || failure.deadline) {
				// No provider could serve this request (it exceeds the model
				// context, or every attempted provider refused the remaining
				// deadline). Mark it not-servable so the rejection ledger's
				// counterfactual reflects the terminal decision.
				info.servabilityComputed = true
				info.candidateCount = 0
			}
			s.recordRejection(info)
		} else {
			s.recordRejection(d.rejectionInfo("dispatch", reason, statusCode, 0))
		}
		rateLimitMessage := fmt.Sprintf(
			"all providers at capacity after %d attempt(s): %s",
			d.exhaustionAttemptCount(), failure.errText)
		if reason == rejectionReasonDeadlineUnreachable {
			rateLimitMessage = fmt.Sprintf(
				"no provider could produce first content within the remaining deadline for model %q",
				d.publicModel)
		}
		if statusCode == http.StatusTooManyRequests {
			writeJSON(w, statusCode, errorResponse("rate_limit_exceeded",
				rateLimitMessage,
				withCode("rate_limit_exceeded")))
		} else if d.terminalClientError {
			// Surface the provider's client-shape error verbatim as an
			// invalid_request_error, with no misleading "after N attempt(s)" framing
			// (it was returned once, deterministically). A jinja_* latch surfaces
			// the curated model_capability message instead of the provider's raw
			// template backtrace.
			if d.terminalClientErrorMessage != "" {
				errorCode := "model_capability"
				if d.terminalClientErrorReason == "payload_too_large" {
					errorCode = "payload_too_large"
				}
				writeJSON(w, statusCode, errorResponse(
					"invalid_request_error", d.terminalClientErrorMessage, withCode(errorCode)))
			} else {
				writeJSON(w, statusCode, errorResponse("invalid_request_error", failure.errText))
			}
		} else {
			writeJSON(w, statusCode, errorResponse("provider_error",
				fmt.Sprintf("inference failed after %d attempt(s): %s", d.exhaustionAttemptCount(), failure.errText)))
		}
		return
	}
	if s.metrics != nil {
		s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "success"})
	}
	s.ddIncr("inference.dispatches", []string{"status:success"})
	// OR-uptime outcome. For STREAMING this is a commit-time approximation (the
	// consumer got content; a later post-commit mid-stream failure is still counted
	// as success — the persisted route-outcome rows hold the exact breakdown). For
	// NON-streaming, "committed" only means a provider chunk arrived and the writer
	// can still fail with a 5xx/504, so the outcome is recorded in
	// writeCommittedResponse from the status it actually writes. Emitted exactly
	// once per dispatched request (disjoint from the exhausted branch above and
	// from pre-dispatch rejections).
	if d.stream {
		d.recordDispatchedRequestOutcome(d.kvBackendAttribution(), orClassSuccess)
	}

	d.writeCommittedResponse()
}
