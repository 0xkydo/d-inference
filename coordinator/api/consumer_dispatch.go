package api

// Provider dispatch, reservation hand-off, cancellation, and provider-body preparation.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/eigeninference/d-inference/coordinator/internal/e2e"
	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/store"
	"github.com/google/uuid"
)

const (
	// inferenceTimeout is the maximum time to wait between chunks (streaming)
	// or for the full response (non-streaming). For streaming, the deadline
	// resets on each received chunk so long-running generations don't time out.
	// 10 minutes allows 32k tokens at ~55 tok/s on slower hardware.
	inferenceTimeout = 600 * time.Second

	// defaultFirstContentDeadlineBase preserves the ordinary coordinator and
	// unit-test budget. Production overrides it to 9s through validated startup
	// configuration; exact model overrides live in modelpolicy and every request
	// adds 1ms per estimated prompt token.
	defaultFirstContentDeadlineBase = 5 * time.Second

	// preambleContentTimeout is the relative cap from the first boilerplate
	// chunk to the first CONTENT chunk. A provider that produced only preamble
	// (role delta / Responses lifecycle) has written ZERO bytes to the client,
	// so a role-then-stall zombie must fail over instead of pinning the request
	// for the full inferenceTimeout. 90s covers the measured pre-content tail
	// (vision prefill is 6-30s). When ReceivedAt is stamped this cap cannot
	// exceed leftover request-absolute first-token budget: AcceptedCh is not a
	// completion token and must not reset that clock.
	preambleContentTimeout = 90 * time.Second

	// chunkBufferSize is the channel buffer size for SSE chunks flowing from
	// the provider to the consumer. A larger buffer prevents dropped chunks
	// when the consumer reads slowly.
	chunkBufferSize = 256

	// maxDispatchAttempts is a SAFETY CEILING on per-request provider failover,
	// not the normal stopping point. A request keeps failing over to fresh
	// healthy providers until one succeeds, OR candidates are exhausted (every
	// failed provider is excluded from re-selection, so dispatchPrimary returns
	// outcomeFailFast on the next attempt once no eligible provider remains), OR
	// the request's deadline/context fires (run() checks r.Context() each
	// attempt). This ceiling only guards against a pathological retry path that
	// fails to exclude a provider (an unbounded hot loop); it is set well above
	// any realistic per-request fault count. Retries never re-queue — only the
	// first attempt may wait for capacity — so failover stays fast, walking the
	// immediately-available healthy providers rather than waiting on busy ones.
	maxDispatchAttempts = 64

	// maxCapacityClassRetries bounds failover specifically for TRANSIENT-capacity
	// rejections (this provider's live KV budget, a full queue, an update drain).
	// Such a shortage MAY clear on another provider, so we fail over — but only a
	// few times, so a fleet-wide transient (or an oversized request the determinism
	// check didn't tag) cannot walk all maxDispatchAttempts providers and 503 each
	// (the prod storm: median 22, max 63 attempts, ~8.7 min, 0% eventual success).
	// A DETERMINISTIC-context rejection (prompt > model context, identical on every
	// provider) stops on the FIRST attempt regardless — see classifyRejection.
	maxCapacityClassRetries = 3

	// maxFirstChunkTimeoutRetries bounds failover for coordinator-synthesized
	// first-chunk TIMEOUTS (the untyped 504 the exhausted ladder reclassifies
	// to a retryable 429 with reason "first_chunk_timeout"). Unlike capacity
	// rejections these carried NO cap: every retry re-ran a full fleet
	// reservation scan (~1,260 providers, registry.ReserveProviderEx), and in
	// the 2026-09-01 congestion collapse retry-amplified inbound (~100 req/s
	// of retryable 429 traffic from OpenRouter) times per-request fleet scans
	// saturated every coordinator CPU — attempt-0 route p50 went 40ms → 4.6s,
	// success ~40%, 429s were delivered after 11s, inbound ~6k/min vs served
	// ~550/min. The request-absolute first-content budget already bounds WALL
	// time per request; this bounds CPU: after this many timed-out attempts
	// (each on a distinct provider — a timed-out provider is excluded from
	// re-selection) the ladder exhausts immediately into the existing
	// synthetic-timeout → 429 reclassification (classifyExhaustedStatus).
	maxFirstChunkTimeoutRetries = 3

	// speculativeTimerRatio is the fraction of the TTFT deadline at which
	// the coordinator launches a speculative backup dispatch. The primary
	// provider gets this fraction of the deadline before the backup is
	// started, and then both race until one produces the first chunk.
	speculativeTimerRatio = 0.5

	// maxHeldBoilerplate bounds how many pre-content boilerplate chunks the
	// dispatch loop holds per provider before committing anyway. Real
	// preambles are one chunk (chat role delta) or two (Responses
	// created/in_progress), so the cap exists only to stop a misbehaving
	// provider from growing the held buffer for the whole inference window.
	// Excess boilerplate is dropped while the first-content clock continues;
	// it must never be mistaken for content and commit a bad provider.
	maxHeldBoilerplate = 8

	// cancelWriteTimeout bounds how long a cancel write to the provider can
	// block. Using context.Background() unbounded here risks hanging the HTTP
	// handler goroutine when a WebSocket is half-dead.
	cancelWriteTimeout = 2 * time.Second
)

// sendProviderCancel sends a Cancel message for the given request to the
// provider with a bounded timeout so a half-dead WebSocket doesn't hang the
// caller. It reports whether the frame was handed to the writer. Failures are
// logged at debug level because a disconnect race is the expected case — the
// provider may already be gone — but every one is metered
// (inference.cancel_send_failed{reason}) since a dropped cancel is the only
// silent-loss path on the coordinator side of cancel delivery.
//
// This is the raw primitive. Abandon paths that may leave the provider
// generating go through sendAbandonCancel / cancelDispatch so the cancel is
// recorded for terminal correlation and zombie re-sends.
func (s *Server) sendProviderCancel(provider *registry.Provider, requestID string) bool {
	if provider == nil || provider.Conn == nil {
		return false
	}
	cancelMsg := protocol.CancelMessage{Type: protocol.TypeCancel, RequestID: requestID}
	cancelData, err := json.Marshal(cancelMsg)
	if err != nil {
		s.logger.Error("failed to marshal cancel message", "request_id", requestID, "error", err)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), cancelWriteTimeout)
	defer cancel()
	if err := provider.EnqueueText(ctx, cancelData); err != nil {
		s.ddIncr(metricCancelSendFailed, []string{"reason:" + cancelSendFailureReason(err)})
		s.logger.Debug("failed to send cancel (provider may have disconnected)",
			"request_id", requestID, "error", err)
		return false
	}
	return true
}

func writeProviderInferenceRequestDeferred(
	ctx context.Context,
	provider *registry.Provider,
	builder registry.TextFrameBuilder,
	onHandoff registry.TextFrameHandoff,
) (registry.TextFrameWriteMetadata, error) {
	if provider == nil || provider.Conn == nil {
		return registry.TextFrameWriteMetadata{}, errors.New("provider websocket is not connected")
	}
	return provider.WriteTextDeferred(ctx, builder, onHandoff)
}

// cancelDispatch abandons a dispatch attempt that may still be generating
// (hedge loser, client gone before content): removes the pending request,
// marks the provider idle, sends a cancel over WebSocket so the provider stops,
// and refunds this attempt's provider-specific reservation top-up. cause is
// the bounded cancel cause recorded for terminal correlation.
//
// The cancel is sent only when THIS call removed a live pending record and no
// clean terminal has been ingressed for it. A missing record means a provider
// terminal already claimed the attempt (handleInferenceError removes pending
// before publishing on ErrorCh); a completion parked on the speculative
// empty-completion decision leaves the record but marks completion ingress.
// In both cases nothing is running provider-side, and cancelling would only
// cost the provider a no-op frame per request. Attempts whose terminal was
// observed by the caller use cancelDispatchAfterTerminal instead.
//
// The top-up refund only runs if THIS call actually removed the pending request
// (RemovePending returned non-nil). If settlement (handleComplete) already
// claimed it via its own RemovePending, we must not also refund — that would
// double-credit the consumer.
func (s *Server) cancelDispatch(provider *registry.Provider, pr *registry.PendingRequest, cause string) {
	if provider == nil || pr == nil {
		return
	}
	pr.ResolveSpeculativeEmptyCompletion(false)
	now := time.Now()
	// Record before RemovePending: a terminal racing this cleanup looks the
	// id up only after its own RemovePending returns nil, and must find the
	// entry rather than log the terminal as unknown.
	created, expired := s.zombieCanceller.record(pr.RequestID, pr.Model, cause, now)
	s.emitExpiredCancelEntries(expired)
	removed := provider.RemovePending(pr.RequestID)
	s.registry.SetProviderIdle(provider.ID)
	if removed != nil && !pr.HasCompletionIngress() {
		pr.Profile.Mark(registry.StampCancelSent)
		s.sendRecordedCancel(provider, pr.RequestID, pr.Model, cause)
	} else if created {
		s.zombieCanceller.forget(pr.RequestID)
	}
	if removed != nil {
		s.refundProviderExtra(pr)
	}
}

// cancelDispatchAfterTerminal is cancelDispatch for an attempt whose provider
// terminal the caller has already observed (ErrorCh value / ChunkCh closed).
// The terminal handler removed the pending record before publishing it, so
// nothing is running provider-side and no cancel frame is sent — only the
// speculative arbitration, idle transition and top-up refund remain.
func (s *Server) cancelDispatchAfterTerminal(provider *registry.Provider, pr *registry.PendingRequest) {
	if provider == nil || pr == nil {
		return
	}
	pr.ResolveSpeculativeEmptyCompletion(false)
	removed := provider.RemovePending(pr.RequestID)
	s.registry.SetProviderIdle(provider.ID)
	if removed != nil {
		s.refundProviderExtra(pr)
	}
}

// cancelDispatchForFirstContentTimeout atomically arbitrates timeout cleanup
// against provider ingress. false means an on-time event or another terminal
// already owns the request, so the wait loop must keep draining its channels.
func (s *Server) cancelDispatchForFirstContentTimeout(
	provider *registry.Provider,
	pr *registry.PendingRequest,
) bool {
	if provider == nil || pr == nil {
		return false
	}
	now := time.Now()
	created, expired := s.zombieCanceller.record(pr.RequestID, pr.Model, cancelCauseFirstChunkTimeout, now)
	s.emitExpiredCancelEntries(expired)
	removed, deferred := provider.RemovePendingForFirstContentTimeout(pr.RequestID)
	if deferred || removed == nil {
		if created {
			s.zombieCanceller.forget(pr.RequestID)
		}
		return false
	}
	pr.ResolveSpeculativeEmptyCompletion(false)
	s.registry.SetProviderIdle(provider.ID)
	pr.Profile.Mark(registry.StampCancelSent)
	s.sendRecordedCancel(provider, pr.RequestID, pr.Model, cancelCauseFirstChunkTimeout)
	s.refundProviderExtra(pr)
	return true
}

// refundProviderExtra refunds the provider-specific surcharge charged on top of
// the shared base reservation when an attempt is abandoned. It is idempotent:
// after refunding it resets ReservedMicroUSD to the base so a second call (or a
// later settlement) cannot double-refund. The shared base is never refunded
// here — that is handled once by refundReservation (full failure) or by the
// winning attempt's settlement.
func (s *Server) refundProviderExtra(pr *registry.PendingRequest) {
	if pr == nil {
		return
	}
	extra := pr.ReservedMicroUSD - pr.BaseReservedMicroUSD
	if extra <= 0 {
		return
	}
	_ = s.store.Credit(pr.ConsumerKey, extra, store.LedgerRefund, "reservation_extra_refund:"+pr.RequestID)
	pr.ReservedMicroUSD = pr.BaseReservedMicroUSD
	s.ddIncr("billing.reservation_extra_refunds", []string{"model:" + pr.Model})
}

// errModelTooLarge is the dispatch error returned when providers serve the
// requested model but none of them has enough total memory to ever load it.
// Distinct from "no provider available" so the caller rejects fast instead of
// queuing for 120s — queueing can't help a model that will never fit.
const errModelTooLarge = "model too large for any available provider"

// errTTFTTooSlow is the dispatch error returned when providers are available
// but all of them exceed the per-request TTFT ceiling. Distinct from
// "no provider available" so the caller returns a retryable 429 instead of
// queueing for a provider that would miss the OpenRouter SLA target.
const errTTFTTooSlow = "all available providers exceed the TTFT target"

// errFirstContentDeadlineExpired is returned when the request-absolute
// first-content clock runs out before an inference_request reaches the provider
// wire. No provider work was started, so callers surface a deadline 429 without
// charging provider health.
const errFirstContentDeadlineExpired = "first-content deadline expired before provider dispatch"

// errRoutingScanSaturated is returned when no provider-selection scan slot
// (Server.routingScanSem) freed up within the request's remaining
// first-content budget: the coordinator itself is the bottleneck (the
// 2026-09-01 congestion collapse). No provider was scanned or contacted, so
// callers shed ONE capacity-shaped retryable 429 — never a 5xx, never more
// scans.
const errRoutingScanSaturated = "routing scan capacity saturated — coordinator busy"

// errClientGoneBeforeScan is returned when the caller's context fired while
// the dispatch goroutine was parked for a provider-selection scan slot. No
// provider was scanned or contacted; the dispatch loop takes its ordinary
// client-gone terminal (cancelled route outcome, refund, no response body) —
// never the routing_saturated 429 or a rejection-ledger row.
const errClientGoneBeforeScan = "client disconnected before provider selection"

// attempt0RouteAnchor returns the instant the attempt-0 route-latency EWMA
// sample is measured from — the SAME anchor applyTimingDecomposition uses for
// route_ms (MediaFetchedAt when a remote-media fetch happened, else
// ReservedAt) — so download or parse time can never fake routing distress.
// Zero when the request never stamped a reservation (bare test fixtures):
// the caller then records no sample.
func attempt0RouteAnchor(t *registry.RequestTiming) time.Time {
	if t == nil {
		return time.Time{}
	}
	if !t.MediaFetchedAt.IsZero() {
		return t.MediaFetchedAt
	}
	return t.ReservedAt
}

type routeDecisionRecorder func(*registry.Provider, *registry.PendingRequest, registry.RoutingDecision)

// dispatchReserver selects and atomically reserves a provider for an
// already-constructed PendingRequest. It is the ONE seam between provider
// SELECTION and the single prepare/encrypt/write funnel in
// dispatchWithReserver: wave-2 callers plug in the retained-plan variants
// (ReserveNextFromPlan / RefreshDispatchPlan) without forking the funnel.
// The returned plan is non-nil only for scan-backed reservers that retain
// alternates.
type dispatchReserver func(pr *registry.PendingRequest, excludeIDs []string) (*registry.Provider, registry.RoutingDecision, *registry.DispatchPlan)

// dispatchOneProvider encrypts and sends an inference request to a single
// provider selected by a fresh full scan. It returns the pending request and
// provider on success, or an error string on failure, plus the bounded
// DispatchPlan of provisional alternates retained from the SAME scan (nil
// whenever no provider was reserved) so retries and speculative backups can
// consume retained identities instead of rescanning the fleet (Routing v2
// Phase 3). The excludeProviders set is updated on failure. selfRoutePolicy
// and its resolvers live in self_route.go.
func (s *Server) dispatchOneProvider(
	r *http.Request,
	model string,
	publicModel string,
	rawBody []byte,
	consumerKey string,
	consumerLocation *store.ProviderLocation,
	reservedMicroUSD int64,
	estimatedPromptTokens int,
	requestDeadline time.Duration,
	requestedMaxTokens int,
	tokenAdmission registry.TokenAdmission,
	requiresVision bool,
	traits registry.RequestTraits,
	allowedProviderSerials []string,
	isResponsesAPI bool,
	policy selfRoutePolicy,
	timing *registry.RequestTiming,
	serviceReservation bool,
	cachePlan registry.CachePlan,
	excludeProviders map[string]struct{},
	attempt int,
	rp *registry.RequestProfile,
	backupOf string,
	recordRoute routeDecisionRecorder,
	onDispatched func(),
) (
	provider *registry.Provider,
	pr *registry.PendingRequest,
	decision registry.RoutingDecision,
	plan *registry.DispatchPlan,
	lastErr string,
	lastErrCode int,
) {
	return s.dispatchWithReserver(
		r, model, publicModel, rawBody, consumerKey, consumerLocation,
		reservedMicroUSD, estimatedPromptTokens, requestDeadline,
		requestedMaxTokens, tokenAdmission, requiresVision, traits,
		allowedProviderSerials, isResponsesAPI, policy, timing,
		serviceReservation, cachePlan, excludeProviders, attempt, rp, backupOf,
		recordRoute, onDispatched,
		true, // ReserveProviderWithPlan is the O(fleet) full scan
		func(pr *registry.PendingRequest, excludeIDs []string) (*registry.Provider, registry.RoutingDecision, *registry.DispatchPlan) {
			return s.registry.ReserveProviderWithPlan(model, pr, excludeIDs...)
		},
	)
}

// dispatchWithReserver is the single prepare/encrypt/write funnel behind every
// provider dispatch: pending construction and admission stamps, the pluggable
// reservation, the billing surcharge, E2E encryption, and the
// deadline-bounded provider write, with releaseUnsentDispatch cleanup on every
// failure path. onDispatched (nil-safe) fires inside the write handoff
// callback — the same instant Timing.DispatchedAt is stamped — so
// providerDispatches counts frames that actually reached a provider, never
// loop attempts.
func (s *Server) dispatchWithReserver(
	r *http.Request,
	model string,
	publicModel string,
	rawBody []byte,
	consumerKey string,
	consumerLocation *store.ProviderLocation,
	reservedMicroUSD int64,
	estimatedPromptTokens int,
	requestDeadline time.Duration,
	requestedMaxTokens int,
	tokenAdmission registry.TokenAdmission,
	requiresVision bool,
	traits registry.RequestTraits,
	allowedProviderSerials []string,
	isResponsesAPI bool,
	policy selfRoutePolicy,
	timing *registry.RequestTiming,
	serviceReservation bool,
	cachePlan registry.CachePlan,
	excludeProviders map[string]struct{},
	attempt int,
	rp *registry.RequestProfile,
	backupOf string,
	recordRoute routeDecisionRecorder,
	onDispatched func(),
	fullScan bool,
	reserve dispatchReserver,
) (
	provider *registry.Provider,
	pr *registry.PendingRequest,
	decision registry.RoutingDecision,
	plan *registry.DispatchPlan,
	lastErr string,
	lastErrCode int,
) {
	receivedAt := timingReceivedAt(timing)
	_, dispatchable := firstContentBudgetMillis(receivedAt, requestDeadline)
	if !dispatchable {
		return nil, nil, decision, nil, errFirstContentDeadlineExpired, http.StatusGatewayTimeout
	}

	requestID := uuid.New().String()
	ap := rp.NewAttempt(requestID, attempt, backupOf)
	ap.Mark(registry.StampAttemptStart)
	// Any failure return closes the attempt as not dispatched (terminal half; the handler half lands in finalizeProfile);
	// a dispatched attempt is left for the provider terminal / relay to close.
	defer func() {
		if provider == nil {
			closeUndispatchedAttempt(ap, lastErr, lastErrCode)
		}
	}()
	pr = &registry.PendingRequest{
		RequestID: requestID,
		Profile:   ap,
		// Attempt is stamped at construction — BEFORE the request is encrypted
		// and sent to the provider — so a fast provider that returns
		// inference_complete immediately is correlated to the right route row.
		// Setting it after the send (on the dispatch goroutine) would race the
		// provider WS reader goroutine's handleComplete read of pr.Attempt.
		Attempt:                attempt,
		Model:                  model,
		PublicModel:            publicModel,
		ConsumerKey:            consumerKey,
		KeyID:                  keyIDFromContext(r.Context()),
		KeyLimitMicroUSD:       keyLimitMicroFromContext(r.Context()),
		KeyLimitReset:          keyLimitResetFromContext(r.Context()),
		ConsumerLocation:       consumerLocation,
		IsResponsesAPI:         isResponsesAPI,
		EstimatedPromptTokens:  estimatedPromptTokens,
		RequiresVision:         requiresVision,
		Traits:                 traits,
		RequestedMaxTokens:     requestedMaxTokens,
		TokenAdmission:         tokenAdmission,
		CachePlan:              cachePlan,
		ReservedMicroUSD:       reservedMicroUSD,
		BaseReservedMicroUSD:   reservedMicroUSD,
		ServiceReservation:     serviceReservation,
		AllowedProviderSerials: allowedProviderSerials,
		SelfRouteOnly:          policy.enabled,
		PreferOwner:            policy.prefer,
		OwnerAccountID:         policy.ownerAccountID,
		FreeSelfRoute:          policy.enabled,
		MetadataDetails:        metadataDetailsFromRequest(r),
		AcceptedCh:             make(chan struct{}, 1),
		ChunkCh:                make(chan registry.ProviderChunk, chunkBufferSize),
		CompleteCh:             make(chan protocol.UsageInfo, 1),
		ErrorCh:                make(chan protocol.InferenceErrorMessage, 1),
		Timing:                 timing,
	}
	if !receivedAt.IsZero() {
		pr.FirstContentDeadline = receivedAt.Add(requestDeadline)
	}

	// Public inference routes (not self-route / prefer-owner) enforce the
	// OpenRouter TTFT ceiling inside the scheduler. This makes the preflight
	// check authoritative: the router cannot select a provider whose estimated
	// TTFT is above the threshold.
	// Routing v2 (P1 fix): only enforce the TTFT ceiling inside the scheduler when
	// the HARD gate is on. In soft mode (default) MaxTTFTMs stays 0 so the primary
	// dispatch serves the best-available provider instead of re-rejecting an
	// over-threshold request the preflight already chose to soft-serve. (Mirrors
	// queueMaxTTFTMs, which already returns 0 in soft mode.)
	if !policy.enabled && !policy.prefer && s.hardTTFTGateApplies(requiresVision) {
		pr.MaxTTFTMs = float64(requestDeadline.Milliseconds())
	}
	// Refresh immediately before reservation: every retry spends the same
	// absolute clock, so the scheduler must never see the original ceiling.
	if !pr.RefreshFirstContentBudget(time.Now()) {
		return nil, nil, decision, nil, errFirstContentDeadlineExpired, http.StatusGatewayTimeout
	}
	// Routing v2 W2: soft per-request decode floor (0 = off). Applies to all
	// routes; it only ranks providers, never rejects.
	pr.MinDecodeTPS = s.minDecodeTPS

	excludeList := func() []string {
		ids := make([]string, 0, len(excludeProviders))
		for id := range excludeProviders {
			ids = append(ids, id)
		}
		return ids
	}

	// noteSelectionSample feeds the attempt-0 route-latency distress EWMA
	// behind estimateRetryAfter (2026-09-01: route p50 40ms → 4.6s while the
	// empty-queue heuristic kept answering "retry in 2s"). Anchored exactly
	// where applyTimingDecomposition anchors route_ms (MediaFetchedAt when
	// set, else ReservedAt) so a multi-second media download or slow body
	// parse can never masquerade as routing distress. Called on BOTH the
	// successful reservation (at the RoutedAt stamp) and every failed
	// attempt-0 selection (semaphore acquisition timeout, scan that yields no
	// provider): under TOTAL overload no selection ever succeeds, and an
	// EWMA fed only by successes would sit at 0 — keeping Retry-After at the
	// legacy 2s exactly when distress scaling matters most.
	noteSelectionSample := func() {
		if attempt != 0 {
			return
		}
		if anchor := attempt0RouteAnchor(timing); !anchor.IsZero() {
			s.noteAttempt0RouteLatency(time.Since(anchor))
		}
	}

	// Bound concurrent provider-selection scans (2026-09-01 congestion
	// collapse: retry-amplified inbound × a fresh full fleet scan per attempt
	// saturated every coordinator CPU). Only O(fleet) reservers take a slot —
	// the full scan, the plan REFRESH (itself a full re-scan), and the
	// speculative-backup scan. A retained-plan step (ReserveNextFromPlan)
	// revalidates at most the plan's bounded entries, so it bypasses the
	// semaphore: a held slot must never starve the cheap retry path that
	// exists precisely to avoid rescans. The wait is bounded by the request's
	// remaining first-content budget: a goroutine parks cheaply on the channel
	// and either scans as soon as a slot frees or sheds capacity-shaped
	// (errRoutingScanSaturated → one retryable 429) once the budget is gone.
	if fullScan {
		switch s.acquireRoutingScanSlot(
			firstTokenRemainingSince(receivedAt, requestDeadline),
			r.Context().Done(),
		) {
		case scanSlotClientGone:
			// The caller vanished while parked for a slot: this is the
			// ordinary client-gone terminal, never the routing_saturated
			// 429/rejection row (and no distress sample — a vanished caller
			// proves nothing about selection latency).
			return nil, nil, decision, nil, errClientGoneBeforeScan, 0
		case scanSlotTimeout:
			noteSelectionSample()
			return nil, nil, decision, nil, errRoutingScanSaturated, http.StatusTooManyRequests
		}
	}
	provider, decision, plan = reserve(pr, excludeList())
	ap.Mark(registry.StampReserveDone)
	ap.SetDecision(decision)
	if fullScan {
		s.releaseRoutingScanSlot()
	}
	if provider == nil {
		noteSelectionSample()
		// Providers serve this model but none can physically fit it: don't make
		// the caller queue/retry for something that will never load.
		if decision.CandidateCount == 0 && decision.CapacityRejections == 0 && decision.ModelTooLargeRejections > 0 {
			return nil, nil, decision, plan, errModelTooLarge, http.StatusServiceUnavailable
		}
		// Providers are available but all exceed the TTFT ceiling. Fail fast
		// with a retryable 429 rather than queueing or routing to a slow
		// provider.
		if decision.TTFTRejections > 0 {
			return nil, nil, decision, plan, errTTFTTooSlow, http.StatusTooManyRequests
		}
		return nil, nil, decision, plan, "no provider available", http.StatusServiceUnavailable
	}
	pendingCleanup := true
	cleanupPending := func() {
		if pendingCleanup {
			s.releaseUnsentDispatch(provider, pr)
			pendingCleanup = false
		}
	}
	defer cleanupPending()
	if pr.Timing != nil {
		pr.Timing.RoutedAt = time.Now()
	}
	noteSelectionSample()
	if ap != nil {
		ap.ProviderID = provider.ID
		provider.Mu().Lock()
		ap.ProviderVersion = provider.Version
		ap.ChipFamily = provider.Hardware.ChipFamily
		provider.Mu().Unlock()
		ap.KVBackend, _ = provider.SlotKVBackendTags(model)
	}
	if recordRoute != nil {
		recordRoute(provider, pr, decision)
	}

	// A request settles FREE when it's served by a machine the caller owns:
	// exclusive self-route (policy.enabled) always, OR a prefer request whose
	// SELECTED provider is the caller's own machine (settlement refunds it to
	// zero). In that case there is no payout and no reservation to top up — and
	// applying a provider custom price above the platform rate would wrongly 429
	// the free owned route, so skip both the payout warning and the top-up.
	settlesFree := policy.enabled
	if !settlesFree && policy.prefer {
		provider.Mu().Lock()
		settlesFree = policy.ownerAccountID != "" && provider.AccountID == policy.ownerAccountID
		provider.Mu().Unlock()
	}

	if s.billing != nil && !settlesFree && !providerHasPayoutDestination(provider) {
		s.logger.Warn("provider missing payout destination, crediting to internal ledger",
			"provider_id", provider.ID)
	}

	// Free (owned) requests are settled at zero cost (handleComplete), so there
	// is no reservation to top up for a provider's custom price.
	if s.billing != nil && !settlesFree {
		_, err := s.reserveAdditionalForProvider(pr, provider)
		if err != nil {
			cleanupPending()
			excludeProviders[provider.ID] = struct{}{}
			if errors.Is(err, store.ErrInsufficientBalance) {
				return nil, nil, decision, plan, "insufficient funds for provider price", http.StatusPaymentRequired
			}
			s.logger.Error("provider reservation failed (DB error)", "provider_id", provider.ID, "error", err)
			return nil, nil, decision, plan, "service temporarily unavailable — please retry", http.StatusServiceUnavailable
		}
	}
	ap.Mark(registry.StampTopupDone)
	// refundExtra credits back the provider-specific surcharge that
	// reserveAdditionalForProvider may have added. The caller's
	// refundReservation only covers the base reservation.
	refundExtra := func() {
		extra := pr.ReservedMicroUSD - reservedMicroUSD
		if extra > 0 {
			start := time.Now()
			_ = s.store.Credit(consumerKey, extra, store.LedgerRefund, "reservation_extra_refund:"+requestID)
			s.ddIncr("billing.reservation_extra_refunds", []string{"model:" + model})
			s.ddHistogram("store.credit.latency_ms", float64(time.Since(start).Milliseconds()), []string{"op:reservation_extra_refund"})
			pr.ReservedMicroUSD = reservedMicroUSD
		}
	}

	// E2E encryption
	if provider.PublicKey == "" {
		refundExtra()
		cleanupPending()
		excludeProviders[provider.ID] = struct{}{}
		return nil, nil, decision, plan, "no provider with E2E encryption", http.StatusServiceUnavailable
	}

	providerPubKey, err := e2e.ParsePublicKey(provider.PublicKey)
	if err != nil {
		refundExtra()
		cleanupPending()
		excludeProviders[provider.ID] = struct{}{}
		return nil, nil, decision, plan, "provider public key invalid", http.StatusServiceUnavailable
	}

	sessionKeys, err := e2e.GenerateSessionKeys()
	if err != nil {
		refundExtra()
		cleanupPending()
		return nil, nil, decision, plan, "failed to generate session keys", http.StatusInternalServerError
	}

	if err := s.registry.PrepareCacheAttempt(pr, provider); err != nil {
		s.registry.ForgetCacheAttempt(pr)
		refundExtra()
		cleanupPending()
		return nil, nil, decision, plan, "failed to prepare cache-safe request", http.StatusInternalServerError
	}
	// Pre-fix providers crash on a vision request carrying sampling penalties;
	// strip them for those providers only. Protocol-0 providers additionally get
	// a coordinator-authored prompt_cache_key only inside this sealed body.
	sealedBody, err := bodyForCacheAttempt(rawBody, requiresVision, provider, pr)
	if err != nil {
		s.registry.ForgetCacheAttempt(pr)
		refundExtra()
		cleanupPending()
		if errors.Is(err, errProviderBodyTooLarge) {
			excludeProviders[provider.ID] = struct{}{}
			return nil, nil, decision, plan, err.Error(), http.StatusRequestEntityTooLarge
		}
		return nil, nil, decision, plan, "failed to prepare provider request", http.StatusInternalServerError
	}
	encrypted, err := e2e.Encrypt(sealedBody, providerPubKey, sessionKeys)
	if err != nil {
		s.registry.ForgetCacheAttempt(pr)
		refundExtra()
		cleanupPending()
		return nil, nil, decision, plan, "failed to encrypt request", http.StatusInternalServerError
	}
	if pr.Timing != nil {
		pr.Timing.EncryptedAt = time.Now()
	}
	ap.Mark(registry.StampEncrypted)
	pr.SessionPrivKey = &sessionKeys.PrivateKey
	// pr.ReservedMicroUSD was already set in the struct literal and may have
	// been increased by reserveAdditionalForProvider above. Don't overwrite.

	// Bound the provider write by the request-absolute first-token clock (see
	// firstTokenWriteContext): a congested write lane must not silently eat
	// the budget while the aggregator's cancel clock keeps running.
	writeCtx, cancelWrite := firstTokenWriteContext(
		r.Context(), receivedAt, requestDeadline)
	ap.Mark(registry.StampWriteSubmitted)
	_, writeErr := writeProviderInferenceRequestDeferred(
		writeCtx,
		provider,
		providerInferenceFrameBuilder(
			requestID, encrypted.EphemeralPublicKey, encrypted.Ciphertext, pr),
		func(metadata registry.TextFrameWriteMetadata) {
			if pr.Timing != nil {
				pr.Timing.DispatchedAt = metadata.DequeuedAt
			}
			if onDispatched != nil {
				onDispatched()
			}
			ap.MarkAt(registry.StampWriteDequeued, metadata.DequeuedAt)
		},
	)
	cancelWrite()
	if writeErr == nil {
		ap.Mark(registry.StampWriteDone)
	}
	if writeErr != nil {
		s.registry.ForgetCacheAttempt(pr)
		refundExtra()
		cleanupPending()
		excludeProviders[provider.ID] = struct{}{}
		if errors.Is(writeErr, context.DeadlineExceeded) ||
			errors.Is(writeErr, errFirstContentDeadlineAtWriter) {
			// The writer either discarded the frame before handoff or aborted
			// its connection during an in-flight write. Cancel defensively in
			// case the provider decoded the final bytes before disconnect.
			ap.Mark(registry.StampCancelSent)
			s.sendProviderCancel(provider, requestID)
			return nil, nil, decision, plan, errFirstContentDeadlineExpired, http.StatusGatewayTimeout
		}
		return nil, nil, decision, plan, "failed to send request to provider", http.StatusBadGateway
	}
	pendingCleanup = false

	return provider, pr, decision, plan, "", 0
}

// releaseUnsentDispatch returns a reservation after frame construction or
// socket handoff fails. Resolving speculative completion arbitration first
// guarantees a provider completion already waiting off the read loop cannot
// remain stranded after pending state is removed.
func (s *Server) releaseUnsentDispatch(
	provider *registry.Provider,
	pr *registry.PendingRequest,
) {
	if provider == nil || pr == nil {
		return
	}
	pr.ResolveSpeculativeEmptyCompletion(false)
	provider.RemovePending(pr.RequestID)
	s.registry.SetProviderIdle(provider.ID)
}

// penaltySafeProviderVersion is the first provider release whose VLM penalty
// path handles repetition/presence/frequency penalties without crashing (the
// TokenRing 2D-prompt fix). Providers below it crash on a vision request that
// carries any of these fields, so the coordinator strips them before sealing
// for such a provider. Keep in sync with the release that ships the fix.
const penaltySafeProviderVersion = "0.6.7"

// visionPenaltyFields crash the pre-fix VLM penalty path on image requests.
var visionPenaltyFields = []string{"repetition_penalty", "presence_penalty", "frequency_penalty"}

// bodyForProvider returns the request body to seal for `provider`. It equals
// rawBody, except a vision request routed to a pre-fix provider has the
// crash-inducing penalty fields stripped. Fixed providers receive the penalties
// unchanged. Per-provider (not pre-routing) so a retry on a fixed provider keeps
// them. Remove once MIN_PROVIDER_VERSION clears all pre-fix builds.
func bodyForProvider(rawBody []byte, requiresVision bool, provider *registry.Provider) []byte {
	if !requiresVision {
		return rawBody
	}
	if provider.Version != "" && !semverLess(provider.Version, penaltySafeProviderVersion) {
		return rawBody // fixed provider — pass penalties through
	}
	// A body carrying none of the penalty fields at its top level is returned
	// unchanged without decoding it — the same outcome the decode path reaches
	// through changed=false, minus a full-body parse per sizing probe.
	if has, ok := topLevelObjectHasAnyKey(rawBody, visionPenaltyFields); ok && !has {
		return rawBody
	}
	parsed, err := decodeInferenceJSONObject(rawBody)
	if err != nil {
		return rawBody
	}
	changed := false
	for _, key := range visionPenaltyFields {
		if _, ok := parsed[key]; ok {
			delete(parsed, key)
			changed = true
		}
	}
	if !changed {
		return rawBody
	}
	if stripped, err := marshalForwardBody(parsed); err == nil {
		return stripped
	}
	return rawBody
}

var errProviderBodyTooLarge = errors.New("provider request body too large")

type providerBodyTooLargeError struct {
	size int
}

func (e *providerBodyTooLargeError) Error() string {
	return fmt.Sprintf("%s: %d bytes exceeds the %d-byte limit after cache isolation",
		errProviderBodyTooLarge, e.size, maxInferenceBodyBytes)
}

func (e *providerBodyTooLargeError) Unwrap() error {
	return errProviderBodyTooLarge
}

func oversizedProviderBodyBytes(err error) int {
	var sizeErr *providerBodyTooLargeError
	if errors.As(err, &sizeErr) {
		return sizeErr.size
	}
	return 0
}

func legacyCacheBustBodyBytes(
	rawBody []byte,
	requiresVision bool,
	provider *registry.Provider,
) (int, error) {
	if provider == nil {
		return 0, nil
	}
	return cacheAttemptSizeError(
		bodyForProvider(rawBody, requiresVision, provider),
		strings.Repeat("x", registry.LegacyCacheBustKeyLength))
}

func providerBodySizeError(
	rawBody []byte,
	requiresVision bool,
	provider *registry.Provider,
) (int, error) {
	if provider == nil {
		return 0, nil
	}
	provider.Mu().Lock()
	usesLegacyCacheBust := provider.PrefixCacheProtocol < 1
	provider.Mu().Unlock()
	legacyKey := ""
	if usesLegacyCacheBust {
		legacyKey = strings.Repeat("x", registry.LegacyCacheBustKeyLength)
	}
	return cacheAttemptSizeError(
		bodyForProvider(rawBody, requiresVision, provider), legacyKey)
}

func minimumLegacyCacheBustOverflow(rawBody []byte, requiresVision bool) (int, error) {
	// An empty-version provider exercises the only provider-specific shrinking
	// transform: legacy vision penalty removal. Raise a fleet-wide protocol floor
	// only when even that smallest valid protocol-0 body exceeds the cap.
	return legacyCacheBustBodyBytes(rawBody, requiresVision, &registry.Provider{})
}

func routingTraitsForProviderBody(
	hasTools bool,
	providerBody []byte,
	requiresVision bool,
) (registry.RequestTraits, error) {
	traits := registry.RequestTraits{HasTools: hasTools}
	_, err := minimumLegacyCacheBustOverflow(providerBody, requiresVision)
	if errors.Is(err, errProviderBodyTooLarge) {
		traits.MinPrefixCacheProtocol = 1
	}
	return traits, err
}

func exhaustedProviderPreparationError(
	decision registry.RoutingDecision,
	reservationErr error,
	providerBodyOverflowErr error,
) error {
	if decision.CapacityRejections > 0 {
		return nil
	}
	if reservationErr != nil {
		return reservationErr
	}
	return providerBodyOverflowErr
}

// bodyForCacheAttempt returns the body to seal for one dispatch attempt: the
// provider-specific body (bodyForProvider) with the protocol-0 cache-bust key
// added as prompt_cache_key when the attempt carries one, size-checked
// against the sealed-frame cap.
func bodyForCacheAttempt(rawBody []byte, requiresVision bool, provider *registry.Provider, pr *registry.PendingRequest) ([]byte, error) {
	body := bodyForProvider(rawBody, requiresVision, provider)
	if pr == nil || pr.LegacyCacheBustKey == "" {
		if len(body) > maxInferenceBodyBytes {
			return nil, &providerBodyTooLargeError{size: len(body)}
		}
		return body, nil
	}
	keyJSON, err := json.Marshal(pr.LegacyCacheBustKey)
	if err != nil {
		return nil, err
	}
	sealed, ok := spliceTopLevelMember(body, legacyCacheBustField, keyJSON)
	if !ok {
		if sealed, err = sealLegacyCacheBust(body, keyJSON); err != nil {
			return nil, err
		}
	}
	if len(sealed) > maxInferenceBodyBytes {
		return nil, &providerBodyTooLargeError{size: len(sealed)}
	}
	return sealed, nil
}
