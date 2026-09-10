package registry

// Routing cost model, TPS resolution, and tunables.

import (
	"math"
	"time"

	"github.com/eigeninference/d-inference/coordinator/env"
	"github.com/eigeninference/d-inference/coordinator/protocol"
)

// buildCandidateWithReason returns the candidate plus, on rejection,
// the reason so callers can split metrics by failure mode.
// now is the caller's scan clock (see snapshotProviderLockedEx). This is the
// by-value convenience form for cold callers (the commit re-check, the plan
// revalidation, tests); the fleet scan uses buildCandidateInto on an arena
// slot whose snapshot was filled in place.
func (r *Registry) buildCandidateWithReason(snap routingSnapshot, pr *PendingRequest, now time.Time) (*routingCandidate, candidateRejection, bool) {
	c := &routingCandidate{snapshot: snap}
	reason, _, ok := r.buildCandidateInto(c, pr, now)
	if !ok {
		return nil, reason, false
	}
	return c, rejectNone, true
}

// buildCandidateInto computes the routing cost for c from c.snapshot (already
// filled by snapshotProviderIntoLockedEx) and writes the cost fields into c.
// On rejection it returns the candidateRejection class the legacy counters
// split on (capacity / model-too-large / vision) AND the closed GateReason
// naming the exact drop, including the drops the candidateRejection enum
// reports as rejectNone (crashed/reloading slot, thermal critical), so the
// system-profiler routing record can tally them; c is then left partially
// written and must be discarded by the caller (the arena releases the slot).
// The counter semantics of candidateRejection are unchanged. Caller holds r.mu.
func (r *Registry) buildCandidateInto(c *routingCandidate, pr *PendingRequest, now time.Time) (candidateRejection, GateReason, bool) {
	snap := &c.snapshot
	statePenalty, eligible := slotStatePenalty(snap.slotState)
	if !eligible {
		if snap.slotState == "crashed" {
			return rejectNone, GateSlotCrashed, false
		}
		return rejectNone, GateSlotReloading, false
	}
	if !snap.hasHeadroom {
		return rejectCapacity, GateNoHeadroom, false
	}

	if snap.systemMetrics.ThermalState == "critical" {
		return rejectNone, GateThermalCritical, false
	}

	reqMax := pr.RequestedMaxTokens
	if reqMax <= 0 {
		reqMax = defaultRequestedMaxTokens
	}
	reqPrompt := pr.EstimatedPromptTokens
	if reqPrompt < 0 {
		reqPrompt = 0
	}

	// Absolute hardware-fit gate (cold-load only, both admission modes). A model
	// whose footprint can never fit in this node's total memory must not be
	// routed here regardless of advertised token budget — otherwise the provider
	// 503s at load time ("Insufficient memory … need Y GB") and the request
	// bounces. This is the hole that let a 93.7 GB model get dispatched to 48/64
	// GB boxes: the token-budget admission path below never checked physical fit.
	//
	// Skip the gate whenever the model is already RESIDENT — a resident model has
	// demonstrably fit, so the heuristic must never reject it. The provider
	// reports "running" while actively serving and "idle" when loaded with no
	// in-flight requests (BatchScheduler+Telemetry: activeRequests>0 ? running :
	// idle); BOTH mean the weights are in GPU memory. `snap.modelLoaded` only
	// tracks "running", so we check the slot state directly here — otherwise an
	// idle-but-loaded provider would be wrongly excluded. Reported as
	// rejectModelTooLarge (permanent, not capacity).
	if !slotStateModelLoaded(snap.slotState) && !modelFitsHardware(snap.minRAMGb, snap.modelSizeGB, snap.totalMemoryGB) {
		return rejectModelTooLarge, GateModelTooLarge, false
	}

	// Free-memory admission gate (Phase 1). A provider that claims to
	// serve the model but doesn't have headroom for weights + KV cache
	// is rejected here so we don't OOM the backend post-routing.
	if !freeMemoryAdmits(snap, reqPrompt, reqMax) {
		return rejectCapacity, GateFreeMemory, false
	}

	effectiveQueue := snapshotOccupancy(snap)

	waitingBacklogTokens := float64(snap.backendWaiting * reqMax)
	unaccountedPendingTokens := float64(snap.pendingMaxTokens) - float64(snap.maxTokensPotential) - waitingBacklogTokens
	if unaccountedPendingTokens < 0 {
		unaccountedPendingTokens = 0
	}

	effectiveTPS := resolveEffectiveTPS(snap)

	queueMs := float64(effectiveQueue) * queueDepthPenaltyMs
	pendingMs := float64(snap.totalPending) * totalPendingPenaltyMs
	var backlogMs float64
	if snap.activeTokenBudgetMax > 0 {
		tokensAhead := float64(snap.activeTokenBudgetUsed) + float64(snap.queuedTokenBudget)
		backlogMs = tokensAhead / effectiveTPS * 1000.0
	} else {
		backlogMs = backlogTokenMs(snap.maxTokensPotential, waitingBacklogTokens, unaccountedPendingTokens, effectiveTPS)
	}
	// Prefill resolves through resolvePrefillTPS for BOTH the base cost term and
	// the long-prompt bias below, so provider ranking follows the live measured
	// prefill EWMA when a slot reports one and only falls back to the static
	// registration/x12 chain when it does not. Reading snap.prefillTPS directly
	// here pinned the dominant prefill term to the static rate, which left a box
	// whose measured prefill had degraded looking as cheap as its benchmark.
	prefillTPS := resolvePrefillTPS(snap)
	thisReqMs := float64(reqPrompt)/prefillTPS*1000.0 + float64(reqMax)/effectiveTPS*1000.0
	// Long-prompt fastest-tier preference: amplify the first-token-blocking time
	// for very long prompts so the provider that reaches first token soonest is
	// strongly preferred, reducing pre-first-token client_gone. The amplified
	// quantity is the FULL time-to-first-token (TTFT): prefill PLUS, for a COLD
	// provider, the model-load latency (statePenalty, ~30s). Prefill uses
	// resolvePrefillTPS (the live, observed-preferred prefill signal) — not the
	// static rate — so the bias follows real measured prefill and does not favor a
	// box whose static rate looks good but whose measured prefill is degraded.
	// Amplifying the full cold-load+prefill TTFT — not just prefill — prevents the
	// long-prompt bias from pulling a long prompt onto a cold box whose fast
	// prefill is dwarfed by the load and which is therefore slower end-to-end than
	// the fastest warm provider. Folded into thisReqMs so the cost breakdown
	// invariant (sum of terms == Total) holds. Returns 0 — and so leaves the cost
	// byte-for-byte unchanged — for short prompts and when the knob is off.
	prefillMs := float64(reqPrompt) / prefillTPS * 1000.0
	ttftBlockMs := prefillMs
	if !snap.modelLoaded {
		// A cold provider must load before it can prefill; amplify its full
		// first-token latency (load + prefill), not just prefill, so the long-
		// prompt bias does not pull a long prompt onto a cold box that is slower
		// end-to-end than the fastest warm provider.
		ttftBlockMs += statePenalty
	}
	thisReqMs += longPromptPenalty(reqPrompt, ttftBlockMs)
	healthMs := healthPenaltyMs(snap.systemMetrics, snap.gpuMemoryActiveGB, snap.totalMemoryGB)
	// Gray-box capacity-503 rate penalty (capacity_rate.go): a pair rejecting
	// a material fraction of dispatches with capacity 503s — while serving the
	// rest, so no zero-accepts breaker can see it — sinks in cost ranking
	// proportionally to its windowed reject rate. A soft derater, never an
	// ejection: the candidate stays in the pool, so a degraded-but-only fleet
	// still serves, and the penalty decays as outcomes age out of the window.
	capacityRateMs, capacityRejectRate := r.capacityRatePenaltyFor(snap.provider, snap.model, now)
	cost := statePenalty + queueMs + pendingMs + backlogMs + thisReqMs + healthMs + capacityRateMs

	// Estimated time-to-first-token for this candidate. Used for the
	// OpenRouter TTFT ceiling: public routes only select providers whose
	// estimated TTFT is within the per-request threshold. Providers without
	// BackendCapacity get 0 (unreliable estimate) and are not rejected by the
	// ceiling, matching the preflight behavior. The gate/ceiling input is the
	// CALIBRATED estimate (raw × learned actual/predicted ratio, see
	// ttft_calibration.go); the raw value is kept alongside so the calibrator
	// learns against what the formula actually predicted.
	rawTTFTMs := ttftMsFromSnapshot(snap, reqPrompt)
	if rawTTFTMs <= 0 || math.IsNaN(rawTTFTMs) || math.IsInf(rawTTFTMs, 0) {
		rawTTFTMs = 0
	}
	// Read the calibration ratio once and score with it, so the ratio the
	// profiler records is exactly the one this candidate was gated on.
	calibrationRatio := ttftCalibration.appliedRatio(snap.model, snap.chipFamily)
	ttftMs := calibratedTTFTMsWithRatio(snap, rawTTFTMs, calibrationRatio)

	c.provider = snap.provider
	// The ratio the profiler records is exactly the one this candidate was
	// gated on (TTFTCalibrationRatio on the decision).
	c.calibrationRatio = calibrationRatio
	c.costMs = cost
	c.effectiveQueue = effectiveQueue
	c.effectiveTPS = effectiveTPS
	c.capacityRejectRate = capacityRejectRate
	c.breakdown = costBreakdown{
		StateMs:        statePenalty,
		QueueMs:        queueMs,
		PendingMs:      pendingMs,
		BacklogMs:      backlogMs,
		ThisReqMs:      thisReqMs,
		HealthMs:       healthMs,
		CapacityRateMs: capacityRateMs,
		TTFTMs:         ttftMs,
		RawTTFTMs:      rawTTFTMs,
		Total:          cost,
	}
	return rejectNone, GateReasonCount, true
}

func slotStatePenalty(state string) (float64, bool) {
	switch state {
	case "", "running", "idle":
		return slotStatePenaltyRunning, true
	case "unknown":
		// Model is available but not loaded. The provider must evict the
		// current model and load this one — typically 15–60 seconds for
		// large models (depends on model size and disk speed). Warm
		// providers are strongly preferred but cold providers are still
		// eligible when no warm alternative exists.
		return slotStatePenaltyUnknown, true
	case "idle_shutdown":
		return slotStatePenaltyIdleShutdown, true
	case "reloading", "crashed":
		return math.Inf(1), false
	default:
		return slotStatePenaltyUnknown, true
	}
}

func slotStateModelLoaded(state string) bool {
	return state == "running" || state == "idle"
}

func backlogTokenMs(maxTokensPotential int64, waitingTokens, unaccountedPendingTokens, decodeTPS float64) float64 {
	if decodeTPS <= 0 {
		decodeTPS = 1.0
	}
	totalTokensAhead := float64(maxTokensPotential) + waitingTokens + unaccountedPendingTokens
	if totalTokensAhead < 0 {
		totalTokensAhead = 0
	}
	return totalTokensAhead / decodeTPS * 1000.0
}

func healthPenaltyMs(m protocol.SystemMetrics, gpuActiveGB, totalMemGB float64) float64 {
	penalty := m.MemoryPressure*memoryPressurePenaltyMs + m.CPUUsage*cpuUsagePenaltyMs
	switch m.ThermalState {
	case "fair":
		penalty += thermalPenaltyFairMs
	case "serious":
		penalty += thermalPenaltySeriousMs
	}
	if totalMemGB > 0 {
		gpuUtil := gpuActiveGB / totalMemGB
		if gpuUtil < 0 {
			gpuUtil = 0
		}
		if gpuUtil > 1 {
			gpuUtil = 1
		}
		penalty += gpuUtil * gpuUtilizationPenaltyMs
	}
	return penalty
}

// resolveEffectiveTPS returns the best available decode TPS estimate.
// Fallback chain: observed EWMA → fleet median → load-scaled benchmark.
func resolveEffectiveTPS(snap *routingSnapshot) float64 {
	if snap.observedDecodeTPS > 0 {
		return snap.observedDecodeTPS
	}
	if snap.fleetMedianTPS > 0 {
		return snap.fleetMedianTPS
	}
	return effectiveDecodeTPS(snap.decodeTPS, snap.backendRunning)
}

// resolvePrefillTPS returns the best available prefill TPS estimate for TTFT.
// Fallback chain: measured per-slot observed prefill EWMA → snap.prefillTPS (the
// resolvedPrefillTPS chain: registration benchmark → decode×prefillToDecodeRatio
// ×12 fallback). This mirrors how resolveEffectiveTPS prefers the measured
// decode rate over the static estimate. The result is clamped to maxPrefillTPS
// so a single outlier heartbeat cannot collapse the TTFT estimate.
//
// observedPrefillTPS stays 0 until providers ship the W1 measurement, so on
// today's fleet this is a no-op that returns the existing ×12-chain value.
func resolvePrefillTPS(snap *routingSnapshot) float64 {
	tps := snap.prefillTPS
	if snap.observedPrefillTPS > 0 {
		tps = snap.observedPrefillTPS
	}
	if tps > maxPrefillTPS {
		tps = maxPrefillTPS
	}
	return tps
}

// effectiveDecodeTPS scales the static decode TPS down by current
// backend batch size. Returns the static value when the load factor is
// disabled or batch is unknown. Floored at 1 token/s to avoid divide-
// by-zero.
//
// Note on the floor + large reqMax: when effectiveTPS bottoms out, the
// per-request decode cost (reqMax / effectiveTPS * 1000) can become
// very large for big reqMax values. This is intentional — a saturated
// provider should look strictly worse than less-saturated peers — and
// the maxConcurrency gate in snapshotProviderIntoLockedEx already prevents
// us from getting here when batchSize exceeds the per-tier cap.
func effectiveDecodeTPS(staticTPS float64, backendRunning int) float64 {
	if staticTPS <= 0 {
		return 1.0
	}
	if effectiveTPSLoadFactor <= 0 || backendRunning <= 0 {
		return staticTPS
	}
	tps := staticTPS / (1.0 + effectiveTPSLoadFactor*float64(backendRunning))
	if tps < 1.0 {
		tps = 1.0
	}
	return tps
}

func resolvedDecodeTPS(p *Provider) float64 {
	if p.DecodeTPS > 0 {
		return p.DecodeTPS
	}
	bw := float64(p.Hardware.MemoryBandwidthGBs)
	if bw > 0 {
		return math.Sqrt(bw)
	}
	return 1.0
}

// resolvedModelTPSLocked returns the best per-model decode/prefill TPS samples
// for a provider. BackendCapacity.Slots is authoritative for Swift providers:
// when the matching slot reports observed EWMAs, prefer them over static
// registration benchmarks. Non-positive observed values are treated as missing.
// Caller must hold p.mu.
func resolvedModelTPSLocked(p *Provider, model string) (decodeTPS, prefillTPS float64) {
	decodeTPS = resolvedDecodeTPS(p)
	prefillTPS = resolvedPrefillTPS(p)
	if p.BackendCapacity == nil {
		return decodeTPS, prefillTPS
	}
	for _, slot := range p.BackendCapacity.Slots {
		if slot.Model != model {
			continue
		}
		if slot.ObservedDecodeTPS > 0 {
			decodeTPS = slot.ObservedDecodeTPS
		}
		if slot.ObservedPrefillTPS > 0 {
			prefillTPS = slot.ObservedPrefillTPS
		}
		break
	}
	return decodeTPS, prefillTPS
}

// defaultPrefillToDecodeRatio is the fallback multiplier applied to a provider's
// decode TPS to estimate its prefill TPS when the provider does not report a
// measured prefill rate (prefill_tps). Apple-Silicon MLX prefills the prompt in
// large parallel batches, so prefill throughput is roughly an order of magnitude
// above decode throughput. The historical 4x was far too conservative: combined
// with the 5s+1ms/token TTFT deadline it estimated ~100 tok/s prefill (vs the
// ~1000 tok/s the deadline implicitly assumes), so the TTFT gate wrongly
// rejected warm, capable providers on any prompt above ~550 tokens. No provider
// currently reports prefill_tps, so this fallback is the production path.
const defaultPrefillToDecodeRatio = 12.0

// prefillToDecodeRatio is configured once at startup (via SetPrefillToDecodeRatio,
// e.g. from EIGENINFERENCE_PREFILL_DECODE_RATIO) before the server begins
// serving, then only read on routing paths.
var prefillToDecodeRatio = defaultPrefillToDecodeRatio

// SetPrefillToDecodeRatio overrides the decode→prefill fallback multiplier.
// Values <= 0 are ignored. Must be called before serving starts (read-only after).
func SetPrefillToDecodeRatio(ratio float64) {
	if ratio > 0 {
		prefillToDecodeRatio = ratio
	}
}

// PrefillToDecodeRatio returns the current decode→prefill fallback multiplier
// (the value used by resolvedPrefillTPS when a provider does not report a
// measured prefill rate). Exposed for the routing simulation harness.
func PrefillToDecodeRatio() float64 {
	return prefillToDecodeRatio
}

// ttftOccupancyAlpha scales the Phase-0 occupancy term (see ttftOccupancyMs),
// which is added ONLY inside occupancyAwareTTFTMsFromSnapshot — the shadow
// evaluator's estimate — NEVER inside the live ttftMsFromSnapshot. It is the
// decode-token-times of head-of-line wait charged per occupying peer, divided by
// the per-request decode rate the new request would see. Because the term never
// reaches ttftMsFromSnapshot, the routing cost's TTFTMs, the candidate-loop
// MaxTTFTMs ceiling, and the preflight bestTTFT are occupancy-free at ANY alpha:
// raising alpha changes only the shadow signal, not the live routing decision
// (the HARD_REJECT safety invariant — see occupancyAwareTTFTMsFromSnapshot). 0
// (the default) also makes ttftOccupancyMs itself a no-op. Configured once at
// startup via SetTTFTOccupancyAlpha (EIGENINFERENCE_TTFT_OCCUPANCY_ALPHA),
// read-only on routing paths thereafter, mirroring prefillToDecodeRatio.
var ttftOccupancyAlpha = 0.0

// SetTTFTOccupancyAlpha overrides the occupancy-term coefficient. Negative
// values are clamped to 0 (term disabled). Must be called before serving starts.
func SetTTFTOccupancyAlpha(alpha float64) {
	if alpha < 0 {
		alpha = 0
	}
	ttftOccupancyAlpha = alpha
}

// TTFTOccupancyAlpha returns the configured occupancy-term coefficient.
func TTFTOccupancyAlpha() float64 {
	return ttftOccupancyAlpha
}

// defaultLongPromptThresholdTokens gates the long-prompt fastest-tier routing
// preference. 0 disables it entirely (behavior-neutral): the routing
// cost is unchanged for every request, short or long. A positive value turns the
// preference ON for requests whose estimated prompt is at or above the threshold.
const defaultLongPromptThresholdTokens = 0

// defaultLongPromptPrefillWeight is the multiplier applied to the prefill term of
// the routing cost for long prompts. 1.0 is behavior-neutral; >1 amplifies the
// prefill component so the fastest-prefill (== fastest chip tier) warm provider is
// strongly preferred once the prompt is long enough that prefill dominates TTFT.
const defaultLongPromptPrefillWeight = 2.0

// longPromptThresholdTokens / longPromptPrefillWeight are configured once at
// startup (via SetLongPromptThreshold / SetLongPromptPrefillWeight, e.g. from
// EIGENINFERENCE_LONG_PROMPT_TOKENS) before serving begins, then only read on the
// routing path. Default-off so the scheduler is byte-for-byte unchanged unless an
// operator opts in.
var (
	longPromptThresholdTokens = defaultLongPromptThresholdTokens
	longPromptPrefillWeight   = defaultLongPromptPrefillWeight
)

// SetLongPromptThreshold sets the estimated-prompt-token count at/above which the
// long-prompt fastest-tier routing preference activates. A value <= 0 disables the
// preference (behavior-neutral). Must be called before serving starts.
func SetLongPromptThreshold(tokens int) {
	if tokens < 0 {
		tokens = 0
	}
	longPromptThresholdTokens = tokens
}

// LongPromptThreshold returns the current long-prompt token threshold (0 = off).
func LongPromptThreshold() int {
	return longPromptThresholdTokens
}

// SetLongPromptPrefillWeight overrides the prefill-term multiplier used for long
// prompts. Non-finite values (NaN/±Inf — which slip through a naive `< 1` clamp
// because NaN comparisons are always false, then poison every candidate cost) are
// reset to the default. Values < 1 are clamped to 1.0 (no amplification). Must be
// called before serving starts.
func SetLongPromptPrefillWeight(w float64) {
	weight := w
	if math.IsNaN(weight) || math.IsInf(weight, 0) {
		weight = defaultLongPromptPrefillWeight
	}
	if weight < 1.0 {
		weight = 1.0
	}
	longPromptPrefillWeight = weight
}

// LongPromptPrefillWeight returns the current long-prompt prefill-term multiplier.
func LongPromptPrefillWeight() float64 {
	return longPromptPrefillWeight
}

// longPromptPenalty returns the EXTRA first-token-blocking cost (ms) added to a
// candidate's per-request cost so very long prompts prefer the provider that
// reaches first token soonest. It amplifies the supplied time-to-first-token
// (ttftBlockMs) by (weight-1). The caller passes the FULL TTFT: prefill for a warm
// provider, or model-load latency + prefill for a cold one. Amplifying the full
// TTFT (rather than prefill alone) means a cold box's fast prefill cannot win a
// long prompt when its ~30s load makes it slower end-to-end than the fastest warm
// provider, while a warm provider with twice the prefill throughput still sees
// half the penalty so the fastest chip-tier wins decisively.
//
// Returns 0 (fully behavior-preserving) when the preference is disabled
// (threshold <= 0), the prompt is below the threshold (short prompts unaffected),
// the weight is neutral (<= 1), or the blocking time is non-positive. It is a SOFT
// ranking bias only: no candidate is dropped and no TTFT 429 is introduced.
func longPromptPenalty(reqPromptTokens int, ttftBlockMs float64) float64 {
	if longPromptThresholdTokens <= 0 || reqPromptTokens < longPromptThresholdTokens {
		return 0
	}
	if ttftBlockMs <= 0 || longPromptPrefillWeight <= 1.0 {
		return 0
	}
	return (longPromptPrefillWeight - 1.0) * ttftBlockMs
}

func resolvedPrefillTPS(p *Provider) float64 {
	if p.PrefillTPS > 0 {
		return p.PrefillTPS
	}
	return resolvedDecodeTPS(p) * prefillToDecodeRatio
}

// projectedPerRequestDecodeTPS estimates the decode tokens/sec a NEWLY admitted
// request would receive on this snapshot's provider once it joins the batch
// (backendRunning+1 concurrent). Continuous batching is memory-bandwidth bound,
// so per-request decode degrades with batch size by the same effectiveTPSLoadFactor
// model used elsewhere: rate(b) = solo / (1 + k·b). The measured observed decode
// rate (when present) is unwound from the current batch to a solo rate and then
// reapplied at b+1; otherwise the static benchmark is the solo proxy. Used by the
// decode-floor quality preference (PendingRequest.MinDecodeTPS).
func projectedPerRequestDecodeTPS(snap *routingSnapshot) float64 {
	return projectedPerRequestDecodeTPSAtBatch(snap, snap.backendRunning)
}

// projectedPerRequestDecodeTPSAtBatch is projectedPerRequestDecodeTPS with an
// EXPLICIT batch the new request would join, used when the heartbeat gauge
// (backend_running) understates real contention. The observed-rate UNWIND always
// uses the batch the observation was actually taken at (snap.backendRunning —
// the heartbeat's observedDecodeTPS pairs with that gauge), while the REAPPLY
// uses joinBatch. Passing joinBatch == snap.backendRunning reproduces the
// original result exactly, so the decode-floor caller is byte-for-byte unchanged;
// the occupancy term passes joinBatch == occ so a herd that has already reserved
// peers the heartbeat has not yet reflected (occ > backend_running) is charged at
// the contended rate it will actually see — not the idle/low-batch rate.
func projectedPerRequestDecodeTPSAtBatch(snap *routingSnapshot, joinBatch int) float64 {
	k := effectiveTPSLoadFactor
	if k < 0 {
		k = 0
	}
	bObserved := snap.backendRunning
	if bObserved < 0 {
		bObserved = 0
	}
	if joinBatch < 0 {
		joinBatch = 0
	}
	// Solo (b=0) decode-rate base, durable 3-tier chain:
	solo := snap.decodeTPS // tier 3: static benchmark (last resort)
	switch {
	case snap.observedDecodeTPS > 0:
		// tier 1: this box's own LIVE measured rate, unwound from the batch it
		// was measured at (bObserved) to solo.
		solo = snap.observedDecodeTPS * (1 + k*float64(bObserved))
	case decodeFloorUseFleetMedian() && snap.fleetMedianTPS > 0:
		// tier 2: durable per-(model,chip) observed median from the tps registry.
		// Exists even when this box is IDLE, so a historically-slow chip (e.g. the
		// ~9 tok/s gemma boxes driving client_gone) is deprioritized BEFORE it gets
		// packed — the static benchmark (~23) otherwise made idle slow boxes look
		// fast. Conservative for a quality floor: a median that understates true
		// solo biases AWAY from borderline boxes (the safe direction).
		solo = snap.fleetMedianTPS
	}
	if solo <= 0 {
		return 0
	}
	return solo / (1 + k*float64(joinBatch+1))
}

// decodeFloorUseFleetMedian gates the tier-2 (fleet-median) solo-rate source in
// projectedPerRequestDecodeTPS. Read LIVE (no restart); default ON. Set
// EIGENINFERENCE_DECODE_FLOOR_USE_FLEET_MEDIAN=false for byte-for-byte pre-fix
// behavior (idle boxes fall straight to the static benchmark).
func decodeFloorUseFleetMedian() bool {
	return env.EnvBool(env.EnvPrefix+"_DECODE_FLOOR_USE_FLEET_MEDIAN", true)
}

func estimatedTTFTFromSnapshot(snap *routingSnapshot, reqPromptTokens int) time.Duration {
	ttftMs := ttftMsFromSnapshot(snap, reqPromptTokens)
	if ttftMs <= 0 || math.IsNaN(ttftMs) || math.IsInf(ttftMs, 0) {
		return 0
	}
	// Same calibration as the scheduler's gate input (buildCandidateWithReason)
	// so the preflight bestTTFT and the hard-reject ceiling cannot drift.
	ttftMs = calibratedTTFTMs(snap, ttftMs)
	return time.Duration(ttftMs * float64(time.Millisecond))
}

// ttftMsFromSnapshot returns the estimated time-to-first-token in milliseconds
// for a candidate/provider snapshot. It is shared between the preflight
// (QuickCapacityCheckWithTTFTForRequest) and the scheduler
// (buildCandidateWithReason) so the two paths cannot drift on what "TTFT"
// means.
//
// Token-budget fields are admission/memory reservations, not decode work that
// must fully drain before this request can emit a first token. Continuous
// batching lets a newly-admitted request join the decode loop once its prefill
// completes; existing active max-output reservations only slow the next decode
// step, which is already reflected by effectiveTPS. Count waiting prefills ahead
// and this request's own prefill instead of treating active_token_budget_used as
// a serial decode backlog.
func ttftMsFromSnapshot(snap *routingSnapshot, reqPromptTokens int) float64 {
	if !snap.hasBackendCapacity {
		return 0
	}
	statePenalty, _ := slotStatePenalty(snap.slotState)
	if reqPromptTokens < 0 {
		reqPromptTokens = 0
	}
	prefillTPS := resolvePrefillTPS(snap)
	if prefillTPS <= 0 {
		prefillTPS = 1.0
	}
	effectiveTPS := resolveEffectiveTPS(snap)
	if effectiveTPS <= 0 {
		effectiveTPS = 1.0
	}

	queuedPrefillMs := queuedPrefillTokensAhead(snap, reqPromptTokens) / prefillTPS * 1000.0
	thisPrefillMs := float64(reqPromptTokens) / prefillTPS * 1000.0
	firstDecodeMs := 1000.0 / effectiveTPS
	// NOTE: the Phase-0 occupancy term (ttftOccupancyMs) is deliberately NOT added
	// here. ttftMsFromSnapshot is the LIVE estimate consumed by the routing cost's
	// TTFTMs, the candidate-loop MaxTTFTMs ceiling, and the preflight bestTTFT — so
	// it must stay occupancy-FREE regardless of EIGENINFERENCE_TTFT_OCCUPANCY_ALPHA.
	// The occupancy-aware estimate (base + occupancy term) lives in
	// occupancyAwareTTFTMsFromSnapshot and is used ONLY by the shadow evaluator.
	return statePenalty + queuedPrefillMs + thisPrefillMs + firstDecodeMs
}

// occupancyAwareTTFTMsFromSnapshot is the occupancy-aware TTFT estimate: the base
// estimate (ttftMsFromSnapshot — what the LIVE cost / MaxTTFTMs ceiling / bestTTFT
// consume) PLUS the Phase-0 head-of-line occupancy term (ttftOccupancyMs, gated by
// EIGENINFERENCE_TTFT_OCCUPANCY_ALPHA).
//
// It is used ONLY by the shadow evaluator today; a future enforce step will wire
// it (against the verified ~10s base) into the live path. Keeping the occupancy
// term OUT of ttftMsFromSnapshot is a SAFETY INVARIANT: prod runs HARD_REJECT
// (pr.MaxTTFTMs set from the pinned request-local deadline), so if the term
// leaked into ttftMsFromSnapshot, raising alpha would tighten the live ceiling
// and over-shed ~2x (telemetry-db findings §2). The term may therefore only
// ever reach the shadow estimate, never breakdown.TTFTMs.
func occupancyAwareTTFTMsFromSnapshot(snap *routingSnapshot, reqPromptTokens int) float64 {
	base := ttftMsFromSnapshot(snap, reqPromptTokens)
	if base <= 0 {
		// No reliable base (provider without BackendCapacity) → no occupancy-aware
		// estimate either, matching ttftMsFromSnapshot's contract.
		return base
	}
	return base + ttftOccupancyMs(snap)
}

// ttftOccupancyMs is the Phase-0 occupancy term: the head-of-line wait while the
// box's already-occupying work (the herd) clears enough for a newly admitted
// request to emit its first token. The base estimate (ttftMsFromSnapshot) counts
// only WAITING prefill and a single decode step, so it is flat in running
// occupancy — exactly where the ~11s of "dark time" lives. It is added ONLY in
// occupancyAwareTTFTMsFromSnapshot (the shadow estimate), never in the live
// ttftMsFromSnapshot.
//
// The term reuses the occupancy the snapshot ALREADY carries
// (snapshotOccupancy = max(pendingForModel, backend_running+backend_waiting)),
// not a new parallel counter, so it is herd-aware for free: a burst onto a box
// still reporting backend_running=0 shows up through pendingForModel. Magnitude
// per occupying peer is alpha decode-token-times divided by the per-request
// decode rate the new request will actually see — projected at the SAME occupancy
// (occ), not the stale backend_running gauge, so in the herd case (pendingForModel
// > backend_running) it is charged the contended rate, not an idle-batch rate.
// The rate itself shrinks with occ, making the term super-linear in occupancy.
//
// Returns 0 when EIGENINFERENCE_TTFT_OCCUPANCY_ALPHA is 0 (the default) or
// occupancy is 0 (an idle box never pays the term, so route-to-idle is
// preserved). The deadline this is gated against in the shadow evaluator is the
// model's upstream SLA (standard ~10s), not the shorter live coordinator cutoff.
// Conflating those clocks over-sheds (telemetry-db findings §2).
func ttftOccupancyMs(snap *routingSnapshot) float64 {
	alpha := ttftOccupancyAlpha
	if alpha <= 0 {
		return 0
	}
	occ := snapshotOccupancy(snap)
	if occ <= 0 {
		return 0
	}
	// Project the per-request rate at the batch the request ACTUALLY joins (occ),
	// not the bare heartbeat backend_running: in the herd case the new request
	// waits behind occ peers, so charging the idle/low-batch rate would under-
	// state the term in exactly the case it exists to catch.
	perReqDecodeTPS := projectedPerRequestDecodeTPSAtBatch(snap, occ)
	if perReqDecodeTPS <= 0 {
		perReqDecodeTPS = 1.0
	}
	return alpha * float64(occ) * 1000.0 / perReqDecodeTPS
}

func queuedPrefillTokensAhead(snap *routingSnapshot, reqPromptTokens int) float64 {
	if reqPromptTokens <= 0 {
		return 0
	}
	waiting := snap.backendWaiting
	reflected := snap.backendRunning + snap.backendWaiting
	if extraPending := snap.pendingForModel - reflected; extraPending > 0 {
		waiting += extraPending
	}
	if waiting <= 0 {
		return 0
	}
	return float64(waiting * reqPromptTokens)
}
