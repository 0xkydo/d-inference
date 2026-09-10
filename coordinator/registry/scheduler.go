package registry

import (
	"context"
	"log/slog"
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
)

const (
	// Coordinator-side defaults for request sizing. These are only used for
	// routing heuristics and queue admission, not billing or protocol limits.
	defaultRequestedMaxTokens = 256

	slotStatePenaltyRunning      = 0.0
	slotStatePenaltyUnknown      = 30_000.0
	slotStatePenaltyIdleShutdown = 20_000.0

	// Penalty constants. Phase 3 raised queueDepthPenaltyMs (1000→3000),
	// totalPendingPenaltyMs (250→750), and nearTieCostWindowMs (750→2500).
	// The old values let a fast provider with 1-2 in-flight requests
	// outscore an idle slow provider, because the per-request decode-cost
	// gap (~3-10 s) dwarfed the queue penalty (~1 s/request). The new
	// values make one queued request roughly equivalent to one
	// slow-provider decode, so the cost function actually spreads load
	// across the fleet. Wider tie window admits more candidates to the
	// queue-depth tie-break + random distribution.
	queueDepthPenaltyMs      = 3_000.0
	totalPendingPenaltyMs    = 750.0
	memoryPressurePenaltyMs  = 4_000.0
	cpuUsagePenaltyMs        = 1_500.0
	gpuUtilizationPenaltyMs  = 5_000.0
	thermalPenaltyFairMs     = 2_000.0
	thermalPenaltySeriousMs  = 8_000.0
	nearTieCostWindowMs      = 3_000.0
	challengeFreshnessMaxAge = 16 * time.Minute

	// kvCacheBytesPerToken is a per-token KV-cache size estimate used by
	// the free-memory admission gate.
	//
	// Measured on M4 Max (Qwen2.5-7B-4bit, prompt≈2330 + completion≈72):
	// 357,615 bytes/token (0.34 MB). Prior default of 0.5 MB was ~47%
	// too conservative — providers were being rejected for "no fit"
	// when they actually had room. Rounded up slightly to 400,000 to
	// leave headroom for larger models (70B class may be ~2x) without
	// re-running the gate per architecture. Refine per-model via
	// catalog metadata once more measurements exist.
	kvCacheBytesPerToken = 400_000 // ~0.38 MB; covers 7-8B with slack
	bytesPerGB           = 1 << 30

	// effectiveTPSLoadFactor controls how aggressively decode TPS
	// degrades as a provider takes on more concurrent requests. The
	// effective TPS used in cost is `decodeTPS / (1 + k * batchSize)`
	// where batchSize is the backend's currently-running request count.
	//
	// Measured on M4 Max against the CBv2 engine and a model this
	// coordinator actually serves — gemma-4-26b-qat-4bit, per-request
	// decode at B = 1/2/4/8 = 101.8 / 59.6 / 38.0 / 24.7 (v2 rows of
	// libs/mlx-swift-lm/benchmarks/reports/gemma4-26b-qat4bit-paged-gate-2026-07-09.md).
	// Method: median of the implied k over B = 2/4/8, solo pinned to the
	// B=1 measurement — 0.354 / 0.420 / 0.390 -> 0.39. A least-squares fit
	// of 1/rate against B agrees (0.3895). The SAME method reproduces the
	// previous 0.27 exactly from the legacy rows (Qwen2.5-7B-4bit on the
	// legacy engine: 92.8 / 69.5 / 35.9 / 29.6 -> 0.2669), so this is a
	// change of engine and model, not of method. Cross-checks: gemma
	// v2-paged 0.388, v2-compiled 0.419; gpt-oss-20b v2-eager 0.432,
	// v2-paged 0.325.
	//
	// 0.27 errs in the LENIENT direction against CBv2 — it UNDER-predicts
	// degradation, i.e. over-predicts the surviving rate, and the error
	// grows with batch:
	//
	//	B    measured    k=0.27 pred       k=0.39 pred
	//	2    59.6        66.1   (+10.9%)   57.2   (-4.1%)
	//	4    38.0        48.9   (+28.8%)   39.8   (+4.6%)
	//	8    24.7        32.2   (+30.4%)   24.7   (-0.0%)
	//	                 MAPE 23.4%        MAPE 2.9%
	//
	// B=1 is the model's INPUT (solo), not a prediction, so it is not
	// scored. Mind the SIGN: 0.27 is too SMALL, not too large. A reading
	// that it was wildly "too aggressive" comes from comparing a
	// prediction made with the coordinator's sqrt(memory_bandwidth) proxy
	// solo (16-28 tok/s) against a rate measured at the engine's real solo
	// (101.8) — that gap is a bad SOLO rate, not a bad k, and it has its
	// own lever (modelSoloTPSSeedEnv in concurrency_cap.go). Raising k
	// makes every derived cap TIGHTER, never looser.
	//
	// Four systems consume this and a too-small k over-states the quality
	// batch in all of them at once: the admission cap (concurrency_cap.go),
	// effectiveDecodeTPS and projectedPerRequestDecodeTPSAtBatch below, and
	// the warm-pool target (warm_pool_controller.go) — which then
	// under-warms the pool while admission packs batches that miss the
	// decode floor.
	// Set to 0 to disable load scaling.
	effectiveTPSLoadFactor = 0.39
)

type routingSnapshot struct {
	provider   *Provider
	model      string
	chipFamily string // hardware chip family (e.g. "M3"); keys the TTFT calibrator
	// binaryVersion is the provider's reported binary version (p.Version, read
	// under p.mu at snapshot time; empty = unreported/legacy). Feeds the
	// version-gated activation-reserve selection in the cold servability
	// estimate (servabilityActivationFloor) so a mixed-version fleet
	// is charged the reserve each binary actually holds.
	binaryVersion    string
	slotState        string
	hasHeadroom      bool
	totalPending     int
	pendingForModel  int
	pendingMaxTokens int
	// pendingMaxTokensAllModels is pendingMaxTokens WITHOUT the model filter:
	// the token budgets of every coordinator-pending request on this provider,
	// any model. Feeds the pooled-budget admission check (pooledBudgetAdmits)
	// so co-resident models cannot double-spend shared legacy headroom and do
	// not lose additive private-grant capacity on v0.7.5+ providers.
	pendingMaxTokensAllModels int
	// pendingMaxBytesAllModels is the byte-normalized analog: each pending
	// request's token budget × its model's reported KVBytesPerToken. Valid
	// only when pendingBytesKnown. A cold request without a reported model rate
	// is charged at the bounded conservative default (see
	// fillSnapshotPendingAndPool), so it cannot disable byte accounting for a
	// reconstructable pool. Co-resident models have different per-token byte
	// rates, so tokens are not a common unit across models (pooled_admission.go).
	pendingMaxBytesAllModels int64
	pendingBytesKnown        bool
	backendRunning           int
	backendWaiting           int
	maxTokensPotential       int64
	decodeTPS                float64
	prefillTPS               float64
	systemMetrics            protocol.SystemMetrics
	gpuMemoryActiveGB        float64
	totalMemoryGB            float64
	// freeForLoadGB is the provider-reported max additional model-weight (GB) it
	// can load right now (net of cap/reserve/headroom, idle models reclaimed).
	// When non-nil it is the authoritative cold-load gate; nil = legacy provider
	// (fall back to the total-memory heuristic). See protocol.BackendCapacity.
	freeForLoadGB   *float64
	modelSizeGB     float64 // catalog-reported weight footprint (0 = unknown, gate disabled)
	minRAMGb        int     // catalog authoritative min RAM (GB) to run the model (0 = unknown)
	modelLoaded     bool    // true when the requested model is resident (running or idle)
	availableOnDisk bool    // model is in provider's Models list but not currently loaded

	observedDecodeTPS     float64
	observedPrefillTPS    float64 // measured per-slot prefill EWMA; 0 = unreported (fall back to prefillTPS chain)
	activeTokenBudgetUsed int64
	activeTokenBudgetMax  int64
	queuedTokenBudget     int64
	// pooledTokenBudget is the provider's reconstructed whole-box token budget
	// (all budget slots; layout selected from the provider release version).
	// Zero value when the provider reports no backend capacity / no budget
	// slots, which disables the pooled admission check.
	pooledTokenBudget pooledTokenBudget
	// budgetClamped means the gray-box budget clamp (budget_clamp.go) is
	// active for this (provider, model) pair: a capacity-shaped 503 proved the
	// provider's LIVE admission gate is rejecting, so the heartbeat budget
	// above is stale-optimistic and admission must treat the slot as FULL
	// (freeMemoryAdmits rejects; providerBudgetFits reports zero live
	// headroom). The budget fields themselves stay RAW — cost/backlog math,
	// the structural servability ceiling (snapshotStructuralBudget), and
	// telemetry keep reading the provider-reported truth. Only set when the
	// slot reports a token budget (activeTokenBudgetMax > 0).
	budgetClamped bool
	// kvBytesPerToken is the provider-reported per-token KV-cache cost (bytes)
	// for THIS model's slot (BackendSlotCapacity.KVBytesPerToken). 0 = unreported
	// (callers fall back to the kvCacheBytesPerToken default). Used by the
	// servability predictor to estimate a cold provider's post-load token budget
	// the same way the provider does, instead of the fixed default.
	kvBytesPerToken    int64
	fleetMedianTPS     float64
	hasBackendCapacity bool // provider reports BackendCapacity; TTFT estimates are reliable

	// Engine-health (first-token wedge) signals, decoded from the slot's
	// BackendSlotCapacity (see docs/reports/2026-06-22-cancel-root-cause-and-fix.md
	// §C). MEASUREMENT ONLY: surfaced here so routing/observability code can read
	// a wedge ("admits climbing, first-tokens flat, steps frozen") — this PR does
	// NOT gate any routing decision on them. 0/false for legacy providers.
	stepsExecuted              int64
	admits                     int64
	firstTokensEmitted         int64
	secondsSinceLastStep       float64
	secondsSinceLastFirstToken float64
	wedgeSuspected             bool
	evalInFlightMs             int64
	idleClearInFlightMs        int64

	// hbAgeMs is the age of p.LastHeartbeat at snapshot time (now − LastHeartbeat,
	// clamped to int32), computed from the `now` the snapshot already reads — no
	// extra clock read. It is the "how stale were the routing inputs" signal of
	// the system-profiler routing record (RoutingDecision.SnapshotAgeMs for the
	// winner, CandidateSummary.HBAgeMs for the top candidates). Observability
	// only; routing is NOT gated on it.
	hbAgeMs int32
	// queuedPrefillTokens is the slot's provider-reported Σ prompt tokens of
	// requests whose engine submit has not returned (slice-2 SlotTelemetry
	// producer). 0 until the wire field exists: BackendSlotCapacity has no
	// telemetry sub-object at this compile point, so nothing populates it yet.
	queuedPrefillTokens int64
}

type routingCandidate struct {
	provider       *Provider
	snapshot       routingSnapshot
	costMs         float64
	effectiveQueue int
	breakdown      costBreakdown
	effectiveTPS   float64 // Phase 4 load-scaled TPS used in this candidate's cost
	// capacityRejectRate is the pair's windowed capacity-503 rate
	// (capacity_rate.go), captured at candidate build so the winning
	// RoutingDecision can expose it. 0 when no rejects are in the window.
	capacityRejectRate        float64
	cacheTier                 string
	cacheEstimatedTTFTSavedMs float64
	// calibrationRatio is the TTFT calibration ratio this candidate was
	// scored with (recorded on the RoutingDecision for the profiler).
	calibrationRatio float64
}

// candidateRejection enumerates why a provider that passed structural
// gates (status, trust, slot state, thermal) was nonetheless excluded
// from selection. Used to populate RoutingDecision counters so callers
// can distinguish "no provider serves this model" from "every fitting
// provider is full".
type candidateRejection int

const (
	rejectNone candidateRejection = iota
	rejectCapacity
	// rejectModelTooLarge means the model's resident footprint cannot fit in
	// this provider's total memory under any load state. Unlike rejectCapacity
	// (transient "full, retry later") this is permanent for this provider, so
	// it must NOT inflate the busy/429 signal.
	rejectModelTooLarge
	// rejectVisionUnsupported means the request carries image/video input but
	// this provider only advertises a text-only build of the model. Permanent for
	// this provider (until it loads a VLM build), so like rejectModelTooLarge it
	// must NOT inflate the transient busy/429 signal.
	rejectVisionUnsupported
)

// modelMemoryHeadroomFactor is the FALLBACK multiple of the on-disk weight size
// used to estimate a model's resident footprint ONLY when the catalog has no
// authoritative min_ram_gb. Prefer min_ram_gb (see modelFitsHardware): a
// synthetic multiple of the raw weight does not match what the operator
// published or what the provider actually loads, and at 2.x it wrongly rejected
// catalog-qualified nodes (e.g. gpt-oss-20b min_ram_gb=24 vs 12.1*2.x>24, and
// gemma-4-26b min_ram_gb=36 vs 28*2.x rejecting the whole 64 GB tier).
const modelMemoryHeadroomFactor = 2.0

// modelFitsHardware reports whether a model can run on a node with the given
// total unified memory (GB). It prefers the catalog's authoritative min_ram_gb
// (the operator-published requirement) and only falls back to a heuristic
// multiple of the on-disk weight size when min_ram_gb is unknown. Fails OPEN
// when nothing is known. The provider still performs the final precise check at
// load time; this gate only filters models that clearly cannot fit per the
// catalog's own contract.
func modelFitsHardware(minRAMGb int, modelSizeGB, totalMemoryGB float64) bool {
	if totalMemoryGB <= 0 {
		return true
	}
	if minRAMGb > 0 {
		return float64(minRAMGb) <= totalMemoryGB
	}
	if modelSizeGB > 0 {
		return modelSizeGB*modelMemoryHeadroomFactor <= totalMemoryGB
	}
	return true
}

// costBreakdown decomposes the routing cost so callers can log or
// expose individual contributions. The numeric values match the terms
// added in buildCandidate; total should equal costMs (modulo float
// rounding).
type costBreakdown struct {
	StateMs   float64
	QueueMs   float64
	PendingMs float64
	BacklogMs float64
	ThisReqMs float64
	HealthMs  float64
	// CapacityRateMs is the gray-box capacity-503 rate penalty
	// (capacity_rate.go): rate × EIGENINFERENCE_CAPACITY_RATE_PENALTY_MS once
	// the pair's windowed reject rate clears the threshold with a minimum
	// sample. 0 for healthy pairs, so the cost is byte-for-byte unchanged.
	CapacityRateMs float64
	TTFTMs         float64 // calibrated TTFT estimate for this candidate (gate/ceiling input)
	// RawTTFTMs is the pre-calibration ttftMsFromSnapshot value. The calibrator
	// learns against it (see ttft_calibration.go) so the feedback loop converges
	// on the absolute actual/predicted ratio instead of compounding.
	RawTTFTMs float64
	// CacheDiscountMs is subtracted only after every normal eligibility and
	// admission gate has passed. It never reduces reservations or token budgets.
	CacheDiscountMs float64
	Total           float64
}

// RoutingDecision is the public, exportable record of a routing
// selection. Returned by ReserveProviderEx so callers can emit metrics
// and structured logs without reaching into registry internals.
type RoutingDecision struct {
	ProviderID string  // winning provider, empty if no selection
	Model      string  // requested model
	CostMs     float64 // total cost of the winning candidate
	StateMs    float64 // slot-state penalty contribution
	QueueMs    float64 // pendingForModel × queueDepthPenaltyMs
	PendingMs  float64 // totalPending × totalPendingPenaltyMs
	BacklogMs  float64 // tokens-ahead / decodeTPS contribution
	ThisReqMs  float64 // this request's prefill+decode contribution
	HealthMs   float64 // memory/CPU/thermal/GPU-util contribution
	// CapacityRateMs is the gray-box capacity-503 rate penalty added to the
	// winner's cost (capacity_rate.go); 0 for healthy pairs. In-memory
	// observability only — not persisted (inference_routes has no column and
	// the schema is not altered for it).
	CapacityRateMs float64
	// CapacityRejectRate is the winner's windowed capacity-503 rate at
	// selection time (rejects / (rejects + accepts)); 0 when no rejects are in
	// the window. Same persistence note as CapacityRateMs.
	CapacityRejectRate float64
	EffectiveQueue     int // max(pendingForModel, backendRunning+backendWaiting)
	CandidateCount     int // total candidates that passed all gates
	CapacityRejections int // candidates rejected by the free-memory admission gate (transient: full)
	// ModelTooLargeRejections counts providers that serve the model but whose
	// total memory can never fit it (permanent). Kept separate from
	// CapacityRejections so callers don't emit a 429/"over capacity, retry"
	// signal for a model that will never fit anywhere of this size.
	ModelTooLargeRejections int
	// VisionRejections counts providers that serve the model but only as a
	// text-only build, when the request requires vision. Lets the caller return a
	// precise "no vision-capable provider for this model" error instead of a
	// generic capacity/queue signal.
	VisionRejections int
	// TTFTRejections counts providers that passed all other gates but exceeded
	// the per-request MaxTTFTMs ceiling. Lets the caller fail fast with a 429
	// instead of queueing or routing to a provider that misses the SLA.
	TTFTRejections int
	EffectiveTPS   float64 // load-scaled decode TPS used in cost (Phase 4)
	StaticTPS      float64 // benchmarked decode TPS before load scaling
	// BestTTFTMs is the lowest TTFT estimate seen during selection, even if it
	// exceeded MaxTTFTMs. Used to compute an accurate Retry-After when all
	// candidates are too slow.
	BestTTFTMs float64
	// TTFTMs is the estimated time-to-first-token of the selected provider
	// (CALIBRATED: raw × learned ratio). RawTTFTMs is the pre-calibration
	// ttftMsFromSnapshot value the calibrator learns against; the api layer
	// persists the two side by side.
	TTFTMs          float64
	RawTTFTMs       float64
	CacheTier       string
	CacheDiscountMs float64
	// CacheEstimatedTTFTSavedMs is the uncapped estimated prefill-time benefit
	// net of SSD stage time. CacheDiscountMs remains separately bounded by the
	// routing cost safety caps.
	CacheEstimatedTTFTSavedMs float64

	// Phase-0 shadow TTFT admission/spread evaluation (see ttft_shadow.go).
	// Populated ONLY when EIGENINFERENCE_TTFT_ADMISSION_MODE != off and a
	// provider was selected. Purely observational — it never changes the
	// selection; the API layer emits routing.ttft_admission / routing.ttft_spread
	// from these fields so the spread-to-idle opportunity and the would-shed rate
	// can be measured before any enforce flips them on.
	ShadowEvaluated             bool
	ShadowMode                  string
	ShadowWouldShed             bool
	ShadowIdleAlternativeExists bool
	ShadowEstimateMs            float64
	ShadowDeadlineMs            float64
	ShadowOccupancy             int

	// ---- System-profiler routing context (Contract B). All of the fields
	// below are filled by value from fixed-size candidateScan fields under r.mu
	// with ZERO heap allocation (hot-path review C5); the api layer copies the
	// decision into the request profile AFTER ReserveProviderEx returns and
	// serialises it on the profile sink worker, never under the registry lock.

	// Scanned is the number of providers the candidate loop visited.
	// Since the per-model provider index (model_index.go) the loop walks only
	// the providers ADVERTISING the requested model — so Scanned is the
	// advertising count, not the fleet size, and GateRejections[
	// GateNotServingModel] is 0 unless an advertiser still fails the catalog
	// rule (off-catalog model on a public route). CandidateSetSize is
	// Scanned − GateNotServingModel rejections either way; providers skipped by
	// the exclude/allowlist filters, which run before the catalog check, are
	// counted as advertising. The same applies to the GateAllowlist /
	// GateExcluded tallies themselves: they now count only advertisers (a
	// serial-allowlist miss used to tally ~fleet size per request). Pre-index
	// records have Scanned == fleet size.
	CandidateSetSize, Scanned int
	// GateRejections tallies, per closed GateReason, the providers dropped
	// before cost ranking. Index with GateReason; GateReason.String() is the
	// persisted JSON key.
	GateRejections [GateReasonCount]uint16
	// Top is the winner (Top[0], when a winner exists) followed by the
	// lowest-cost OTHER candidates of the narrowed pool in ascending cost.
	// Present=false marks unfilled slots.
	Top [4]CandidateSummary
	// RunnerUp is the lowest-cost candidate of the narrowed pool other than
	// the winner ("what we would have chosen instead"); Present=false when the
	// pool had a single candidate.
	RunnerUp CandidateSummary
	// BestIdle is the lowest-TTFT candidate whose slot was warm (model
	// resident) with backendRunning+backendWaiting == 0, computed
	// unconditionally over every candidate that passed the routing gates
	// (before pool narrowing). Present=false when no such candidate existed.
	BestIdle CandidateSummary
	// NearTiePoolSize is the number of candidates inside the near-tie cost
	// window of the minimum; SelectionPath says which branch chose the winner.
	NearTiePoolSize int
	SelectionPath   SelectionPath
	// SnapshotAgeMs is the winner's heartbeat age (now − LastHeartbeat) at the
	// moment its routing snapshot was taken.
	SnapshotAgeMs int
	// PredictedDecodeTPS is projectedPerRequestDecodeTPS(winner snapshot): the
	// per-request decode rate this request is predicted to receive once admitted.
	PredictedDecodeTPS float64
	// PendingForModel / TotalPending are the winner's coordinator-side pending
	// counts (this model / all models) at snapshot time, before this reservation.
	PendingForModel, TotalPending int
	// ScanCount is how many candidate scans this reservation attempt ran —
	// one for a clean commit, more when a commit had to rescan (winner gone
	// or full between scan and commit, cache-routing reconfiguration). Zero
	// for a plan-based retry, which reuses the previous scan. The api layer
	// emits it as the routing.scans counter so scan CPU per attempt is
	// measured, not inferred from the profile.
	ScanCount int
	// LockWaitUS / ScanUS / AdmitUS are the three phases of ReserveProviderEx:
	// waiting for r.mu, the candidate scan + selection (+ shadow evaluation),
	// and the admit re-check under p.mu. Microseconds.
	LockWaitUS, ScanUS, AdmitUS int64
	// TTFTCalibrationRatio is the ratio the TTFT calibrator applied to the
	// winner's (model, chip) raw estimate (1.0 = uncalibrated or kill switch off).
	// PrefillDecodeRatio is the decode→prefill fallback multiplier in effect.
	TTFTCalibrationRatio, PrefillDecodeRatio float64
	// Queue path only (filled by the drain from the QueuedRequest): position in
	// the model queue at enqueue (0 = head), queue depth at enqueue (before the
	// append), and the bounded trigger that ran the drain which reserved it.
	QueuePosition, QueueDepth int
	DrainTrigger              string
}

// ReserveProvider selects a hardware-routable provider for the request and
// atomically reserves capacity by registering the request in the provider's
// pending set before returning.
func (r *Registry) ReserveProvider(model string, pr *PendingRequest, excludeIDs ...string) *Provider {
	p, _ := r.ReserveProviderEx(model, pr, excludeIDs...)
	return p
}

// ReserveProviderEx is the metrics-aware variant of ReserveProvider. It
// returns the same Provider plus a RoutingDecision describing the cost
// breakdown of the winning candidate (or, on selection failure, an
// empty decision with CandidateCount=0). Callers wire the decision into
// Prometheus counters/histograms without the registry needing to import
// the metrics package.
func (r *Registry) ReserveProviderEx(model string, pr *PendingRequest, excludeIDs ...string) (*Provider, RoutingDecision) {
	p, decision, _ := r.reserveProvider(model, pr, false, excludeIDs...)
	return p, decision
}

type reservationCommitOutcome uint8

const (
	reservationCommitted reservationCommitOutcome = iota
	reservationNeedsRescan
	reservationCandidateRejected
	reservationDeadlineExpired
)

type providerReservationScan struct {
	selected     *routingCandidate
	candidates   candidateScan
	cacheTracker *cacheRoutingTracker
	cacheMode    string
	// Profiler stamps for the decision: time waiting for the scan RLock and
	// the scan+selection itself, in microseconds.
	lockWaitUS int64
	scanUS     int64
}

// reserveProvider is the single selection+reservation implementation behind
// ReserveProviderEx and ReserveProviderWithPlan (dispatch_plan.go). wantPlan
// additionally retains a bounded DispatchPlan of provisional alternates drawn
// from the SAME scan that picked the winner — the plan is a byproduct of the
// one existing pass, never a second scan — and is nil whenever no provider is
// reserved. Selection and reservation are identical in both modes;
// wantPlan=false skips plan construction entirely so legacy callers pay nothing.
//
// In-flight token-budget ledger: the reservation itself IS the debit. Expensive
// fleet scans share r.mu for reading; the winner is then re-snapshotted and
// committed inside a short section under the winner's p.mu (r.mu is only read
// — see commitProviderReservation; the global mode is the kill switch).
// addPendingLocked records the request before that section ends, so every
// later commit on that provider sees the debit through
// fillSnapshotPendingAndPool and freeMemoryAdmits (including the reconstructed
// whole-box pool) before it can reserve. Concurrent scans therefore do not
// double-spend reported headroom across models. Heartbeat re-sync remains safe:
// coordinatorExtra subtracts committedTokenBudget, so the coordinator-side
// charge shrinks as the provider begins reporting the admitted work. Completion
// and cancel credit through RemovePending; disconnect drops the whole pending
// set; the budget clamp remains the stale-optimistic backstop.
//
// The two-phase boundary preserves the canonical r.mu → p.mu order. A changed
// ranking or cache configuration requests a fresh shared scan. A candidate that
// became ineligible is excluded from this request's later scans, so reservation
// keeps progressing through untried providers until the scan truthfully finds
// none. The request-absolute first-content clock bounds the loop.
func (r *Registry) reserveProvider(model string, pr *PendingRequest, wantPlan bool, excludeIDs ...string) (*Provider, RoutingDecision, *DispatchPlan) {
	if pr == nil || pr.RequestID == "" {
		return nil, RoutingDecision{Model: model}, nil
	}
	if pr.Model == "" {
		pr.Model = model
	}
	if pr.RequestedMaxTokens <= 0 {
		pr.RequestedMaxTokens = defaultRequestedMaxTokens
	}

	excluded := append([]string(nil), excludeIDs...)
	carried := RoutingDecision{Model: model}
	var last providerReservationScan
	var admitUS int64
	scans := 0
	failedDecision := func() RoutingDecision {
		decision := routingDecisionForFailedScan(model, last.candidates)
		addRoutingRejections(&decision, carried)
		decision.LockWaitUS, decision.ScanUS, decision.AdmitUS = last.lockWaitUS, last.scanUS, admitUS
		decision.ScanCount = scans
		return decision
	}
	for pr.RefreshFirstContentBudget(time.Now()) {
		last = r.scanProviderReservation(model, pr, excluded...)
		scans++
		if last.selected == nil {
			return nil, failedDecision(), nil
		}

		// AdmitUS covers the commit phase: the lock waits plus the
		// current-state re-check and the pending debit.
		tCommitStart := time.Now()
		provider, candidate, outcome, rejected := r.commitProviderReservation(
			model, pr, last, excluded...)
		admitUS = time.Since(tCommitStart).Microseconds()
		switch outcome {
		case reservationNeedsRescan:
			continue
		case reservationCandidateRejected:
			addRoutingRejections(&carried, rejected)
			excluded = append(excluded, last.selected.provider.ID)
			continue
		case reservationDeadlineExpired:
			return nil, failedDecision(), nil
		case reservationCommitted:
			decision := routingDecisionForCandidate(
				model, provider, candidate, last.candidates)
			addRoutingRejections(&decision, carried)
			decision.LockWaitUS, decision.ScanUS, decision.AdmitUS = last.lockWaitUS, last.scanUS, admitUS
			decision.ScanCount = scans
			r.currentTTFTShadow(
				model, pr, candidate, excluded...).applyTo(&decision)
			var plan *DispatchPlan
			if wantPlan {
				// The scan pool is immutable value snapshots plus provider
				// identities. Plan consumption revalidates both before use.
				plan = newDispatchPlan(model, last.candidates, last.selected)
			}
			return provider, decision, plan
		}
	}

	return nil, failedDecision(), nil
}

// scanProviderReservation performs the expensive fleet walk under a shared
// registry lock. Concurrent requests may scan together; no provider capacity is
// consumed until commitProviderReservation takes the winner's p.mu and
// revalidates it against current cross-model pending debits.
func (r *Registry) scanProviderReservation(model string, pr *PendingRequest, excludeIDs ...string) providerReservationScan {
	// Profiler stamps: scan-lock wait (from here to the scan RLock) and the
	// scan itself land on the decision as LockWaitUS / ScanUS; ~25 ns each.
	tScanStart := time.Now()
	// Snapshot receipt-confirmed cache hints before taking the registry scan lock.
	// The tracker has its own mutex and must never be nested under r.mu.
	r.mu.RLock()
	cacheTracker, cacheMode := r.cacheRouting, r.cacheRoutingMode
	// cacheRoutingTracker.hints returns nil unless ALL of these hold, so the
	// capability walk (a second full-fleet pass that takes p.mu on every
	// provider) and the route-key copy are skipped exactly when they could not
	// influence selection: cache routing off, no plan on the request, or no
	// route key configured. Same predicate as hints(); keep them in sync.
	wantHints := cacheTracker != nil && cacheMode == CacheRoutingOn &&
		pr.CachePlan.present() && len(r.cacheRouteKeys.route) > 0
	var cacheRouteKey []byte
	if wantHints {
		cacheRouteKey = append([]byte(nil), r.cacheRouteKeys.route...)
	}
	r.mu.RUnlock()
	pr.cacheRoutingHints = nil
	if wantHints {
		cacheCapabilities := r.prefixCacheV2CapabilitiesForModel(model)
		pr.cacheRoutingHints = cacheTracker.hints(
			pr.CachePlan, cacheCapabilities, cacheRouteKey, cacheMode, time.Now())
	}
	pr.CacheSelectionMode = ""
	pr.CacheSelectionTier = ""
	pr.CacheSelectionDiscountMs = 0
	pr.CacheSelectionEstimatedTTFTSavedMs = 0
	pr.CacheSelectionSelected = false
	if pr.CachePlan.present() && cacheMode == CacheRoutingOn {
		pr.CacheSelectionMode = "active"
	}

	r.mu.RLock()
	tLocked := time.Now()
	// Configuration can change while tracker hints are computed outside r.mu.
	// Revalidate under the scan lock so an off/reconfigure transition is
	// linearizable and stale hints never affect selection.
	if cacheMode == CacheRoutingOff || r.cacheRoutingMode != cacheMode ||
		r.cacheRouting != cacheTracker {
		pr.cacheRoutingHints = nil
		pr.CacheSelectionMode = ""
	}
	selected, candidates := r.selectBestCandidateLockedFull(model, pr, excludeIDs...)
	if r.reservationAfterScan != nil {
		// Test-only deterministic barrier. Production never configures this hook.
		r.reservationAfterScan(model)
	}
	tScanned := time.Now()
	result := providerReservationScan{
		selected:     selected,
		candidates:   candidates,
		cacheTracker: r.cacheRouting,
		cacheMode:    r.cacheRoutingMode,
		lockWaitUS:   tLocked.Sub(tScanStart).Microseconds(),
		scanUS:       tScanned.Sub(tLocked).Microseconds(),
	}
	r.mu.RUnlock()
	return result
}

// commitProviderReservation is the short commit phase. It repeats the full
// current-state capacity chain before adding the pending debit, so concurrent
// scans cannot double-spend a provider's shared cross-model token pool.
//
// Locking (reserveCommitShared, the default): r.mu is held for READING — the
// commit needs the provider identity, catalog and cache-routing configuration
// to be stable, not the fleet to be frozen — and everything that decides the
// reservation runs under the winner's p.mu in ONE section: the fresh snapshot,
// the cost rebuild, the "winner unchanged since scan" compare, the admit
// re-check, the probe claim and the pending debit. Double-booking is prevented
// where it always was (providerCanAdmitLockedEx + addPendingLocked under
// p.mu); the herd compare is exact because it compares the winner's own
// counters read under the same p.mu that debits them; the half-open probe
// claim is check-and-claim under gate.mu. Nothing here drains the fleet-scan
// reader batch, which is what each write acquisition cost before.
// reserveCommitGlobal takes r.mu for writing instead — the previous
// fleet-wide serialization, kept as the kill switch.
func (r *Registry) commitProviderReservation(
	model string,
	pr *PendingRequest,
	scan providerReservationScan,
	excludeIDs ...string,
) (*Provider, *routingCandidate, reservationCommitOutcome, RoutingDecision) {
	lock := r.commitLock("commit")
	lock.lock()
	defer lock.unlock()

	// The shared scan and any lock wait consume the same absolute request
	// clock as queueing and provider handoff. Never debit capacity for work whose
	// first-content budget is already gone. One clock read serves the whole
	// commit section (deadline, re-snapshot, cost, admit, probe claim).
	now := time.Now()
	if !pr.RefreshFirstContentBudget(now) {
		return nil, nil, reservationDeadlineExpired, RoutingDecision{}
	}

	// Cache routing reconfiguration after the shared scan invalidates its cost
	// ordering. Retry from a new scan rather than committing a stale discount.
	if r.cacheRouting != scan.cacheTracker || r.cacheRoutingMode != scan.cacheMode {
		return nil, nil, reservationNeedsRescan, RoutingDecision{}
	}
	selected := scan.selected
	if selected == nil || selected.provider == nil {
		return nil, nil, reservationCandidateRejected, RoutingDecision{}
	}
	p := selected.provider
	if current, ok := r.providers[p.ID]; !ok || current != p {
		return nil, nil, reservationCandidateRejected, RoutingDecision{}
	}

	// A breaker bypass is valid only while breaker-open providers remain the
	// sole route. Re-run the normal pass at commit time; this rare emergency path
	// re-scans under the commit lock so a newly healthy provider is preferred
	// over the fail-open choice.
	if scan.candidates.ignoreProviderBreaker {
		normalWinner, normal := r.selectBestCandidateScanLocked(
			model, pr, false, excludeIDs...)
		if normalWinner != nil || !shouldBypassBreakerFailOpen(
			normalWinner, normal.breakerRejected,
			normal.capacityRejections, normal.ttftRejections) {
			return nil, nil, reservationNeedsRescan, RoutingDecision{}
		}
	}

	// Ownership / serial filters take p.mu themselves — evaluate them before
	// the commit section below acquires it.
	owned := providerOwnedBy(p, pr.OwnerAccountID)
	if pr.SelfRouteOnly && !owned {
		return nil, nil, reservationCandidateRejected, RoutingDecision{}
	}
	if len(pr.AllowedProviderSerials) > 0 {
		allowed := make(map[string]struct{}, len(pr.AllowedProviderSerials))
		for _, serial := range pr.AllowedProviderSerials {
			allowed[serial] = struct{}{}
		}
		if !providerMatchesAllowedSerial(p, allowed) {
			return nil, nil, reservationCandidateRejected, RoutingDecision{}
		}
	}
	relaxTrust := owned && (pr.SelfRouteOnly || pr.PreferOwner)

	// Commit section: snapshot, cost, compare, admit and debit under ONE p.mu
	// hold, so no other commit can change this provider between the compare
	// and the debit.
	p.mu.Lock()
	defer p.mu.Unlock()
	var snapshot routingSnapshot
	if ok, _ := r.snapshotProviderIntoPLockedEx(
		&snapshot, p, model, pr.Traits, relaxTrust, scan.candidates.ignoreProviderBreaker, now); !ok {
		return nil, nil, reservationCandidateRejected, RoutingDecision{}
	}
	if pr.RequiresVision && !r.providerServesVisionModelLocked(p, model, relaxTrust) {
		return nil, nil, reservationCandidateRejected,
			routingDecisionForCommitRejection(model, rejectVisionUnsupported, false)
	}
	candidate, reason, ok := r.buildCandidateWithReason(snapshot, pr, now)
	if !ok {
		return nil, nil, reservationCandidateRejected,
			routingDecisionForCommitRejection(model, reason, false)
	}
	if pr.MaxTTFTMs > 0 && !pr.RequiresVision && snapshot.hasBackendCapacity &&
		candidate.breakdown.TTFTMs > pr.MaxTTFTMs {
		return nil, nil, reservationCandidateRejected,
			routingDecisionForCommitRejection(model, rejectNone, true)
	}
	r.applyCacheRoutingDiscountPLocked(p, model, pr, candidate)

	// Another reservation changed this winner after the shared scan. Re-scan the
	// fleet so cost ranking observes that debit instead of herding the whole scan
	// cohort onto the formerly-cheapest provider. The counters compared here
	// were read under the p.mu this section still holds, so a concurrent commit
	// on the same provider is either fully before (and visible) or fully after.
	if snapshot.pendingForModel != selected.snapshot.pendingForModel ||
		snapshot.totalPending != selected.snapshot.totalPending ||
		candidate.effectiveQueue != selected.effectiveQueue ||
		candidate.costMs != selected.costMs {
		return nil, nil, reservationNeedsRescan, RoutingDecision{}
	}

	if !r.providerCanAdmitLockedEx(
		p, model, pr.Traits, relaxTrust, scan.candidates.ignoreProviderBreaker, now) ||
		(pr.RequiresVision && !r.providerServesVisionModelLocked(p, model, relaxTrust)) {
		return nil, nil, reservationCandidateRejected, RoutingDecision{}
	}
	// Half-open capacity probe: check-and-claim under gate.mu (p.mu → gate.mu).
	// A pair whose expired cooldown was claimed by a concurrent commit for the
	// same identity is closed again; reject rather than leak a second probe.
	if !r.tryClaimCapacityProbe(p, model, now) {
		return nil, nil, reservationCandidateRejected,
			routingDecisionForCommitRejection(model, rejectCapacity, false)
	}

	pr.ProviderID = p.ID
	p.addPendingLocked(pr)
	if p.Status != StatusUntrusted && p.Status != StatusOffline {
		p.Status = StatusServing
	}
	if !slotStateModelLoaded(candidate.snapshot.slotState) {
		r.RecordWarmPoolColdDispatch(model)
	}
	if !pr.RequiresVision && candidate.breakdown.RawTTFTMs > 0 && candidate.breakdown.StateMs == 0 {
		ttftCalibration.notePrediction(
			pr.RequestID, pr.Attempt, model, candidate.snapshot.chipFamily,
			candidate.breakdown.RawTTFTMs)
	}
	if candidate.breakdown.CacheDiscountMs > 0 {
		pr.CacheSelectionMode = "active"
		pr.CacheSelectionTier = candidate.cacheTier
		pr.CacheSelectionDiscountMs = candidate.breakdown.CacheDiscountMs
		pr.CacheSelectionEstimatedTTFTSavedMs = candidate.cacheEstimatedTTFTSavedMs
		pr.CacheSelectionSelected = true
	}
	return p, candidate, reservationCommitted, RoutingDecision{}
}

// currentTTFTShadow recomputes the observational signal from the winner's
// commit-time pre-reserve snapshot and a fresh, shared-lock candidate pool. It
// runs after the pending debit is committed, so concurrent reservations cannot
// leave occupancy and idle-alternative telemetry pinned to the original scan.
func (r *Registry) currentTTFTShadow(
	model string,
	pr *PendingRequest,
	winner *routingCandidate,
	excludeIDs ...string,
) ttftShadowEval {
	if TTFTAdmissionModeValue() == TTFTAdmissionOff || winner == nil || pr == nil {
		return ttftShadowEval{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var current candidateScan
	if snapshotOccupancy(&winner.snapshot) > 0 {
		current = r.scanCandidatesLocked(model, pr, false, excludeIDs...)
	}
	return r.evaluateTTFTShadowLocked(model, pr, winner, current)
}

func routingDecisionForCommitRejection(model string, reason candidateRejection, ttft bool) RoutingDecision {
	decision := RoutingDecision{Model: model}
	switch reason {
	case rejectCapacity:
		decision.CapacityRejections = 1
	case rejectModelTooLarge:
		decision.ModelTooLargeRejections = 1
	case rejectVisionUnsupported:
		decision.VisionRejections = 1
	}
	if ttft {
		decision.TTFTRejections = 1
	}
	return decision
}

func addRoutingRejections(dst *RoutingDecision, src RoutingDecision) {
	if dst == nil {
		return
	}
	dst.CapacityRejections += src.CapacityRejections
	dst.ModelTooLargeRejections += src.ModelTooLargeRejections
	dst.VisionRejections += src.VisionRejections
	dst.TTFTRejections += src.TTFTRejections
	if dst.BestTTFTMs == 0 {
		dst.BestTTFTMs = src.BestTTFTMs
	}
}

func routingDecisionForFailedScan(model string, scan candidateScan) RoutingDecision {
	return RoutingDecision{
		Model:                   model,
		CandidateCount:          scan.candidateCount,
		CapacityRejections:      scan.capacityRejections,
		ModelTooLargeRejections: scan.tooLargeRejections,
		VisionRejections:        scan.visionRejections,
		TTFTRejections:          scan.ttftRejections,
		BestTTFTMs:              scan.bestTTFTMs,
		// System-profiler routing context (by value, filled during the scan).
		CandidateSetSize:   scan.candidateSetSize,
		Scanned:            scan.scanned,
		GateRejections:     scan.gateRejections,
		Top:                scan.top,
		RunnerUp:           scan.runnerUp,
		BestIdle:           scan.bestIdle,
		NearTiePoolSize:    int(scan.nearTieSize),
		SelectionPath:      scan.path,
		PrefillDecodeRatio: prefillToDecodeRatio,
	}
}

func routingDecisionForCandidate(model string, provider *Provider, candidate *routingCandidate, scan candidateScan) RoutingDecision {
	bd := candidate.breakdown
	decision := routingDecisionForFailedScan(model, scan)
	decision.ProviderID = provider.ID
	decision.CostMs = bd.Total
	decision.StateMs = bd.StateMs
	decision.QueueMs = bd.QueueMs
	decision.PendingMs = bd.PendingMs
	decision.BacklogMs = bd.BacklogMs
	decision.ThisReqMs = bd.ThisReqMs
	decision.HealthMs = bd.HealthMs
	decision.CapacityRateMs = bd.CapacityRateMs
	decision.CapacityRejectRate = candidate.capacityRejectRate
	decision.EffectiveQueue = candidate.effectiveQueue
	decision.TTFTMs = bd.TTFTMs
	decision.RawTTFTMs = bd.RawTTFTMs
	decision.CacheTier = candidate.cacheTier
	decision.CacheDiscountMs = bd.CacheDiscountMs
	decision.CacheEstimatedTTFTSavedMs = candidate.cacheEstimatedTTFTSavedMs
	decision.EffectiveTPS = candidate.effectiveTPS
	decision.StaticTPS = candidate.snapshot.decodeTPS
	// Winner context for the system-profiler routing record: how stale the
	// winner's inputs were, what it was predicted to deliver, and what it was
	// already carrying — all from the pre-reserve snapshot, no extra locking.
	decision.SnapshotAgeMs = int(candidate.snapshot.hbAgeMs)
	decision.PredictedDecodeTPS = projectedPerRequestDecodeTPS(&candidate.snapshot)
	decision.PendingForModel = candidate.snapshot.pendingForModel
	decision.TotalPending = candidate.snapshot.totalPending
	// The ratio this candidate was actually scored with (captured at build,
	// no second read of the mutable calibrator): the TTFTMs/RawTTFTMs
	// quotient would be wrong for cold slots (the state penalty is
	// deliberately unscaled).
	decision.TTFTCalibrationRatio = candidate.calibrationRatio
	return decision
}

// OwnedProviderSummary reports, for the given account, how many of its
// currently-connected providers are online and how many can serve `model` for
// a request with the given traits/media shape. It powers self-route pre-flight
// error messaging: distinguishing "your machine is offline" from "your machine
// can't serve this request". The model-serving check applies the same
// privacy/runtime/challenge gates as routing but deliberately ignores the
// hardware-trust gate, which self-route relaxes for a caller's own machine.
// traits/requiresVision mirror the dispatch-time gates
// (providerEligibleForTraitsLocked, the vision gate): without them a tool call
// to an owned box below the tools floor — or a media request to a text-only
// build — would pass this preflight, queue for up to 120s, and die as
// machine_busy instead of failing fast with the real cause. Callers asking the
// base-shape question ("any owned box serves this model at all?") pass zero
// traits and requiresVision=false. "Linked but offline" providers are not
// counted here (they are not in the registry); callers detect zero linked
// machines via store.ListProvidersByAccount.
func (r *Registry) OwnedProviderSummary(accountID, model string, traits RequestTraits, requiresVision bool) (online, servesModel int) {
	if accountID == "" {
		return 0, 0
	}
	now := time.Now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.providers {
		p.mu.Lock()
		if p.AccountID == "" || p.AccountID != accountID {
			p.mu.Unlock()
			continue
		}
		if p.Status == StatusOffline || p.Status == StatusUntrusted {
			p.mu.Unlock()
			continue
		}
		online++
		// Owner-servability (not bare advertisement) so the self-route error
		// messaging matches what routing would actually admit: an owned box
		// advertising a stale-hash catalog build reports "model not loaded"
		// instead of proceeding into a dispatch that can only be rejected.
		serves := r.providerServesOwnedRoutableModelLocked(p, model) &&
			r.providerEligibleForTraitsLocked(p, model, traits) &&
			(!requiresVision || r.providerServesVisionModelLocked(p, model, true)) &&
			p.RuntimeVerified &&
			r.providerSupportsPrivateTextAtLocked(p, now) &&
			!p.LastChallengeVerified.IsZero() &&
			now.Sub(p.LastChallengeVerified) <= challengeFreshnessMaxAge
		p.mu.Unlock()
		if serves {
			servesModel++
		}
	}
	return online, servesModel
}

// logRoutingDecision emits a structured debug-level record of the
// winning candidate and its cost breakdown. Cheap when the level is
// disabled, since slog short-circuits before formatting.
func (r *Registry) logRoutingDecision(model string, pr *PendingRequest, winner *routingCandidate, candidates int) {
	if r.logger == nil || winner == nil {
		return
	}
	// Level check BEFORE the variadic call: slog boxes every key/value pair
	// into `any` at the call site (≈15 heap allocations) even when the level
	// is disabled, and this runs under r.mu on every reserve.
	if !r.logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	bd := winner.breakdown
	r.logger.Debug("routing_decision",
		"request_id", pr.RequestID,
		"model", model,
		"winner", winner.provider.ID,
		"cost_ms", bd.Total,
		"state_ms", bd.StateMs,
		"queue_ms", bd.QueueMs,
		"pending_ms", bd.PendingMs,
		"backlog_ms", bd.BacklogMs,
		"this_req_ms", bd.ThisReqMs,
		"health_ms", bd.HealthMs,
		"cache_tier", winner.cacheTier,
		"cache_discount_ms", bd.CacheDiscountMs,
		"effective_tps", winner.effectiveTPS,
		"effective_queue", winner.effectiveQueue,
		"candidates", candidates,
	)
}
