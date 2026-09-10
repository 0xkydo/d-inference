package registry

// Per-provider routing snapshots and memory admission.

import (
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
)

// snapshotProviderIntoLockedEx builds a routing snapshot for p into
// caller-owned storage, returning ok=false and the closed GateReason when p
// fails any structural/privacy/capacity/trait gate. selfRouteOwner is true
// when this is a self-route request and p is owned by the requesting account:
// it (1) drops the hardware-trust floor to TrustNone — a personal Mac will not
// be MDM/MDA enrolled, so without this it would be unroutable to its own owner
// — and (2) admits a private-only machine, which is otherwise excluded from
// the public fleet. Every privacy-critical gate (RuntimeVerified, private-text
// support, challenge freshness) still applies. traits carry the request shape
// into the shape-keyed inference-error cooldown and the render-broken /
// version-floor eligibility gates. ignoreProviderBreaker is threaded into the
// routing gate: only the selectBestCandidateLockedFull fail-open fallback pass
// sets it true. now is the scan clock: hot-path callers walk the whole fleet
// and must read the wall clock ONCE per scan, not once per provider; every
// time-keyed gate (challenge freshness, cooldowns, breaker, clamp) evaluates
// against that single instant.
//
// Writes into caller-owned storage instead of returning the (large) snapshot
// by value, and names WHICH gate dropped a failing provider. The fleet walks
// (scanCandidatesLocked, PredictServable) and the fleet sampler
// (slotEligibilityReasonLocked) hand it the final resting place of the
// snapshot — a candidate-arena slot or a reused buffer — so a routable
// provider's snapshot is written exactly once and never copied
// (routingSnapshot is ~600 bytes; the per-provider return + candidate copies
// were ~9% of the fleet-scale scan). On a gate failure it returns (false, the
// closed GateReason that dropped p) WITHOUT touching *dst; on success *dst is
// fully overwritten — including hbAgeMs, stamped from the threaded now so the
// system-profiler record carries the heartbeat age the scan actually saw —
// and the reason is GateReasonCount.
func (r *Registry) snapshotProviderIntoLockedEx(dst *routingSnapshot, p *Provider, model string, traits RequestTraits, selfRouteOwner bool, ignoreProviderBreaker bool, now time.Time) (bool, GateReason) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return r.snapshotProviderIntoPLockedEx(dst, p, model, traits, selfRouteOwner, ignoreProviderBreaker, now)
}

// snapshotProviderIntoPLockedEx is snapshotProviderIntoLockedEx for a caller
// that ALREADY holds p.mu — the reservation commit and the plan consumption,
// which take the snapshot, rebuild the cost, compare and debit inside one p.mu
// section so nothing can change the provider in between. Caller holds r.mu
// (either mode) and p.mu.
func (r *Registry) snapshotProviderIntoPLockedEx(dst *routingSnapshot, p *Provider, model string, traits RequestTraits, selfRouteOwner bool, ignoreProviderBreaker bool, now time.Time) (bool, GateReason) {
	if ok, reason := r.providerRoutingGateReasonLockedEx(p, model, traits, selfRouteOwner, now, ignoreProviderBreaker, false); !ok {
		return false, reason
	}

	*dst = routingSnapshot{}
	snap := dst
	snap.provider = p
	snap.model = model
	snap.chipFamily = p.Hardware.ChipFamily
	snap.binaryVersion = p.Version
	snap.slotState = "unknown"
	snap.totalPending = p.pendingCount()
	snap.systemMetrics = p.SystemMetrics
	snap.decodeTPS = resolvedDecodeTPS(p)
	snap.prefillTPS = resolvedPrefillTPS(p)
	snap.totalMemoryGB = float64(p.Hardware.MemoryGB)
	snap.modelSizeGB = r.modelSizeGBForFitLocked(p, model)
	snap.minRAMGb = r.catalogMinRAMGbLocked(model)
	// Heartbeat age from the scan clock (system-profiler record); a zero
	// LastHeartbeat saturates rather than reading as "fresh".
	snap.hbAgeMs = heartbeatAgeMs(now, p.LastHeartbeat)

	fillSnapshotPendingAndPool(snap, p, model)
	// Concurrency headroom with the quality-concurrency cap: a slow model whose
	// quality batch is below the flat fallback (e.g. Gemma at ~14 tok/s solo →
	// batch 1-2) stops being admittable once it is at its quality cap, so load
	// spreads across boxes instead of collapsing a few. The cap resolves the
	// model's own static solo rate internally (solo median / seed → provider
	// benchmark fallback) — NOT snap.decodeTPS, which stays the provider-level
	// rate for TTFT/cost estimation, and NOT the observed-under-load value.
	// No-op (legacy flat cap) when the cap is disabled.
	snap.hasHeadroom = r.hasConcurrencyHeadroomForModelCapResolvedLocked(p, model)
	snap.hasBackendCapacity = p.BackendCapacity != nil

	if p.BackendCapacity != nil {
		snap.gpuMemoryActiveGB = p.BackendCapacity.GPUMemoryActiveGB
		snap.freeForLoadGB = p.BackendCapacity.FreeForLoadGB
		if p.BackendCapacity.TotalMemoryGB > 0 {
			snap.totalMemoryGB = p.BackendCapacity.TotalMemoryGB
		}
		for _, slot := range p.BackendCapacity.Slots {
			if slot.Model != model {
				continue
			}
			snap.slotState = slot.State
			snap.backendRunning = int(slot.NumRunning)
			snap.backendWaiting = int(slot.NumWaiting)
			snap.maxTokensPotential = slot.MaxTokensPotential
			snap.observedDecodeTPS = slot.ObservedDecodeTPS
			snap.observedPrefillTPS = slot.ObservedPrefillTPS
			snap.activeTokenBudgetUsed = slot.ActiveTokenBudgetUsed
			snap.activeTokenBudgetMax = slot.ActiveTokenBudgetMax
			snap.queuedTokenBudget = slot.QueuedTokenBudget
			snap.kvBytesPerToken = clampKVBytesPerToken(slot.KVBytesPerToken)
			snap.stepsExecuted = slot.StepsExecuted
			snap.admits = slot.Admits
			snap.firstTokensEmitted = slot.FirstTokensEmitted
			snap.secondsSinceLastStep = slot.SecondsSinceLastStep
			snap.secondsSinceLastFirstToken = slot.SecondsSinceLastFirstToken
			snap.wedgeSuspected = slot.WedgeSuspected
			snap.evalInFlightMs = slot.EvalInFlightMs
			snap.idleClearInFlightMs = slot.IdleClearInFlightMs
			break
		}
	}
	snap.modelLoaded = slotStateModelLoaded(snap.slotState)
	snap.availableOnDisk = !snap.modelLoaded
	snap.fleetMedianTPS = r.tpsRegistry.Median(model, p.Hardware.ChipFamily)

	// Gray-box budget clamp (budget_clamp.go): when a capacity-503 has proven
	// the pair's live gate is rejecting, admission must not believe the
	// stale-optimistic heartbeat budget. Evaluated for budgetless snapshots
	// too — a reconnected session has no BackendCapacity until its first
	// heartbeat, and a clamp armed on a budget-reporting pair must keep
	// holding through that window instead of shedding onto the legacy memory
	// path (never-budget-reporting legacy pairs stay exempt inside the check).
	// p.LastHeartbeat is when the CURRENT BackendCapacity was delivered
	// (Heartbeat stamps both in one critical section), which is what the
	// release-freshness check compares against the clamp time. p.mu and r.mu
	// are both held here (see lock discipline above); the clamp read is one
	// lock-free flag load unless the identity actually carries a clamp, and is
	// confirmed against p.gate like the gates above (gateView).
	rawRemaining := snap.activeTokenBudgetMax - snap.activeTokenBudgetUsed - snap.queuedTokenBudget
	snap.budgetClamped = r.budgetClampedFor(p, model, p.LastHeartbeat, rawRemaining, snap.activeTokenBudgetMax > 0, now)

	return true, GateReasonCount
}

// heartbeatAgeMs is now − lastHeartbeat in milliseconds, clamped to int32
// (a zero LastHeartbeat saturates rather than producing a nonsense value).
func heartbeatAgeMs(now, lastHeartbeat time.Time) int32 {
	if lastHeartbeat.IsZero() {
		return clampMsInt32(int64(^uint32(0) >> 1))
	}
	return clampMsInt32(now.Sub(lastHeartbeat).Milliseconds())
}

// coldLoadCatalogGBToMemGiB converts a model's catalog on-disk size (decimal GB,
// TotalSizeBytes/1e9, unpadded) into the provider's load-gate basis (padded GiB).
// The provider's ModelLoadAdmission.canLoad weighs estimatedMemoryGb = on-disk
// bytes × 1.2 (scanner memory-overhead) / 2^30, and free_for_load_gb is reported
// in that same padded-GiB basis. So a raw catalog size must be padded+converted
// the same way before comparing, or a near-threshold model whose RAW size fits
// but whose PADDED estimate doesn't would be admitted here and then 503'd at load
// (Codex #390). 1.2 mirrors the provider scanner's overhead factor; (1e9/2^30)
// converts decimal GB → GiB. Conservative: if the scanner's factor ever drops,
// this stays safe (slightly stricter); it must not be set BELOW the provider's.
const coldLoadCatalogGBToMemGiB = 1.2 * (1e9 / float64(int64(1)<<30)) // ≈ 1.1176

// backendFreeForLoadGB returns the provider-reported free_for_load_gb (nil-safe).
// Caller must hold the provider lock when passing p.BackendCapacity.
func backendFreeForLoadGB(bc *protocol.BackendCapacity) *float64 {
	if bc == nil {
		return nil
	}
	return bc.FreeForLoadGB
}

// reportedFreeForLoadAdmits reports whether a cold load of a model with the given
// catalog size (decimal GB) fits the provider's reported free_for_load_gb (max
// loadable model weight, padded GiB — the provider's authoritative gate). The
// second return is whether the provider reported the value at all; false means
// the caller should fall back to its static hardware heuristic (legacy provider,
// or unknown catalog size that can't be normalized). Used by every cold-load
// decision path (direct admission, the swap planner, the warm pool, and the
// cold-spill predicate) so they cannot drift.
// The PADDED conversion on purpose, for every binary and model: this
// mirrors the provider's ADMIT gate, which deliberately charges the
// disk×1.2 load-transient figure (shard staging exceeds steady residency).
// Measured post-load residency (servabilityMeasuredResidentGiB) informs
// only coldTokenBudgetEstimate — the POST-load arithmetic. binaryVersion
// and modelID are accepted for parity with that estimate's selection and
// for future load-peak measurements; today they are deliberately unused.
func reportedFreeForLoadAdmits(
	catalogSizeGB float64, freeForLoadGB *float64, binaryVersion, modelID string,
) (admit bool, reported bool) {
	_, _ = binaryVersion, modelID
	if freeForLoadGB == nil || catalogSizeGB <= 0 {
		return false, false
	}
	return catalogSizeGB*coldLoadCatalogGBToMemGiB <= *freeForLoadGB, true
}

// freeMemoryAdmits returns true when the provider has enough headroom.
// Providers that report a token budget use budget-based admission;
// legacy providers fall back to memory-based estimation.
func freeMemoryAdmits(snap *routingSnapshot, reqPromptTokens, reqMaxTokens int) bool {
	// Gray-box budget clamp: a capacity-503 proved the provider's live gate
	// rejects while the heartbeat budget below still advertises headroom
	// (stale-optimistic). While the clamp holds, the slot is FULL — no
	// request fits — until the provider proves recovery (fresh heartbeat with
	// headroom + an accept) or the clamp TTL fail-opens. Checked BEFORE the
	// budget branch: a clamped budget-reporting pair whose current session
	// has no budget snapshot yet (reconnect before the first heartbeat) must
	// reject here, not fall through to the legacy memory path below. See
	// budget_clamp.go.
	if snap.budgetClamped {
		return false
	}
	requestTokens := int64(reqPromptTokens) + int64(reqMaxTokens)
	// Engine V2 keeps reporting a positive KV rate when its live fleet clamp
	// drives this model's budget to zero. That is authoritative known-full
	// capacity, not the legacy "budget unavailable" shape (both fields absent).
	// Bind it before consulting co-resident pooled headroom: another model's
	// positive budget cannot widen this model-local zero.
	if knownZeroTokenBudget(snap.activeTokenBudgetMax, snap.kvBytesPerToken) {
		return false
	}
	if snap.activeTokenBudgetMax > 0 {
		// Include coordinator-side pending tokens not yet reflected in the
		// provider's heartbeat. Avoid double-counting active/queued backend
		// budgets that are still present in the coordinator pending set until
		// completion/cancellation removes them.
		coordinatorExtra := int64(snap.pendingMaxTokens) - committedTokenBudget(snap)
		if coordinatorExtra < 0 {
			coordinatorExtra = 0
		}
		if snap.activeTokenBudgetUsed+snap.queuedTokenBudget+coordinatorExtra+requestTokens > snap.activeTokenBudgetMax {
			return false
		}
		// The per-slot max encodes this model's own context/KV ceiling. Through
		// v0.7.4 each slot embeds the same shared headroom; v0.7.5+ reports a
		// private re-sliced grant. The request must also fit the correctly
		// reconstructed whole-box pool with EVERY model's
		// coordinator-pending tokens charged — byte-normalized per slot KV rate
		// when reported, since co-resident models spend the pool at different
		// bytes/token (see pooled_admission.go). Reduces exactly to the per-slot
		// check for single-model providers.
		return pooledBudgetAdmits(snap, requestTokens)
	}

	// Cold-slot pooled gate: this model reports no budget slot (not loaded
	// here), but when ANY resident slot reports a token budget this request lands
	// in the same box after load. In-gap pending on a resident model must not be double-spendable
	// by a cold request that skips the budget branch above. The reconstructed
	// pool charges all-models coordinator pending plus this request; a cold
	// model has no reported KV rate (snap.kvBytesPerToken == 0), so on a
	// byte-reconstructable pool it is priced conservatively in bytes at the
	// bounded unknown-model default (resolvedPooledKVBytesPerToken),
	// falling to token units only when the pool is not byte-reconstructable.
	// No-op for legacy providers with neither budget nor KV-rate reports.
	if !pooledBudgetAdmits(snap, requestTokens) {
		return false
	}

	if !snap.modelLoaded {
		if fits, known := providerBudgetFits(snap, reqPromptTokens, reqMaxTokens); known && !fits {
			return false
		}
	}

	if snap.modelSizeGB <= 0 || snap.totalMemoryGB <= 0 {
		return true
	}
	required := snap.modelSizeGB
	if snap.modelLoaded {
		required = 0
	}
	tokens := int64(reqPromptTokens) + int64(reqMaxTokens)
	if tokens < 0 {
		tokens = 0
	}
	const maxTokensForCalc = 16 << 20
	if tokens > maxTokensForCalc {
		tokens = maxTokensForCalc
	}
	kvCacheGB := float64(tokens*kvCacheBytesPerToken) / float64(bytesPerGB)
	required += kvCacheGB

	// When the model is available on disk but not currently loaded, the
	// provider will evict idle models to make room (LRU eviction), so we check
	// whether the model can be loaded rather than requiring it to fit alongside
	// existing loaded models. The provider handles the swap autonomously.
	//
	// However, if the provider has in-flight requests (totalPending > 0), it
	// cannot evict the currently-serving model. In that case, fall through to the
	// standard free-memory check which requires room alongside active models.
	if snap.availableOnDisk && !snap.modelLoaded && snap.totalPending == 0 {
		// Preferred: the provider reports freeForLoadGB — the max model WEIGHT it
		// can load right now, already net of the 90% unified cap, OS/operator
		// reserve, activation+min-KV headroom, real OS-available memory, and
		// eviction of idle models. The single source of truth, normalized to the
		// provider's padded-GiB load basis so it exactly mirrors the provider's own
		// ModelLoadAdmission gate (no over-admit → OOM, no under-admit on evictable
		// weights).
		if admit, reported := reportedFreeForLoadAdmits(snap.modelSizeGB, snap.freeForLoadGB, snap.binaryVersion, snap.model); reported {
			return admit
		}
		// Fallback for legacy providers that don't report freeForLoadGB: the old
		// total-memory heuristic (provider evicts idle models, so compare against
		// total rather than free). Coarser — can't see the unified cap or OS
		// baseline — but only used until the fleet reports the field.
		const osReserveGB = 4.0
		return snap.modelSizeGB+kvCacheGB+osReserveGB <= snap.totalMemoryGB
	}

	free := snap.totalMemoryGB - snap.gpuMemoryActiveGB
	return free >= required
}

// fillSnapshotPendingAndPool populates snap's reconstructed pooled budget and
// its coordinator-pending aggregates — the per-model filtered pair
// (pendingForModel / pendingMaxTokens) and the all-models totals in token and,
// when normalizable, byte units. Byte normalization uses each resident model's
// reported KVBytesPerToken and the same bounded conservative default as
// incoming/capacity math for a cold model with no resident slot. Only a legacy
// pool that cannot be reconstructed in bytes leaves pendingBytesKnown false.
// Shared by the dispatch
// snapshot (snapshotProviderLockedEx) and the queue preflight
// (QuickCapacityCheck…) so the two admission paths cannot drift. Caller holds
// p.mu.
func fillSnapshotPendingAndPool(snap *routingSnapshot, p *Provider, model string) {
	if p.BackendCapacity != nil {
		snap.pooledTokenBudget = providerPooledTokenBudgetForVersion(
			p.BackendCapacity.Slots, p.Version)
	}
	bytesKnown := snap.pooledTokenBudget.byteMode
	for _, pr := range p.pendingReqs {
		tokens := pendingTokenBudget(pr)
		snap.pendingMaxTokensAllModels += tokens
		if bytesKnown {
			rate := resolvedPooledKVBytesPerToken(&snap.pooledTokenBudget, snap.pooledTokenBudget.kvRateFor(pr.Model))
			snap.pendingMaxBytesAllModels = addPooledKVByteCharge(snap.pendingMaxBytesAllModels, int64(tokens), rate)
		}
		if pr.Model != model {
			continue
		}
		snap.pendingForModel++
		snap.pendingMaxTokens += tokens
	}
	snap.pendingBytesKnown = bytesKnown
}

func pendingTokenBudget(pr *PendingRequest) int {
	if pr == nil {
		return 0
	}
	prompt := pr.EstimatedPromptTokens
	if prompt < 0 {
		prompt = 0
	}
	maxTok := pr.RequestedMaxTokens
	if maxTok <= 0 {
		maxTok = defaultRequestedMaxTokens
	}
	return prompt + maxTok
}

func committedTokenBudget(snap *routingSnapshot) int64 {
	committed := snap.activeTokenBudgetUsed + snap.queuedTokenBudget
	if snap.maxTokensPotential > committed {
		committed = snap.maxTokensPotential
	}
	if committed < 0 {
		return 0
	}
	return committed
}

// snapshotOccupancy is the per-(provider,model) in-flight occupancy the
// coordinator already tracks: max(pendingForModel, backend_running +
// backend_waiting). pendingForModel is the coordinator's own dispatched-but-not-
// yet-terminal count (incremented at reserve, held the whole dark-time), so this
// is herd-aware even when the heartbeat gauge still reads backend_running=0 — no
// parallel reservation counter is needed. It is the same quantity the routing
// cost's effectiveQueue and the quality-concurrency cap consume; the Phase-0
// occupancy-aware TTFT term and the shadow admission/spread evaluator reuse it so
// every occupancy-keyed decision reads one signal.
func snapshotOccupancy(snap *routingSnapshot) int {
	occ := snap.pendingForModel
	if backendDepth := snap.backendRunning + snap.backendWaiting; backendDepth > occ {
		occ = backendDepth
	}
	if occ < 0 {
		occ = 0
	}
	return occ
}
