package registry

// Candidate scanning, ranking, routing gates, and tie-breaking.

import (
	"math"
	"math/rand"
	"time"
)

// applyCacheRoutingDiscount reads the candidate's own snapshot (the scan
// builds it in place; no copy is taken). The caller does NOT hold p.mu (the
// scan): the hint currency check takes it.
func (r *Registry) applyCacheRoutingDiscount(p *Provider, model string, pr *PendingRequest, candidate *routingCandidate) {
	hint, ok := pr.cacheRoutingHints[p.ID]
	if !ok || !hint.currentForProvider(p, model) {
		return
	}
	r.applyCacheHintDiscount(hint, candidate)
}

// applyCacheRoutingDiscountPLocked is applyCacheRoutingDiscount for a caller
// that already holds p.mu (the reservation commit).
func (r *Registry) applyCacheRoutingDiscountPLocked(p *Provider, model string, pr *PendingRequest, candidate *routingCandidate) {
	hint, ok := pr.cacheRoutingHints[p.ID]
	if !ok || !hint.currentForProviderLocked(p, model) {
		return
	}
	r.applyCacheHintDiscount(hint, candidate)
}

func (r *Registry) applyCacheHintDiscount(hint cacheRoutingHint, candidate *routingCandidate) {
	prefillTPS := resolvePrefillTPS(&candidate.snapshot)
	if prefillTPS <= 0 || math.IsNaN(prefillTPS) || math.IsInf(prefillTPS, 0) {
		return
	}
	netSavedMs := float64(hint.PrefillTokensSaved)/prefillTPS*1000 - hint.StageMs
	if netSavedMs <= 0 || math.IsNaN(netSavedMs) || math.IsInf(netSavedMs, 0) {
		return
	}
	capMs := math.Min(r.cacheRoutingMaxDiscountMs, candidate.costMs*r.cacheRoutingMaxCostFraction)
	discount := math.Min(netSavedMs, capMs)
	if discount <= 0 {
		return
	}
	candidate.breakdown.CacheDiscountMs = discount
	candidate.cacheTier = "ssd"
	candidate.cacheEstimatedTTFTSavedMs = netSavedMs
	candidate.costMs -= discount
	candidate.breakdown.Total = candidate.costMs
}

// selectBestCandidateLockedFull is the full-fidelity selection that
// also reports how many providers were rejected by capacity-style
// gates (memory). Capacity rejection count lets ReserveProviderEx
// distinguish "no provider serves this model" from "every fitting
// provider is over-subscribed", which is the difference between the
// no_provider and over_capacity outcome counters.
// Returns the winner plus the candidateScan of the pass that produced it, so
// the caller can read the rejection tallies AND (for plan retention) the
// ranked pool itself without a second scan.
//
// FAIL-OPEN SAFETY VALVE: selection runs in two passes. Pass 1 honors the
// per-provider node-health breaker. If pass 1 finds ZERO candidates AND the
// breaker is the SOLE reason — it rejected at least one provider AND no healthy
// provider was merely busy or too slow — pass 2 re-runs the whole scan with the
// breaker BYPASSED (ignoreProviderBreaker=true), so a bad fleet-wide rollout
// that fault-503s every node can never deroute the entire fleet. When healthy
// providers are simply over capacity or above the TTFT ceiling, pass 1's signal
// is returned instead, so the request queues / 429s and waits for a healthy node
// rather than being routed to a known-bad provider. Pass 2's result is used only
// when it yields a candidate, and its counters (not pass 1's) are returned so
// metrics are never double-counted. This mirrors servability.go's fail-open
// philosophy: when in doubt, keep serving.
func (r *Registry) selectBestCandidateLockedFull(model string, pr *PendingRequest, excludeIDs ...string) (*routingCandidate, candidateScan) {
	winner, scan := r.selectBestCandidateScanLocked(model, pr, false, excludeIDs...)
	if !shouldBypassBreakerFailOpen(winner, scan.breakerRejected, scan.capacityRejections, scan.ttftRejections) {
		return winner, scan
	}
	// The node-health breaker is the SOLE reason this request has no route: re-scan
	// with the breaker bypassed. Use pass 2 only when it actually finds a candidate,
	// so a genuinely empty fleet still reports pass 1's (accurate) counters.
	if w2, scan2 := r.selectBestCandidateScanLocked(model, pr, true, excludeIDs...); w2 != nil {
		return w2, scan2
	}
	return winner, scan
}

// shouldBypassBreakerFailOpen decides whether selection should retry with the
// node-health breaker bypassed (the fail-open safety valve). It fails open ONLY
// when the breaker is the SOLE reason no route was found:
//   - pass 1 produced no winner, AND
//   - the breaker rejected at least one provider, AND
//   - no healthy provider was merely busy (capacityRejections) or too slow
//     (ttftRejections).
//
// If a healthy provider was just over capacity or above the TTFT ceiling, we
// surface that signal (so the request queues / 429s and waits for a healthy
// node) rather than routing to a known-bad, breaker-open provider. Model-too-
// large and vision-unsupported rejections are deliberately NOT counted: those
// providers cannot serve this request at all, so they are not a healthy
// alternative to a fail-open probe.
func shouldBypassBreakerFailOpen(winner *routingCandidate, breakerRejected, capacityRejections, ttftRejections int) bool {
	return winner == nil && breakerRejected > 0 && capacityRejections == 0 && ttftRejections == 0
}

// candidateScan is the result of building the eligible candidate pool for a
// request: the cost-rankable pool (after every per-provider gate AND the
// post-candidate pool narrowing) plus the rejection tallies. It is the SINGLE
// SOURCE of routing eligibility, shared by
// the cost-ranking selector (selectBestCandidateScanLocked) and the Phase-0
// idle-spread shadow scan (loadedIdleAlternativeExistsLocked) so the two can
// never drift on which providers are routable.
type candidateScan struct {
	pool                  []*routingCandidate
	candidateCount        int
	capacityRejections    int
	tooLargeRejections    int
	visionRejections      int
	ttftRejections        int
	bestTTFTMs            float64
	breakerRejected       int
	ignoreProviderBreaker bool

	// System-profiler routing context — fixed-size value fields filled inside
	// the existing loops with ZERO heap allocation (hot-path review C5). See the
	// matching RoutingDecision fields for semantics.
	scanned          int
	candidateSetSize int
	gateRejections   [GateReasonCount]uint16
	top              [4]CandidateSummary
	runnerUp         CandidateSummary
	bestIdle         CandidateSummary
	nearTieSize      int32
	path             SelectionPath
}

// tallyGate records one gate rejection, saturating at the uint16 ceiling.
func (s *candidateScan) tallyGate(reason GateReason) {
	if reason >= GateReasonCount {
		return
	}
	if s.gateRejections[reason] < ^uint16(0) {
		s.gateRejections[reason]++
	}
}

// insertTop inserts a candidate summary into the fixed top-4 array, keeping it
// sorted by ascending cost. Allocation-free: at most 3 element moves.
func (s *candidateScan) insertTop(c *routingCandidate) {
	pos := len(s.top)
	id := ""
	if c.provider != nil {
		id = c.provider.ID
	}
	for i := range s.top {
		// Equal costs are ordered by provider id so the recorded top-4 is
		// deterministic regardless of map iteration order.
		if !s.top[i].Present || c.costMs < s.top[i].CostMs ||
			(c.costMs == s.top[i].CostMs && id < s.top[i].ProviderID) {
			pos = i
			break
		}
	}
	if pos >= len(s.top) {
		return
	}
	copy(s.top[pos+1:], s.top[pos:len(s.top)-1])
	s.top[pos] = candidateSummaryOf(c)
}

// promoteWinnerTop moves the winner to top[0] (contract: "winner is Top[0] when
// present"), keeping the remaining slots in ascending cost. When the winner is
// not among the top-4 by cost (possible after a random near-tie pick), it is
// inserted at the head and the last slot is dropped.
func (s *candidateScan) promoteWinnerTop(winner *routingCandidate) {
	if winner == nil || winner.provider == nil {
		return
	}
	id := winner.provider.ID
	for i := range s.top {
		if s.top[i].Present && s.top[i].ProviderID == id {
			if i == 0 {
				return
			}
			w := s.top[i]
			copy(s.top[1:i+1], s.top[0:i])
			s.top[0] = w
			return
		}
	}
	copy(s.top[1:], s.top[0:len(s.top)-1])
	s.top[0] = candidateSummaryOf(winner)
}

// noteBestIdle updates the best-idle slot: the lowest-TTFT candidate whose slot
// is warm (model resident) and whose backend reports zero running + waiting.
func (s *candidateScan) noteBestIdle(c *routingCandidate) {
	snap := &c.snapshot
	if !snap.modelLoaded || snap.backendRunning+snap.backendWaiting != 0 || !snap.hasBackendCapacity {
		return
	}
	ttft := c.breakdown.TTFTMs
	if s.bestIdle.Present {
		// Deterministic tie-break (lower cost, then provider id) so the record
		// does not depend on map iteration order.
		if ttft > s.bestIdle.TTFTMs {
			return
		}
		if ttft == s.bestIdle.TTFTMs {
			if c.costMs > s.bestIdle.CostMs {
				return
			}
			if c.costMs == s.bestIdle.CostMs && (c.provider == nil || c.provider.ID >= s.bestIdle.ProviderID) {
				return
			}
		}
	}
	s.bestIdle = candidateSummaryOf(c)
}

// scanCandidatesLocked builds the eligible candidate pool for a request — every
// per-provider gate (self-route, allowlist, exclude, structural/trait/trust via
// snapshotProviderLockedEx, vision, capacity via buildCandidateWithReason, plus
// the per-request TTFT ceiling) followed by the post-candidate pool narrowing
// (prefer-owner / AvoidVersion / MinDecodeTPS) — i.e. exactly the set the
// selector ranks by cost. When ignoreProviderBreaker is true the node-health
// breaker gate is skipped (every other gate still applies); breakerRejected is
// always 0 in that mode. Caller holds r.mu and no provider lock.
func (r *Registry) scanCandidatesLocked(model string, pr *PendingRequest, ignoreProviderBreaker bool, excludeIDs ...string) candidateScan {
	// Nil maps read as empty; only allocate when there is something to hold.
	var excludeSet map[string]struct{}
	if len(excludeIDs)+len(pr.ExcludedProviderIDs) > 0 {
		excludeSet = make(map[string]struct{}, len(excludeIDs)+len(pr.ExcludedProviderIDs))
		for _, id := range excludeIDs {
			excludeSet[id] = struct{}{}
		}
		for _, id := range pr.ExcludedProviderIDs {
			excludeSet[id] = struct{}{}
		}
	}
	var allowedSerials map[string]struct{}
	if len(pr.AllowedProviderSerials) > 0 {
		allowedSerials = make(map[string]struct{}, len(pr.AllowedProviderSerials))
		for _, serial := range pr.AllowedProviderSerials {
			allowedSerials[serial] = struct{}{}
		}
	}

	// Two-pass selection: collect all eligible candidates first, then
	// compute best + tie pool. The single-pass approach was order-
	// dependent — when a new best replaced an older one within the tie
	// window, candidates near the OLD best (and still near the NEW
	// best) were dropped from the pool, making the queue-depth tie-
	// break flaky under map iteration randomness.
	// Only providers advertising the model can pass the first gate; the
	// per-model index (model_index.go) prunes the rest without touching any
	// gate. Copied before any p.mu is taken (index lock discipline).
	providers := r.providersForModelLocked(model)
	candidates := make([]*routingCandidate, 0, len(providers))
	// Candidates live in arena chunks: one allocation per candidateArenaChunk
	// candidates instead of one per candidate, and each snapshot is written
	// straight into its slot (candidate_arena.go).
	var arena candidateArena
	candidateCount := 0
	capacityRejections := 0
	tooLargeRejections := 0
	visionRejections := 0
	ttftRejections := 0
	bestTTFTMs := 0.0
	breakerRejected := 0
	// scan carries the fixed-size system-profiler context (gate-reason tallies,
	// best-idle, top-4) filled inside this loop with zero heap allocation; the
	// legacy counters above are assigned into it at the end, unchanged.
	var scan candidateScan
	now := time.Now()
	// Vision preparation is absent from the token-prefill projection, so media
	// estimates are advisory even if a caller accidentally supplies a ceiling.
	// The request-absolute first-content deadline remains authoritative.
	enforceTTFT := pr.MaxTTFTMs > 0 && !pr.RequiresVision
	for _, p := range providers {
		scan.scanned++
		owned := providerOwnedBy(p, pr.OwnerAccountID)
		// Exclusive self-route: restrict to the caller's own machines and never
		// fall back to the public fleet. Tallied as an allowlist drop: the caller
		// restricted routing to a set of providers this one is not in.
		if pr.SelfRouteOnly && !owned {
			scan.tallyGate(GateAllowlist)
			continue
		}
		if len(allowedSerials) > 0 {
			if !providerMatchesAllowedSerial(p, allowedSerials) {
				scan.tallyGate(GateAllowlist)
				continue
			}
		}
		if _, excluded := excludeSet[p.ID]; excluded {
			scan.tallyGate(GateExcluded)
			continue
		}
		// Relax the hardware-trust floor ONLY for the caller's own (possibly
		// un-enrolled) machine — whether exclusive self-route or prefer — never
		// for public providers.
		relaxTrust := owned && (pr.SelfRouteOnly || pr.PreferOwner)
		// snapshotProviderIntoLockedEx applies every per-provider gate via the shared
		// providerPassesRoutingGatesLocked, INCLUDING the shape-keyed
		// inference-error cooldown and the trait gates (render-broken fences all
		// shapes; the tools version floor fences tool requests). A failing
		// provider is simply dropped here — the returned gate reason names WHICH
		// gate dropped it for the profiler tally without changing the verdict.
		// The snapshot is written straight into an arena slot (candidate_arena.go).
		c := arena.next()
		ok, gateReason := r.snapshotProviderIntoLockedEx(&c.snapshot, p, model, pr.Traits, relaxTrust, ignoreProviderBreaker, now)
		if !ok {
			arena.release(c)
			scan.tallyGate(gateReason)
			breaker, capacity := r.classifyRejectedProvider(
				r.gateViewOf(p), model, pr.Traits, relaxTrust, ignoreProviderBreaker, now)
			if breaker {
				breakerRejected++
			}
			if capacity {
				capacityRejections++
			}
			continue
		}
		// Vision gate: a media request must only go to a provider advertising a
		// vision-capable build of this model. Providers reach here only if they
		// already serve the model (snapshot ok), so a miss here means "serves it,
		// but text-only" — counted separately so the caller can return a precise
		// "no vision-capable provider" error rather than a busy/429. snapshot
		// released p.mu, so re-take it for the p.Models read.
		if pr.RequiresVision {
			p.mu.Lock()
			servesVision := r.providerServesVisionModelLocked(p, model, relaxTrust)
			p.mu.Unlock()
			if !servesVision {
				arena.release(c)
				visionRejections++
				scan.tallyGate(GateVision)
				continue
			}
		}
		reason, gateReason, ok := r.buildCandidateInto(c, pr, now)
		if !ok {
			arena.release(c)
			switch reason {
			case rejectCapacity:
				capacityRejections++
			case rejectModelTooLarge:
				tooLargeRejections++
			case rejectVisionUnsupported:
				visionRejections++
			}
			scan.tallyGate(gateReason)
			continue
		}

		// Track the best reliable TTFT seen among providers that passed all
		// structural and capacity gates. Even if this candidate is over the
		// ceiling, the value is used for Retry-After on the TTFT 429 path.
		// Providers without BackendCapacity do not contribute a reliable TTFT
		// estimate, so they are skipped here.
		if c.snapshot.hasBackendCapacity && (c.breakdown.TTFTMs < bestTTFTMs || bestTTFTMs == 0) {
			bestTTFTMs = c.breakdown.TTFTMs
		}

		// Enforce the per-request TTFT ceiling for public inference routes.
		// Providers above the threshold are counted as TTFT rejections and
		// excluded from cost-based selection so the router cannot pick a
		// provider that misses the OpenRouter SLA target. Providers without
		// BackendCapacity have no reliable TTFT estimate, so the ceiling is
		// not enforced on them (matching the preflight behavior).
		if enforceTTFT && c.snapshot.hasBackendCapacity && c.breakdown.TTFTMs > pr.MaxTTFTMs {
			arena.release(c)
			ttftRejections++
			scan.tallyGate(GateTTFTCeiling)
			continue
		}

		r.applyCacheRoutingDiscount(p, model, pr, c)
		// Best-idle is computed UNCONDITIONALLY over every routable candidate
		// (before pool narrowing) so the record can answer "was an idle warm box
		// available?" whether or not the shadow evaluator is on.
		scan.noteBestIdle(c)
		candidates = append(candidates, c)
		candidateCount++
	}
	// With the per-model index scan.scanned counts the providers advertising
	// the model (the index members), not the whole fleet, and the
	// GateNotServingModel tally is normally 0 — the relation below still holds
	// (see RoutingDecision.Scanned).
	scan.candidateSetSize = scan.scanned - int(scan.gateRejections[GateNotServingModel])

	// Prefer-with-fallback: if the caller asked to prefer their own machine and
	// at least one owned candidate can serve, choose among owned candidates
	// only; otherwise fall back to the full pool (a public provider, charged
	// normally). Exclusive self-route already filtered to owned above.
	pool := candidates
	if pr.PreferOwner {
		owned := make([]*routingCandidate, 0, len(candidates))
		for _, c := range candidates {
			if providerOwnedBy(c.provider, pr.OwnerAccountID) {
				owned = append(owned, c)
			}
		}
		if len(owned) > 0 {
			pool = owned
		}
	}

	// Version-diverse retry (SOFT): when a previous attempt failed on a given
	// binary version, prefer candidates running any OTHER version so a
	// deterministic per-version bug (e.g. a chat-template render crash) cannot
	// consume every retry on identical binaries. Diversity never fails closed:
	// when every candidate runs the avoided version, keep the full pool rather
	// than failing the request.
	if pr.Traits.AvoidVersion != "" {
		diverse := make([]*routingCandidate, 0, len(pool))
		for _, c := range pool {
			if providerVersion(c.provider) != pr.Traits.AvoidVersion {
				diverse = append(diverse, c)
			}
		}
		if len(diverse) > 0 {
			pool = diverse
		}
	}

	// Decode-floor quality preference (SOFT, Routing v2 W2): when a per-request
	// decode floor is set, prefer candidates that would still deliver
	// >= MinDecodeTPS to a newly admitted request, so the router does not overpack
	// a provider into a degraded (low tok/s) stream. Never fails closed — if no
	// candidate clears the floor, keep the full pool so the request is still
	// served (growing warm capacity / queueing to protect quality is handled
	// upstream, not by dropping the request here).
	if pr.MinDecodeTPS > 0 {
		quality := make([]*routingCandidate, 0, len(pool))
		for _, c := range pool {
			if projectedPerRequestDecodeTPS(&c.snapshot) >= pr.MinDecodeTPS {
				quality = append(quality, c)
			}
		}
		if len(quality) > 0 {
			pool = quality
		}
	}

	// Top-4 by cost over the NARROWED pool (the set the selector ranks); the
	// winner is promoted to top[0] after selection. ≤ 4 compares per candidate,
	// fixed array, no allocation.
	for _, c := range pool {
		scan.insertTop(c)
	}

	scan.pool = pool
	scan.candidateCount = candidateCount
	scan.capacityRejections = capacityRejections
	scan.tooLargeRejections = tooLargeRejections
	scan.visionRejections = visionRejections
	scan.ttftRejections = ttftRejections
	scan.bestTTFTMs = bestTTFTMs
	scan.breakerRejected = breakerRejected
	scan.ignoreProviderBreaker = ignoreProviderBreaker
	return scan
}

// selectBestCandidateScanLocked is one pass of candidate selection: it builds the
// eligible pool (scanCandidatesLocked — the single source of eligibility) and
// ranks it by cost, returning the winner plus the whole scan (rejection tallies
// and the ranked pool). When ignoreProviderBreaker is true the node-health
// breaker gate is skipped; scan.breakerRejected (providers dropped while their
// breaker was OPEN) is the signal selectBestCandidateLockedFull uses to decide
// whether a breaker-bypassed fail-open re-scan could help, and is always 0 in
// that mode.
func (r *Registry) selectBestCandidateScanLocked(model string, pr *PendingRequest, ignoreProviderBreaker bool, excludeIDs ...string) (*routingCandidate, candidateScan) {
	scan := r.scanCandidatesLocked(model, pr, ignoreProviderBreaker, excludeIDs...)
	if len(scan.pool) == 0 {
		return nil, scan
	}

	winner, runnerUp, nearTieSize, path := selectRoutingCandidate(scan.pool, func(candidate *routingCandidate) float64 {
		return candidate.costMs
	})
	scan.runnerUp = candidateSummaryOf(runnerUp)
	scan.nearTieSize = clampInt32(nearTieSize)
	scan.path = path
	scan.promoteWinnerTop(winner)
	r.logRoutingDecision(model, pr, winner, scan.candidateCount)
	return winner, scan
}

// selectRoutingCandidate centralizes cost ranking, near-tie admission, and
// queue-depth tie-breaking. Besides the winner it reports the runner-up (the
// lowest-cost candidate other than the winner — "what we would have chosen
// instead"; nil for a single-candidate pool), the size of the near-tie pool,
// and WHICH branch chose the winner (SelectionPath). The selection itself is
// byte-for-byte the pre-profiler algorithm; the extra outputs are derived from
// state the algorithm already computes and add no heap allocation.
func selectRoutingCandidate(
	pool []*routingCandidate,
	cost func(*routingCandidate) float64,
) (winner, runnerUp *routingCandidate, nearTieSize int, path SelectionPath) {
	if len(pool) == 0 {
		return nil, nil, 0, SelectionNone
	}
	best := pool[0]
	for _, candidate := range pool[1:] {
		if cost(candidate) < cost(best) {
			best = candidate
		}
	}
	nearTies := make([]*routingCandidate, 0, len(pool))
	for _, candidate := range pool {
		if math.Abs(cost(candidate)-cost(best)) <= nearTieCostWindowMs {
			nearTies = append(nearTies, candidate)
		}
	}
	winner = nearTies[0]
	for _, candidate := range nearTies[1:] {
		if candidate.effectiveQueue < winner.effectiveQueue ||
			(candidate.effectiveQueue == winner.effectiveQueue && candidate.snapshot.totalPending < winner.snapshot.totalPending) {
			winner = candidate
		}
	}
	equivalent := make([]*routingCandidate, 0, len(nearTies))
	for _, candidate := range nearTies {
		if candidate.effectiveQueue == winner.effectiveQueue &&
			candidate.snapshot.totalPending == winner.snapshot.totalPending &&
			math.Abs(cost(candidate)-cost(winner)) <= nearTieCostWindowMs {
			equivalent = append(equivalent, candidate)
		}
	}
	nearTieSize = len(nearTies)
	// Normal near-tie spreading must not erase a bounded exact-cache discount
	// (the default 1s cap is intentionally smaller than the 3s spread window).
	// Once queue/backlog equivalence is established, exact evidence resolves the
	// tie by adjusted cost. A busier holder never reaches this set, and a holder
	// whose adjusted cost is still worse loses to the lower-cost cold provider.
	hasCacheDiscount := false
	for _, candidate := range equivalent {
		if candidate.breakdown.CacheDiscountMs > 0 {
			hasCacheDiscount = true
			break
		}
	}
	switch {
	case hasCacheDiscount:
		bestCost := cost(equivalent[0])
		best := equivalent[:1]
		for _, candidate := range equivalent[1:] {
			candidateCost := cost(candidate)
			switch {
			case candidateCost < bestCost:
				bestCost = candidateCost
				best = []*routingCandidate{candidate}
			case candidateCost == bestCost:
				best = append(best, candidate)
			}
		}
		switch {
		case len(best) > 1:
			winner = best[rand.Intn(len(best))]
			path = SelectionRandom
		case len(equivalent) > 1:
			winner = best[0]
			path = SelectionCacheTiebreak
		default:
			// A single equivalent candidate that happens to carry a discount:
			// the discount did not break any tie; the queue/pending tie-break
			// (or unique minimum) did.
			winner = best[0]
			path = tieBreakPath(nearTies, winner)
		}
	case len(equivalent) > 1:
		winner = equivalent[rand.Intn(len(equivalent))]
		path = SelectionRandom
	default:
		path = tieBreakPath(nearTies, winner)
	}
	runnerUp = lowestCostOther(pool, winner, cost)
	return winner, runnerUp, nearTieSize, path
}

// tieBreakPath names the deterministic branch that produced winner from the
// near-tie pool: unique minimum, lowest effectiveQueue, or (queue tied with
// another near-tie) lowest totalPending.
func tieBreakPath(nearTies []*routingCandidate, winner *routingCandidate) SelectionPath {
	if len(nearTies) <= 1 {
		return SelectionUniqueMin
	}
	for _, c := range nearTies {
		if c != winner && c.effectiveQueue == winner.effectiveQueue {
			return SelectionTiePending
		}
	}
	return SelectionTieQueue
}

// lowestCostOther returns the lowest-cost candidate in pool other than winner
// (nil when the pool has no other candidate). One pass, no allocation.
func lowestCostOther(pool []*routingCandidate, winner *routingCandidate, cost func(*routingCandidate) float64) *routingCandidate {
	var other *routingCandidate
	for _, c := range pool {
		if c == winner {
			continue
		}
		if other == nil || cost(c) < cost(other) {
			other = c
		}
	}
	return other
}

func providerMatchesAllowedSerial(p *Provider, allowed map[string]struct{}) bool {
	if p == nil || len(allowed) == 0 {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.AttestationResult != nil {
		if _, ok := allowed[p.AttestationResult.SerialNumber]; ok && p.AttestationResult.SerialNumber != "" {
			return true
		}
	}
	if p.MDAResult != nil {
		if _, ok := allowed[p.MDAResult.DeviceSerial]; ok && p.MDAResult.DeviceSerial != "" {
			return true
		}
	}
	return false
}

// providerOwnedBy reports whether p is owned by accountID. Ownership is the
// coordinator-stamped Provider.AccountID (set at registration from the device
// auth token), never a client-supplied value — so it cannot be forged by a
// caller. An empty accountID never matches.
func providerOwnedBy(p *Provider, accountID string) bool {
	if p == nil || accountID == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.AccountID != "" && p.AccountID == accountID
}

// providerVersion reads the provider's binary version under p.mu (set by the
// API layer after registration; p.mu guards provider field access — mirrors
// providerOwnedBy). Used by the version-diverse retry pool filter.
func providerVersion(p *Provider) string {
	if p == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Version
}

// providerPassesRoutingGatesLocked is the single source of truth for the
// per-provider structural/privacy/cooldown/trait gates a request must clear
// before a provider is eligible to serve it. snapshotProviderIntoLockedEx (the
// production dispatch hot path) and QuickCapacityCheck (the preflight) BOTH call
// it so the two can never drift — a prior bug had QuickCapacityCheck silently
// missing the dispatch-load cooldown, the inference-error cooldown, and the
// trait gates, so the preflight reported capacity that routing then refused.
//
// Gates, in evaluation order:
//   - catalog membership (advertises an allowed build of the model)
//   - dispatch-load cooldown (pair instant-503'd on "insufficient memory")
//   - inference-error cooldown, SHAPE-KEYED to traits.CooldownShape() (pair
//     returning repeated provider-side 5xx for THIS request shape)
//   - capacity-reject cooldown (pair capacity-rejecting everything with ZERO
//     interleaved accepts — the black-hole signature)
//   - status not offline/untrusted
//   - private-only admission (only the owner's self-route may use it)
//   - hardware-trust floor (relaxed to TrustNone for the owner's own machine)
//   - runtime verified
//   - private-text support (E2E privacy backstop)
//   - challenge freshness
//   - trait eligibility: render-broken fences EVERY request shape; version
//     floors are trait-scoped (tools-only today)
//
// selfRouteOwner relaxes only the trust floor and private-only admission for a
// caller's own (possibly un-enrolled) machine; every privacy-critical gate
// still applies. Caller holds r.mu and p.mu.
func (r *Registry) providerPassesRoutingGatesLocked(p *Provider, model string, traits RequestTraits, selfRouteOwner bool, now time.Time) bool {
	return r.providerPassesRoutingGatesLockedEx(p, model, traits, selfRouteOwner, now, false, false)
}

// providerPassesRoutingGatesLockedEx is providerPassesRoutingGatesLocked with
// two explicit switches. ignoreProviderBreaker skips ONLY the per-provider
// node-health breaker (and health ejection); it exists solely for the
// selectBestCandidateLockedFull fail-open fallback pass, so a fleet-wide fault
// rollout that trips the breaker on every provider can never deroute the
// entire fleet. ignoreCapacityCooldown skips ONLY the capacity-reject cooldown;
// it exists solely for the "would this pair otherwise pass?" re-check that
// lets the candidate scan and the QuickCapacityCheck preflight count a
// capacity-cooled pair as a TRANSIENT capacityRejection (429/queue material)
// instead of structural absence (a "no providers" 503) — it must never be set
// on an actual routing/admission decision. Every other caller goes through the
// default wrapper above (both always honored). Caller holds r.mu and p.mu.
func (r *Registry) providerPassesRoutingGatesLockedEx(p *Provider, model string, traits RequestTraits, selfRouteOwner bool, now time.Time, ignoreProviderBreaker, ignoreCapacityCooldown bool) bool {
	ok, _ := r.providerRoutingGateReasonLockedEx(p, model, traits, selfRouteOwner, now, ignoreProviderBreaker, ignoreCapacityCooldown)
	return ok
}

// providerRoutingGateReasonLockedEx is providerPassesRoutingGatesLockedEx
// returning the FIRST failing gate as a closed GateReason (meaningful only when
// ok is false; GateReasonCount when ok). It IS the gate — the boolean form is a
// wrapper — so the verdict and the reason can never drift. Allocation-free.
// Caller holds r.mu and p.mu.
func (r *Registry) providerRoutingGateReasonLockedEx(p *Provider, model string, traits RequestTraits, selfRouteOwner bool, now time.Time, ignoreProviderBreaker, ignoreCapacityCooldown bool) (bool, GateReason) {
	// Catalog membership + dedicated-box isolation: a request for a dedicated
	// model family (e.g. Gemma 4) may ONLY route to a provider whose ENTIRE
	// advertised catalog is that family. This single gate is shared by the
	// dispatch hot path and the OpenRouter capacity preflight, so the filter
	// restricts the routing candidate set AND the shed (429) decision together
	// with no drift. A caller self-routing to its OWN machine is exempt — owners
	// may run mixed boxes.
	if ok, reason := r.providerServesRoutableModelReasonLocked(p, model, selfRouteOwner); !ok {
		return false, reason
	}
	// The identity's fault-tracker gates (gate_state.go): cached on the
	// connected provider, so the five reads are atomic loads for a provider
	// with no fault state and one short gate.mu section per tracker that has
	// state — and confirmed against p.gate afterwards (gateView), so a rebind
	// landing mid-read cannot hand the scan an emptied gate.
	if !ignoreCapacityCooldown && providerDrainingLocked(p, now) {
		return false, GateCapacityCooldown
	}
	view := r.gateViewOf(p)
	if ok, reason := r.gateStateReasonLocked(&view, model, traits, now, ignoreProviderBreaker, ignoreCapacityCooldown); !ok {
		return false, reason
	}
	// Liveness/trust/privacy core. selfRouteOwner relaxes ONLY the hardware-trust
	// floor (to TrustNone) and private-only admission for a caller's own
	// (possibly un-enrolled) machine; every privacy-critical gate still applies.
	minTrust := r.MinTrustLevel
	if selfRouteOwner {
		minTrust = TrustNone
	}
	if ok, reason := r.providerLivenessGateReasonLocked(p, minTrust, selfRouteOwner, now); !ok {
		return false, reason
	}
	// Trait eligibility: a render-broken build is fenced for EVERY request shape
	// (a crashing chat template breaks plain text, tools, and multimodal alike),
	// while the capability version floors stay trait-scoped (tools-only today).
	if !r.providerEligibleForTraitsLocked(p, model, traits) {
		return false, GateTraitFloor
	}
	return true, GateReasonCount
}

// gateStateReasonLocked evaluates the five fault-tracker gates for the session
// behind view against its identity's gate and returns the first closed one
// (GateReasonCount when all pass), in the documented gate precedence. The
// verdict is confirmed against p.gate (gateView.moved) and re-read from the
// session's new gate when a rebind landed between the view's load and the
// reads — the scan, the commit's admit re-check and the preflight all come
// through here, so none of them can dispatch a session past a breaker or
// cooldown that moved with it. Caller holds p.mu (for the identity read).
func (r *Registry) gateStateReasonLocked(view *gateView, model string, traits RequestTraits, now time.Time, ignoreProviderBreaker, ignoreCapacityCooldown bool) (bool, GateReason) {
	nowNS := now.UnixNano()
	for {
		g := view.g
		reason := GateReasonCount
		switch {
		// Skip a provider-model pair cooling down after a dispatch-time load
		// failure ("insufficient memory") — it would instant-503 again, burning a
		// dispatch attempt.
		case g.dispatchLoadCooled(model, now):
			reason = GateDispatchLoadCooldown
		// Skip a triple quarantined by the inference-error circuit breaker for THIS
		// request shape: repeated provider-side (5xx) failures — e.g. a deterministic
		// chat-template render crash on tool schemas — mean a retry here fails
		// identically, so routing must fall to a different provider. Shape-keyed so a
		// tool failure does not deroute clean text traffic. Cleared by
		// RecordInferenceSuccess (same shape) or by TTL expiry.
		case g.inferenceErrorCooled(model, traits.CooldownShape(), now):
			reason = GateErrorCooldown
		// Skip a (provider, model) pair quarantined by the capacity-reject cooldown:
		// it kept capacity-rejecting with ZERO interleaved accepts (the black-hole
		// signature — e.g. a box whose engine misreports its token budget), so a
		// dispatch here is a guaranteed bounce while its idle-looking heartbeats
		// keep winning the cost scheduler. A busy box that is also SERVING never
		// trips this (any accept resets the streak), and the pair is re-probed once
		// its TTL expires. See capacity_cooldown.go.
		case !ignoreCapacityCooldown && g.capacityCooled(model, now):
			reason = GateCapacityCooldown
		// Skip a provider quarantined by the per-provider node-health breaker: a
		// node returning GENUINE-FAULT errors (500/502/504 or a
		// fault-shaped 503) for ~all of its requests is sick regardless of model or
		// shape, so it is derouted fleet-wide. This catches the node that fault-503s
		// every request — invisible to the shape-keyed inference-error breaker above
		// (which skips 503 as a capacity signal). Honored on the normal routing
		// path; the selectBestCandidateLockedFull fail-open pass sets
		// ignoreProviderBreaker so a bad fleet-wide rollout can't deroute everyone.
		case !ignoreProviderBreaker && g.breakerOpenAt(nowNS):
			reason = GateBreaker
		// Skip a provider EJECTED by the stable-identity health breaker (health_ejection.go):
		// a node whose serial/SE-key/account has collapsed to a near-total served-fault
		// rate is derouted even across reconnects (the session breaker above is wiped on
		// every disconnect, which the constantly-disconnecting zombies exploit). Same
		// fail-open contract: skipped on the ignoreProviderBreaker rescan, and an
		// un-attestable provider (empty stable id) is never ejected.
		case !ignoreProviderBreaker && healthEjectionEnabled() && r.ejectionOpenFor(g, stableProviderIdentityLocked(view.p), nowNS):
			reason = GateEjection
		}
		if !view.moved() {
			return reason == GateReasonCount, reason
		}
	}
}

func providerModelIDs(p *Provider) []string {
	if p == nil {
		return nil
	}
	// p.Models is replaced (copy-on-write) by UpdateModelWeightHashes when a
	// challenge response carries refreshed weight hashes, so the slice header
	// must be read under p.mu. All callers invoke this helper after releasing
	// p.mu (verified: Heartbeat, RecordChallengeSuccess, SetProviderIdle,
	// DrainQueuedRequestsForProvider), so taking the lock here cannot deadlock.
	p.mu.Lock()
	defer p.mu.Unlock()
	ids := make([]string, 0, len(p.Models))
	for _, m := range p.Models {
		ids = append(ids, m.ID)
	}
	return ids
}

// providerCanAdmitLockedEx is providerCanAdmitLocked with an explicit
// ignoreProviderBreaker switch. ReserveProviderEx sets it true ONLY when the
// selected winner is itself node-health-breaker-open — which can happen only
// because the selectBestCandidateLockedFull fail-open fallback pass chose it.
// Without this, the admit re-check would re-apply the breaker and reject the
// very candidate the fail-open valve just selected, derouting the fleet anyway.
// The default wrapper (breaker honored) is unchanged for every other caller.
// Caller holds r.mu and p.mu.
func (r *Registry) providerCanAdmitLockedEx(p *Provider, model string, traits RequestTraits, selfRouteOwner bool, ignoreProviderBreaker bool, now time.Time) bool {
	if !r.providerPassesRoutingGatesLockedEx(p, model, traits, selfRouteOwner, now, ignoreProviderBreaker, false) {
		return false
	}
	// Apply the SAME quality-concurrency cap as the selection snapshot and the
	// preflight. This is the final admit re-check in ReserveProviderEx; if a
	// heartbeat bumped NumRunning after the snapshot was built, the legacy flat-cap
	// check here would let a box that just reached its quality cap be over-admitted.
	if !r.hasConcurrencyHeadroomForModelCapResolvedLocked(p, model) {
		return false
	}
	if p.BackendCapacity != nil {
		for _, slot := range p.BackendCapacity.Slots {
			if slot.Model != model {
				continue
			}
			switch slot.State {
			case "crashed", "reloading":
				return false
			}
			break
		}
	}
	return true
}
