package registry

// Quick capacity checks and queued-request draining.

import (
	"time"
)

// QuickCapacityCheck performs a fast, read-only scan of the provider fleet to
// determine whether any provider could serve a request for the given model
// right now. It runs the SAME per-provider gates as the full routing path —
// via the shared providerPassesRoutingGatesLocked (status, trust, runtime,
// privacy, challenge freshness, dispatch-load + shape-keyed inference-error
// cooldowns, and the trait gates: render-broken fences every shape, the tools
// version floor fences tool requests) — plus the capacity gates (concurrency
// headroom, slot state, free memory) but does NOT reserve capacity or create
// pending requests. traits carry the request shape so the preflight excludes a
// provider for exactly the reasons routing would, instead of reporting phantom
// capacity that routing then refuses (the drift this consolidation closes).
//
// Returns:
//   - candidateCount: providers that passed ALL gates (could route right now)
//   - capacityRejections: providers that serve the model and passed structural
//     gates but were rejected for capacity reasons (full concurrency, no free
//     memory, etc.)
//
// This is used for the pre-flight 429 check: if candidateCount == 0 &&
// capacityRejections > 0, providers exist but are all at capacity (429).
// If candidateCount == 0 && capacityRejections == 0, no provider serves
// the model at all (404/503).
//
//   - modelTooLarge: providers that serve the model but whose memory can never
//     fit it. Kept separate from capacityRejections so the caller does NOT 429
//     a model that will never fit (the client would retry forever) — it should
//     surface model_too_large / 503 instead.
func (r *Registry) QuickCapacityCheck(model string, estimatedPromptTokens, requestedMaxTokens int, traits RequestTraits, allowedSerials ...string) (candidateCount, capacityRejections, modelTooLarge int) {
	candidateCount, capacityRejections, modelTooLarge, _, _ = r.quickCapacityCheck(model, estimatedPromptTokens, requestedMaxTokens, traits, false, allowedSerials...)
	return candidateCount, capacityRejections, modelTooLarge
}

func (r *Registry) QuickCapacityCheckForRequest(model string, estimatedPromptTokens, requestedMaxTokens int, traits RequestTraits, requiresVision bool, allowedSerials ...string) (candidateCount, capacityRejections, modelTooLarge int) {
	candidateCount, capacityRejections, modelTooLarge, _, _ = r.quickCapacityCheck(model, estimatedPromptTokens, requestedMaxTokens, traits, requiresVision, allowedSerials...)
	return candidateCount, capacityRejections, modelTooLarge
}

func (r *Registry) QuickCapacityCheckWithTTFTForRequest(model string, estimatedPromptTokens, requestedMaxTokens int, traits RequestTraits, requiresVision bool, allowedSerials ...string) (candidateCount, capacityRejections, modelTooLarge int, bestTTFT time.Duration, hasTTFT bool) {
	return r.quickCapacityCheck(model, estimatedPromptTokens, requestedMaxTokens, traits, requiresVision, allowedSerials...)
}

func (r *Registry) quickCapacityCheck(model string, estimatedPromptTokens, requestedMaxTokens int, traits RequestTraits, requiresVision bool, allowedSerials ...string) (candidateCount, capacityRejections, modelTooLarge int, bestTTFT time.Duration, hasTTFT bool) {
	// Use a dummy PendingRequest with the caller's actual token estimates
	// for the admission gate (freeMemoryAdmits).
	if estimatedPromptTokens <= 0 {
		estimatedPromptTokens = 500
	}
	if requestedMaxTokens <= 0 {
		requestedMaxTokens = defaultRequestedMaxTokens
	}
	dummyPR := &PendingRequest{
		RequestID:             "capacity-check",
		Model:                 model,
		EstimatedPromptTokens: estimatedPromptTokens,
		RequestedMaxTokens:    requestedMaxTokens,
	}

	// Build allowed serial set for optional provider filtering.
	allowedSet := make(map[string]struct{}, len(allowedSerials))
	for _, s := range allowedSerials {
		allowedSet[s] = struct{}{}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	unknownTTFTCandidate := false
	now := time.Now()
	// Per-model index: visit only providers advertising the model (gates
	// unchanged; see model_index.go).
	for _, p := range r.providersForModelLocked(model) {
		// Filter by allowed serials before acquiring the provider lock
		// (providerMatchesAllowedSerial takes p.mu internally).
		if len(allowedSet) > 0 && !providerMatchesAllowedSerial(p, allowedSet) {
			continue
		}

		p.mu.Lock()

		// Per-provider routing gates (same source of truth as snapshotProviderIntoLockedEx
		// and the admit re-check). This pre-flight only runs for public
		// (non-self-route) requests, so selfRouteOwner is false — private-only
		// machines are excluded unconditionally.
		//
		// ignoreProviderBreaker=true: the per-provider node-health
		// breaker is a SELECTION-time gate that fails open in the dispatch path
		// (selectBestCandidateScanLocked / ReserveProviderEx). The preflight must
		// fail open on it too — otherwise an all-breaker-open fleet reports 0
		// candidates AND 0 capacity-rejections here, and the consumer hard-503s
		// "no_provider" BEFORE dispatch's fail-open valve can serve a probe,
		// re-introducing the very model-wide outage the valve exists to prevent.
		// Every other gate (incl. the shape-keyed inference-error cooldown) is
		// still honored; the breaker still steers SELECTION away from bad nodes.
		if !r.providerPassesRoutingGatesLockedEx(p, model, traits, false, now, true, false) {
			// A pair blocked ONLY by the capacity-reject cooldown is TRANSIENT
			// capacity, not structural absence: the box exists, serves the model,
			// and will be re-probed when its TTL lapses. Count it as a
			// capacityRejection so an all-cooled model surfaces to the consumer
			// as capacity (429 + Retry-After / queue-before-shed) instead of a
			// "no providers" 503 — the cooldown must read as "busy fleet", never
			// as "the model vanished". The ignoreCapacityCooldown re-check keeps
			// a pair that ALSO fails a structural gate (offline, untrusted,
			// render-broken, …) out of the count. Structural filters applied
			// AFTER the gates on the main path must apply here too:
			// thermal-critical and vision-blind pairs are excluded outright
			// (same as the main path just below), and a pair whose model can
			// never fit the hardware counts as modelTooLarge — never as
			// transient capacity, or a fleet of undersized cooled boxes would
			// read as "busy, retry" for a model that will never fit.
			if (r.gateOf(p).capacityCooled(model, now) || providerDrainingLocked(p, now)) &&
				r.providerPassesRoutingGatesLockedEx(p, model, traits, false, now, true, true) &&
				p.SystemMetrics.ThermalState != "critical" &&
				(!requiresVision || r.providerServesVisionModelLocked(p, model, false)) {
				// Mirror the absolute hardware-fit gate (skipped for a
				// resident model, which has demonstrably fit).
				slotState := "unknown"
				totalMemGB := float64(p.Hardware.MemoryGB)
				if p.BackendCapacity != nil {
					if p.BackendCapacity.TotalMemoryGB > 0 {
						totalMemGB = p.BackendCapacity.TotalMemoryGB
					}
					for _, slot := range p.BackendCapacity.Slots {
						if slot.Model == model {
							slotState = slot.State
							break
						}
					}
				}
				if !slotStateModelLoaded(slotState) &&
					!modelFitsHardware(r.catalogMinRAMGbLocked(model), r.catalogSizeGBLocked(model), totalMemGB) {
					modelTooLarge++
				} else {
					capacityRejections++
				}
			}
			p.mu.Unlock()
			continue
		}
		if p.SystemMetrics.ThermalState == "critical" {
			p.mu.Unlock()
			continue
		}
		if requiresVision && !r.providerServesVisionModelLocked(p, model, false) {
			p.mu.Unlock()
			continue
		}

		// Concurrency gate (with the quality-concurrency cap, same as the dispatch
		// snapshot — resolves the model's own static solo rate internally so
		// routing and the shed preflight stay consistent and a slow model's
		// quality cap counts a saturated box as a capacity rejection here too).
		if !r.hasConcurrencyHeadroomForModelCapResolvedLocked(p, model) {
			p.mu.Unlock()
			capacityRejections++
			continue
		}

		// Build a snapshot for the admission gate (slot state + free memory).
		snap := routingSnapshot{
			provider:           p,
			model:              model,
			chipFamily:         p.Hardware.ChipFamily,
			binaryVersion:      p.Version,
			slotState:          "unknown",
			totalPending:       p.pendingCount(),
			systemMetrics:      p.SystemMetrics,
			decodeTPS:          resolvedDecodeTPS(p),
			prefillTPS:         resolvedPrefillTPS(p),
			totalMemoryGB:      float64(p.Hardware.MemoryGB),
			modelSizeGB:        r.modelSizeGBForFitLocked(p, model),
			minRAMGb:           r.catalogMinRAMGbLocked(model),
			hasBackendCapacity: p.BackendCapacity != nil,
		}
		fillSnapshotPendingAndPool(&snap, p, model)
		if snap.hasBackendCapacity {
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
				snap.observedDecodeTPS = slot.ObservedDecodeTPS
				snap.observedPrefillTPS = slot.ObservedPrefillTPS
				snap.activeTokenBudgetUsed = slot.ActiveTokenBudgetUsed
				snap.activeTokenBudgetMax = slot.ActiveTokenBudgetMax
				snap.queuedTokenBudget = slot.QueuedTokenBudget
				snap.maxTokensPotential = slot.MaxTokensPotential
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

		// Gray-box budget clamp — same evaluation as snapshotProviderLockedEx
		// (including the budgetless-snapshot hold for reconnecting sessions)
		// so the preflight cannot report capacity that routing then refuses.
		rawRemaining := snap.activeTokenBudgetMax - snap.activeTokenBudgetUsed - snap.queuedTokenBudget
		snap.budgetClamped = r.budgetClampedFor(p, model, p.LastHeartbeat, rawRemaining, snap.activeTokenBudgetMax > 0, now)

		p.mu.Unlock()

		// Absolute hardware-fit gate (mirrors buildCandidateWithReason). A model
		// that can never fit this node is a permanent miss, not transient
		// capacity pressure — count it separately so the caller never 429s it.
		// Skipped for a resident ("running"/"idle") model, which has demonstrably
		// fit.
		if !slotStateModelLoaded(snap.slotState) && !modelFitsHardware(snap.minRAMGb, snap.modelSizeGB, snap.totalMemoryGB) {
			modelTooLarge++
			continue
		}

		// Slot state gate (crashed/reloading are ineligible).
		if _, eligible := slotStatePenalty(snap.slotState); !eligible {
			continue
		}

		// Free memory / token budget admission gate.
		if !freeMemoryAdmits(&snap, dummyPR.EstimatedPromptTokens, dummyPR.RequestedMaxTokens) {
			capacityRejections++
			continue
		}

		candidateCount++
		if snap.hasBackendCapacity {
			ttft := estimatedTTFTFromSnapshot(&snap, estimatedPromptTokens)
			if !hasTTFT || ttft < bestTTFT {
				bestTTFT = ttft
				hasTTFT = true
			}
		} else {
			unknownTTFTCandidate = true
		}
	}
	if unknownTTFTCandidate {
		return candidateCount, capacityRejections, modelTooLarge, 0, false
	}
	return candidateCount, capacityRejections, modelTooLarge, bestTTFT, hasTTFT
}

// DrainQueuedRequestsForModel attempts to assign queued requests for a
// single model to available providers. Called when a load_model completes
// so requests don't have to wait for the next heartbeat cycle.
func (r *Registry) DrainQueuedRequestsForModel(model string) {
	r.DrainQueuedRequestsForModelWithReason(model, DrainTriggerUnknown)
}

// DrainQueuedRequestsForModelWithReason is DrainQueuedRequestsForModel with
// the bounded drain trigger (DrainTrigger* constants) the api layer knows at
// its call site — DrainTriggerLoad for a load_model success, for example — so
// the queued request's routing record names what unblocked it. Unknown values
// fold to DrainTriggerUnknown.
func (r *Registry) DrainQueuedRequestsForModelWithReason(model, reason string) {
	r.drainQueuedRequestsForModelsWithReason([]string{model}, reason)
}

// DrainQueuedRequestsForProvider attempts to assign queued requests for every
// model a provider serves. Called when a provider becomes newly eligible for
// routing (e.g. it just passed APNs code-identity attestation) so queued
// demand is satisfied immediately instead of waiting for the next heartbeat.
func (r *Registry) DrainQueuedRequestsForProvider(p *Provider) {
	r.DrainQueuedRequestsForProviderWithReason(p, DrainTriggerUnknown)
}

// DrainQueuedRequestsForProviderWithReason is DrainQueuedRequestsForProvider
// with the bounded drain trigger the api layer knows at its call site (e.g.
// DrainTriggerChallenge after an attestation pass). Unknown values fold to
// DrainTriggerUnknown.
func (r *Registry) DrainQueuedRequestsForProviderWithReason(p *Provider, reason string) {
	if p == nil {
		return
	}
	r.drainQueuedRequestsForModelsWithReason(providerModelIDs(p), reason)
}

// drainQueuedRequestsForModels is the legacy entry point (reason "unknown");
// callers should migrate to drainQueuedRequestsForModelsWithReason so the
// queued request's routing record names what unblocked it.
func (r *Registry) drainQueuedRequestsForModels(models []string) {
	r.drainQueuedRequestsForModelsWithReason(models, DrainTriggerUnknown)
}

// drainQueuedRequestsForModelsWithReason drains the per-model queues for
// models, stamping the bounded drain trigger (see DrainTrigger* constants) on
// every QueuedRequest whose routing decision this drain records, together with
// the request's enqueue position/depth, so the api layer can persist why and
// from where a queued request was dispatched.
func (r *Registry) drainQueuedRequestsForModelsWithReason(models []string, reason string) {
	reason = foldDrainTrigger(reason)
	queue := r.Queue()
	if queue == nil || len(models) == 0 {
		return
	}
	for _, model := range models {
		r.drainModelQueue(queue, model, reason)
	}
}

// drainModelQueue runs the drain pass for one model under the per-model claim
// (queue_drain_coalesce.go): a trigger that finds a pass in flight hands its
// reason to that pass and returns, and the pass reruns once for it after
// requeueing. A pass that does not complete releases the claim on the way out
// so a recovered panic cannot leave the model undrainable.
func (r *Registry) drainModelQueue(queue *RequestQueue, model, reason string) {
	if !r.drainPasses.begin(model, reason) {
		return
	}
	released := false
	defer func() {
		if !released {
			r.drainPasses.abandon(model)
		}
	}()
	for {
		r.drainModelQueuePass(queue, model, reason)
		next, again := r.drainPasses.end(model)
		if !again {
			released = true
			return
		}
		reason = next
	}
}

// drainModelQueuePass pops every fresh queued request for model once and
// either assigns it, fails it deterministically, or requeues it in order.
// Fleet state is read live per scan; verdicts are reused within the pass only
// through the dominance skip, whose records this pass owns.
func (r *Registry) drainModelQueuePass(queue *RequestQueue, model, reason string) {
	var skipped []*QueuedRequest
	// rejected anchors the per-pass dominance skip (queue_drain_dominance.go)
	// and deliberately survives requeueSkipped: an admission only removes
	// capacity, so this pass's verdicts stay valid for the requeued waiters
	// the next PopNextFresh hands back.
	var rejected []drainRejectionRecord
	admitted := 0
	saturated := false
	requeueSkipped := func() {
		for i := len(skipped) - 1; i >= 0; i-- {
			queue.RequeueFront(skipped[i])
		}
		skipped = nil
	}
	for {
		if r.drainBeforePop != nil {
			r.drainBeforePop(model)
		}
		req := queue.PopNextFresh(model)
		if req == nil {
			requeueSkipped()
			break
		}
		if req.Pending == nil {
			req.Pending = &PendingRequest{
				RequestID:          req.RequestID,
				Model:              model,
				RequestedMaxTokens: defaultRequestedMaxTokens,
			}
		}
		// Queue time spends the same absolute first-content clock as
		// parsing, admission, and provider dispatch. Refresh immediately
		// before reservation so hard TTFT admission never reuses the
		// enqueue-time ceiling.
		if !req.Pending.RefreshFirstContentBudget(time.Now()) {
			req.failWithReason(ErrQueueFirstContentDeadline)
			continue
		}
		// A waiter at least as demanding as one this pass already rejected
		// purely on capacity/TTFT gets the same verdict from the same fleet
		// state; requeue it without paying for another full fleet scan.
		if drainDominated(req.Pending, rejected) {
			saturated = true
			skipped = append(skipped, req)
			continue
		}
		provider, decision := r.ReserveProviderEx(model, req.Pending)
		// Queue context for the routing record: where the request sat at
		// enqueue and which event ran the drain that produced this decision.
		decision.QueuePosition = req.EnqueuePosition
		decision.QueueDepth = req.DepthAtEnqueue
		decision.DrainTrigger = reason
		if provider == nil {
			if req.Pending.Traits.RequiresToolConstraint &&
				!r.hasToolConstraintProviderForPending(model, req.Pending) {
				req.DrainTrigger = reason
				req.Decision = decision
				req.failWithReason(ErrQueueToolConstraintUnavailable)
				continue
			}
			// A pure-TTFT rejection (hard-reject mode, no capacity-rejected
			// provider that could free up) is deterministic for this pass:
			// requeueing would only make the waiter hang until maxWait for
			// the same answer. Fail it now; the API waiter turns
			// ErrQueueTTFTTooSlow into the standard ttft_too_slow 429 using
			// the decision's BestTTFTMs for Retry-After.
			if drainRejectionTTFTTerminal(req.Pending, decision) {
				req.DrainTrigger = reason
				req.Decision = decision
				req.failWithReason(ErrQueueTTFTTooSlow)
				continue
			}
			if rec, ok := drainRejectionRecordFor(req.Pending, decision); ok {
				rejected = append(rejected, rec)
			}
			saturated = saturated || drainPureCapacityRejection(decision)
			skipped = append(skipped, req)
			continue
		}
		admitted++
		req.DrainTrigger = reason
		req.Decision = decision
		requeueSkipped()

		releaseReservation := func() {
			provider.RemovePending(req.Pending.RequestID)
			r.SetProviderIdle(provider.ID)
		}
		if !req.offerAssignment(provider, releaseReservation) {
			releaseReservation()
			continue
		}
		if req.beforeAssignmentSend != nil {
			req.beforeAssignmentSend()
		}
		select {
		case req.ResponseCh <- provider:
			// The reservation remains scheduler-owned until the waiter
			// acknowledges it in WaitForProviderContext. Cancellation after
			// this buffered send rejects the published assignment and runs
			// releaseReservation exactly once.
		case <-req.Done():
			req.rejectAssignment()
			continue
		}
	}
	// Heartbeat-triggered passes are suppressed for a short window after
	// a saturated pass (queue_drain_suppress.go); an admission proves
	// capacity moved and lifts the mark.
	switch {
	case admitted > 0:
		r.drainSuppress.clear(model)
	case saturated:
		r.drainSuppress.markSaturated(model)
	}
}
