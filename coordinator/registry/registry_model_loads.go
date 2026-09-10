package registry

// Model-load commands, warm-pool planning, and pending loads.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
)

// SendLoadModel instructs a provider to eagerly load a model so it becomes
// warm for incoming requests. The provider will autonomously evict idle
// models to make room. This is a fire-and-forget call — the coordinator
// does not block waiting for the load to complete. The provider replies
// asynchronously with a load_model_status message.
func (r *Registry) SendLoadModel(providerID, modelID string) error {
	r.mu.RLock()
	p, ok := r.providers[providerID]
	if !ok {
		r.mu.RUnlock()
		return fmt.Errorf("provider %q not found", providerID)
	}
	p.mu.Lock()
	eligible := r.providerServesCatalogModelLocked(p, modelID)
	p.mu.Unlock()
	r.mu.RUnlock()
	if !eligible {
		return fmt.Errorf(
			"provider %q does not satisfy requirements for model %q", providerID, modelID)
	}
	if r.loadModelSender != nil {
		if err := r.loadModelSender(providerID, modelID); err != nil {
			return err
		}
		r.logger.Info("sent load_model to provider", "provider_id", providerID, "model_id", modelID)
		return nil
	}

	msg := protocol.LoadModelMessage{
		Type:    protocol.TypeLoadModel,
		ModelID: modelID,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal load_model message: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), providerControlWriteTimeout)
	defer cancel()
	if err := p.WriteText(ctx, data); err != nil {
		return fmt.Errorf("failed to send load_model to provider %q: %w", providerID, err)
	}

	r.logger.Info("sent load_model to provider",
		"provider_id", providerID,
		"model_id", modelID,
	)
	return nil
}

// SendPrefetchModel instructs a provider to download + verify a model build
// in the background without loading it into GPU memory. It mirrors
// SendLoadModel but carries no expectation that the model becomes warm; the
// provider replies asynchronously with prefetch_model_status messages and
// re-advertises the build once it is verified on disk. It is the download-only
// primitive a provider's declarative reconciler uses internally to pre-stage a
// desired build before the hard-swap; the coordinator no longer drives a
// weighted migration with it.
func (r *Registry) SendPrefetchModel(providerID, modelID string, priority int) error {
	r.mu.RLock()
	p, ok := r.providers[providerID]
	if !ok {
		r.mu.RUnlock()
		return fmt.Errorf("provider %q not found", providerID)
	}
	p.mu.Lock()
	eligible := r.providerCanAcquireCatalogModelLocked(p, modelID)
	p.mu.Unlock()
	r.mu.RUnlock()
	if !eligible {
		return fmt.Errorf(
			"provider %q does not satisfy requirements for model %q", providerID, modelID)
	}
	if r.prefetchModelSender != nil {
		return r.prefetchModelSender(providerID, modelID, priority)
	}

	msg := protocol.PrefetchModelMessage{
		Type:     protocol.TypePrefetchModel,
		ModelID:  modelID,
		Priority: priority,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal prefetch_model message: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), providerControlWriteTimeout)
	defer cancel()
	if err := p.WriteText(ctx, data); err != nil {
		return fmt.Errorf("failed to send prefetch_model to provider %q: %w", providerID, err)
	}

	r.logger.Info("sent prefetch_model to provider",
		"provider_id", providerID,
		"model_id", modelID,
		"priority", priority,
	)
	return nil
}

// SendDesiredModels tells a provider, declaratively, the desired build per
// public alias it should converge to (plus the still-acceptable previous build).
// The provider reconciles on its own: background-prefetch any missing desired
// build, then hard-swap (advertise new, drop old) once verified. Mirrors
// SendPrefetchModel — fire-and-forget over the provider's WebSocket.
//
// An EMPTY entries set is still sent ("nothing is desired"): the provider's
// reconcile treats any build it was previously converging to but that is absent
// from the latest set as stale, so an alias delete/repoint that leaves a
// provider with no remaining entries MUST reach it — otherwise an in-flight
// prefetch for the removed alias would complete and hard-swap anyway. Callers
// MUST gate this on backend == mlx-swift AND a provider version that
// understands desired_models, because a pre-feature provider's strict decoder
// throws on unknown message types.
func (r *Registry) SendDesiredModels(providerID string, entries []protocol.DesiredModelEntry) error {
	originallyNonEmpty := len(entries) > 0
	r.mu.RLock()
	p, ok := r.providers[providerID]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("provider %q not found", providerID)
	}

	// Serialize compute → wire write → last-snapshot update per connection.
	// Registry/provider locks are released before I/O; sendMu preserves order so
	// an untrust revoke always lands after any already-started nonempty frame.
	p.desiredModelsSendMu.Lock()
	defer p.desiredModelsSendMu.Unlock()

	r.mu.RLock()
	current, ok := r.providers[providerID]
	if !ok || current != p {
		r.mu.RUnlock()
		return fmt.Errorf("provider %q not found", providerID)
	}
	p.mu.Lock()
	forceEmpty := p.Status == StatusOffline || p.Status == StatusUntrusted
	eligibleEntries := make([]protocol.DesiredModelEntry, 0, len(entries))
	if !forceEmpty {
		for _, entry := range entries {
			if entry.DesiredBuild == "" ||
				!r.providerCanAcquireCatalogModelLocked(p, entry.DesiredBuild) {
				continue
			}
			if entry.PreviousBuild != "" &&
				!r.providerCanAcquireCatalogModelLocked(p, entry.PreviousBuild) {
				entry.PreviousBuild = ""
			}
			eligibleEntries = append(eligibleEntries, entry)
		}
	}
	if !forceEmpty && originallyNonEmpty && len(eligibleEntries) == 0 {
		p.mu.Unlock()
		r.mu.RUnlock()
		return fmt.Errorf(
			"provider %q does not satisfy desired model requirements", providerID)
	}
	entries = eligibleEntries
	if entries == nil {
		entries = []protocol.DesiredModelEntry{}
	}
	if p.desiredModelsSent && desiredModelEntriesEqual(p.lastDesiredModels, entries) {
		p.mu.Unlock()
		r.mu.RUnlock()
		return nil
	}
	p.mu.Unlock()
	r.mu.RUnlock()

	if r.desiredModelsSender != nil {
		if err := r.desiredModelsSender(providerID, entries); err != nil {
			return err
		}
		recordDesiredModelsSent(p, entries)
		return nil
	}

	msg := protocol.DesiredModelsMessage{
		Type:   protocol.TypeDesiredModels,
		Models: entries,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal desired_models message: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), providerControlWriteTimeout)
	defer cancel()
	if err := p.WriteText(ctx, data); err != nil {
		return fmt.Errorf("failed to send desired_models to provider %q: %w", providerID, err)
	}
	recordDesiredModelsSent(p, entries)
	r.logger.Info("sent desired_models to provider",
		"provider_id", providerID,
		"entries", len(entries),
	)
	return nil
}

func desiredModelEntriesEqual(left, right []protocol.DesiredModelEntry) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func recordDesiredModelsSent(p *Provider, entries []protocol.DesiredModelEntry) {
	p.mu.Lock()
	p.desiredModelsSent = true
	p.lastDesiredModels = append([]protocol.DesiredModelEntry(nil), entries...)
	p.mu.Unlock()
}

// DesiredModelsForProvider builds the desired_models entries to push to a
// provider. Policy (conservative for this release): emit an entry only for
// aliases where the provider ALREADY advertises the desired OR previous build —
// i.e. the provider is already part of this alias's fleet and should converge to
// the desired build. Aliases the provider has never served are not offered (a
// brand-new provider must advertise some member of an alias to be told its
// desired build). An alias with an empty desired build is skipped.
func (r *Registry) DesiredModelsForProvider(providerID string) []protocol.DesiredModelEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.providers[providerID]
	if !ok || len(r.modelAliases) == 0 {
		return nil
	}
	p.mu.Lock()
	if p.Status == StatusOffline || p.Status == StatusUntrusted {
		p.mu.Unlock()
		return []protocol.DesiredModelEntry{}
	}
	advertised := make(map[string]struct{}, len(p.Models))
	for _, m := range p.Models {
		if m.ID != "" {
			advertised[m.ID] = struct{}{}
		}
	}

	var entries []protocol.DesiredModelEntry
	for alias, t := range r.modelAliases {
		if t.OpenRouterOnly || t.Desired == "" {
			continue
		}
		if !r.providerCanAcquireCatalogModelLocked(p, t.Desired) {
			continue
		}

		_, hasDesired := advertised[t.Desired]
		_, hasPrevious := advertised[t.Previous]
		// A provider advertising only a RETIRED member (offline through a
		// retirement, e.g. previous_build cleared at the end of a rollout) is
		// still part of this alias's fleet — without this it would never learn
		// the desired build and serve zero alias traffic until manual action.
		hasRetired := false
		if !hasDesired && !hasPrevious {
			for _, b := range t.Retired {
				if _, ok := advertised[b]; ok {
					hasRetired = true
					break
				}
			}
		}
		if !hasDesired && !(t.Previous != "" && hasPrevious) && !hasRetired {
			continue
		}
		previous := t.Previous
		if previous != "" && !r.providerCanAcquireCatalogModelLocked(p, previous) {
			previous = ""
		}
		entries = append(entries, protocol.DesiredModelEntry{
			ModelName:     alias,
			DesiredBuild:  t.Desired,
			PreviousBuild: previous,
		})
	}
	p.mu.Unlock()
	// Stable ordering keeps the wire output deterministic (and tests simple).
	sort.Slice(entries, func(i, j int) bool { return entries[i].ModelName < entries[j].ModelName })
	return entries
}

// TriggerModelSwaps checks for queued requests that have no warm provider
// and sends load_model to cold providers that have the model available on
// disk. This enables demand-driven model swapping: when requests queue for
// a model that no provider has warm, the coordinator proactively triggers
// a swap on an idle provider.
//
// Called after heartbeat processing and queue drain to catch demand that
// can't be satisfied by warm providers alone.
func (r *Registry) TriggerModelSwaps() {
	queue := r.Queue()
	if queue == nil {
		return
	}

	queuedModels := queue.QueuedModels()
	if len(queuedModels) == 0 {
		return
	}

	now := time.Now()
	r.expirePendingModelLoads(now)

	actions := r.planModelLoadActions(queuedModels, now)
	actions = r.reservePendingModelLoads(actions, now)
	r.sendModelLoadActions(actions)
}

func (r *Registry) expirePendingModelLoads(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, expiresAt := range r.pendingModelLoads {
		if now.After(expiresAt) {
			delete(r.pendingModelLoads, key)
			delete(r.pendingModelLoadStarted, key)
		}
	}
}

func (r *Registry) planModelLoadActions(queuedModels []string, now time.Time) []modelLoadAction {
	r.mu.RLock()
	defer r.mu.RUnlock()

	selectedProviders := make(map[string]struct{})
	actions := make([]modelLoadAction, 0, len(queuedModels))
	for _, model := range queuedModels {
		if r.hasWarmProviderLocked(model, now) {
			continue
		}

		providerID := r.bestModelLoadProviderLocked(model, now, selectedProviders)
		if providerID == "" {
			continue
		}
		selectedProviders[providerID] = struct{}{}
		actions = append(actions, modelLoadAction{providerID: providerID, modelID: model})
	}
	return actions
}

// hasWarmProviderLocked reports whether a connected provider already has the
// model warm. Caller must hold r.mu (read or write).
func (r *Registry) hasWarmProviderLocked(model string, now time.Time) bool {
	// Only advertisers can hold the model warm (warm/slot reports are
	// canonicalized against p.Models; providerHasWarmModelLocked also requires
	// providerServesRoutableModelLocked), so the per-model index prunes the
	// walk losslessly (model_index.go).
	for _, p := range r.providersForModelLocked(model) {
		p.mu.Lock()
		warm := r.providerHasWarmModelLocked(p, model, now)
		p.mu.Unlock()
		if warm {
			return true
		}
	}
	return false
}

// providerHasWarmModelLocked checks whether the provider has the model warm
// AND passes the same routing safety gates used by the scheduler. A provider
// with stale attestation or failed privacy checks should not suppress swap
// planning. Caller must hold p.mu. Caller must hold r.mu (read or write).
func (r *Registry) providerHasWarmModelLocked(p *Provider, model string, now time.Time) bool {
	// Liveness/trust/privacy core, with NO owner relaxation: private-only
	// providers serve only their owner's self-route traffic, never the public
	// fleet, and must not suppress public swap planning — otherwise a
	// private-only machine that happens to hold a queued public model warm makes
	// the planner believe the model is already served and skip load_model to an
	// eligible public node, stranding public requests until queue timeout.
	if !r.providerLivenessGateLocked(p, r.MinTrustLevel, false, now) {
		return false
	}
	// Catalog membership + dedicated-box isolation: for a dedicated-family model
	// (e.g. Gemma 4), a warm mixed-catalog box is not a usable warm provider —
	// routing won't send the model there. Treat it as not warm so it neither
	// suppresses cold-spill/swap planning onto a real dedicated box nor counts
	// toward the model's warm-capacity demand target.
	if !r.providerServesRoutableModelLocked(p, model, false) {
		return false
	}
	if p.BackendCapacity != nil {
		for _, slot := range p.BackendCapacity.Slots {
			if slot.Model == model {
				// BackendCapacity is authoritative when present.
				// Only "running" and "idle" mean the model is warm.
				return slot.State == "running" || slot.State == "idle"
			}
		}
		// Model has no slot in BackendCapacity -- it's not loaded.
		return false
	}
	// Legacy provider without BackendCapacity: fall back to WarmModels.
	for _, warmModel := range p.WarmModels {
		if warmModel == model {
			return true
		}
	}
	return false
}

// bestModelLoadProviderLocked selects the eligible provider with the fewest
// pending requests. Caller must hold r.mu (read or write).
func (r *Registry) bestModelLoadProviderLocked(model string, now time.Time, selectedProviders map[string]struct{}) string {
	bestProviderID := ""
	// Only advertisers qualify (modelLoadCandidatePendingLocked requires
	// providerServesRoutableModelLocked), so the per-model index prunes the
	// walk losslessly (model_index.go).
	for _, p := range r.providersForModelLocked(model) {
		id := p.ID
		if _, selected := selectedProviders[id]; selected {
			continue
		}
		// Skip providers that have any pending model load -- sending a
		// second load_model while the first is in progress can cause
		// swap oscillation on single-slot providers.
		if r.providerHasPendingLoad(id) {
			continue
		}

		pendingCount, ok := r.modelLoadCandidatePendingLocked(p, model, now)
		if !ok {
			continue
		}
		// Only consider idle providers (no in-flight requests). Sending
		// load_model to a provider that is actively serving another model
		// will fail because the active slot cannot be evicted.
		if pendingCount == 0 {
			bestProviderID = id
			break
		}
	}
	return bestProviderID
}

// modelLoadCandidatePendingLocked applies the same routing safety gates used by
// the scheduler, then returns the provider's current pending request count.
// Caller must hold r.mu (read or write).
func (r *Registry) modelLoadCandidatePendingLocked(p *Provider, model string, now time.Time) (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Liveness/trust/privacy core + catalog membership + dedicated-box
	// isolation, with NO owner relaxation: this is a public load_model target
	// picker, so private-only machines never qualify and a dedicated-family
	// model (e.g. Gemma 4) may only be loaded onto a provider dedicated to it,
	// never a mixed-catalog box (routing would never use it). Mirrors
	// providerHasWarmModelLocked.
	if !r.providerLivenessGateLocked(p, r.MinTrustLevel, false, now) {
		return 0, false
	}
	if !r.providerServesRoutableModelLocked(p, model, false) {
		return 0, false
	}

	// Memory gate: reject providers that cannot run the model per the catalog's
	// authoritative min_ram_gb (falling back to the weight heuristic only when
	// unknown). Shares modelFitsHardware with the consumer-routing admission
	// gate so the two can never drift. This prevents the coordinator from
	// sending load_model commands to machines that clearly cannot fit it, while
	// trusting the operator-published requirement rather than a synthetic
	// multiple that would exclude catalog-qualified nodes.
	if entry, ok := r.modelCatalog[model]; ok && (entry.MinRAMGB > 0 || entry.SizeGB > 0) {
		if !modelFitsHardware(entry.MinRAMGB, entry.SizeGB, float64(p.Hardware.MemoryGB)) {
			return 0, false
		}
		// Live free-capacity gate (shared helper with the direct path): don't plan
		// a load the provider already reports it cannot fit. Mirrors freeMemoryAdmits
		// so the warming planner can't send a load_model the provider then
		// OOM-rejects, which would leave queued cold-dispatch requests sitting until
		// they time out. Legacy providers (no report) fall through to the static gate.
		if admit, reported := reportedFreeForLoadAdmits(entry.SizeGB, backendFreeForLoadGB(p.BackendCapacity), p.Version, model); reported && !admit {
			return 0, false
		}
	}

	return p.pendingCount(), true
}

func (r *Registry) reservePendingModelLoads(actions []modelLoadAction, now time.Time) []modelLoadAction {
	if len(actions) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pendingModelLoads == nil {
		r.pendingModelLoads = make(map[modelLoadKey]time.Time)
	}

	reserved := actions[:0]
	for _, action := range actions {
		if p, ok := r.providers[action.providerID]; ok {
			p.mu.Lock()
			eligible := r.providerCanAcquireCatalogModelLocked(p, action.modelID)
			p.mu.Unlock()
			if !eligible {
				continue
			}
		}
		// Check per-provider (not just per-key) to prevent concurrent
		// heartbeat goroutines from reserving the same idle provider
		// for different models.
		if r.providerHasPendingLoad(action.providerID) {
			continue
		}
		key := modelLoadKey{ProviderID: action.providerID, ModelID: action.modelID}
		r.pendingModelLoads[key] = now.Add(pendingModelLoadTTL)
		r.pendingModelLoadStarted[key] = now
		reserved = append(reserved, action)
	}
	return reserved
}

func (r *Registry) sendModelLoadActions(actions []modelLoadAction) {
	for _, action := range actions {
		if err := r.SendLoadModel(action.providerID, action.modelID); err != nil {
			r.logger.Warn("failed to trigger model swap",
				"provider_id", action.providerID,
				"model_id", action.modelID,
				"error", err,
			)
			r.ClearPendingModelLoad(action.providerID, action.modelID)
		}
	}
}

// providerHasPendingLoad reports whether the provider has any pending
// load_model command. Caller must hold r.mu (read or write).
func (r *Registry) providerHasPendingLoad(providerID string) bool {
	for key := range r.pendingModelLoads {
		if key.ProviderID == providerID && key.ModelID != "" {
			return true
		}
	}
	return false
}

// ClearIneligiblePendingModelLoads releases warm-pool reservations whose
// provider/model pair no longer passes the command-side catalog and capability
// gate. Runtime-policy revocation calls this after capability reconciliation so
// stale protected loads cannot consume the global pending-load budget.
func (r *Registry) ClearIneligiblePendingModelLoads(providerID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.providers[providerID]
	if !ok {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	cleared := 0
	for key := range r.pendingModelLoads {
		if key.ProviderID != providerID || key.ModelID == "" {
			continue
		}
		if r.providerCanAcquireCatalogModelLocked(p, key.ModelID) {
			continue
		}
		delete(r.pendingModelLoads, key)
		delete(r.pendingModelLoadStarted, key)
		cleared++
	}
	return cleared
}

// MarkModelWarm adds a model to the provider's WarmModels list if not already
// present. Called when load_model_status:succeeded arrives before the next
// heartbeat, so the scheduler sees the provider as warm during queue drain.
func (r *Registry) MarkModelWarm(providerID, modelID string) {
	r.mu.RLock()
	p, ok := r.providers[providerID]
	if !ok {
		r.mu.RUnlock()
		return
	}
	p.mu.Lock()
	if !r.providerServesCatalogModelLocked(p, modelID) {
		p.mu.Unlock()
		r.mu.RUnlock()
		return
	}
	defer func() {
		p.mu.Unlock()
		r.mu.RUnlock()
	}()
	for _, wm := range p.WarmModels {
		if wm == modelID {
			return // already warm
		}
	}
	p.WarmModels = append(p.WarmModels, modelID)
	p.CurrentModel = modelID

	// Inject a synthetic "idle" slot into BackendCapacity so the scheduler
	// sees the model as warm. Without this, the scheduler only checks
	// BackendCapacity.Slots (not WarmModels) for Swift providers, and a
	// stale snapshot without the new model's slot would treat it as cold
	// until the next heartbeat arrives.
	//
	// We only add/update the new model's slot and leave existing slots
	// untouched — the provider may have multiple model slots loaded
	// simultaneously (maxModelSlots defaults to 3). The next heartbeat
	// will provide the authoritative slot list.
	if p.BackendCapacity != nil {
		found := false
		for i, slot := range p.BackendCapacity.Slots {
			if slot.Model == modelID {
				p.BackendCapacity.Slots[i].State = "idle"
				found = true
				break
			}
		}
		if !found {
			p.BackendCapacity.Slots = append(p.BackendCapacity.Slots, protocol.BackendSlotCapacity{
				Model: modelID,
				State: "idle",
			})
		}
	}
}

// ClearPendingModelLoad removes a pending model load entry after a terminal
// load_model_status response.
func (r *Registry) ClearPendingModelLoad(providerID, modelID string) time.Duration {
	r.mu.Lock()
	key := modelLoadKey{ProviderID: providerID, ModelID: modelID}
	started := r.pendingModelLoadStarted[key]
	delete(r.pendingModelLoads, key)
	delete(r.pendingModelLoadStarted, key)
	r.mu.Unlock()
	if started.IsZero() {
		return 0
	}
	return time.Since(started)
}

func (r *Registry) PendingModelLoadDuration(providerID, modelID string) time.Duration {
	r.mu.RLock()
	started := r.pendingModelLoadStarted[modelLoadKey{ProviderID: providerID, ModelID: modelID}]
	r.mu.RUnlock()
	if started.IsZero() {
		return 0
	}
	return time.Since(started)
}

// HasPendingModelLoad reports whether an unexpired coordinator-issued
// load_model command exists for exactly this provider/model pair. It lets the
// WebSocket boundary reject unsolicited load_model_status messages before
// allowing them to mutate warm-model state.
func (r *Registry) HasPendingModelLoad(providerID, modelID string) bool {
	r.mu.RLock()
	expiresAt, ok := r.pendingModelLoads[modelLoadKey{ProviderID: providerID, ModelID: modelID}]
	r.mu.RUnlock()
	return ok && time.Now().Before(expiresAt)
}

// backoffPendingModelLoad re-stamps a pending load entry's expiry to
// now+backoff, seeding pendingModelLoadStarted when this is the first time the
// pair is seen (the coordinator may learn of a rejection for a load_model whose
// reservation already expired or was cleared). Shared by the drain and
// memory/generic-failure backoff paths so a failed load is reconsidered after a
// short cooldown instead of the full pendingModelLoadTTL. Caller must NOT hold
// r.mu.
func (r *Registry) backoffPendingModelLoad(providerID, modelID string, backoff time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pendingModelLoads == nil {
		r.pendingModelLoads = make(map[modelLoadKey]time.Time)
	}
	if r.pendingModelLoadStarted == nil {
		r.pendingModelLoadStarted = make(map[modelLoadKey]time.Time)
	}
	key := modelLoadKey{ProviderID: providerID, ModelID: modelID}
	now := time.Now()
	r.pendingModelLoads[key] = now.Add(backoff)
	if r.pendingModelLoadStarted[key].IsZero() {
		r.pendingModelLoadStarted[key] = now
	}
}

// BackoffPendingModelLoadForDrain re-stamps a pending load entry with the
// short drain backoff. Called when a provider rejects load_model because it
// is draining ahead of an auto-update restart: clearing the entry outright
// would re-send load_model to the same draining provider on the very next
// TriggerModelSwaps pass, while the full failure cooldown would suppress the
// provider long after a failed restart resumed serving. A successful restart
// clears the entry anyway via Disconnect.
func (r *Registry) BackoffPendingModelLoadForDrain(providerID, modelID string) {
	r.backoffPendingModelLoad(providerID, modelID, pendingModelLoadDrainBackoff)
}

// BackoffPendingModelLoadForMemory re-stamps a pending load entry with the
// short memory backoff after a NON-draining load_model failure (see
// pendingModelLoadMemoryBackoff). Memory-pressure load failures recover in
// seconds, so the entry must not keep the provider unplannable for the full
// pendingModelLoadTTL — that window (~2 min) is ≈ the 120s queue timeout, so a
// request queued right after the failure would time out before the provider is
// reconsidered by TriggerModelSwaps. The ~10s warm-pool sweep reaps the
// re-stamped entry.
func (r *Registry) BackoffPendingModelLoadForMemory(providerID, modelID string) {
	r.backoffPendingModelLoad(providerID, modelID, pendingModelLoadMemoryBackoff)
}

// RejectUnservableQueuedRequests checks whether any eligible provider can
// serve the given model. If not, all queued requests for the model are
// rejected immediately rather than waiting for the 120s queue timeout.
// Called after a load_model failure to give consumers a fast error.
func (r *Registry) RejectUnservableQueuedRequests(modelID string) {
	queue := r.Queue()
	if queue == nil {
		return
	}
	if queue.QueueSize(modelID) == 0 {
		return
	}

	// Check if any provider can still serve this model. Only reject when
	// NO provider serves the model at all. If providers exist but are
	// temporarily at capacity (capacityRejections > 0), the requests
	// should wait — those providers may finish current work and become
	// available.
	// modelTooLarge is intentionally ignored here: a model that can never fit
	// any provider should NOT keep its queued requests waiting (they'd time out
	// after 120s) — fall through to fail them fast.
	// Base-shape check: "can any provider serve this model at all?" carries no
	// tool/vision constraint, so use the default (base) traits.
	candidates, capacityRejections, _ := r.QuickCapacityCheck(modelID, 500, defaultRequestedMaxTokens, RequestTraits{})
	if candidates > 0 || capacityRejections > 0 {
		return
	}

	// Prefer waiters are preserved only when their owner actually has an owned
	// provider serving this model (it may free up). A prefer waiter with no
	// owned provider is just waiting on the (now-unservable) public fleet, so it
	// should fail fast like any public request. Compute eligibility here —
	// OUTSIDE the queue lock — since OwnedProviderSummary takes the registry lock.
	preferOwnerEligible := make(map[string]bool)
	for _, owner := range queue.PreferWaiterOwners(modelID) {
		// Base-shape question (like the QuickCapacityCheck above): does the
		// owner have ANY box serving this model — no per-request trait/vision
		// constraint at this granularity.
		_, servesModel := r.OwnedProviderSummary(owner, modelID, RequestTraits{}, false)
		preferOwnerEligible[owner] = servesModel > 0
	}

	failed := queue.FailQueuedRequestsForModel(modelID, preferOwnerEligible)
	if failed > 0 {
		r.logger.Warn("rejected queued requests for unservable model",
			"model_id", modelID,
			"rejected", failed,
		)
	}
}
