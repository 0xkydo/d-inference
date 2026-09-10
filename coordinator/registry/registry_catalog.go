package registry

// Model catalog, aliases, routing resolution, and catalog aggregates.

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/eigeninference/d-inference/coordinator/modelpolicy"
	"github.com/eigeninference/d-inference/coordinator/protocol"
)

// CatalogEntry holds metadata about an active model in the catalog.
type CatalogEntry struct {
	ID                           string
	WeightHash                   string  // expected SHA-256 weight fingerprint (empty = not enforced)
	SizeGB                       float64 // disk/GPU footprint of the model weights (zero = unknown, gate disabled)
	RequiredProviderCapabilities []string
	// MinRAMGB is the catalog's authoritative minimum unified memory (GB) to run
	// this model — the operator-published requirement. The hardware-fit gate
	// prefers this over any heuristic multiple of SizeGB. Zero = unknown.
	MinRAMGB int
}

// SetModelCatalog updates the set of active models. Only models in this
// set will be accepted from providers during registration and routable to
// consumers. Pass nil to disable catalog filtering for tests/dev flows. Passing
// an empty non-nil slice configures a deny-all catalog, which is what a fresh
// DB-backed registry should do until an operator registers and promotes models.
func (r *Registry) SetModelCatalog(entries []CatalogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entries == nil {
		r.modelCatalog = nil
		return
	}
	catalog := make(map[string]CatalogEntry, len(entries))
	for _, e := range entries {
		e.RequiredProviderCapabilities = effectiveRequiredProviderCapabilities(
			e.ID, e.RequiredProviderCapabilities)
		catalog[e.ID] = e
	}
	r.modelCatalog = catalog
}

// AliasTarget is the declarative resolution target for a public alias: a single
// Desired build the fleet converges to, with an optional still-acceptable
// Previous build during a staggered rollout. No weights, no ramp. Retired holds
// former members (rotated out by later upserts) — never routed, but used to
// recognize a returning provider that was offline through a retirement as part
// of this alias's fleet so it still receives desired_models. OpenRouterOnly
// targets resolve requests but never drive provider convergence or canonical
// build-to-public-name mapping.
type AliasTarget struct {
	Desired        string
	Previous       string
	Retired        []string
	OpenRouterOnly bool
}

// SetModelAliases installs the public-alias → {desired, previous} mapping. Pass
// nil (or an empty map) to clear all aliases. Callers pass only ACTIVE aliases
// (the store/sync layer filters inactive ones out). An alias whose Desired is
// empty contributes nothing routable.
func (r *Registry) SetModelAliases(aliases map[string]AliasTarget) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(aliases) == 0 {
		r.modelAliases = nil
		return
	}
	m := make(map[string]AliasTarget, len(aliases))
	for alias, t := range aliases {
		m[alias] = t
	}
	r.modelAliases = m
}

// PublicNameForBuild returns the public alias a concrete build is exposed under
// (the consumer-facing name), or the build id unchanged if it isn't the desired
// or previous build of any alias. This lets consumer-facing surfaces (e.g. usage
// history) show the alias while billing/stats/earnings keep storing the concrete
// build. If several aliases map to the build, the lexicographically-first is
// returned for stability.
func (r *Registry) PublicNameForBuild(buildID string) string {
	if buildID == "" {
		return buildID
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	best := ""
	for alias, t := range r.modelAliases {
		if t.OpenRouterOnly {
			continue
		}
		if t.Desired == buildID || t.Previous == buildID {

			if best == "" || alias < best {
				best = alias
			}
		}
	}
	if best == "" {
		return buildID
	}
	return best
}

// IsAlias reports whether requested is a configured public alias.
func (r *Registry) IsAlias(requested string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.modelAliases[requested]
	return ok
}

// AliasTarget returns the configured desired/previous build pointers for alias.
func (r *Registry) AliasTarget(alias string) (AliasTarget, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.modelAliases[alias]
	return t, ok
}

// ResolveModel maps a requested model id to a concrete build id for routing.
//
//   - If requested is NOT an alias, it is returned unchanged (isAlias=false,
//     ok=true) — raw build ids keep working for backward compatibility.
//   - If requested IS an alias, it resolves to the Desired build when at least
//     one provider can route it; otherwise to the Previous build when that is
//     routable; otherwise it returns Desired so the request queues against a
//     real build instead of black-holing. ok=false only when Desired is empty.
func (r *Registry) ResolveModel(requested string) (buildID string, isAlias bool, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, found := r.modelAliases[requested]
	if !found {
		return requested, false, true
	}
	if t.Desired == "" {
		return "", true, false
	}
	if r.anyProviderCanRouteBuildLocked(t.Desired) {
		return t.Desired, true, true
	}
	if t.Previous != "" && r.anyProviderCanRouteBuildLocked(t.Previous) {
		return t.Previous, true, true
	}
	// Neither build is routable yet — resolve to Desired so the request queues
	// against a real build instead of failing outright.
	return t.Desired, true, true
}

// ResolveModelConstrained is ResolveModel, but when a request is restricted to
// specific providers — a serial allowlist or self-route to the owner's own
// machines — it only treats a build as servable if an ELIGIBLE provider (one
// that both matches the constraint and can route the build) can serve it. This
// stops an alias from resolving to a build that's routable somewhere globally
// but absent from the request's allowed provider set (which would then fail at
// dispatch). With no constraints it is identical to ResolveModel.
func (r *Registry) ResolveModelConstrained(requested string, allowedSerials []string, ownerAccountID string, selfRouteOnly, preferOwner bool) (buildID string, isAlias bool, ok bool) {
	return r.ResolveModelConstrainedWithTraits(
		requested, allowedSerials, ownerAccountID, selfRouteOnly, preferOwner,
		RequestTraits{})
}

// ResolveModelConstrainedWithTraits extends ResolveModelConstrained with the
// same request-shape gates used at dispatch. During a mixed-version rollout an
// alias must not resolve to Desired merely because an old provider can serve
// ordinary text when Previous has a provider capable of the requested shape.
func (r *Registry) ResolveModelConstrainedWithTraits(
	requested string,
	allowedSerials []string,
	ownerAccountID string,
	selfRouteOnly, preferOwner bool,
	traits RequestTraits,
) (buildID string, isAlias bool, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, found := r.modelAliases[requested]
	if !found {
		return requested, false, true
	}
	if t.Desired == "" {
		return "", true, false
	}
	allowed := make(map[string]struct{}, len(allowedSerials))
	for _, s := range allowedSerials {
		if s != "" {
			allowed[s] = struct{}{}
		}
	}
	now := time.Now()
	hardConstrained := len(allowed) > 0 || selfRouteOnly
	if preferOwner && ownerAccountID != "" {
		if r.anyProviderCanServeAliasWithTraitsLocked(
			t.Desired, nil, ownerAccountID, true, true, now, traits, false,
		) {
			return t.Desired, true, true
		}
		if t.Previous != "" && r.anyProviderCanServeAliasWithTraitsLocked(
			t.Previous, nil, ownerAccountID, true, true, now, traits, false,
		) {
			return t.Previous, true, true
		}
	}
	if !hardConstrained {
		if r.anyProviderCanServeAliasWithTraitsLocked(
			t.Desired, nil, "", false, false, now, traits, false,
		) {
			return t.Desired, true, true
		}
		if t.Previous != "" && r.anyProviderCanServeAliasWithTraitsLocked(
			t.Previous, nil, "", false, false, now, traits, false,
		) {
			return t.Previous, true, true
		}
		if r.anyProviderCanServeAliasWithTraitsLocked(
			t.Desired, nil, "", false, false, now, traits, true,
		) {
			return t.Desired, true, true
		}
		if t.Previous != "" && r.anyProviderCanServeAliasWithTraitsLocked(
			t.Previous, nil, "", false, false, now, traits, true,
		) {
			return t.Previous, true, true
		}
		return t.Desired, true, true
	}
	if t.Desired != "" && r.anyProviderCanServeAliasWithTraitsLocked(
		t.Desired, allowed, ownerAccountID, selfRouteOnly, preferOwner, now, traits, false,
	) {
		return t.Desired, true, true
	}
	if t.Previous != "" && r.anyProviderCanServeAliasWithTraitsLocked(
		t.Previous, allowed, ownerAccountID, selfRouteOnly, preferOwner, now, traits, false,
	) {
		return t.Previous, true, true
	}
	if t.Desired != "" && r.anyProviderCanServeAliasWithTraitsLocked(
		t.Desired, allowed, ownerAccountID, selfRouteOnly, preferOwner, now, traits, true,
	) {
		return t.Desired, true, true
	}
	if t.Previous != "" && r.anyProviderCanServeAliasWithTraitsLocked(
		t.Previous, allowed, ownerAccountID, selfRouteOnly, preferOwner, now, traits, true,
	) {
		return t.Previous, true, true
	}
	// Only HARD-constrained requests (serial pin / self-route-only) reach here —
	// the unconstrained path returned ResolveModel above. So if no allowed+
	// eligible provider can serve either build, do NOT fall back to Desired: that
	// would resolve to a build the allowed providers can't serve (the exact thing
	// this function exists to prevent) and then queue/fail against the wrong
	// build, or for self-route leak toward the fleet. Return unavailable.
	return "", true, false
}

// anyProviderCanServeAliasWithTraitsLocked reports whether some provider
// matches the request's routing constraints and exact capability traits.
// structural=true ignores transient slot/cooldown state so alias resolution can
// queue against a capable build instead of falling back to an incapable one.
// Self-route to an owned machine relaxes trust and allows private-only
// providers, mirroring snapshotProviderIntoLockedEx. Caller holds r.mu.
func (r *Registry) anyProviderCanServeAliasWithTraitsLocked(
	buildID string,
	allowedSerials map[string]struct{},
	ownerAccountID string,
	selfRouteOnly, preferOwner bool,
	now time.Time,
	traits RequestTraits,
	structural bool,
) bool {
	// Only providers advertising the build can route it; the per-model index
	// prunes the rest (gates unchanged). Copied before any p.mu is taken.
	for _, p := range r.providersForModelLocked(buildID) {
		p.mu.Lock()
		ok := func() bool {
			if len(allowedSerials) > 0 {
				// A provider with no attestation result can't be serial-matched
				// (and dereferencing it would panic) — treat as not eligible.
				serial := ""
				if p.AttestationResult != nil {
					serial = p.AttestationResult.SerialNumber
				}
				if _, in := allowedSerials[serial]; !in || serial == "" {
					return false
				}
			}
			owned := p.AccountID != "" && p.AccountID == ownerAccountID
			if selfRouteOnly && !owned {
				return false
			}
			minTrust := r.MinTrustLevel
			allowPrivate := false
			if owned && (selfRouteOnly || preferOwner) {
				minTrust = TrustNone
				allowPrivate = true
			}
			canRoute := r.providerCanRouteBuildLocked(
				p, buildID, minTrust, now, allowPrivate)
			if structural {
				canRoute = r.providerStructurallyCanRouteBuildLocked(
					p, buildID, minTrust, now, allowPrivate)
			}
			return canRoute && r.providerEligibleForTraitsLocked(p, buildID, traits)
		}()
		p.mu.Unlock()
		if ok {
			return true
		}
	}
	return false
}

// providerStructurallyCanRouteBuildLocked reports whether a provider has every
// non-capacity prerequisite for serving a build. Transient load cooldowns and
// slot states are intentionally excluded so queued requests can wait for a
// reloading capable provider instead of being misreported as capability-
// unavailable. Caller holds r.mu (RLock) and p.mu.
func (r *Registry) providerStructurallyCanRouteBuildLocked(
	p *Provider,
	buildID string,
	minTrust TrustLevel,
	now time.Time,
	allowPrivate bool,
) bool {
	// Catalog membership + dedicated-box isolation, mirroring
	// providerPassesRoutingGatesLockedEx so alias routability (and rollout/drop
	// measurement) matches actual dispatch routability: a dedicated-family build
	// is only routable on a provider dedicated to that family. Without this, an
	// alias whose Desired build is advertised only by a mixed box would resolve
	// to Desired (then 429 at dispatch) instead of failing over to a Previous
	// build on a dedicated box. allowPrivate marks the owner self-route context,
	// exempt like selfRouteOwner.
	if !r.providerServesRoutableModelLocked(p, buildID, allowPrivate) {
		return false
	}
	// Liveness/trust/privacy core. allowPrivate marks the owner self-route
	// context (relax private-only admission); the trust-floor relaxation is
	// folded into the minTrust the caller passes (TrustNone for owner routes).
	if !r.providerLivenessGateLocked(p, minTrust, allowPrivate, now) {
		return false
	}
	// Hardware fit: don't count a provider whose RAM can't hold the build (e.g.
	// migrating to a larger build than the source). totalMemory prefers the
	// backend-reported figure, matching snapshotProviderIntoLockedEx. A resident
	// running/idle slot has already demonstrated fit and must bypass the
	// heuristic. Owner-only off-catalog models use their advertised size.
	totalMemoryGB := float64(p.Hardware.MemoryGB)
	slotState := "unknown"
	if p.BackendCapacity != nil && p.BackendCapacity.TotalMemoryGB > 0 {
		totalMemoryGB = p.BackendCapacity.TotalMemoryGB
	}
	if p.BackendCapacity != nil {
		for _, slot := range p.BackendCapacity.Slots {
			if slot.Model == buildID {
				slotState = slot.State
				break
			}
		}
	}
	return slotStateModelLoaded(slotState) ||
		modelFitsHardware(
			r.catalogMinRAMGbLocked(buildID),
			r.modelSizeGBForFitLocked(p, buildID),
			totalMemoryGB)
}

// providerCanRouteBuildLocked is the single source of truth for "could this
// provider actually serve this build right now". It adds transient cooldown and
// slot-state gates to providerStructurallyCanRouteBuildLocked, while still
// omitting per-request capacity/headroom checks. Cold-but-healthy providers
// pass (no warm slot required — they load on first demand). Caller holds r.mu
// (RLock) and p.mu.
func (r *Registry) providerCanRouteBuildLocked(p *Provider, buildID string, minTrust TrustLevel, now time.Time, allowPrivate bool) bool {
	if !r.providerStructurallyCanRouteBuildLocked(
		p, buildID, minTrust, now, allowPrivate,
	) {
		return false
	}
	// The session's current gate, read under p.mu — which the identity bind
	// also holds (bindStableFaultKey) — so a rebind cannot repoint p.gate, or
	// migrate the cooldown away from the gate read here, mid-check. Without
	// that, a read of a shared source gate emptied by this session's own
	// rebind would say "not cooled" and let an alias resolve to a Desired
	// build whose only provider is cooled (the request then queues or 429s
	// instead of taking the routable Previous build). No gateView
	// confirmation is needed under p.mu.
	if r.gateOf(p).dispatchLoadCooled(buildID, now) {
		return false
	}
	if p.BackendCapacity != nil {
		for _, slot := range p.BackendCapacity.Slots {
			if slot.Model != buildID {
				continue
			}
			if _, eligible := slotStatePenalty(slot.State); !eligible {
				return false
			}
			break
		}
	}
	return true
}

// anyProviderCanRouteBuildLocked reports whether at least one provider could
// route the build right now. Caller holds r.mu.
func (r *Registry) anyProviderCanRouteBuildLocked(buildID string) bool {
	now := time.Now()
	minTrust := r.MinTrustLevel
	// Per-model index: only advertisers can route the build (model_index.go).
	for _, p := range r.providersForModelLocked(buildID) {
		p.mu.Lock()
		ok := r.providerCanRouteBuildLocked(p, buildID, minTrust, now, false)
		p.mu.Unlock()
		if ok {
			return true
		}
	}
	return false
}

// MergeProviderModels applies a provider's authoritative models_update to its
// advertised Models in place — used for the message a provider sends after it
// converges on a desired build (background prefetch verified, then hard-swap),
// so a new build becomes routable WITHOUT a reconnect and WITHOUT resetting
// trust/reputation/challenge state. It is authoritative for each alias whose
// desired build appears in the validated update: that alias's previous build is
// dropped if omitted. Seeing a build only as another alias's previous build is
// not enough to drop that other alias's desired build, which keeps aliases that
// share a concrete build independent.
//
// Each model's WeightHash is cross-checked against the catalog's expected hash;
// a mismatch is REJECTED (the build is not made routable) so a bad or buggy
// prefetch/swap can never take traffic. Returns build ids that were merged and
// build ids that were dropped from this provider.
func (r *Registry) MergeProviderModels(providerID string, models []protocol.ModelInfo) (merged, dropped []string) {
	return r.mergeProviderModels(providerID, models, 0, nil)
}

// MergeProviderModelsWithCapabilities is the current-provider models_update
// path. Capability fields are authoritative for the concrete models carried by
// this update; omitted fields preserve legacy-provider behavior.
func (r *Registry) MergeProviderModelsWithCapabilities(
	providerID string,
	models []protocol.ModelInfo,
	toolConstraintProtocol int,
	toolConstraintModels []string,
) (merged, dropped []string) {
	return r.mergeProviderModels(
		providerID,
		models,
		toolConstraintProtocol,
		toolConstraintModels,
	)
}

func (r *Registry) mergeProviderModels(
	providerID string,
	models []protocol.ModelInfo,
	toolConstraintProtocol int,
	toolConstraintModels []string,
) (merged, dropped []string) {
	if len(models) == 0 {
		return nil, nil
	}
	updatedToolConstraintModels := toolConstraintModelSet(
		toolConstraintModels, models)
	r.mu.RLock()
	p, ok := r.providers[providerID]
	// hasCatalog mirrors modelAllowedByCatalogLocked: a nil catalog (dev/test
	// setups) imposes no membership gate; a present catalog makes membership
	// mandatory for merging.
	hasCatalog := r.modelCatalog != nil
	expected := make(map[string]CatalogEntry, len(models))
	for _, m := range models {
		if e, has := r.modelCatalog[m.ID]; has {
			expected[m.ID] = e
		}
	}
	// Snapshot the alias targets under the read lock so the drop set can be
	// computed later (under p.mu) without nesting r.mu — and, crucially, from
	// the builds that actually PASS validation below, not from the raw message.
	aliasTargets := make([]AliasTarget, 0, len(r.modelAliases))
	for _, t := range r.modelAliases {
		if !t.OpenRouterOnly {
			aliasTargets = append(aliasTargets, t)
		}
	}

	r.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	p.mu.Lock()
	// present tracks only builds that passed validation and were merged — the
	// hard-swap drop is derived from THIS set, never from the raw message. A
	// desired build rejected for a bad weight hash therefore does NOT cause its
	// previous sibling to be dropped (which would strand the provider on neither
	// build — the exact failure the hash check exists to prevent).
	present := make(map[string]struct{}, len(models))
	cacheStateInvalidated := make(map[string]struct{})
	for _, m := range models {
		if m.ID == "" {
			continue
		}
		// A build the catalog has never heard of is rejected outright (when a
		// catalog exists). It could never be routed anyway
		// (modelAllowedByCatalogLocked), and merging it would let a provider
		// grow its own p.Models without bound via repeated models_update
		// messages carrying fabricated ids.
		entry, inCatalog := expected[m.ID]
		if hasCatalog && !inCatalog {
			r.logger.Warn("models_update for build not in catalog; rejecting",
				"provider_id", providerID, "model_id", m.ID)
			continue
		}
		required := effectiveRequiredProviderCapabilities(
			m.ID, entry.RequiredProviderCapabilities)
		if !capabilitySetContainsAll(p.RuntimeCapabilities, required) {
			r.logger.Warn("models_update provider capability mismatch; rejecting build",
				"provider_id", providerID, "model_id", m.ID)
			continue
		}
		// When the catalog pins an expected hash, a models_update MUST carry a
		// non-empty MATCHING hash. A missing hash is rejected just like a
		// mismatched one.
		if exp := entry.WeightHash; exp != "" && !strings.EqualFold(m.WeightHash, exp) {
			r.logger.Warn("models_update weight-hash missing or mismatched; rejecting build",
				"provider_id", providerID, "model_id", m.ID, "expected", exp, "got", m.WeightHash)
			continue
		}
		replaced := false
		for i := range p.Models {
			if p.Models[i].ID == m.ID {
				if !strings.EqualFold(p.Models[i].WeightHash, m.WeightHash) {
					delete(p.PrefixCacheStatuses, m.ID)
					delete(p.PrefixCacheV2Models, m.ID)
					p.prefixCacheRevision++
					cacheStateInvalidated[m.ID] = struct{}{}
				}
				p.Models[i] = m
				replaced = true
				break
			}
		}
		if !replaced {
			p.Models = append(p.Models, m)
		}
		merged = append(merged, m.ID)
		present[m.ID] = struct{}{}
		if toolConstraintProtocol != 0 {
			p.ToolConstraintProtocol = toolConstraintProtocol
			if _, supported := updatedToolConstraintModels[m.ID]; toolConstraintProtocol == ToolConstraintProtocolV1 && supported {
				if p.ToolConstraintModels == nil {
					p.ToolConstraintModels = make(map[string]struct{})
				}
				p.ToolConstraintModels[m.ID] = struct{}{}
			} else {
				delete(p.ToolConstraintModels, m.ID)
			}
		}
	}
	// Compute the hard-swap drop set: a VALIDATED desired build authorizes
	// dropping only that alias's previous build. This is intentionally
	// directional; if two aliases share a build, updating one alias to that shared
	// desired build must not drop the desired build of another alias where the
	// shared build is merely "previous".
	drop := make(map[string]struct{})
	for _, t := range aliasTargets {
		if t.Desired == "" || t.Previous == "" || t.Desired == t.Previous {
			continue
		}
		if _, desiredPresent := present[t.Desired]; !desiredPresent {
			continue
		}
		if _, previousStillPresent := present[t.Previous]; !previousStillPresent {
			drop[t.Previous] = struct{}{}
		}
	}
	// Apply the hard-swap drop: remove any alias-sibling build the provider no
	// longer advertises.
	if len(drop) > 0 {
		kept := p.Models[:0]
		for _, m := range p.Models {
			if _, gone := drop[m.ID]; gone {
				r.logger.Info("models_update hard-swap: dropping retired build",
					"provider_id", providerID, "model_id", m.ID)
				dropped = append(dropped, m.ID)
				delete(p.ToolConstraintModels, m.ID)
				delete(p.PrefixCacheStatuses, m.ID)
				delete(p.PrefixCacheV2Models, m.ID)
				p.prefixCacheRevision++
				cacheStateInvalidated[m.ID] = struct{}{}
				continue
			}
			kept = append(kept, m)
		}
		p.Models = kept
	}
	p.PrefixCacheStatuses, p.PrefixCacheStatusReported =
		reconcilePrefixCacheStatuses(
			p.PrefixCacheProtocol,
			p.PrefixCacheV2Models,
			p.PrefixCacheStatuses,
			p.PrefixCacheStatusReported,
		)
	p.syncModelIndexLocked()
	p.mu.Unlock()
	if len(cacheStateInvalidated) > 0 {
		r.mu.RLock()
		tracker := r.cacheRouting
		r.mu.RUnlock()
		if tracker != nil {
			for modelID := range cacheStateInvalidated {
				tracker.invalidateProviderModel(
					providerID, modelID, cacheHolderRemovalCapabilityChange)
			}
		}
	}
	return merged, dropped
}

// RoutableProviderIDsForBuild returns the ids of providers that would actually
// pass the routing gate for the build right now — the SAME checks
// snapshotProviderIntoLockedEx applies (advertises the build, not offline/untrusted,
// public, trust ≥ floor, runtime verified, private-text capable, fresh
// challenge), minus per-request capacity/headroom. Cold-but-healthy providers
// count (no warm slot required — they load on first demand). Used to measure how
// much of the fleet can truly serve a build (e.g. rollout progress / hard-swap
// drop verification in tests) without counting capacity it can't actually route.
func (r *Registry) RoutableProviderIDsForBuild(buildID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	now := time.Now()
	minTrust := r.MinTrustLevel
	var ids []string
	for id, p := range r.providers {
		p.mu.Lock()
		ok := r.providerCanRouteBuildLocked(p, buildID, minTrust, now, false)
		p.mu.Unlock()
		if ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// ModelType returns the model type string for the given model ID, or
// "unknown" if no provider is currently serving it.
func (r *Registry) ModelType(model string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.providers {
		p.mu.Lock()
		for _, m := range p.Models {
			if m.ID == model && m.ModelType != "" {
				p.mu.Unlock()
				return m.ModelType
			}
		}
		p.mu.Unlock()
	}
	return "unknown"
}

// IsModelInCatalog returns true if the model is in the active catalog, or if
// catalog filtering has been explicitly disabled by setting a nil catalog.
func (r *Registry) IsModelInCatalog(model string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.modelCatalog == nil {
		return true
	}
	_, ok := r.modelCatalog[model]
	return ok
}

// UpdateModelWeightHashes replaces stored per-model weight hashes from a
// verified attestation challenge response. A present empty value deliberately
// clears a registration-time hash that the provider could not re-verify; an
// omitted model remains unchanged because unloaded advertised models are absent
// from the challenge snapshot.
//
// Concurrency: the p.Models slice header is replaced (copy-on-write, never
// mutated in place) under p.mu — NOT under the registry-wide r.mu, which is held
// only as a read lock to look the provider up in the map. p.mu is therefore the
// sole lock guarding p.Models, so every reader that ranges p.Models must hold
// p.mu (see providerModelIDs and the *Locked helpers). Do not rely on r.mu to
// serialize reads against this write: it does not.
func (r *Registry) UpdateModelWeightHashes(providerID string, hashes map[string]string) {
	if len(hashes) == 0 {
		return
	}
	r.mu.RLock()
	p, ok := r.providers[providerID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	changed := false
	models := make([]protocol.ModelInfo, len(p.Models))
	copy(models, p.Models)
	for i := range models {
		if h, ok := hashes[models[i].ID]; ok && models[i].WeightHash != h {
			models[i].WeightHash = h
			changed = true
		}
	}
	if changed {
		p.Models = models
		p.syncModelIndexLocked() // ids unchanged; keeps the invariant explicit
	}
}

// CatalogWeightHash returns the expected weight hash for a model, or empty
// string if not set or not in catalog.
func (r *Registry) CatalogWeightHash(model string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.modelCatalog[model]; ok {
		return e.WeightHash
	}
	return ""
}

// IsAliasLineageBuild reports whether buildID is a PREVIOUS or RETIRED member of
// any active alias — i.e. an old build that a hot-swap migration legitimately
// leaves GPU-resident on providers after it drops from the advertised set. Used
// to scope the attestation active-hash alibi to exactly that migration case, so
// a provider can't use the alibi to claim an arbitrary unrelated catalog model
// as active. (Desired members are still advertised, so they never need it.)
func (r *Registry) IsAliasLineageBuild(buildID string) bool {
	if buildID == "" {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, t := range r.modelAliases {
		if t.Previous == buildID {
			return true
		}
		for _, retired := range t.Retired {
			if retired == buildID {
				return true
			}
		}
	}
	return false
}

// modelAllowedByCatalogLocked returns whether a provider-reported model is
// allowed by the current catalog. Caller must hold r.mu (read or write). A nil
// catalog disables filtering; an empty non-nil catalog denies all models.
func (r *Registry) modelAllowedByCatalogLocked(model protocol.ModelInfo) bool {
	if r.modelCatalog == nil {
		return true
	}
	entry, ok := r.modelCatalog[model.ID]
	if !ok {
		return false
	}
	return entry.WeightHash == "" || model.WeightHash == "" || model.WeightHash == entry.WeightHash
}

// providerServesCatalogModelLocked returns true if the provider advertises the
// model and that model is currently allowed by the catalog. Caller must hold
// r.mu and p.mu.
func (r *Registry) providerServesCatalogModelLocked(p *Provider, model string) bool {
	for _, m := range p.Models {
		if m.ID == model && r.providerModelAllowedByCatalogLocked(p, m) {
			return true
		}
	}
	return false
}

// modelTrackedByCatalogLocked reports whether the catalog has an entry for the
// model id at all (regardless of weight-hash agreement). A nil catalog tracks
// nothing — filtering is disabled and modelAllowedByCatalogLocked admits
// everything, so callers never reach the off-catalog distinction. Caller must
// hold r.mu.
func (r *Registry) modelTrackedByCatalogLocked(id string) bool {
	if r.modelCatalog == nil {
		return false
	}
	_, ok := r.modelCatalog[id]
	return ok
}

// modelServableForOwnerLocked is the owner self-route admission for a single
// advertised build: a model the catalog does NOT track is servable on the
// owner's box, while catalog builds retain their integrity and provider
// capability requirements. The exact protected Qwen build keeps its
// requirements even with catalog filtering disabled. Caller holds r.mu and
// p.mu.
func (r *Registry) modelServableForOwnerLocked(p *Provider, m protocol.ModelInfo) bool {
	return (r.modelAllowedByCatalogLocked(m) || !r.modelTrackedByCatalogLocked(m.ID)) &&
		r.providerMeetsModelRequirementsLocked(p, m.ID)
}

// providerServesOwnedRoutableModelLocked is providerServesCatalogModelLocked's
// owner self-route counterpart: true when the provider advertises the model
// and that build is servable for its owner (catalog-allowed, or absent from
// the catalog entirely). Caller must hold r.mu and p.mu.
func (r *Registry) providerServesOwnedRoutableModelLocked(p *Provider, model string) bool {
	for _, m := range p.Models {
		if m.ID == model && r.modelServableForOwnerLocked(p, m) {
			return true
		}
	}
	return false
}

// providerServesVisionModelLocked reports whether the provider advertises the
// model as a vision-capable (VLM) build — required to route image/video requests
// so the media is actually perceived rather than silently dropped. allowOffCatalog
// is the owner self-route context (mirrors providerServesRoutableModelLocked's
// allowDedicated): an owner's off-catalog local VLM passes the routable gate, so
// the vision gate must accept the same advertisement or media requests would be
// listed/accepted but never routable. It relaxes only catalog MEMBERSHIP — a
// catalog-tracked build still has to pass the weight-hash gate, mirroring the
// routable gate. Caller must hold r.mu AND p.mu (mirrors
// providerServesCatalogModelLocked): p.Models is guarded by p.mu and mutated by
// MergeProviderModels/UpdateModelWeightHashes. Pre-0.6.0 providers never set
// IsVision, so they are correctly excluded.
func (r *Registry) providerServesVisionModelLocked(p *Provider, model string, allowOffCatalog bool) bool {
	for _, m := range p.Models {
		if m.ID != model || !m.IsVision {
			continue
		}
		if allowOffCatalog {
			if !r.modelServableForOwnerLocked(p, m) {
				continue
			}
		} else if !r.providerModelAllowedByCatalogLocked(p, m) {
			continue
		}
		if model == modelpolicy.Qwen3VL30BA3BInstructModelID &&
			strings.EqualFold(strings.TrimSpace(p.Hardware.ChipFamily), "M5") {
			// This concrete VLM produces incorrect visual inference on M5.
			return false
		}
		return true
	}
	return false
}

// HasVisionProviderForModel reports whether any online, non-untrusted provider
// advertises a vision-capable build for the resolved model id. The consumer uses
// it to fail a media request fast with a clear error when the fleet has no
// VLM-capable provider for the model (e.g. before the gemma fleet finishes
// updating to 0.6.0), instead of queueing the request to a timeout.
//
// When allowedSerials is non-empty the check is restricted to providers whose
// attested serial is in the set, exactly as the routing path constrains the
// candidate pool. Without this filter a constrained media request would be
// falsely reported as serviceable by an unrelated public provider (the same
// latent gap as HasToolCapableProviderForModel).
func (r *Registry) HasVisionProviderForModel(model string, allowedSerials ...string) bool {
	allowedSet := make(map[string]struct{}, len(allowedSerials))
	for _, s := range allowedSerials {
		allowedSet[s] = struct{}{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.providers {
		// Allowed-serial filter first (providerMatchesAllowedSerial takes p.mu
		// internally), mirroring the routing candidate filter and QuickCapacityCheck.
		if len(allowedSet) > 0 && !providerMatchesAllowedSerial(p, allowedSet) {
			continue
		}
		// p.Status and p.Models are guarded by p.mu (writers hold it), so the
		// whole eligibility read must happen under the provider lock.
		p.mu.Lock()
		eligible := p.Status != StatusOffline && p.Status != StatusUntrusted &&
			r.providerServesVisionModelLocked(p, model, false)
		p.mu.Unlock()
		if eligible {
			return true
		}
	}
	return false
}

// catalogSizeGBLocked returns the model's reported weight footprint in GB,
// or 0 when unknown. Caller must hold r.mu (read or write). Zero means the
// memory-admission gate should not enforce for this model — typically a
// catalog entry that pre-dates the SizeGB field, or a model the operator
// hasn't sized yet.
func (r *Registry) catalogSizeGBLocked(model string) float64 {
	if e, ok := r.modelCatalog[model]; ok {
		return e.SizeGB
	}
	return 0
}

// advertisedModelSizeGBLocked returns the provider-advertised on-disk weight
// size for model in decimal GB (SizeBytes/1e9 — the same unpadded basis as the
// catalog's SizeGB), or 0 when the provider does not advertise the model or
// reports no size. Caller must hold p.mu.
func advertisedModelSizeGBLocked(p *Provider, model string) float64 {
	for _, m := range p.Models {
		if m.ID == model && m.SizeBytes > 0 {
			return float64(m.SizeBytes) / 1e9
		}
	}
	return 0
}

// modelSizeGBForFitLocked returns the weight footprint (GB) the hardware-fit
// and free-memory admission gates should use for a provider/model pair: the
// catalog's authoritative SizeGB when present, else — for a model with NO
// catalog entry (an owner's off-catalog local model, reachable only via
// self-route) — the provider-advertised size. Without the fallback an
// off-catalog model snapshots as size 0, disabling both gates, so routing
// could pick a machine whose oversized local model can never load and turn a
// deterministic model_too_large into a provider-side load failure. A nil
// catalog (dev/test: filtering disabled) and a catalog entry the operator left
// unsized both keep the gate disabled, as before. Caller holds r.mu and p.mu.
func (r *Registry) modelSizeGBForFitLocked(p *Provider, model string) float64 {
	if size := r.catalogSizeGBLocked(model); size > 0 {
		return size
	}
	if r.modelCatalog == nil {
		return 0
	}
	if _, ok := r.modelCatalog[model]; ok {
		return 0
	}
	return advertisedModelSizeGBLocked(p, model)
}

// catalogMinRAMGbLocked returns the model's authoritative minimum-RAM
// requirement (GB) from the catalog, or 0 when unknown. Caller must hold r.mu.
func (r *Registry) catalogMinRAMGbLocked(model string) int {
	if e, ok := r.modelCatalog[model]; ok {
		return e.MinRAMGB
	}
	return 0
}

// ProviderCount returns the number of registered providers.
// modelProviderInc increments the provider count for a model. Must be called
// with r.mu held.
func (r *Registry) modelProviderInc(model string) {
	r.modelProvidersMu.Lock()
	c, ok := r.modelProviders[model]
	if !ok {
		c = &atomic.Int64{}
		r.modelProviders[model] = c
	}
	r.modelProvidersMu.Unlock()
	c.Add(1)
}

// modelProviderDec decrements the provider count for a model. Must be called
// with r.mu held.
func (r *Registry) modelProviderDec(model string) {
	r.modelProvidersMu.Lock()
	c, ok := r.modelProviders[model]
	r.modelProvidersMu.Unlock()
	if ok {
		v := c.Add(-1)
		if v <= 0 {
			r.modelProvidersMu.Lock()
			delete(r.modelProviders, model)
			r.modelProvidersMu.Unlock()
		}
	}
}
