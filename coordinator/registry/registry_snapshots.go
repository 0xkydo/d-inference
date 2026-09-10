package registry

// Read-only registry snapshots and aggregate fleet views.

import (
	"sort"
	"strings"
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
)

// AttestationSummary provides aggregate attestation status for a model's providers.
type AttestationSummary struct {
	SecureEnclave bool `json:"secure_enclave"`
	SIPEnabled    bool `json:"sip_enabled"`
	SecureBoot    bool `json:"secure_boot"`
}

// AggregateModel is a deduplicated model entry for the /v1/models endpoint.
type AggregateModel struct {
	ID                string              `json:"id"`
	ModelType         string              `json:"model_type"`
	Quantization      string              `json:"quantization"`
	Providers         int                 `json:"providers"`          // number of providers offering this model
	AttestedProviders int                 `json:"attested_providers"` // number of attested providers
	TrustLevel        TrustLevel          `json:"trust_level"`        // highest trust level among providers
	Attestation       *AttestationSummary `json:"attestation,omitempty"`
}

// ListModels returns deduplicated models from all online providers.
func (r *Registry) ListModels() []AggregateModel {
	r.mu.RLock()
	defer r.mu.RUnlock()

	type modelAgg struct {
		modelType     string
		quantization  string
		count         int
		attestedCount int
		highestTrust  TrustLevel
		secureEnclave bool
		sipEnabled    bool
		secureBoot    bool
	}

	// Aggregate by model ID only — consumers request by ID, so providers
	// offering the same model ID should be counted together regardless of
	// minor metadata differences.
	//
	// The whole per-provider step runs under p.mu: it reads only strings and
	// booleans, retains nothing from the provider, and costs a few map lookups.
	// There is deliberately NO per-provider snapshot slice — at fleet scale
	// (~1,260 providers) that was one heap allocation per provider per call,
	// and /v1/models (uncached, two ListModels calls per request) paid for it
	// mostly as GC pressure rather than as the walk itself.
	agg := make(map[string]*modelAgg, len(r.modelCatalog))
	for _, p := range r.providers {
		p.mu.Lock()
		// Provider-level gates first, so an ineligible provider costs one lock
		// and a handful of field reads — never a walk of its inventory.
		// Private-only providers serve only their owner's self-route traffic, so
		// they must not appear in or inflate the public /v1/models aggregation.
		if p.Status == StatusOffline || p.Status == StatusUntrusted ||
			p.PrivateOnly ||
			!r.trustMeetsMinimum(p.TrustLevel) ||
			!r.providerSupportsPrivateTextLocked(p) {
			p.mu.Unlock()
			continue
		}
		trust := p.TrustLevel
		attestResult := p.AttestationResult
		attested := p.Attested && attestResult != nil
		for _, m := range p.Models {
			// Count only provider-model pairs that satisfy the live catalog and
			// connection-scoped capability requirements.
			if !r.providerModelAllowedByCatalogLocked(p, m) {
				continue
			}
			a, ok := agg[m.ID]
			if !ok {
				a = &modelAgg{
					modelType:    m.ModelType,
					quantization: m.Quantization,
					highestTrust: TrustNone,
				}
				agg[m.ID] = a
			}
			a.count++

			// Update highest trust level
			if trustRank(trust) > trustRank(a.highestTrust) {
				a.highestTrust = trust
			}

			if attested {
				a.attestedCount++
				a.secureEnclave = a.secureEnclave || attestResult.SecureEnclaveAvailable
				a.sipEnabled = a.sipEnabled || attestResult.SIPEnabled
				a.secureBoot = a.secureBoot || attestResult.SecureBootEnabled
			}
		}
		p.mu.Unlock()
	}

	models := make([]AggregateModel, 0, len(agg))
	for k, a := range agg {
		am := AggregateModel{
			ID:                k,
			ModelType:         a.modelType,
			Quantization:      a.quantization,
			Providers:         a.count,
			AttestedProviders: a.attestedCount,
			TrustLevel:        a.highestTrust,
		}
		if a.attestedCount > 0 {
			am.Attestation = &AttestationSummary{
				SecureEnclave: a.secureEnclave,
				SIPEnabled:    a.sipEnabled,
				SecureBoot:    a.secureBoot,
			}
		}
		models = append(models, am)
	}

	return models
}

// OwnedModels returns deduplicated live models advertised by providers owned by
// accountID. Unlike ListModels, it intentionally does not apply the public
// catalog filter; self-route keys may target off-catalog local models.
func (r *Registry) OwnedModels(accountID string) []AggregateModel {
	if accountID == "" {
		return nil
	}
	now := time.Now()
	agg := make(map[string]*AggregateModel)

	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.providers {
		p.mu.Lock()
		eligible := p.AccountID == accountID &&
			p.Status != StatusOffline &&
			p.Status != StatusUntrusted &&
			p.RuntimeVerified &&
			r.providerSupportsPrivateTextLocked(p) &&
			!p.LastChallengeVerified.IsZero() &&
			now.Sub(p.LastChallengeVerified) <= challengeFreshnessMaxAge
		if !eligible {
			p.mu.Unlock()
			continue
		}
		trust := p.TrustLevel
		attested := p.Attested
		attestResult := p.AttestationResult
		models := make([]protocol.ModelInfo, 0, len(p.Models))
		for _, model := range p.Models {
			if r.modelServableForOwnerLocked(p, model) {
				models = append(models, model)
			}
		}
		p.mu.Unlock()

		for _, m := range models {
			if m.ID == "" {
				continue
			}
			// Same principle for the template-render gate: an explicit
			// template_render_ok=false fences EVERY request shape at dispatch
			// (see providerTemplateRenderBrokenLocked / the trait gate), so a
			// render-broken build must not be listed either. nil (pre-0.6.5, no
			// opinion) stays listed, matching dispatch.
			if m.TemplateRenderOK != nil && !*m.TemplateRenderOK {
				continue
			}
			a, ok := agg[m.ID]
			if !ok {
				a = &AggregateModel{
					ID:         m.ID,
					TrustLevel: TrustNone,
				}
				agg[m.ID] = a
			}
			// Metadata backfill rather than first-writer-wins: two owned boxes
			// can advertise the same id with one omitting metadata, and map
			// iteration order must not decide which copy the owner sees.
			if a.ModelType == "" {
				a.ModelType = m.ModelType
			}
			if a.Quantization == "" {
				a.Quantization = m.Quantization
			}
			a.Providers++
			if trustRank(trust) > trustRank(a.TrustLevel) {
				a.TrustLevel = trust
			}
			if attested && attestResult != nil {
				a.AttestedProviders++
				if a.Attestation == nil {
					a.Attestation = &AttestationSummary{}
				}
				a.Attestation.SecureEnclave = a.Attestation.SecureEnclave || attestResult.SecureEnclaveAvailable
				a.Attestation.SIPEnabled = a.Attestation.SIPEnabled || attestResult.SIPEnabled
				a.Attestation.SecureBoot = a.Attestation.SecureBoot || attestResult.SecureBootEnabled
			}
		}
	}

	models := make([]AggregateModel, 0, len(agg))
	for _, a := range agg {
		models = append(models, *a)
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	return models
}

// ModelCountryCodes returns the sorted, de-duplicated ISO 3166-1 alpha-2
// country codes of online providers serving the given model. Used to populate
// the OpenRouter "datacenters" field. Only routing-eligible providers count —
// the same gates as ListModels (online, meets the minimum trust level, and
// private-text ready) — so a country whose providers can't actually serve the
// model is not advertised. Providers without a known location are skipped.
func (r *Registry) ModelCountryCodes(modelID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool)
	for _, p := range r.providers {
		p.mu.Lock()
		status := p.Status
		trust := p.TrustLevel
		privateReady := r.providerSupportsPrivateTextLocked(p)
		var cc string
		if p.Location != nil {
			cc = strings.ToUpper(strings.TrimSpace(p.Location.CountryCode))
		}
		serves := cc != "" && r.providerServesCatalogModelLocked(p, modelID)
		p.mu.Unlock()
		if !serves {
			continue
		}
		// Apply the same routing-eligibility gates as ListModels.
		if status == StatusOffline || status == StatusUntrusted {
			continue
		}
		if !r.trustMeetsMinimum(trust) || !privateReady {
			continue
		}
		seen[cc] = true
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// OnlineCount returns the number of online providers.
func (r *Registry) OnlineCount() int64 {
	return r.onlineCount.Load()
}

// ModelProviderSnapshot returns live catalog-eligible provider-model counts.
// Raw inventory counters remain forensic bookkeeping; this public snapshot is
// derived so catalog requirement changes take effect immediately.
func (r *Registry) ModelProviderSnapshot() map[string]int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snap := make(map[string]int64)
	for _, p := range r.providers {
		p.mu.Lock()
		if p.Status == StatusOffline || p.Status == StatusUntrusted {
			p.mu.Unlock()
			continue
		}
		seen := make(map[string]struct{}, len(p.Models))
		for _, model := range p.Models {
			if model.ID == "" || !r.providerModelAllowedByCatalogLocked(p, model) {
				continue
			}
			if _, duplicate := seen[model.ID]; duplicate {
				continue
			}
			seen[model.ID] = struct{}{}
			snap[model.ID]++
		}
		p.mu.Unlock()
	}
	return snap
}

func (r *Registry) ProviderCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.providers)
}

func (r *Registry) ProviderCountByVersion() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	counts := make(map[string]int)
	for _, p := range r.providers {
		p.mu.Lock()
		online := p.Status != StatusOffline && p.Status != StatusUntrusted
		p.mu.Unlock()
		if !online {
			continue
		}
		ver := p.Version
		if ver == "" {
			ver = "unknown"
		}
		counts[ver]++
	}
	return counts
}

// TrustStatusCount is one bucket of the fleet trust-state gauge.
type TrustStatusCount struct {
	TrustLevel string
	Status     string
	Count      int
}

// ProviderCountByTrustStatus buckets every connected provider by
// (trust_level, status) so the coordinator can alert on a growing
// self_signed/untrusted cohort. Offline providers are excluded (they are not a
// live routability problem). Unlike most gauges this includes untrusted, since
// the untrusted cohort is exactly what we want visibility into.
func (r *Registry) ProviderCountByTrustStatus() []TrustStatusCount {
	r.mu.RLock()
	defer r.mu.RUnlock()
	type key struct{ trust, status string }
	counts := make(map[key]int)
	for _, p := range r.providers {
		p.mu.Lock()
		status := p.Status
		trust := p.TrustLevel
		p.mu.Unlock()
		if status == StatusOffline {
			continue
		}
		counts[key{string(trust), string(status)}]++
	}
	out := make([]TrustStatusCount, 0, len(counts))
	for k, n := range counts {
		out = append(out, TrustStatusCount{TrustLevel: k.trust, Status: k.status, Count: n})
	}
	return out
}

// ProviderCountByMDMFailure buckets connected, non-hardware providers by their
// last MDM verification failure reason (device-not-found, found-not-enrolled,
// securityinfo-timeout, posture-mismatch, error). This is the stuck-cohort
// breakdown: it distinguishes "never enrolled" from "enrolled but the live
// SecurityInfo check is timing out" so an operator knows whether the problem is
// provider-side enrollment or APNs/MDM delivery. Hardware providers (reason
// cleared) are excluded.
func (r *Registry) ProviderCountByMDMFailure() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	counts := make(map[string]int)
	for _, p := range r.providers {
		p.mu.Lock()
		status := p.Status
		trust := p.TrustLevel
		reason := p.MDMFailureReason
		p.mu.Unlock()
		if status == StatusOffline || trust == TrustHardware {
			continue
		}
		if reason == "" {
			reason = "pending"
		}
		counts[reason]++
	}
	return counts
}

// FleetSnapshot is the read-only summary used by metrics polling. We
// don't lock individual providers — counts may be off-by-one under
// heavy churn — that's acceptable for gauges.
type FleetSnapshot struct {
	Connected  int
	Idle       int
	QueueDepth int
}

// Snapshot returns aggregate counts for /metrics gauges. Cheap enough
// to call every few seconds. Takes the registry's read lock for the
// outer iteration AND each provider's mutex briefly to read Status and
// pending count — those fields are written under p.mu elsewhere
// (Heartbeat, AddPending, RemovePending), so reading them without
// p.mu is a data race even if the gauge value is only advisory.
func (r *Registry) Snapshot() FleetSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	idle := 0
	for _, p := range r.providers {
		p.mu.Lock()
		isIdle := p.Status == StatusOnline && len(p.pendingReqs) == 0
		p.mu.Unlock()
		if isIdle {
			idle++
		}
	}
	q := 0
	if r.queue != nil {
		q = r.queue.TotalSize()
	}
	return FleetSnapshot{
		Connected:  len(r.providers),
		Idle:       idle,
		QueueDepth: q,
	}
}

// ModelCapacity describes the live capacity for a single model.
type ModelCapacity struct {
	ModelID              string  `json:"id"`
	Ready                bool    `json:"ready"`                  // at least one routable provider with headroom
	CanAccept            bool    `json:"can_accept"`             // ready AND queue not full
	RoutableProviders    int     `json:"routable_providers"`     // passed all gates
	WarmProviders        int     `json:"warm_providers"`         // model loaded (slot state "running" or "idle")
	RunningProviders     int     `json:"running_providers"`      // model loaded with active requests (slot state "running")
	ColdProviders        int     `json:"cold_providers"`         // model available but not loaded
	ActiveRequests       int     `json:"active_requests"`        // in-flight across fleet
	QueuedRequests       int     `json:"queued_requests"`        // waiting in coordinator queue
	QueueLimit           int     `json:"queue_limit"`            // max queue depth per model
	AggregateTPS         float64 `json:"aggregate_tps"`          // sum of effective decode TPS
	EstimatedTTFTMs      int64   `json:"estimated_ttft_ms"`      // best-case TTFT from lowest-cost warm provider
	TokenBudgetRemaining int64   `json:"token_budget_remaining"` // aggregate free budget across providers
	TokenBudgetTotal     int64   `json:"token_budget_total"`     // aggregate total budget
}

// providerCapSnap is a per-provider snapshot collected under the registry
// lock, then aggregated into ModelCapacity outside the lock.
type providerCapSnap struct {
	model                 string
	warm                  bool
	running               bool
	hasHeadroom           bool // pending < maxConcurrency
	effectiveTPS          float64
	prefillTPS            float64
	activeRequests        int // numRunning + numWaiting from backend slot, or pendingCount
	backlogTokens         float64
	activeTokenBudgetMax  int64
	activeTokenBudgetUsed int64
	queuedTokenBudget     int64
	// tokenBudgetKnownZero distinguishes an Engine V2 model whose positive KV
	// rate makes max==0 authoritative from a legacy model that omitted both.
	tokenBudgetKnownZero bool
	// pooledBudgetRemaining is the provider's whole-box pooled token budget
	// left after charging ALL models' coordinator-pending tokens — the same
	// pool the admission gate (pooledBudgetAdmits) enforces, so this public
	// capacity feed cannot advertise per-slot headroom dispatch would reject.
	// Reconstruction counts legacy shared headroom once and v0.7.5+ private
	// grants additively. -1 means no pooled budget report.
	pooledBudgetRemaining int64
}

// publiclyRoutableLocked reports whether a provider passes the public routing
// gates (status, privacy, trust, runtime, private-text support, challenge
// freshness). The caller must hold r.mu (read) and p.mu. It is shared by
// ModelCapacitySnapshot and FleetCapacitySnapshot so both count the same set of
// providers.
func (r *Registry) publiclyRoutableLocked(p *Provider, now time.Time) bool {
	// The public routing gate is exactly the liveness/trust/privacy core with no
	// owner relaxation — private-only machines never serve the public fleet.
	return r.providerLivenessGateLocked(p, r.MinTrustLevel, false, now)
}

// ModelCapacitySnapshot returns a capacity snapshot for every model served
// by at least one provider. Providers must pass the same routing gates as
// snapshotProviderIntoLockedEx (status, trust, runtime, privacy, challenge
// freshness, concurrency headroom) to be counted as routable.
func (r *Registry) ModelCapacitySnapshot() []ModelCapacity {
	now := time.Now()

	// Phase 1: collect per-provider snapshots under the lock.
	var snaps []providerCapSnap

	r.mu.RLock()
	for _, p := range r.providers {
		p.mu.Lock()

		// Apply the same gates as snapshotProviderIntoLockedEx. Private-only machines
		// never serve the public fleet, so they do not count toward public
		// model capacity.
		if !r.publiclyRoutableLocked(p, now) {
			p.mu.Unlock()
			continue
		}

		decodeTPS := resolvedDecodeTPS(p)
		prefillTPS := resolvedPrefillTPS(p)

		// Reconstruct the whole-box pooled budget and its all-models
		// coordinator-pending charges (token and, when every pending request
		// normalizes, byte) ONCE per provider — the SAME accumulation the
		// admission gate uses (fillSnapshotPendingAndPool). The per-model
		// remaining differs only by that model's KV rate in byte mode, so it is
		// finalized inside the model loop via pooledRemainingTokens, keeping this
		// feed's verdict identical to pooledBudgetAdmits' on a mixed-KV box (a
		// pool exhausted in BYTES by a small-KV burst must not surface token
		// headroom for a big-KV co-resident). Token units out; byte
		// normalization stays internal.
		var poolSnap routingSnapshot
		if p.BackendCapacity != nil {
			fillSnapshotPendingAndPool(&poolSnap, p, "")
		}

		// Enumerate every model this provider serves.
		for _, m := range p.Models {
			if !r.providerModelAllowedByCatalogLocked(p, m) {
				continue
			}
			// Use the SAME quality-concurrency-capped headroom the routing/preflight
			// path enforces, so the public capacity feed doesn't advertise a capped
			// box (e.g. Gemma at 2) as routable up to the flat fallback (24) and lure
			// upstream routers into sending requests this coordinator immediately 429s.
			hasHeadroom := r.hasConcurrencyHeadroomForModelCapResolvedLocked(p, m.ID)
			// Count only pending requests for this specific model, not the
			// total across all models. Using the total inflates
			// activeRequests for multi-model providers.
			modelPending := 0
			for _, pr := range p.pendingReqs {
				if pr.Model == m.ID {
					modelPending++
				}
			}

			// Per-model pooled remaining: byte-aware when the box is byte-
			// reconstructable, else token accounting — exactly pooledBudgetAdmits'
			// branch. Cold/absent slots have no rate (map miss ⇒ 0); on a byte-
			// reconstructable pool they are priced at the greater of the
			// conservative coordinator default and the box's max resident rate
			// (the same cold-rate resolver the gate uses), so this feed stays
			// equivalent to the gate on the cold path too. Inert for legacy boxes.
			pooledRemaining := pooledRemainingTokens(
				poolSnap.pooledTokenBudget,
				poolSnap.pendingMaxTokensAllModels,
				poolSnap.pendingMaxBytesAllModels,
				poolSnap.pendingBytesKnown,
				poolSnap.pooledTokenBudget.kvRateFor(m.ID),
			)

			snap := providerCapSnap{
				model:                 m.ID,
				hasHeadroom:           hasHeadroom,
				effectiveTPS:          decodeTPS,
				prefillTPS:            prefillTPS,
				activeRequests:        modelPending,
				pooledBudgetRemaining: pooledRemaining,
			}

			// Check backend capacity for this model's slot.
			if p.BackendCapacity != nil {
				for _, slot := range p.BackendCapacity.Slots {
					if slot.Model != m.ID {
						continue
					}
					snap.warm = slotStateModelLoaded(slot.State)
					snap.running = slot.State == "running"
					slotActive := int(slot.NumRunning) + int(slot.NumWaiting)
					if slotActive > snap.activeRequests {
						snap.activeRequests = slotActive
					}
					if slot.ObservedDecodeTPS > 0 {
						snap.effectiveTPS = slot.ObservedDecodeTPS
					}
					// Prefer the measured per-slot prefill EWMA over the ×12
					// fallback for the capacity TTFT estimate, mirroring the
					// routing path (resolvePrefillTPS). 0 = unreported.
					if slot.ObservedPrefillTPS > 0 {
						snap.prefillTPS = slot.ObservedPrefillTPS
					}
					snap.activeTokenBudgetMax = slot.ActiveTokenBudgetMax
					snap.activeTokenBudgetUsed = slot.ActiveTokenBudgetUsed
					snap.queuedTokenBudget = slot.QueuedTokenBudget
					snap.tokenBudgetKnownZero = knownZeroTokenBudget(slot.ActiveTokenBudgetMax, slot.KVBytesPerToken)
					snap.backlogTokens = float64(slot.MaxTokensPotential)
					break
				}
			} else {
				// Without backend capacity, warm if currently serving this model.
				snap.warm = p.CurrentModel == m.ID
			}

			snaps = append(snaps, snap)
		}
		p.mu.Unlock()
	}
	r.mu.RUnlock()

	// Phase 2: aggregate per-model outside the lock.
	type modelAgg struct {
		routable         int
		warm             int
		running          int
		cold             int
		activeRequests   int
		aggregateTPS     float64
		budgetRemaining  int64
		budgetTotal      int64
		bestWarmTTFTMs   int64 // -1 = not set
		bestColdTTFTMs   int64 // -1 = not set
		anyImmediateSlot bool  // at least one provider with headroom
	}
	agg := make(map[string]*modelAgg)
	for _, s := range snaps {
		a, ok := agg[s.model]
		if !ok {
			a = &modelAgg{bestWarmTTFTMs: -1, bestColdTTFTMs: -1}
			agg[s.model] = a
		}
		if s.warm {
			a.warm++
			if s.running {
				a.running++
			}
		} else {
			a.cold++
		}
		a.activeRequests += s.activeRequests
		a.aggregateTPS += s.effectiveTPS
		if s.activeTokenBudgetMax > 0 {
			headroom := s.activeTokenBudgetMax - s.activeTokenBudgetUsed - s.queuedTokenBudget
			if headroom < 0 {
				headroom = 0
			}
			// Per-slot headroom cannot exceed the provider's pooled remaining
			// after all-model pending charges. Without the clamp this surface can
			// advertise capacity pooledBudgetAdmits rejects.
			if s.pooledBudgetRemaining >= 0 && headroom > s.pooledBudgetRemaining {
				headroom = s.pooledBudgetRemaining
			}
			a.budgetRemaining += headroom
			a.budgetTotal += s.activeTokenBudgetMax
		}
		// Routable providers require both concurrency headroom AND token-budget
		// headroom. A provider with exhausted token budget should not make the
		// model appear immediately ready. An exhausted POOLED budget (0 — not
		// the -1 no-budget sentinel) counts as exhausted for every model on the
		// box, cold ones included: the admission gate charges those against the
		// whole-box pool too (freeMemoryAdmits' cold-slot pooled gate).
		hasBudgetHeadroom := !s.tokenBudgetKnownZero && (s.activeTokenBudgetMax <= 0 ||
			s.activeTokenBudgetUsed+s.queuedTokenBudget < s.activeTokenBudgetMax) &&
			s.pooledBudgetRemaining != 0
		if s.hasHeadroom && hasBudgetHeadroom {
			a.routable++
			a.anyImmediateSlot = true
		}

		// Estimate TTFT for this provider: prefill 500 tokens + backlog drain.
		const defaultPromptTokens = 500
		ttftMs := int64(0)
		if s.prefillTPS > 0 {
			ttftMs = int64(float64(defaultPromptTokens) / s.prefillTPS * 1000)
		}
		if s.effectiveTPS > 0 {
			ttftMs += int64(s.backlogTokens / s.effectiveTPS * 1000)
		}
		if s.warm {
			if a.bestWarmTTFTMs < 0 || ttftMs < a.bestWarmTTFTMs {
				a.bestWarmTTFTMs = ttftMs
			}
		} else {
			coldTTFT := ttftMs + 20_000 // 20s cold-start penalty
			if a.bestColdTTFTMs < 0 || coldTTFT < a.bestColdTTFTMs {
				a.bestColdTTFTMs = coldTTFT
			}
		}
	}

	// Phase 3: read queue sizes (separate lock, safe to call after releasing r.mu).
	queue := r.Queue()
	queueLimit := 0
	if queue != nil {
		queueLimit = queue.MaxSize()
	}

	result := make([]ModelCapacity, 0, len(agg))
	for model, a := range agg {
		queued := 0
		if queue != nil {
			queued = queue.QueueSize(model)
		}
		ready := a.routable > 0
		canAccept := ready && (queued < queueLimit || a.anyImmediateSlot)

		ttft := a.bestWarmTTFTMs
		if ttft < 0 {
			ttft = a.bestColdTTFTMs
		}
		if ttft < 0 {
			ttft = 0
		}

		result = append(result, ModelCapacity{
			ModelID:              model,
			Ready:                ready,
			CanAccept:            canAccept,
			RoutableProviders:    a.routable,
			WarmProviders:        a.warm,
			RunningProviders:     a.running,
			ColdProviders:        a.cold,
			ActiveRequests:       a.activeRequests,
			QueuedRequests:       queued,
			QueueLimit:           queueLimit,
			AggregateTPS:         a.aggregateTPS,
			EstimatedTTFTMs:      ttft,
			TokenBudgetRemaining: a.budgetRemaining,
			TokenBudgetTotal:     a.budgetTotal,
		})
	}
	return result
}
