package registry

// Provider registration, heartbeat, disconnect, and eviction lifecycle.

import (
	"context"
	"encoding/base64"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/saferun"
	"nhooyr.io/websocket"
)

// Sanity caps on provider-reported stats. A malicious (or broken) provider
// could otherwise report absurd values to monopolize routing. These caps are
// ~3-4x current hardware ceilings (M2 Ultra is ~800 GB/s, MLX decode is ~120
// tok/s, max Mac Studio RAM is 512 GB) so legitimate future hardware isn't
// clamped unnecessarily.
const (
	maxDecodeTPS                    = 500.0
	maxPrefillTPS                   = 5000.0
	maxMemoryBandwidthGBs           = 2000.0
	maxMemoryGB                     = 1024
	maxMemoryGBFloat                = 1024.0
	maxReportedMaxConcurrency       = 24
	maxTokensPotential              = 1_000_000
	maxTokenBudgetCap         int64 = 10_000_000_000 // 10 billion — generous safety valve for total token budget capacity
	maxModelLoadTimeMS        int64 = 3_600_000      // 1 hour — generous ceiling for a cold-start model load; larger is implausible/garbage
)

// clampNonNeg returns v clamped into [0, max]; NaN/negative become 0.
// The bool is true if the value was out of range.
func clampNonNeg(v, max float64) (float64, bool) {
	if math.IsNaN(v) || v < 0 {
		return 0, true
	}
	if v > max {
		return max, true
	}
	return v, false
}

// clampBackendCapacity applies sanity caps to provider-reported backend
// capacity fields that feed the routing scorer. A provider reporting
// TotalMemoryGB=1e9 would make gpuUtil ~= 0 and dodge health penalties, so
// we cap it at maxMemoryGBFloat. Same for MaxTokensPotential which directly
// controls backlog cost. NaN/negative become 0.
func clampBackendCapacity(logger *slog.Logger, providerID string, bc *protocol.BackendCapacity) {
	if bc == nil {
		return
	}
	if v, changed := clampNonNeg(bc.TotalMemoryGB, maxMemoryGBFloat); changed {
		logger.Warn("provider total_memory_gb out of range, clamping",
			"provider_id", providerID, "reported", bc.TotalMemoryGB, "clamped", v)
		bc.TotalMemoryGB = v
	}
	if v, changed := clampNonNeg(bc.GPUMemoryActiveGB, maxMemoryGBFloat); changed {
		logger.Warn("provider gpu_memory_active_gb out of range, clamping",
			"provider_id", providerID, "reported", bc.GPUMemoryActiveGB, "clamped", v)
		bc.GPUMemoryActiveGB = v
	}
	if v, changed := clampNonNeg(bc.GPUMemoryPeakGB, maxMemoryGBFloat); changed {
		bc.GPUMemoryPeakGB = v
	}
	if v, changed := clampNonNeg(bc.GPUMemoryCacheGB, maxMemoryGBFloat); changed {
		bc.GPUMemoryCacheGB = v
	}
	// free_for_load_gb: an out-of-range value (NaN/Inf/negative or absurdly high)
	// is treated as NOT reported (nil) so the cold-load gate falls back to the
	// total-memory heuristic, rather than trusting a garbage value that would
	// over- or under-admit. A legitimate 0 ("can't load anything now") is kept.
	if bc.FreeForLoadGB != nil {
		v := *bc.FreeForLoadGB
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > maxMemoryGBFloat {
			logger.Warn("provider free_for_load_gb out of range; ignoring (fall back to heuristic)",
				"provider_id", providerID, "reported", v)
			bc.FreeForLoadGB = nil
		}
	}
	for i := range bc.Slots {
		s := &bc.Slots[i]
		if s.MaxTokensPotential < 0 || s.MaxTokensPotential > maxTokensPotential {
			logger.Warn("provider slot max_tokens_potential out of range, clamping",
				"provider_id", providerID, "model", s.Model, "reported", s.MaxTokensPotential)
			if s.MaxTokensPotential < 0 {
				s.MaxTokensPotential = 0
			} else {
				s.MaxTokensPotential = maxTokensPotential
			}
		}
		if s.NumRunning < 0 {
			s.NumRunning = 0
		}
		if s.NumWaiting < 0 {
			s.NumWaiting = 0
		}
		if s.MaxConcurrency < 0 || s.MaxConcurrency > maxReportedMaxConcurrency {
			logger.Warn("provider slot max_concurrency out of range, clamping",
				"provider_id", providerID, "model", s.Model, "reported", s.MaxConcurrency)
			if s.MaxConcurrency < 0 {
				s.MaxConcurrency = 0
			} else {
				s.MaxConcurrency = maxReportedMaxConcurrency
			}
		}
		if v, changed := clampNonNeg(s.ObservedDecodeTPS, maxDecodeTPS); changed {
			logger.Warn("provider slot observed_decode_tps out of range, clamping",
				"provider_id", providerID, "model", s.Model, "reported", s.ObservedDecodeTPS, "clamped", v)
			s.ObservedDecodeTPS = v
		}
		// observed_prefill_tps: an out-of-range value (NaN/negative, or absurdly
		// high — a known provider-side overflow when the admitted→first-token
		// window collapses on a prefix-cache hit) is treated as NO measurement (0)
		// rather than clamped to the ceiling. Clamping garbage UP to maxPrefillTPS
		// would make the TTFT estimate over-optimistic (prefill looks instant) and
		// the hard gate over-accept; zeroing it makes resolvePrefillTPS fall back to
		// the conservative decode×ratio estimate until the provider reports a sane
		// value (provider fix: only sample cold prefills).
		if math.IsNaN(s.ObservedPrefillTPS) || s.ObservedPrefillTPS < 0 || s.ObservedPrefillTPS > maxPrefillTPS {
			logger.Warn("provider slot observed_prefill_tps out of range; ignoring (fall back to estimate)",
				"provider_id", providerID, "model", s.Model, "reported", s.ObservedPrefillTPS)
			s.ObservedPrefillTPS = 0
		}
		if s.ModelLoadTimeMS < 0 || s.ModelLoadTimeMS > maxModelLoadTimeMS {
			logger.Warn("provider slot model_load_time_ms out of range, clamping",
				"provider_id", providerID, "model", s.Model, "reported", s.ModelLoadTimeMS)
			if s.ModelLoadTimeMS < 0 {
				s.ModelLoadTimeMS = 0
			} else {
				s.ModelLoadTimeMS = maxModelLoadTimeMS
			}
		}
		if s.ActiveTokenBudgetUsed < 0 || s.ActiveTokenBudgetUsed > maxTokenBudgetCap {
			if s.ActiveTokenBudgetUsed < 0 {
				s.ActiveTokenBudgetUsed = 0
			} else {
				s.ActiveTokenBudgetUsed = maxTokenBudgetCap
			}
		}
		if s.ActiveTokenBudgetMax < 0 || s.ActiveTokenBudgetMax > maxTokenBudgetCap {
			if s.ActiveTokenBudgetMax < 0 {
				s.ActiveTokenBudgetMax = 0
			} else {
				s.ActiveTokenBudgetMax = maxTokenBudgetCap
			}
		}
		if s.QueuedTokenBudget < 0 || s.QueuedTokenBudget > maxTokenBudgetCap {
			if s.QueuedTokenBudget < 0 {
				s.QueuedTokenBudget = 0
			} else {
				s.QueuedTokenBudget = maxTokenBudgetCap
			}
		}
		if t := s.Telemetry; t != nil {
			// System-profiler slot telemetry (measurement only). Silent
			// clamps, like the token-budget fields above: nothing routes on
			// these, so a bad value is not worth a log line per heartbeat.
			// t is the registry-owned clone made by canonicalHeartbeatModelState.
			clampTelemetryCount(t.QueuedPrefillTokens)
			clampTelemetryCount(t.PartialPrefillRows)
			clampTelemetryCount(t.PrefillTokensTotal)
			clampTelemetryCount(t.PumpTasks)
			clampTelemetryCount(t.MTPRoundsTotal)
			clampTelemetryCount(t.MTPProposedTotal)
			clampTelemetryCount(t.MTPAcceptedTotal)
			clampTelemetryCount(t.DecodeRowsTotal)
			clampTelemetryInt64(t.KVBytesInUse, maxTelemetryBytes)
			clampTelemetryInt64(t.KVBytesCapacity, maxTelemetryBytes)
			clampTelemetryInt64(t.EvalInFlightMS, maxTelemetryMS)
			// Cumulative ns of engine step wall time: a count cap would wrap
			// after ~17 min of stepping, so it gets the wide ns bound.
			clampTelemetryInt64(t.StepWallNSTotal, maxTelemetryNSTotal)
			if p := t.IsolatedPrefillTPS; p != nil {
				if math.IsNaN(*p) || math.IsInf(*p, 0) {
					t.IsolatedPrefillTPS = nil // garbage reads as "not reported"
				} else if v, changed := clampNonNeg(*p, maxTelemetryTPS); changed {
					*p = v
				}
			}
		}
	}
	if t := bc.Telemetry; t != nil {
		clampTelemetryCount(t.MLXNumResources)
		clampTelemetryCount(t.InAdmission)
		clampTelemetryCount(t.InflightTasks)
		t.MemoryPressureLevel = t.MemoryPressureLevel.Fold()
	}
}

// System-profiler heartbeat telemetry bounds (CONTRACT-WIRE.md §2). Pointer
// numerics are clamped in place into [0, max]; nil (absent) is left alone so
// presence semantics survive.
const (
	maxTelemetryCount   int64   = 1_000_000_000_000 // 1e12
	maxTelemetryBytes   int64   = 1 << 48
	maxTelemetryMS      int64   = 3_600_000                 // 1 h
	maxTelemetryNSTotal int64   = 1_000_000_000_000_000_000 // 1e18 ≈ 31 y of cumulative ns
	maxTelemetryTPS     float64 = 20_000
)

func clampTelemetryInt64(p *int64, limit int64) {
	if p == nil {
		return
	}
	if *p < 0 {
		*p = 0
	} else if *p > limit {
		*p = limit
	}
}

func clampTelemetryCount(p *int64) { clampTelemetryInt64(p, maxTelemetryCount) }

// Register adds a new provider to the registry, returning its assigned ID.
// Provider-reported model inventory is preserved even when the current catalog
// denies every model; catalog checks are applied dynamically during routing so
// providers that connect before a model is promoted become routable immediately
// after the catalog is updated.
func (r *Registry) Register(id string, conn *websocket.Conn, msg *protocol.RegisterMessage) *Provider {
	r.mu.RLock()
	existing := r.providers[id]
	r.mu.RUnlock()
	if existing != nil {
		r.logger.Warn("duplicate provider registration ignored", "provider_id", id)
		return existing
	}
	// Clamp provider-reported performance stats used in routing score.
	// Refuse to trust unbounded values — a malicious provider reporting
	// DecodeTPS=1e9 would otherwise starve all other providers.
	if v, changed := clampNonNeg(msg.DecodeTPS, maxDecodeTPS); changed {
		r.logger.Warn("provider decode_tps out of range, clamping",
			"provider_id", id, "reported", msg.DecodeTPS, "clamped", v)
		msg.DecodeTPS = v
	}
	if v, changed := clampNonNeg(msg.PrefillTPS, maxPrefillTPS); changed {
		r.logger.Warn("provider prefill_tps out of range, clamping",
			"provider_id", id, "reported", msg.PrefillTPS, "clamped", v)
		msg.PrefillTPS = v
	}
	if v, changed := clampNonNeg(msg.Hardware.MemoryBandwidthGBs, maxMemoryBandwidthGBs); changed {
		r.logger.Warn("provider memory_bandwidth_gbs out of range, clamping",
			"provider_id", id, "reported", msg.Hardware.MemoryBandwidthGBs, "clamped", v)
		msg.Hardware.MemoryBandwidthGBs = v
	}
	if msg.Hardware.MemoryGB < 0 || msg.Hardware.MemoryGB > maxMemoryGB {
		r.logger.Warn("provider memory_gb out of range, clamping",
			"provider_id", id, "reported", msg.Hardware.MemoryGB)
		if msg.Hardware.MemoryGB < 0 {
			msg.Hardware.MemoryGB = 0
		} else {
			msg.Hardware.MemoryGB = maxMemoryGB
		}
	}

	models := msg.Models
	modelInventory, _ := uniqueProviderModels(models)
	cacheStatuses, cacheStatusReported := sanitizePrefixCacheStatuses(
		msg.PrefixCacheStatuses, modelInventory)
	cacheDonationOutcomes := sanitizePrefixCacheDonationOutcomes(
		msg.PrefixCacheDonationOutcomes)
	cacheCapabilities := prefixCacheV2CapabilityMap(msg.PrefixCacheV2Models)
	cacheStatuses, cacheStatusReported = reconcilePrefixCacheStatuses(
		msg.PrefixCacheProtocol,
		cacheCapabilities,
		cacheStatuses,
		cacheStatusReported,
	)

	// Validate X25519 public key if provided.
	// Reject invalid keys at registration rather than failing at encryption time.
	pubKey := msg.PublicKey
	if pubKey != "" {
		decoded, err := base64.StdEncoding.DecodeString(pubKey)
		if err != nil || len(decoded) != 32 {
			r.logger.Warn("provider public key invalid, clearing",
				"provider_id", id,
				"error", "must be 32-byte base64-encoded X25519 key",
			)
			pubKey = "" // clear so provider can register but won't receive encrypted requests
		}
	}

	p := &Provider{
		ID:                          id,
		Hardware:                    msg.Hardware,
		Models:                      models,
		Backend:                     msg.Backend,
		ReportedRuntimeCapabilities: normalizeRuntimeCapabilities(msg.RuntimeCapabilities, msg.Hardware),
		RuntimeCapabilities:         nil,
		PublicKey:                   pubKey,
		EncryptedResponseChunks:     msg.EncryptedResponseChunks,
		PrivateOnly:                 msg.PrivateOnly,
		APNsDeviceToken:             msg.APNsDeviceToken,
		APNsEnvironment:             msg.APNsEnvironment,
		PrefillTPS:                  msg.PrefillTPS,
		DecodeTPS:                   msg.DecodeTPS,
		PrefixCacheProtocol:         msg.PrefixCacheProtocol,
		PrefixCacheV2Models:         cacheCapabilities,
		PrefixCacheStatuses:         cacheStatuses,
		PrefixCacheStatusReported:   cacheStatusReported,
		PrefixCacheDonationOutcomes: cacheDonationOutcomes,
		ToolConstraintProtocol:      msg.ToolConstraintProtocol,
		ToolConstraintModels:        toolConstraintModelSet(msg.ToolConstraintModels, msg.Models),
		TrustLevel:                  TrustNone,
		RuntimeVerified:             true,  // default to verified; API layer sets false when manifest check fails
		RuntimeManifestChecked:      true,  // default to true; API layer sets false when no manifest is configured
		ChallengeVerifiedSIP:        false, // starts false; set true by attestation challenge handler after SIP check
		PrivacyCapabilities:         msg.PrivacyCapabilities,
		TemplateHashes:              CloneStringMap(msg.TemplateHashes),
		Status:                      StatusOnline,
		Conn:                        conn,
		writer:                      newProviderWriter(conn),
		LastHeartbeat:               time.Now(),
		Reputation:                  NewReputation(),
		pendingReqs:                 make(map[string]*PendingRequest),
		applicationProofSettled:     make(chan struct{}),
		challengeKick:               make(chan struct{}, 1),
		registry:                    r,
	}

	r.mu.Lock()
	if existing, exists := r.providers[id]; exists {
		// A connection identity owns exactly one Provider state. Returning the
		// original object keeps capabilities, counters, and pending state stable
		// if an accidental second registration reaches this defense.
		r.mu.Unlock()
		r.logger.Warn("duplicate provider registration ignored", "provider_id", id)
		return existing
	}
	r.providers[id] = p
	r.attachSessionGate(p)
	p.mu.Lock()
	r.modelIndex.sync(p)
	p.mu.Unlock()
	r.onlineCount.Add(1)
	for _, m := range models {
		r.modelProviderInc(m.ID)
	}
	// Fault-tracking state (breakers, cooldowns) is deliberately NOT cleared
	// here: it is keyed by stable identity and re-attaches when attestation
	// binds this session id (SetAttestationResult → bindStableFaultKey). The
	// old register-time clear was the reconnect exploit — a churning zombie
	// wiped its record every session.
	r.mu.Unlock()

	// Open a session row for this connection (async; durable uptime history).
	// serial/account are empty here (set after attestation/linking) and are
	// backfilled by the throttled TouchProviderSession in persistProviderNow.
	if r.store != nil {
		sessionID := p.ID
		saferun.Go(r.logger, "registry.openSession", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.store.OpenProviderSession(ctx, sessionID, "", ""); err != nil {
				r.logger.Warn("failed to open provider session", "provider_id", sessionID, "error", err)
			}
		})
	}

	r.logger.Info("provider registered",
		"provider_id", id,
		"chip", msg.Hardware.ChipName,
		"memory_gb", msg.Hardware.MemoryGB,
		"models", len(msg.Models),
		"backend", msg.Backend,
		"prefill_tps", msg.PrefillTPS,
		"decode_tps", msg.DecodeTPS,
	)

	// Persist provider record to store (async).
	r.persistProviderNow(p)

	return p
}

func CloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// DisconnectDuplicatesBySerial disconnects all providers that share the same
// serial number as the given provider, except the given provider itself.
// This prevents multiple WebSocket connections from the same physical machine
// from competing for the same MLX-Swift backend on the host.
func (r *Registry) DisconnectDuplicatesBySerial(keepID string, serial string) {
	if serial == "" {
		return
	}

	var toEvict []string

	r.mu.RLock()
	for id, p := range r.providers {
		if id == keepID {
			continue
		}
		if p.AttestationResult != nil && p.AttestationResult.SerialNumber == serial {
			toEvict = append(toEvict, id)
		}
	}
	r.mu.RUnlock()

	for _, id := range toEvict {
		r.logger.Warn("evicting duplicate provider from same device",
			"evicted_id", id,
			"kept_id", keepID,
			"serial", serial,
		)
		// Disconnect closes the socket itself.
		r.Disconnect(id)
	}
}

// RemoveProviderBySerial reports whether any currently-connected provider
// matches the identity (serial OR session id) and, if force is set, evicts them
// from the in-memory map. The DELETE endpoint calls it first with force=false
// to detect an online box (→409), then after the persisted record is purged it
// may call with force=true to drop a lingering in-memory entry so an evict-race
// can't re-persist. Returns true if a matching provider was connected.
func (r *Registry) RemoveProviderBySerial(serialOrID string, force bool) (online bool) {
	if serialOrID == "" {
		return false
	}

	var matched []string
	r.mu.RLock()
	for id, p := range r.providers {
		match := id == serialOrID
		if !match {
			// AttestationResult is written under p.mu (SetAttestationResult), so
			// read it through the thread-safe accessor — this loop holds only the
			// registry lock, not the per-provider one.
			if ar := p.GetAttestationResult(); ar != nil && ar.SerialNumber == serialOrID {
				match = true
			}
		}
		if match {
			matched = append(matched, id)
			// Presence in the map means a live WebSocket connection; treat it as
			// online regardless of routing status (an untrusted-but-connected box
			// would still re-register and re-persist).
			online = true
		}
	}
	r.mu.RUnlock()

	if force {
		// Disconnect takes r.mu itself — call OUTSIDE the RLock above to avoid a
		// self-deadlock (same pattern as DisconnectDuplicatesBySerial).
		for _, id := range matched {
			r.Disconnect(id)
		}
	}
	return online
}

// Heartbeat updates the provider's status and stats and reports whether the
// snapshot was accepted. Rejected stale snapshots still advance liveness.
func (r *Registry) Heartbeat(id string, msg *protocol.HeartbeatMessage) bool {
	r.mu.RLock()
	p, ok := r.providers[id]
	if !ok {
		r.mu.RUnlock()
		r.logger.Warn("heartbeat from unknown provider", "provider_id", id)
		return false
	}

	// Work from registry-owned copies so clamping and retention never mutate the
	// decoded provider message. Model-bearing fields are canonicalized after
	// taking p.mu below, against the same p.Models snapshot that remains
	// authoritative for the rest of this heartbeat.
	systemMetrics := msg.SystemMetrics
	if v, changed := clampNonNeg(systemMetrics.MemoryPressure, 1.0); changed {
		systemMetrics.MemoryPressure = v
	}
	if v, changed := clampNonNeg(systemMetrics.CPUUsage, 1.0); changed {
		systemMetrics.CPUUsage = v
	}

	p.mu.Lock()
	eligibleModels := make([]protocol.ModelInfo, 0, len(p.Models))
	for _, model := range p.Models {
		if r.providerModelAllowedByCatalogLocked(p, model) {
			eligibleModels = append(eligibleModels, model)
		}
	}
	warmModels, currentModel, backendCapacity := canonicalHeartbeatModelState(
		eligibleModels, msg.WarmModels, msg.ActiveModel, msg.BackendCapacity)
	r.mu.RUnlock()
	// Routing v2 W2 — capacity_seq gate. Event-triggered heartbeats share the
	// bounded data lane with the 5s baseline, so an event frame published
	// AFTER a baseline frame can be decoded BEFORE it (two frames in the
	// writer queue, read-loop dispatch order vs. publish order is not the
	// coordinator's to assume). Applying the older snapshot second would
	// regress fresher slot/budget state — exactly the staleness window the
	// event heartbeats exist to close. Seq ordering is per-connection: a
	// reconnect restarts the provider's counter AND creates a fresh *Provider
	// (capacitySeq zero), so cross-connection comparisons never happen.
	//
	// The gate reads msg.BackendCapacity (the wire truth) rather than the
	// canonicalized copy: canonicalization can drop slots but never reorders
	// frames. Seq 0/omitted is a legacy provider — every legacy heartbeat
	// takes the unguarded path below, byte-identical to today.
	if msg.BackendCapacity != nil && msg.BackendCapacity.CapacitySeq > 0 {
		if msg.BackendCapacity.CapacitySeq <= p.capacitySeq {
			// Stale/reordered frame: discard the ENTIRE application — capacity,
			// KV/TPS observations, warm/current model, status, and the clamp
			// release proof all derive from this one out-of-date snapshot.
			// LastHeartbeat still advances: the frame proves the connection is
			// alive, and eviction must key on liveness, not snapshot ordering.
			// Uptime credit and stats deltas are deliberately NOT applied — a
			// fresher frame just applied them microseconds ago (that is the
			// only way this branch is reachable), so nothing is lost.
			appliedSeq := p.capacitySeq
			p.LastHeartbeat = time.Now()
			p.mu.Unlock()
			r.logger.Debug("discarding stale capacity heartbeat",
				"provider_id", id, "seq", msg.BackendCapacity.CapacitySeq, "applied_seq", appliedSeq)
			return false
		}
		p.capacitySeq = msg.BackendCapacity.CapacitySeq
		// Seq-stamping providers implement the wave-2 capacity protocol:
		// mark the session quote-capable so the probe fanout can find it.
		p.capacityQuoteCapable = true
	}
	// Clamp only after unknown slot identifiers have been removed. Besides
	// keeping them out of routing state, this prevents an unaccepted model ID
	// from reaching clamp diagnostics or TPS/KV observations.
	clampBackendCapacity(r.logger, id, backendCapacity)
	now := time.Now()
	prevHB := p.LastHeartbeat
	p.LastHeartbeat = now
	applyHeartbeatStatsDelta(&p.Stats, p.lastSessionStats, msg.Stats)
	p.lastSessionStats = mergeHeartbeatSessionStats(p.lastSessionStats, msg.Stats)
	p.SystemMetrics = systemMetrics
	// Idle-memory policy: copy so the registry never aliases the decoded
	// message; ignore nonsense (negative) values from an untrusted provider.
	if msg.IdleUnloadMins != nil && *msg.IdleUnloadMins >= 0 {
		v := *msg.IdleUnloadMins
		p.IdleUnloadMins = &v
	}
	// Update backend capacity from heartbeat. A nil report clears prior live
	// capacity so stale slot state cannot keep influencing routing.
	p.BackendCapacity = backendCapacity
	// Per-slot KV backend (v0.8.0 paged rollout). Recorded from the canonical
	// report after unaccepted model identifiers have been removed,
	// BEFORE the nil-clearing semantics above take effect for it: the record is
	// sticky across a slot vanishing from the heartbeat, because attribution of
	// an in-flight request must survive its slot crashing. Measurement only —
	// nothing below reads it. See kv_backend.go.
	p.recordKVBackendsLocked(backendCapacity)
	if p.BackendCapacity != nil {
		chipFamily := p.Hardware.ChipFamily
		// Solo samples are keyed by chip CLASS (family+tier, chipClassKey) so a
		// fast tier (M4 Max) never lends its rate to a slow one (M4 Pro); the
		// load-inclusive Record stays family-keyed (fleetMedianTPS semantics).
		chipClass := chipClassKey(p.Hardware)
		// Solo gate: a slot EWMA is additionally recorded as a SOLO sample only
		// when the whole box is uncontended at heartbeat time (Σ running+waiting
		// ≤ 1 across ALL slots — the one allowance is the sample-generating
		// request itself) AND the slot has an actual RUNNING decode
		// (NumRunning > 0). Both halves matter. Requiring NumRunning (not
		// running+waiting) excludes a purely-QUEUED box: the provider reports
		// NumWaiting from its pending set while ObservedDecodeTPS is a retained
		// EWMA (BatchScheduler+Telemetry.swift), so a box with one queued-but-
		// not-yet-decoding request would otherwise mint that stale EWMA as a
		// fresh solo sample every ~30s heartbeat and, once the min-sample floor
		// is reached, base the model's quality cap on traffic no running request
		// produced. It also keeps the prior round's owner-slot-only rule: an
		// idle co-resident slot with a decayed EWMA is NumRunning == 0, so it is
		// never re-sampled, and a fully idle box records nothing. The
		// unconditional Record keeps its
		// load-inclusive semantics for TTFT estimation (fleetMedianTPS); the
		// gated RecordSolo feeds the quality-concurrency cap's per-model static
		// rate (resolvedSoloModelTPSLocked). See solo_tps.go.
		soloEligible := soloSampleEligible(p.BackendCapacity)
		for _, slot := range p.BackendCapacity.Slots {
			if slot.ObservedDecodeTPS > 0 {
				r.tpsRegistry.Record(slot.Model, chipFamily, slot.ObservedDecodeTPS)
				if soloEligible && slot.NumRunning > 0 {
					r.tpsRegistry.RecordSolo(slot.Model, chipClass, slot.ObservedDecodeTPS)
				}
			}
		}
	}
	// Credit wall-clock time since the previous heartbeat as uptime, so an
	// always-online provider's uptimeRate reaches 1.0 and its reputation can
	// exceed the old 0.85 cap (RecordUptime was never called in prod).
	// Bound the credit to a window just above the heartbeat interval (30s) and
	// within the eviction staleness (90s): a larger gap means the provider was
	// effectively offline (it would have been reaped, or this is an in-process
	// stall) and must NOT be credited. A fresh registration sets LastHeartbeat
	// to registration time, so the first real heartbeat credits ~one interval.
	// Must run under p.mu (held here) — p.Reputation is mutated under p.mu by
	// the job/challenge handlers.
	if !prevHB.IsZero() {
		const maxUptimeCredit = 2 * time.Minute
		if delta := now.Sub(prevHB); delta > 0 && delta <= maxUptimeCredit {
			p.Reputation.RecordUptime(delta)
		}
	}
	// Update warm models from heartbeat. Always overwrite -- an empty list
	// means the provider has no models loaded, and stale entries must be
	// cleared to prevent TriggerModelSwaps from suppressing needed swaps.
	p.WarmModels = warmModels
	// A nil or unaccepted active_model means no coordinator-known model is
	// loaded. Clear stale state so challenge checks never compare against a
	// provider-injected identifier.
	p.CurrentModel = currentModel
	// Drain awareness (drain_state.go): "draining" arms the routing skip,
	// "idle"/"serving" clear it. Independent of p.Status below — a draining
	// provider keeps its online/serving accounting; only routing changes.
	applyHeartbeatDrainStateLocked(p, msg.Status, now)
	// Only update status from heartbeat if provider is not actively serving
	// (serving status is managed by request lifecycle). Crucially, an
	// untrusted provider must NOT transition back to StatusOnline here —
	// that would cause an onlineCount double-decrement when Disconnect
	// later sees StatusOnline and decrements a second time.
	if p.Status == StatusUntrusted {
		// no status transitions allowed
	} else if p.Status != StatusServing || msg.Status == "idle" {
		switch msg.Status {
		case "idle":
			p.Status = StatusOnline
		case "serving":
			p.Status = StatusServing
		}
	}
	// Backstop for the per-model provider index: allocation-free when p.Models
	// is already in step, and self-healing within one heartbeat otherwise.
	p.syncModelIndexLocked()
	p.mu.Unlock()

	// This heartbeat may be the release proof for a budget clamp
	// (budget_clamp.go): drop any clamp entry this heartbeat's snapshot proves
	// inactive so a released pair returns to the accept fast path and cannot
	// be re-blocked by a lingering entry on its next reconnect. The sweep
	// evaluates the heartbeat's OWN stamped time and report (not a re-read of
	// the provider), so a racing disconnect cannot void the release proof.
	// Cheap no-op probe when the provider has no clamp state.
	r.releaseBudgetClampsOnHeartbeat(id, now, backendCapacity)

	r.PersistProviderThrottled(p)
	// Persist accumulated uptime (throttled) so it survives restarts/reconnects;
	// the heartbeat path is otherwise the only place uptime grows.
	r.persistReputationThrottled(p)

	// Heartbeats can make a recovered slot routable again (for example after a
	// crash auto-restart). Drain matching queues using the canonical scheduler
	// rather than the legacy direct queue assignment path. Heartbeats are the
	// one trigger that is rate-limited after a saturated pass
	// (queue_drain_suppress.go); every capacity-freeing trigger drains at once.
	r.drainQueuedRequestsForHeartbeat(providerModelIDs(p))

	// If queue drain didn't satisfy all pending requests (no warm provider),
	// check if a cold provider should swap models to serve queued demand —
	// coalesced fleet-wide to one plan per modelSwapPlanInterval, since N
	// heartbeats inside that window would each re-derive the same plan; a
	// heartbeat the window refuses arms one trailing plan for its end
	// (model_swap_coalesce.go). Drain work can outlast the planning window,
	// so claim against the current time rather than the heartbeat timestamp.
	r.triggerModelSwapsFromHeartbeat(time.Now())
	return true
}

func cumulativeDelta(previous, current int64) int64 {
	if current <= 0 {
		return 0
	}
	if current >= previous {
		return current - previous
	}
	// The provider process restarted and reset its in-memory counters.
	return current
}

func applyHeartbeatStatsDelta(total *protocol.HeartbeatStats, previous, current protocol.HeartbeatStats) {
	total.RequestsServed += cumulativeDelta(previous.RequestsServed, current.RequestsServed)
	total.TokensGenerated += cumulativeDelta(previous.TokensGenerated, current.TokensGenerated)
	total.CancellationsReceived += cumulativeDelta(previous.CancellationsReceived, current.CancellationsReceived)
	total.CancellationsBeforeOutput += cumulativeDelta(previous.CancellationsBeforeOutput, current.CancellationsBeforeOutput)
	total.CancellationsPartialComplete += cumulativeDelta(previous.CancellationsPartialComplete, current.CancellationsPartialComplete)
	total.GenerationErrorsAfterOutput += cumulativeDelta(previous.GenerationErrorsAfterOutput, current.GenerationErrorsAfterOutput)
	total.ChunkEncryptionErrors += cumulativeDelta(previous.ChunkEncryptionErrors, current.ChunkEncryptionErrors)
	total.StreamClosedWithoutTerminal += cumulativeDelta(previous.StreamClosedWithoutTerminal, current.StreamClosedWithoutTerminal)
	total.CancelDuringModelLoad += cumulativeDelta(previous.CancelDuringModelLoad, current.CancelDuringModelLoad)
	total.UsageGaps += cumulativeDelta(previous.UsageGaps, current.UsageGaps)
	// System profiler cancel accountability counters (cumulative per session).
	total.CancelStagePreAcceptTotal += cumulativeDelta(previous.CancelStagePreAcceptTotal, current.CancelStagePreAcceptTotal)
	total.CancelStagePreEngineTotal += cumulativeDelta(previous.CancelStagePreEngineTotal, current.CancelStagePreEngineTotal)
	total.CancelStagePrefillTotal += cumulativeDelta(previous.CancelStagePrefillTotal, current.CancelStagePrefillTotal)
	total.CancelStageDecodeTotal += cumulativeDelta(previous.CancelStageDecodeTotal, current.CancelStageDecodeTotal)
	total.CancelStagePostTerminalTotal += cumulativeDelta(previous.CancelStagePostTerminalTotal, current.CancelStagePostTerminalTotal)
	total.TokensAfterCancelTotal += cumulativeDelta(previous.TokensAfterCancelTotal, current.TokensAfterCancelTotal)
	total.CancelAbortNSSum += cumulativeDelta(previous.CancelAbortNSSum, current.CancelAbortNSSum)
}

func mergeHeartbeatSessionStats(previous, current protocol.HeartbeatStats) protocol.HeartbeatStats {
	merged := current
	if merged.CancellationsReceived == 0 {
		merged.CancellationsReceived = previous.CancellationsReceived
	}
	if merged.CancellationsBeforeOutput == 0 {
		merged.CancellationsBeforeOutput = previous.CancellationsBeforeOutput
	}
	if merged.CancellationsPartialComplete == 0 {
		merged.CancellationsPartialComplete = previous.CancellationsPartialComplete
	}
	if merged.GenerationErrorsAfterOutput == 0 {
		merged.GenerationErrorsAfterOutput = previous.GenerationErrorsAfterOutput
	}
	if merged.ChunkEncryptionErrors == 0 {
		merged.ChunkEncryptionErrors = previous.ChunkEncryptionErrors
	}
	if merged.StreamClosedWithoutTerminal == 0 {
		merged.StreamClosedWithoutTerminal = previous.StreamClosedWithoutTerminal
	}
	if merged.CancelDuringModelLoad == 0 {
		merged.CancelDuringModelLoad = previous.CancelDuringModelLoad
	}
	if merged.UsageGaps == 0 {
		merged.UsageGaps = previous.UsageGaps
	}
	for _, f := range []struct{ cur, prev *int64 }{
		{&merged.CancelStagePreAcceptTotal, &previous.CancelStagePreAcceptTotal},
		{&merged.CancelStagePreEngineTotal, &previous.CancelStagePreEngineTotal},
		{&merged.CancelStagePrefillTotal, &previous.CancelStagePrefillTotal},
		{&merged.CancelStageDecodeTotal, &previous.CancelStageDecodeTotal},
		{&merged.CancelStagePostTerminalTotal, &previous.CancelStagePostTerminalTotal},
		{&merged.TokensAfterCancelTotal, &previous.TokensAfterCancelTotal},
		{&merged.CancelAbortNSSum, &previous.CancelAbortNSSum},
	} {
		if *f.cur == 0 {
			*f.cur = *f.prev
		}
	}
	return merged
}

// Disconnect removes a provider from the registry and cleans up pending
// requests. This is the ABRUPT path: the flushed terminals carry
// CoordinatorCauseProviderDisconnected and strike the provider's stable
// identity. The provider read loop, which knows how the socket ended, calls
// DisconnectWithReason (disconnect_reason.go) so a graceful peer close flushes
// with the health-neutral restart cause instead.
func (r *Registry) Disconnect(id string) {
	r.disconnectWithCause(id, protocol.CoordinatorCauseProviderDisconnected)
}

// disconnectWithCause preserves the read loop's graceful/abrupt classification
// for unconditional disconnects. Eviction adds an identity/freshness guard.
func (r *Registry) disconnectWithCause(id string, cause protocol.CoordinatorInferenceErrorCause) {
	r.disconnectProvider(id, nil, 0, cause)
}

// disconnectProvider applies an optional eviction guard atomically with removal.
// expected is the exact session observed by the stale scan; nil is an ordinary
// unconditional disconnect. Both its identity and latest heartbeat are checked
// while r.mu and p.mu exclude replacement and heartbeat updates. The supplied
// cause is stamped on every flushed pending-request terminal.
func (r *Registry) disconnectProvider(id string, expected *Provider, timeout time.Duration, cause protocol.CoordinatorInferenceErrorCause) bool {
	var disconnectedModels []string
	r.mu.Lock()
	p, ok := r.providers[id]
	if ok {
		if expected != nil && p != expected {
			r.mu.Unlock()
			return false
		}
		p.mu.Lock()
		if expected != nil && time.Since(p.LastHeartbeat) <= timeout {
			p.mu.Unlock()
			r.mu.Unlock()
			return false
		}
		delete(r.providers, id)
		// Clear any pending model load entries for this provider.
		for key := range r.pendingModelLoads {
			if key.ProviderID == id {
				delete(r.pendingModelLoads, key)
				delete(r.pendingModelLoadStarted, key)
			}
		}
		p.detachModelIndexLocked(r)
		// FAULT STATE IS NOT CLEARED ON DISCONNECT. Every fault tracker
		// (node-health breaker, inference-error cooldowns, dispatch-load
		// cooldowns, health ejection, capacity trackers) lives on the STABLE
		// identity's gate when one is bound, so it must survive reconnect
		// churn — wiping it here was the zombie exploit. detachSessionGate
		// caches the identity (keyed by this session id) before the pending
		// flush below so the 502 "provider disconnected" faults — the dominant
		// reconnecting-zombie signal — still resolve to it even though the
		// provider is already gone from r.providers; only a provider that never
		// had a stable identity (sid == "": its gate WAS this session id, which
		// never recurs) has its session-keyed residue dropped for hygiene.
		r.detachSessionGate(p, stableProviderIdentityLocked(p))
		disconnectedModels = make([]string, 0, len(p.Models))
		for _, m := range p.Models {
			disconnectedModels = append(disconnectedModels, m.ID)
		}
		if p.Status != StatusUntrusted {
			r.onlineCount.Add(-1)
			for _, m := range p.Models {
				r.modelProviderDec(m.ID)
			}
		}
		p.mu.Unlock()
	}
	r.mu.Unlock()

	if !ok {
		return false
	}
	// Removing the last capable provider can turn a queued constrained request
	// from temporarily capacity-blocked into permanently unservable. Re-run
	// the canonical drain after removal so those waiters receive the immediate
	// capability-unavailable result instead of sleeping until maxWait.
	r.drainQueuedRequestsForModelsWithReason(disconnectedModels, DrainTriggerDisconnect)
	// Cache holders and nonce-bound attempts are connection-scoped. Clear them
	// after releasing registry/provider locks.
	r.cacheRouting.disconnect(id, cacheHolderRemovalDisconnect)
	// Outstanding capacity-probe waiters bound to this connection can never be
	// answered now (the socket is gone) — resolve them as SendFailed so probe
	// collectors demote the entries immediately instead of burning the full
	// quote window. Like the cache-holder cleanup above, this runs after the
	// registry/provider locks are released (quoteTracker has its own leaf
	// mutex; see capacity_quotes.go).
	r.capacityQuotes.failProvider(id)

	// Close all pending request channels so consumers get errors. Pending
	// requests created by tests may leave these channels nil, and consumer
	// goroutines may have already closed them on a successful/error path. Use
	// non-nil checks and recover so a single bad request cannot hang or panic
	// the disconnect cleanup.
	p.mu.Lock()
	for reqID, pr := range p.pendingReqs {
		if pr == nil {
			continue
		}
		if pr.ErrorCh != nil {
			func() {
				defer func() { recover() }()
				pr.ErrorCh <- protocol.InferenceErrorMessage{
					Type:             protocol.TypeInferenceError,
					RequestID:        reqID,
					Error:            "provider disconnected",
					StatusCode:       502,
					ErrorReason:      disconnectFlushErrorReason(cause),
					CoordinatorCause: cause,
				}
			}()
			func() {
				defer func() { recover() }()
				close(pr.ErrorCh)
			}()
		}
		if pr.ChunkCh != nil {
			func() {
				defer func() { recover() }()
				close(pr.ChunkCh)
			}()
		}
		if pr.CompleteCh != nil {
			func() {
				defer func() { recover() }()
				close(pr.CompleteCh)
			}()
		}
	}
	p.pendingReqs = make(map[string]*PendingRequest)
	p.mu.Unlock()

	// Tear down the socket. Deleting the map entry only makes the provider
	// unroutable; its read loop and challenge loop keep running on the open
	// socket and the coordinator keeps auto-ponging it, so the provider never
	// detects the drop and never reconnects — a "zombie" that's unroutable yet
	// still reports stale trust locally. CloseNow unblocks the read loop, which
	// unwinds the rest, and re-arms the provider's reconnect. CloseNow not Close:
	// Disconnect runs serially in the eviction loop and Close would block ~5s
	// waiting for a handshake the stale peer won't send. No-op if already closed;
	// outside r.mu so it can't stall the registry.
	p.closeWriterNow()

	// Final reputation persist: job successes are persisted on a 30 s throttle
	// (RecordJobSuccess), so flush whatever accumulated since the last window
	// before the row goes cold. Async, like every other persist.
	r.persistReputation(p)

	// Close this connection's session row (async; durable uptime history).
	// Covers both graceful disconnects and evictStale (which calls Disconnect).
	if r.store != nil {
		saferun.Go(r.logger, "registry.closeSession", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.store.CloseProviderSession(ctx, id, "disconnect", time.Now()); err != nil {
				r.logger.Warn("failed to close provider session", "provider_id", id, "error", err)
			}
		})
	}

	r.logger.Info("provider disconnected", "provider_id", id)
	return true
}

// RecordJobSuccess records a successful job completion for the provider's
// reputation. latency is the per-request responsiveness sample (time to first
// content, with the prompt-size prefill removed); a non-positive value records
// the success without touching the latency EWMA. Both updates happen under one
// lock.
//
// Persistence is throttled to the same 30 s window the heartbeat path uses:
// an unthrottled upsert per completion was ~46 statements and goroutines per
// second in production for a row nothing reads until the provider's next
// registration. The in-memory counters keep accumulating and the next window
// (or Disconnect's final persist) writes them. What can be lost is the last
// <=30 s of counts for a provider whose connection ends without Disconnect —
// a coordinator shutdown, which drains without disconnecting providers — the
// same exposure the uptime counter already had. Failures still persist
// immediately (RecordJobFailure).
func (r *Registry) RecordJobSuccess(providerID string, latency time.Duration) {
	r.mu.RLock()
	p, ok := r.providers[providerID]
	r.mu.RUnlock()
	if !ok {
		return
	}

	p.mu.Lock()
	p.Reputation.RecordJobSuccess()
	p.Reputation.RecordLatency(latency)
	p.mu.Unlock()

	r.persistReputationThrottled(p)
}

// RecordLatency folds a per-request responsiveness sample into the provider's
// latency EWMA, independent of job-success counting. It is recorded by the
// consumer/dispatch goroutine (which owns the request timing) at commit, so the
// provider read-loop goroutine never has to read that goroutine's timing. A
// non-positive latency is ignored.
//
// It updates the in-memory EWMA only and does NOT persist. The updated
// AvgResponseTime is persisted by the RecordJobSuccess / RecordJobFailure that
// follows on completion (which snapshots the whole reputation row). Persisting a
// full row here would race that terminal write — a pre-terminal snapshot carrying
// stale TotalJobs/SuccessfulJobs could land after it and clobber the counts.
func (r *Registry) RecordLatency(providerID string, latency time.Duration) {
	if latency <= 0 {
		return
	}
	r.mu.RLock()
	p, ok := r.providers[providerID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	p.RecordLatency(latency)
}

// RecordJobFailure records a failed job for the provider's reputation.
func (r *Registry) RecordJobFailure(providerID string) {
	r.mu.RLock()
	p, ok := r.providers[providerID]
	r.mu.RUnlock()
	if !ok {
		return
	}

	p.mu.Lock()
	p.Reputation.RecordJobFailure()
	p.mu.Unlock()

	// Persist reputation.
	r.persistReputation(p)
}

// StartEvictionLoop starts a background goroutine that removes providers
// that haven't sent a heartbeat within the given timeout. It stops when
// the context is cancelled.
func (r *Registry) StartEvictionLoop(ctx context.Context, timeout time.Duration) {
	ticker := time.NewTicker(timeout / 3)
	saferun.Go(r.logger, "registry.evictionLoop", func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.evictStale(timeout)
			}
		}
	})
}

func (r *Registry) evictStale(timeout time.Duration) {
	now := time.Now()

	// Scan under the READ lock: the walk only reads LastHeartbeat (under p.mu)
	// and the previous sweep's strikes. evictStrikes is written solely by this
	// function on the single eviction goroutine, so a read-scan followed by a
	// short write-locked install is race-free — and the routing scans that
	// share r.mu are no longer blocked for a whole fleet walk every timeout/3.
	// Collect every provider's heartbeat age for the summary, and decide who to
	// evict: a provider is reaped only after it is stale on TWO consecutive
	// sweeps (strike >= 2), so a single transient stall that ages many
	// timestamps at once gives the fleet a sweep to recover instead of a mass
	// reap.
	r.mu.RLock()
	fleet := len(r.providers)
	ages := make([]time.Duration, 0, fleet)
	var nextStrikes map[string]int // allocated lazily: steady state carries nothing
	var toEvict []*Provider
	var evictAges []time.Duration
	for id, p := range r.providers {
		p.mu.Lock()
		lastHeartbeat := p.LastHeartbeat
		p.mu.Unlock()
		age := now.Sub(lastHeartbeat)
		ages = append(ages, age)
		if age > timeout {
			strikes := r.evictStrikes[id] + 1
			if strikes >= evictStrikeThreshold {
				toEvict = append(toEvict, p)
				evictAges = append(evictAges, age)
			} else {
				if nextStrikes == nil {
					nextStrikes = make(map[string]int)
				}
				nextStrikes[id] = strikes // carry the strike to next sweep
			}
		}
	}
	hadStrikes := len(r.evictStrikes) > 0
	r.mu.RUnlock()

	// Install the rebuilt strike map under the write lock only when it changes
	// anything (a strike carried or cleared). The steady state — nobody stale,
	// nothing carried — never takes the write lock at all.
	if hadStrikes || len(nextStrikes) > 0 {
		if nextStrikes == nil {
			nextStrikes = make(map[string]int)
		}
		r.mu.Lock()
		r.evictStrikes = nextStrikes
		r.mu.Unlock()
	}

	if len(ages) > 0 {
		amin, amed, ap90, amax := durationStats(ages)
		// A tight evicted-age spread (emax-emin small) means many providers went
		// stale at the same instant — a coordinator-side stall. A broad spread
		// means independent provider sleeps. The summary makes that diagnosable.
		emin, _, _, emax := durationStats(evictAges)
		r.logger.Info("eviction sweep",
			"fleet", fleet,
			"evicting", len(toEvict),
			"hb_age_min_s", int(amin.Seconds()),
			"hb_age_p50_s", int(amed.Seconds()),
			"hb_age_p90_s", int(ap90.Seconds()),
			"hb_age_max_s", int(amax.Seconds()),
			"evicted_age_min_s", int(emin.Seconds()),
			"evicted_age_max_s", int(emax.Seconds()),
		)
	}

	for _, p := range toEvict {
		// A heartbeat may recover this session after the read scan, or the
		// same id may name a replacement. Revalidate inside the removal lock.
		if r.disconnectProvider(p.ID, p, timeout, protocol.CoordinatorCauseProviderDisconnected) {
			r.logger.Warn("evicted stale provider", "provider_id", p.ID, "timeout", timeout)
		}
	}

	// Bound the per-identity gate index on the same cadence (gate_state.go):
	// prunes dead per-model entries and drops gates no live session references
	// once idle. Off the request path and outside r.mu.
	r.sweepGates(now)
}

// evictStrikeThreshold is how many consecutive stale sweeps trigger eviction.
// With a timeout/3 sweep cadence, 2 strikes ≈ one extra sweep interval of grace.
const evictStrikeThreshold = 2

// durationStats returns min, median, p90, max of ds (zeros for an empty slice).
// Sorts a copy; ds is small (fleet-sized) so this is cheap.
func durationStats(ds []time.Duration) (min, median, p90, max time.Duration) {
	if len(ds) == 0 {
		return 0, 0, 0, 0
	}
	s := make([]time.Duration, len(ds))
	copy(s, ds)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[0], s[len(s)/2], s[(len(s)*9)/10], s[len(s)-1]
}
