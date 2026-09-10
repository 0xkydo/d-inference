package registry

// Registry trust policy, code attestation, and untrust outcomes.

import (
	"strings"
	"time"
)

// SetCodeAttestationConfigured records whether an APNs code-identity attestor is
// wired. When configured the coordinator issues code-identity challenges; whether
// a passing challenge is REQUIRED for routing is governed separately by the
// enforcement deadline (SetCodeAttestationDeadline). Call during server setup.
func (r *Registry) SetCodeAttestationConfigured(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.codeAttestationConfigured = v
}

// SetCodeAttestationDeadline sets the instant at which code-identity attestation
// becomes MANDATORY for routing. A zero time means "grace/observe indefinitely"
// (challenge + measure, but keep routing un-attested providers). Safe to call at
// runtime; the gate re-reads it on every routing decision.
func (r *Registry) SetCodeAttestationDeadline(t time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.codeAttestationDeadline = t
}

// SetCodeAttestationPolicy sets both knobs atomically (used by tests).
func (r *Registry) SetCodeAttestationPolicy(configured bool, deadline time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.codeAttestationConfigured = configured
	r.codeAttestationDeadline = deadline
}

// SetReleasePolicyGeneration atomically publishes the active-release policy
// generation used by routing. Existing application evidence that stillApproved
// reports as valid under the NEW policy is carried forward at the new
// generation — a routine release registration must not deroute the whole fleet
// of healthy, still-approved providers for up to a challenge interval.
// Evidence not carried forward is removed synchronously.
//
// When the new policy is REQUIRED, the returned slice holds every connected
// provider that was NOT carried forward — including providers that held no
// evidence at all (e.g. the first activation of a required policy over a fleet
// that never needed evidence before). The caller must kick each one for an
// immediate re-challenge; otherwise the fleet idles unroutable until the
// periodic ticker, whose interval outlives the request queue. Carried-forward
// providers are never returned, so already-current providers get no duplicate
// kick. When the new policy is NOT required, nothing is returned: evidence is
// not a routing gate, so the periodic ticker is soon enough.
//
// A concurrently completing old challenge cannot install evidence at all:
// GrantApplicationEvidenceIfNotUntrusted refuses any grant whose generation is
// not current (atomically, under the same registry lock) and kicks that
// provider for an immediate re-challenge.
func (r *Registry) SetReleasePolicyGeneration(
	generation uint64, required bool,
	stillApproved func(ApplicationEvidence) bool,
) (needChallenge []string) {
	r.mu.Lock()
	r.releasePolicyGeneration = generation
	r.releasePolicyRequired = required
	enforced := r.releasePolicyEnforcedLocked()
	for id, provider := range r.providers {
		provider.mu.Lock()
		evidence := provider.ApplicationEvidence
		if evidence.EvidenceGeneration != 0 && stillApproved != nil && stillApproved(evidence) {
			provider.ApplicationEvidence.PolicyGeneration = generation
			provider.mu.Unlock()
			continue
		}
		provider.ApplicationEvidence = ApplicationEvidence{}
		// Capability invalidation is an ENFORCE-mode consequence: capabilities
		// gate capability-required catalog models, so clearing them in shadow
		// would let evidence bookkeeping remove real capacity — exactly what
		// shadow mode promises not to do. The re-challenge kicked below
		// re-reconciles capabilities within one challenge round-trip anyway.
		if enforced {
			provider.RuntimeCapabilities = nil
		}
		provider.mu.Unlock()
		if required {
			needChallenge = append(needChallenge, id)
		}
	}
	r.mu.Unlock()
	return needChallenge
}

// providerHoldsCurrentApplicationEvidenceLocked reports whether p holds
// generation-current application evidence bound to its live identity: current
// policy generation, this connection's process key, the current APNs token
// (tokenless providers pass with matching empty tokens; token possession is
// enforced only by the code-identity gate), the registered version/backend,
// and the registration-attested SE identity. Caller must hold r.mu; provider
// fields are read without p.mu, matching the routing chokepoint's semantics.
func (r *Registry) providerHoldsCurrentApplicationEvidenceLocked(p *Provider) bool {
	evidence := p.ApplicationEvidence
	return evidence.EvidenceGeneration != 0 &&
		evidence.PolicyGeneration == r.releasePolicyGeneration &&
		evidence.ProcessPublicKey == p.PublicKey &&
		evidence.APNsToken == p.APNsDeviceToken &&
		evidence.Version == p.Version &&
		evidence.Backend == p.Backend &&
		evidence.BinaryHash != "" &&
		p.AttestationResult != nil &&
		evidence.SEPublicKey == p.AttestationResult.PublicKey &&
		evidence.Serial == p.AttestationResult.SerialNumber
}

// SetReleasePolicyEnforcement switches the release-policy routing gate between
// SHADOW (false, default: evidence derived/granted/swept and counted but never
// blocks routing) and ENFORCE (true: the routing chokepoint requires current
// evidence once any configured enforce-after delay has passed). Thread-safe.
func (r *Registry) SetReleasePolicyEnforcement(enforced bool) {
	r.mu.Lock()
	r.releasePolicyEnforced = enforced
	r.mu.Unlock()
}

// SetReleasePolicyEnforceAfter defers enforcement until t (zero = immediate).
// Set at startup so a restart into enforce mode keeps routing like shadow
// until the reconnected fleet has completed its first challenge cycles and
// re-earned evidence. Thread-safe.
func (r *Registry) SetReleasePolicyEnforceAfter(t time.Time) {
	r.mu.Lock()
	r.releasePolicyEnforceAfter = t
	r.mu.Unlock()
}

// releasePolicyEnforcedLocked reports whether the evidence gate is LIVE right
// now: enforcement configured and past any enforce-after delay. Caller holds r.mu.
func (r *Registry) releasePolicyEnforcedLocked() bool {
	return r.releasePolicyEnforcedAtLocked(time.Now())
}

// releasePolicyEnforcedAtLocked is releasePolicyEnforcedLocked at an explicit
// instant (the fleet walks pass their captured clock). Caller holds r.mu.
func (r *Registry) releasePolicyEnforcedAtLocked(now time.Time) bool {
	return r.releasePolicyEnforced &&
		!now.Before(r.releasePolicyEnforceAfter)
}

// ReleasePolicyEnforced reports whether missing application evidence currently
// blocks routing. Thread-safe.
func (r *Registry) ReleasePolicyEnforced() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.releasePolicyEnforcedLocked()
}

// ModelEvidenceCoverage is the per-model shadow→enforce acceptance record:
// Routable counts providers passing every routing gate EXCEPT the evidence
// gate for that catalog-allowed model; WithEvidence counts the subset also
// holding generation-current application evidence. Enforcement is safe for a
// model only when WithEvidence ≈ Routable.
type ModelEvidenceCoverage struct {
	Routable     int `json:"routable"`
	WithEvidence int `json:"with_evidence"`
}

// ApplicationEvidenceModelCoverage computes ModelEvidenceCoverage for every
// catalog-allowed model advertised by a connected provider, using the same
// liveness surface as public capacity with the evidence gate bypassed — so the
// flip criterion cannot be masked by fleet-wide averages hiding one model
// family's uncovered providers. Thread-safe.
func (r *Registry) ApplicationEvidenceModelCoverage() map[string]ModelEvidenceCoverage {
	now := time.Now()
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]ModelEvidenceCoverage)
	for _, p := range r.providers {
		p.mu.Lock()
		baseline := p.Status != StatusOffline && p.Status != StatusUntrusted &&
			!p.PrivateOnly &&
			trustRank(p.TrustLevel) >= trustRank(r.MinTrustLevel) &&
			p.RuntimeVerified &&
			r.providerSupportsPrivateTextModeLocked(p, false) &&
			!p.LastChallengeVerified.IsZero() &&
			now.Sub(p.LastChallengeVerified) <= challengeFreshnessMaxAge
		holds := baseline && r.providerHoldsCurrentApplicationEvidenceLocked(p)
		if baseline {
			for _, model := range p.Models {
				if !r.providerModelAllowedByCatalogLocked(p, model) {
					continue
				}
				coverage := out[model.ID]
				coverage.Routable++
				if holds {
					coverage.WithEvidence++
				}
				out[model.ID] = coverage
			}
		}
		p.mu.Unlock()
	}
	return out
}

// CountProvidersWithCurrentApplicationEvidence returns (holding, connected):
// how many currently connected providers hold generation-current application
// evidence, and the total connected count. This is the shadow-mode acceptance
// instrument: enforcement must not be enabled until holding is near connected.
// Thread-safe.
func (r *Registry) CountProvidersWithCurrentApplicationEvidence() (int, int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	holding := 0
	for _, provider := range r.providers {
		// Evidence and token fields are written under p.mu by challenge and
		// APNs handlers that do not hold r.mu; lock each provider so this
		// flip-criterion counter never reads a torn record.
		provider.mu.Lock()
		holds := r.providerHoldsCurrentApplicationEvidenceLocked(provider)
		provider.mu.Unlock()
		if holds {
			holding++
		}
	}
	return holding, len(r.providers)
}

// CodeAttestationConfigured reports whether an APNs attestor is wired (so the
// connection handler should issue code-identity challenges). Thread-safe.
func (r *Registry) CodeAttestationConfigured() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.codeAttestationConfigured
}

// CodeAttestationEnforced reports whether code-identity attestation is currently
// mandatory for routing (configured AND past the deadline). Thread-safe.
func (r *Registry) CodeAttestationEnforced() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.codeAttestationEnforcedLocked()
}

// codeAttestationEnforcedLocked reports whether code-identity attestation is
// currently MANDATORY for routing. Caller must hold r.mu. Enforcement begins only
// when an attestor is configured AND a non-zero deadline has been reached; before
// then the fleet routes un-attested providers (grace window) while still being
// challenged.
func (r *Registry) codeAttestationEnforcedLocked() bool {
	return r.codeAttestationEnforcedAtLocked(time.Now())
}

// codeAttestationEnforcedAtLocked is codeAttestationEnforcedLocked at an
// explicit instant (the fleet walks pass their captured clock). Caller holds r.mu.
func (r *Registry) codeAttestationEnforcedAtLocked(now time.Time) bool {
	if !r.codeAttestationConfigured || r.codeAttestationDeadline.IsZero() {
		return false
	}
	return !now.Before(r.codeAttestationDeadline)
}

// CountProvidersByBinaryHash returns the number of currently connected
// providers whose registration or current, connection-bound application
// evidence attests the given provider binary hash. Used by release
// administration to avoid removing a hash from the forced allowlist while
// old-but-still-connected providers are draining/restarting into a newer
// release.
func (r *Registry) CountProvidersByBinaryHash(hash string) int {
	normalized := strings.ToLower(strings.TrimSpace(hash))
	if normalized == "" {
		return 0
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, p := range r.providers {
		p.mu.Lock()
		if p.Status == StatusOffline {
			p.mu.Unlock()
			continue
		}

		registrationMatches := p.AttestationResult != nil &&
			strings.EqualFold(p.AttestationResult.BinaryHash, normalized)
		evidence := p.ApplicationEvidence
		evidenceCurrent := evidence.EvidenceGeneration != 0 &&
			(!r.releasePolicyRequired || evidence.PolicyGeneration == r.releasePolicyGeneration) &&
			evidence.ProcessPublicKey == p.PublicKey &&
			evidence.APNsToken == p.APNsDeviceToken
		if evidenceCurrent && p.AttestationResult != nil {
			evidenceCurrent = evidence.SEPublicKey == p.AttestationResult.PublicKey &&
				evidence.Serial == p.AttestationResult.SerialNumber
		}
		evidenceMatches := evidenceCurrent && strings.EqualFold(evidence.BinaryHash, normalized)
		p.mu.Unlock()

		if registrationMatches || evidenceMatches {
			count++
		}
	}
	return count
}

// MarkUntrusted sets a provider's status to untrusted for a hard/security
// reason (bad encrypted chunk, MDM/MDA failure, SIP disabled, binary or model
// hash mismatch, serial impersonation, attestation failure). The deroute is
// non-recoverable: the provider stays untrusted until it reconnects and
// re-registers. This is the default for every direct deroute call site.
func (r *Registry) MarkUntrusted(providerID string) {
	r.markUntrusted(providerID, false)
}

// MarkUntrustedTransient sets a provider's status to untrusted for a *transient*
// reason — MaxFailedChallenges consecutive missed-challenge timeouts (screen
// sleep, network blip, momentary Secure Enclave inaccessibility). Unlike
// MarkUntrusted, the provider remains eligible to self-recover: the challenge
// loop keeps challenging it (see ChallengeShouldStop), and a subsequent fully
// passing challenge (RecordChallengeSuccess) restores it to online.
//
// A passing challenge re-verifies signature, SIP, secure boot, binary hash,
// model hash and runtime before RecordChallengeSuccess is reached, so using it
// as the recovery trigger is safe.
func (r *Registry) MarkUntrustedTransient(providerID string) {
	r.markUntrusted(providerID, true)
}

// markUntrusted is the shared implementation. recoverable=true marks the untrust
// as transiently recoverable; recoverable=false is a hard deroute.
//
// Transition rules:
//   - not untrusted -> untrusted: decrement online/model counts, set status and
//     the recoverable flag.
//   - already untrusted + hard (recoverable=false): clear the flag. A hard
//     reason always overrides/downgrades a previously-recoverable untrust.
//   - already untrusted + transient (recoverable=true): leave the flag as-is, so
//     a transient timeout can never *upgrade* a hard deroute to recoverable
//     (matters for an in-flight challenge timeout that races a hard deroute).
func (r *Registry) markUntrusted(providerID string, recoverable bool) {
	r.mu.Lock()
	p, ok := r.providers[providerID]
	if !ok {
		r.mu.Unlock()
		return
	}
	hook := r.onHardUntrust // capture under r.mu (race-safe)

	p.mu.Lock()
	if p.Status != StatusUntrusted {
		r.onlineCount.Add(-1)
		for _, m := range p.Models {
			r.modelProviderDec(m.ID)
		}
		p.Status = StatusUntrusted
		p.untrustedRecoverable = recoverable
	} else if !recoverable {
		p.untrustedRecoverable = false
	}
	// Effective claims are connection security state, not durable inventory.
	// A passing fully-signed challenge may restore them only through reconcile.
	capabilitiesChanged := len(p.RuntimeCapabilities) > 0
	p.RuntimeCapabilities = nil
	failed := p.FailedChallenges // read under p.mu (the old code read this unlocked)
	// Capture the SE key for the hard-untrust hook while we hold p.mu.
	var seKey string
	if !recoverable && p.AttestationResult != nil {
		seKey = p.AttestationResult.PublicKey
	}
	if !recoverable {
		p.DeviceEvidence = DeviceEvidence{}
		p.ApplicationEvidence = ApplicationEvidence{}
		p.CodeAttested = false
		p.FreshCodeAttested = false
	}
	p.mu.Unlock()
	r.mu.Unlock()
	if !recoverable {
		p.SignalApplicationProofSettled()
	}
	if capabilitiesChanged {
		_ = r.ReconcileAttestedRuntimeCapabilities(providerID)
	}

	r.logger.Warn("provider marked as untrusted",
		"provider_id", providerID,
		"failed_challenges", failed,
		"recoverable", recoverable,
	)

	// A HARD untrust invalidates the device's trust-reuse record (in-memory +
	// persisted) so a later reconnect cannot fast-skip the live MDM re-verification
	// on a stale, pre-untrust record (DAR-326). Fired after releasing the locks; a
	// transient (recoverable) untrust does NOT invalidate — it can self-recover via
	// a passing challenge.
	if !recoverable {
		// FIX A: bump the hard-untrust epoch BEFORE firing the delete hook. A
		// concurrent recordTrustReuse that captured the old epoch at grant time then
		// sees the change on its pre-upsert recheck and refuses to persist a stale
		// `hardware` row — closing the write-after-delete race (a write landing after
		// the synchronous delete that a restart would otherwise reseed).
		p.untrustEpoch.Add(1)
		if hook != nil && seKey != "" {
			hook(seKey)
		}
	}
}

// SetTrustLevel updates a provider's trust level (thread-safe).
func (r *Registry) SetTrustLevel(providerID string, level TrustLevel) {
	r.mu.RLock()
	p, ok := r.providers[providerID]
	r.mu.RUnlock()
	if !ok {
		return
	}
	p.mu.Lock()
	p.TrustLevel = level
	if level != TrustHardware {
		p.RuntimeCapabilities = nil
	}
	p.mu.Unlock()

	// Persist trust state.
	r.persistProviderNow(p)
	p.reconcileRuntimeCapabilities()
}

// RecordChallengeSuccess records a successful challenge-response verification.
// A fully passing challenge re-verifies signature, SIP, secure boot, binary
// hash, model hash and runtime (see verifyChallengeResponse) before this is
// called, so it doubles as the recovery trigger for a *transiently* untrusted
// provider.
//
// Returns true iff this call recovered a transiently-untrusted provider back to
// online. The caller uses that to push a fresh "online" trust_status so the
// provider's locally persisted operator state reflects recovery.
func (r *Registry) RecordChallengeSuccess(providerID string) bool {
	r.mu.RLock()
	p, ok := r.providers[providerID]
	r.mu.RUnlock()
	if !ok {
		return false
	}

	recovered := r.recoverIfTransientlyUntrusted(providerID, p)

	p.mu.Lock()
	p.LastChallengeVerified = time.Now()
	p.FailedChallenges = 0
	if !p.ChallengeVerifiedSIP {
		p.ChallengeVerifiedSIP = true
	}
	p.Reputation.RecordChallengePass()
	p.mu.Unlock()

	// Persist challenge state and reputation.
	r.persistProviderNow(p)
	r.persistReputation(p)

	if recovered {
		r.logger.Info("provider recovered from transient deroute", "provider_id", providerID)
	}

	p.reconcileRuntimeCapabilities()

	// A newly verified (or newly recovered) provider may unlock queued requests
	// for any model it serves.
	r.drainQueuedRequestsForModelsWithReason(providerModelIDs(p), DrainTriggerChallenge)

	return recovered
}

// recoverIfTransientlyUntrusted promotes a transiently-untrusted provider back
// to online, mirroring markUntrusted's bookkeeping in reverse. Returns true iff
// a transition occurred. It acquires r.mu (write) then p.mu — the same order as
// markUntrusted/Register/Disconnect — so online/model counts stay consistent and
// the path is deadlock-free.
func (r *Registry) recoverIfTransientlyUntrusted(providerID string, p *Provider) bool {
	// Cheap pre-check under p.mu only, so the common (non-recovery) success path
	// never contends on the registry write lock.
	p.mu.Lock()
	eligible := p.Status == StatusUntrusted && p.untrustedRecoverable
	p.mu.Unlock()
	if !eligible {
		return false
	}

	r.mu.Lock()
	// Re-verify membership: RecordChallengeSuccess looked p up under RLock and
	// released it, so Disconnect may have removed (or replaced) it since. A
	// transiently-untrusted provider was already decremented out of the counts,
	// and Disconnect does not decrement an untrusted provider, so incrementing a
	// stale/removed pointer here would permanently corrupt onlineCount and
	// modelProviders. Only recover the provider still registered under this ID.
	if cur, ok := r.providers[providerID]; !ok || cur != p {
		r.mu.Unlock()
		return false
	}
	p.mu.Lock()
	// Re-check under the write lock: a hard deroute may have intervened and
	// cleared the recoverable flag between the pre-check and here.
	if p.Status != StatusUntrusted || !p.untrustedRecoverable {
		p.mu.Unlock()
		r.mu.Unlock()
		return false
	}
	r.onlineCount.Add(1)
	for _, m := range p.Models {
		r.modelProviderInc(m.ID)
	}
	p.Status = StatusOnline
	p.untrustedRecoverable = false
	p.mu.Unlock()
	r.mu.Unlock()
	return true
}

// RecordChallengeFailure records a failed challenge-response. Returns the
// new consecutive failure count.
//
// When transientOnly is true (timeout — the provider didn't respond in time),
// routing eligibility is preserved until MaxFailedChallenges consecutive
// failures. A single transient timeout should not instantly deroute a provider
// that was verified seconds ago.
//
// When transientOnly is false (security failure — wrong signature, SIP
// disabled, binary hash mismatch, etc.), routing eligibility is cleared
// immediately because the provider actively failed a security check.
func (r *Registry) RecordChallengeFailure(providerID string, transientOnly bool) int {
	r.mu.RLock()
	p, ok := r.providers[providerID]
	r.mu.RUnlock()
	if !ok {
		return 0
	}

	p.mu.Lock()
	p.FailedChallenges++
	p.Reputation.RecordChallengeFail()
	count := p.FailedChallenges

	if !transientOnly {
		// Security failure — clear routing eligibility immediately.
		p.LastChallengeVerified = time.Time{}
		p.ChallengeVerifiedSIP = false
	} else if count >= MaxFailedChallenges {
		// Transient failures only clear after hitting the threshold.
		p.LastChallengeVerified = time.Time{}
		p.ChallengeVerifiedSIP = false
	}
	p.mu.Unlock()

	// Persist challenge state and reputation.
	r.persistProviderNow(p)
	r.persistReputation(p)

	return count
}

// CodeAttestationCoverage reports how many currently online (non-offline,
// non-untrusted) providers have passed APNs code-identity attestation, plus the
// online total. Operators watch this during the grace window to judge when it is
// safe to let the APNS_ENFORCE_AFTER deadline pass — after which every
// un-attested provider (incl. all headless / pre-0.6.0 boxes) is derouted.
// Thread-safe.
func (r *Registry) CodeAttestationCoverage() (codeAttested, online int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.providers {
		p.mu.Lock()
		if p.Status != StatusOffline && p.Status != StatusUntrusted {
			online++
			if p.CodeAttested {
				codeAttested++
			}
		}
		p.mu.Unlock()
	}
	return codeAttested, online
}
