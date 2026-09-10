package api

// Binary-hash release policy and runtime-manifest verification.

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/store"
	"golang.org/x/mod/semver"
)

// SetKnownBinaryHashes configures the set of accepted provider binary hashes.
// SetBinaryHashEnforcement toggles whether a self-reported binaryHash mismatch
// deroutes a provider. Default false (v0.6.0): binaryHash is demoted to drift
// telemetry; APNs code-identity attestation is the real signal. Enable only for
// rollback or to test the legacy enforcement path.
func (s *Server) SetBinaryHashEnforcement(enabled bool) {
	s.binaryHashEnforce = enabled
}

// Providers whose binary SHA-256 doesn't match any known hash are rejected.
func (s *Server) SetKnownBinaryHashes(hashes []string) {
	normalized := normalizeKnownBinaryHashes(hashes, s.logger)

	s.binaryHashPolicyMu.Lock()
	defer s.binaryHashPolicyMu.Unlock()

	s.manualKnownBinaryHashes = normalized
	s.manualBinaryHashPolicyConfigured = hasConfiguredHashInput(hashes)
	s.rebuildBinaryHashPolicyLocked()
}

func normalizeKnownBinaryHashes(hashes []string, logger *slog.Logger) map[string]bool {
	normalizedHashes := make(map[string]bool, len(hashes))
	for _, h := range hashes {
		normalized, err := normalizeSHA256Hex(h, "known_binary_hashes")
		if err != nil {
			if strings.TrimSpace(h) != "" {
				logger.Warn("invalid known binary hash ignored", "hash", h, "error", err)
			}
			continue
		}
		normalizedHashes[normalized] = true
	}
	return normalizedHashes
}

// AddKnownBinaryHashes adds hashes to the existing known set (for env var fallback).
func (s *Server) AddKnownBinaryHashes(hashes []string) {
	normalized := normalizeKnownBinaryHashes(hashes, s.logger)

	s.binaryHashPolicyMu.Lock()
	defer s.binaryHashPolicyMu.Unlock()

	if s.manualKnownBinaryHashes == nil {
		s.manualKnownBinaryHashes = make(map[string]bool)
	}
	if hasConfiguredHashInput(hashes) {
		s.manualBinaryHashPolicyConfigured = true
	}
	for h := range normalized {
		s.manualKnownBinaryHashes[h] = true
	}
	s.rebuildBinaryHashPolicyLocked()
}

func hasConfiguredHashInput(hashes []string) bool {
	for _, h := range hashes {
		if strings.TrimSpace(h) != "" {
			return true
		}
	}
	return false
}

// SyncBinaryHashes rebuilds knownBinaryHashes from all active releases.
// Called at startup and after release changes.
//
// An inventory read failure is an OPERATIONAL condition, not a security signal:
// with a previously published policy the last-known-good snapshot is retained
// untouched (mirroring SyncRuntimeManifest's nil handling) so a store hiccup
// can never deroute a healthy fleet. Only a cold start with no prior snapshot
// publishes a deny-all generation — there is nothing known-good to retain, and
// startup refuses to proceed on the returned error.
func (s *Server) SyncBinaryHashes() error {
	s.releasePolicySyncMu.Lock()
	defer s.releasePolicySyncMu.Unlock()
	releases, err := s.store.ListReleasesWithError()
	if err != nil {
		if last := s.releaseTrustPolicy.Load(); last != nil {
			s.logger.Error("release inventory unavailable; retaining last-known-good release policy",
				"generation", last.Generation,
				"error", err,
			)
			s.ddIncr("release_policy.sync_failure", []string{"outcome:retained_last_known_good"})
			return fmt.Errorf("sync binary hashes: %w", err)
		}
		// Cold start: no last-known-good policy exists. Publish deny-all so a
		// half-started coordinator cannot route on an unknown inventory.
		generation := s.releaseTrustPolicyGeneration.Add(1)
		trustSnapshot := &releaseTrustPolicySnapshot{
			Generation:   generation,
			Required:     true,
			ByBinaryHash: make(map[string][]approvedReleasePolicy),
		}
		s.releaseTrustPolicy.Store(trustSnapshot)
		if s.registry != nil {
			s.registry.SetReleasePolicyGeneration(trustSnapshot.Generation, true, nil)
		}
		s.binaryHashPolicyMu.Lock()
		s.releaseKnownBinaryHashes = make(map[string]bool)
		s.releaseBinaryHashPolicyConfigured = true
		s.rebuildBinaryHashPolicyLocked()
		s.binaryHashPolicyMu.Unlock()
		s.logger.Error("release inventory unavailable at cold start; published deny-all release policy",
			"generation", generation,
			"error", err,
		)
		s.ddIncr("release_policy.sync_failure", []string{"outcome:cold_start_deny_all"})
		return fmt.Errorf("sync binary hashes: %w", err)
	}

	hashes := make(map[string]bool)
	generation := s.releaseTrustPolicyGeneration.Add(1)
	everConfigured := s.releaseInventoryEverConfigured.Load()
	if len(releases) > 0 {
		s.releaseInventoryEverConfigured.Store(true)
		everConfigured = true
	}
	trustSnapshot := &releaseTrustPolicySnapshot{
		Generation:   generation,
		Required:     everConfigured,
		ByBinaryHash: make(map[string][]approvedReleasePolicy),
	}

	policyConfigured := false
	for _, r := range releases {
		if !r.Active {
			continue
		}
		policyConfigured = true
		normalized, err := normalizeSHA256Hex(r.BinaryHash, "release.binary_hash")
		if err != nil {
			s.logger.Warn("invalid release binary hash ignored",
				"version", r.Version,
				"platform", r.Platform,
				"error", err,
			)
			continue
		}
		hashes[normalized] = true
		templates := make(map[string]string)
		for _, pair := range strings.Split(r.TemplateHashes, ",") {
			parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
				templates[parts[0]] = parts[1]
			}
		}
		trustSnapshot.ByBinaryHash[normalized] = append(
			trustSnapshot.ByBinaryHash[normalized],
			approvedReleasePolicy{
				Version: r.Version, Platform: r.Platform, Backend: r.Backend,
				BinaryHash: normalized, MetallibHash: r.MetallibHash,
				PythonHash: r.PythonHash, RuntimeHash: r.RuntimeHash,
				TemplateHashes: templates,
			})
	}
	s.releaseTrustPolicy.Store(trustSnapshot)
	if s.registry != nil {
		// Evidence still approved under the NEW snapshot is carried forward at
		// the new generation. For a REQUIRED policy the registry returns every
		// provider NOT carried forward — including providers that held no
		// evidence at all (first required activation over a cold fleet) — and
		// each one is re-challenged immediately instead of waiting for the
		// periodic ticker (whose interval outlives the request queue).
		needChallenge := s.registry.SetReleasePolicyGeneration(
			trustSnapshot.Generation, trustSnapshot.Required,
			func(evidence registry.ApplicationEvidence) bool {
				return releaseEvidenceStillApproved(trustSnapshot, evidence)
			})
		for _, providerID := range needChallenge {
			if provider := s.registry.GetProvider(providerID); provider != nil {
				provider.RequestImmediateChallenge()
			}
		}
		if len(needChallenge) > 0 {
			s.logger.Info("release policy refresh left providers without current evidence; re-challenging immediately",
				"generation", trustSnapshot.Generation,
				"providers", len(needChallenge),
			)
			s.ddIncr("release_policy.evidence_invalidated", []string{fmt.Sprintf("providers:%d", len(needChallenge))})
		}
	}

	s.binaryHashPolicyMu.Lock()
	s.releaseKnownBinaryHashes = hashes
	s.releaseBinaryHashPolicyConfigured = policyConfigured || everConfigured
	s.rebuildBinaryHashPolicyLocked()
	knownHashCount := len(s.knownBinaryHashes)
	effectivePolicyConfigured := s.binaryHashPolicyConfigured
	s.binaryHashPolicyMu.Unlock()

	s.logger.Info("binary hashes synced from releases", "known_hashes", knownHashCount, "policy_configured", effectivePolicyConfigured)
	return nil
}

// convergeReleasePolicyWithCommittedRelease folds an already-committed release
// registration into the in-memory release trust policy when the post-mutation
// inventory read failed. GET /v1/releases/latest serves the committed row
// straight from the store, so retaining the pre-registration snapshot would
// distribute a release the policy can never authorize — providers installing it
// could never earn evidence and, with no background resync, would stay
// unroutable indefinitely. The merged snapshot is exactly what a successful
// rebuild over "last-known-good inventory + this row" publishes: entries for
// the same version/platform are replaced, everything else is carried forward
// (so still-approved evidence survives and routine registration never deroutes
// the fleet), and the newly saved release is immediately authorized. The next
// successful sync rebuilds from the exact inventory.
func (s *Server) convergeReleasePolicyWithCommittedRelease(release *store.Release, cause error) {
	s.releasePolicySyncMu.Lock()
	defer s.releasePolicySyncMu.Unlock()

	normalized, err := normalizeSHA256Hex(release.BinaryHash, "release.binary_hash")
	if err != nil {
		// Unreachable for the register handler (the hash was validated before
		// the row committed), and a full rebuild would skip such a row too.
		s.logger.Error("committed release has invalid binary hash; policy not converged",
			"version", release.Version, "platform", release.Platform, "error", err)
		return
	}

	generation := s.releaseTrustPolicyGeneration.Add(1)
	s.releaseInventoryEverConfigured.Store(true)
	trustSnapshot := &releaseTrustPolicySnapshot{
		Generation:   generation,
		Required:     true,
		ByBinaryHash: make(map[string][]approvedReleasePolicy),
	}
	if last := s.releaseTrustPolicy.Load(); last != nil {
		for hash, policies := range last.ByBinaryHash {
			for _, policy := range policies {
				if policy.Version == release.Version && policy.Platform == release.Platform {
					continue // replaced by this registration
				}
				trustSnapshot.ByBinaryHash[hash] = append(trustSnapshot.ByBinaryHash[hash], policy)
			}
		}
	}
	templates := make(map[string]string)
	for _, pair := range strings.Split(release.TemplateHashes, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			templates[parts[0]] = parts[1]
		}
	}
	trustSnapshot.ByBinaryHash[normalized] = append(
		trustSnapshot.ByBinaryHash[normalized],
		approvedReleasePolicy{
			Version: release.Version, Platform: release.Platform, Backend: release.Backend,
			BinaryHash: normalized, MetallibHash: release.MetallibHash,
			PythonHash: release.PythonHash, RuntimeHash: release.RuntimeHash,
			TemplateHashes: templates,
		})
	s.releaseTrustPolicy.Store(trustSnapshot)
	if s.registry != nil {
		needChallenge := s.registry.SetReleasePolicyGeneration(
			trustSnapshot.Generation, trustSnapshot.Required,
			func(evidence registry.ApplicationEvidence) bool {
				return releaseEvidenceStillApproved(trustSnapshot, evidence)
			})
		for _, providerID := range needChallenge {
			if provider := s.registry.GetProvider(providerID); provider != nil {
				provider.RequestImmediateChallenge()
			}
		}
		if len(needChallenge) > 0 {
			s.ddIncr("release_policy.evidence_invalidated", []string{fmt.Sprintf("providers:%d", len(needChallenge))})
		}
	}
	hashes := make(map[string]bool, len(trustSnapshot.ByBinaryHash))
	for hash := range trustSnapshot.ByBinaryHash {
		hashes[hash] = true
	}
	s.binaryHashPolicyMu.Lock()
	s.releaseKnownBinaryHashes = hashes
	s.releaseBinaryHashPolicyConfigured = true
	s.rebuildBinaryHashPolicyLocked()
	s.binaryHashPolicyMu.Unlock()

	s.logger.Warn("release inventory unreadable after registration; converged policy from the committed release",
		"version", release.Version,
		"platform", release.Platform,
		"generation", generation,
		"error", cause,
	)
	s.ddIncr("release_policy.sync_failure", []string{"outcome:converged_from_mutation"})
}

// convergeReleasePolicyWithCommittedDeactivation folds an already-committed
// release deactivation into the in-memory release trust policy when the
// post-mutation inventory read failed. Retaining the pre-deactivation snapshot
// would keep authorizing the deactivated release indefinitely — there is no
// background resync, so in a force=true emergency pull of a compromised
// release the affected providers would keep routing until an admin retried.
// The merged snapshot is exactly what a successful rebuild over
// "last-known-good inventory minus this row" publishes: entries for the
// deactivated version/platform are dropped, everything else is carried forward
// (so still-approved evidence survives and pulling one release never deroutes
// the rest of the fleet), and providers whose evidence rested on the
// deactivated release are invalidated and kicked for an immediate
// re-challenge. The next successful sync rebuilds from the exact inventory.
func (s *Server) convergeReleasePolicyWithCommittedDeactivation(version, platform string, cause error) {
	s.releasePolicySyncMu.Lock()
	defer s.releasePolicySyncMu.Unlock()

	last := s.releaseTrustPolicy.Load()
	generation := s.releaseTrustPolicyGeneration.Add(1)
	// Deactivation never un-configures the inventory: once releases have been
	// published the evidence gate stays required, exactly as a full rebuild
	// over the remaining (possibly empty) release set would keep it.
	required := s.releaseInventoryEverConfigured.Load()
	if last != nil && last.Required {
		required = true
	}
	trustSnapshot := &releaseTrustPolicySnapshot{
		Generation:   generation,
		Required:     required,
		ByBinaryHash: make(map[string][]approvedReleasePolicy),
	}
	if last != nil {
		for hash, policies := range last.ByBinaryHash {
			for _, policy := range policies {
				if policy.Version == version && policy.Platform == platform {
					continue // removed by this deactivation
				}
				trustSnapshot.ByBinaryHash[hash] = append(trustSnapshot.ByBinaryHash[hash], policy)
			}
		}
	}
	s.releaseTrustPolicy.Store(trustSnapshot)
	if s.registry != nil {
		needChallenge := s.registry.SetReleasePolicyGeneration(
			trustSnapshot.Generation, trustSnapshot.Required,
			func(evidence registry.ApplicationEvidence) bool {
				return releaseEvidenceStillApproved(trustSnapshot, evidence)
			})
		for _, providerID := range needChallenge {
			if provider := s.registry.GetProvider(providerID); provider != nil {
				provider.RequestImmediateChallenge()
			}
		}
		if len(needChallenge) > 0 {
			s.ddIncr("release_policy.evidence_invalidated", []string{fmt.Sprintf("providers:%d", len(needChallenge))})
		}
	}
	hashes := make(map[string]bool, len(trustSnapshot.ByBinaryHash))
	for hash := range trustSnapshot.ByBinaryHash {
		hashes[hash] = true
	}
	s.binaryHashPolicyMu.Lock()
	s.releaseKnownBinaryHashes = hashes
	s.releaseBinaryHashPolicyConfigured = len(hashes) > 0 || required
	s.rebuildBinaryHashPolicyLocked()
	s.binaryHashPolicyMu.Unlock()

	s.logger.Warn("release inventory unreadable after deactivation; converged policy from the committed deactivation",
		"version", version,
		"platform", platform,
		"generation", generation,
		"error", cause,
	)
	s.ddIncr("release_policy.sync_failure", []string{"outcome:converged_from_mutation"})
}

// releaseEvidenceStillApproved reports whether previously granted application
// evidence remains approved under a freshly built release-policy snapshot: the
// same binary hash still maps to an active release with the same version,
// platform, and backend, and that release's metallib hash is unchanged. These
// are the ONLY facts application evidence proves — python/runtime/per-family
// template facts were deliberately removed (mlx-swift providers never report
// them; requiring them made evidence underivable fleet-wide, 2026-08-31
// incident). Binary hash and metallib fail closed on absence or mismatch.
func releaseEvidenceStillApproved(
	snapshot *releaseTrustPolicySnapshot,
	evidence registry.ApplicationEvidence,
) bool {
	if evidence.BinaryHash == "" || evidence.MetallibHash == "" {
		return false
	}
	for _, candidate := range snapshot.ByBinaryHash[evidence.BinaryHash] {
		if candidate.Version != evidence.Version ||
			candidate.Platform != evidence.Platform ||
			candidate.Platform == "" {
			continue
		}
		// Legacy release rows may carry an empty backend (the column was added
		// with an empty default); treat it as matching the evidence backend,
		// mirroring deriveApprovedReleaseTransition.
		if candidate.Backend != "" && candidate.Backend != evidence.Backend {
			continue
		}
		expectedMetallib, err := normalizeSHA256Hex(candidate.MetallibHash, "release.metallib_hash")
		if err != nil || expectedMetallib != evidence.MetallibHash {
			continue
		}
		return true
	}
	return false
}

// Closed outcome set for application-evidence derivation. Every
// deriveApprovedReleaseTransition return path records exactly one of these as
// a release_evidence.outcome DogStatsD counter tag so a candidate coordinator
// can be judged in SHADOW mode from per-reason fleet counts instead of a
// silent boolean (the 2026-08-31 zero-capacity deploys were undiagnosable
// precisely because every rejection branch looked identical). No hashes,
// keys, serials, or tokens ride on these tags.
const (
	evidenceOutcomeGranted                 = "granted"
	evidenceReasonPrecondition             = "precondition"
	evidenceReasonInvalidBinaryHash        = "invalid_binary_hash"
	evidenceReasonPolicyUnavailable        = "policy_unavailable"
	evidenceReasonPolicyNotRequired        = "policy_not_required"
	evidenceReasonProcessIdentity          = "process_identity"
	evidenceReasonRuntimeGate              = "runtime_gate"
	evidenceReasonVersionFloor             = "version_floor"
	evidenceReasonRegistrationHashMismatch = "registration_hash_mismatch"
	evidenceReasonNoActiveRelease          = "no_active_release"
	evidenceReasonMetallibMismatch         = "metallib_mismatch"
)

// recordReleaseEvidenceOutcome counts one application-evidence derivation
// outcome. No-op without DogStatsD.
func (s *Server) recordReleaseEvidenceOutcome(outcome string) {
	s.ddIncr("release_evidence.outcome", []string{"outcome:" + outcome})
}

// evidenceRejected records the typed rejection reason and returns the empty
// derivation result.
func (s *Server) evidenceRejected(reason string) (approvedReleaseTransitionFact, registry.ApplicationEvidence, bool) {
	s.recordReleaseEvidenceOutcome(reason)
	return approvedReleaseTransitionFact{}, registry.ApplicationEvidence{}, false
}

func (s *Server) deriveApprovedReleaseTransition(
	provider *registry.Provider,
	resp *protocol.AttestationResponseMessage,
	statusFieldsTrusted bool,
) (approvedReleaseTransitionFact, registry.ApplicationEvidence, bool) {
	if provider == nil || resp == nil || !statusFieldsTrusted ||
		resp.SIPEnabled == nil || !*resp.SIPEnabled ||
		resp.SecureBootEnabled == nil || !*resp.SecureBootEnabled ||
		provider.ChallengeShouldStop() {
		return s.evidenceRejected(evidenceReasonPrecondition)
	}
	freshHash, err := normalizeSHA256Hex(resp.BinaryHash, "binary_hash")
	if err != nil {
		return s.evidenceRejected(evidenceReasonInvalidBinaryHash)
	}
	snapshot := s.releaseTrustPolicy.Load()
	if snapshot == nil {
		return s.evidenceRejected(evidenceReasonPolicyUnavailable)
	}

	provider.Mu().Lock()
	version, backend, processKey := provider.Version, provider.Backend, provider.PublicKey
	apnsToken := provider.APNsDeviceToken
	runtimeVerified := provider.RuntimeVerified
	manifestChecked := provider.RuntimeManifestChecked
	metallibVerified := provider.MetallibVerified
	attested := provider.AttestationResult
	provider.Mu().Unlock()
	// An APNs device token is deliberately NOT required: application evidence
	// proves the live binary/runtime is an active approved release, while APNs
	// token possession is enforced exclusively by the code-identity gate (with
	// its own grace semantics). Tokenless legacy/headless providers with a
	// valid signed challenge must still derive and keep evidence.
	if !snapshot.Required {
		return s.evidenceRejected(evidenceReasonPolicyNotRequired)
	}
	if processKey == "" || attested == nil || !attested.Valid ||
		attested.PublicKey == "" || attested.SerialNumber == "" {
		return s.evidenceRejected(evidenceReasonProcessIdentity)
	}
	if !runtimeVerified || !manifestChecked || !metallibVerified {
		return s.evidenceRejected(evidenceReasonRuntimeGate)
	}
	if s.minProviderVersion != "" &&
		(version == "" || semverLess(version, s.minProviderVersion)) {
		return s.evidenceRejected(evidenceReasonVersionFloor)
	}
	// Registration-time binary_hash is optional and the production fleet omits
	// it. The fresh hash is carried by this already-signature-verified challenge
	// from the same attested SE identity and is still required to match an active
	// release below. When registration did carry a hash, keep the stronger
	// cross-check and fail closed on a mismatch.
	if strings.TrimSpace(attested.BinaryHash) != "" {
		attestedHash, hashErr := normalizeSHA256Hex(attested.BinaryHash, "attested binary_hash")
		if hashErr != nil || attestedHash != freshHash {
			return s.evidenceRejected(evidenceReasonRegistrationHashMismatch)
		}
	}

	// Legacy release rows can carry an empty backend: the migration added the
	// column with an empty default and registration accepts an omitted backend.
	// Such rows MUST NOT leave providers permanently unroutable — an empty
	// backend matches the provider-reported backend (an exact match is
	// preferred when both exist), and the derived fact/evidence is stamped with
	// the provider-reported backend so routing's evidence.Backend == p.Backend
	// check keeps holding.
	var current approvedReleasePolicy
	found := false
	for _, candidate := range snapshot.ByBinaryHash[freshHash] {
		if candidate.Version == version && candidate.Backend == backend &&
			candidate.Platform != "" {
			current = candidate
			found = true
			break
		}
	}
	if !found {
		for _, candidate := range snapshot.ByBinaryHash[freshHash] {
			if candidate.Version == version && candidate.Backend == "" &&
				candidate.Platform != "" {
				current = candidate
				current.Backend = backend
				found = true
				break
			}
		}
	}
	if !found {
		return s.evidenceRejected(evidenceReasonNoActiveRelease)
	}
	if !releaseMetallibMatches(current, resp) {
		return s.evidenceRejected(evidenceReasonMetallibMismatch)
	}

	approvedFrom := make(map[string]struct{})
	for binaryHash := range snapshot.ByBinaryHash {
		if approvedTransitionPredecessor(
			snapshot, binaryHash,
			current.Platform, current.Backend, current.Version,
		) {
			approvedFrom[binaryHash] = struct{}{}
		}
	}
	metallibHash, _ := normalizeSHA256Hex(
		resp.TemplateHashes["mlx_metallib"], "mlx_metallib")
	fact := approvedReleaseTransitionFact{
		Approved: true, BinaryHash: freshHash, Version: current.Version,
		Platform: current.Platform, Backend: current.Backend,
		PolicyGeneration:         snapshot.Generation,
		ApprovedFromBinaryHashes: approvedFrom,
	}
	evidence := registry.ApplicationEvidence{
		SEPublicKey: attested.PublicKey, Serial: attested.SerialNumber,
		ProcessPublicKey: processKey, APNsToken: apnsToken,
		BinaryHash: freshHash,
		Version:    current.Version, Platform: current.Platform,
		Backend:      current.Backend,
		MetallibHash: metallibHash, VerifiedAt: time.Now().UTC(),
		PolicyGeneration: snapshot.Generation,
	}
	s.recordReleaseEvidenceOutcome(evidenceOutcomeGranted)
	return fact, evidence, true
}

// releaseMetallibMatches verifies the ONE release-specific runtime fact both
// sides always hold: the release row's metallib hash must equal the provider's
// reported mlx_metallib template hash (both normalized 64-hex; absence on
// either side fails closed). Nothing else is compared here by design — the
// python plane is gone (mlx-swift providers hardcode it nil), and release
// rows' per-model-family template hashes were CI fabrications (hashed from
// CDN jinja files by release-swift.yml) that no provider ever reported;
// requiring provider coverage of those made application evidence underivable
// for 100% of the production fleet (2026-08-31 zero-capacity incident).
// Binary-hash ↔ active-release matching is the caller's job.
func releaseMetallibMatches(policy approvedReleasePolicy, resp *protocol.AttestationResponseMessage) bool {
	if policy.MetallibHash == "" {
		return false
	}
	expectedMetallib, err := normalizeSHA256Hex(policy.MetallibHash, "release.metallib_hash")
	if err != nil {
		return false
	}
	gotMetallib, err := normalizeSHA256Hex(resp.TemplateHashes["mlx_metallib"], "mlx_metallib")
	return err == nil && gotMetallib == expectedMetallib
}

func (s *Server) rebuildBinaryHashPolicyLocked() {
	hashes := make(map[string]bool, len(s.manualKnownBinaryHashes)+len(s.releaseKnownBinaryHashes))
	for h := range s.releaseKnownBinaryHashes {
		hashes[h] = true
	}
	for h := range s.manualKnownBinaryHashes {
		hashes[h] = true
	}
	s.knownBinaryHashes = hashes
	s.binaryHashPolicyConfigured = s.manualBinaryHashPolicyConfigured || s.releaseBinaryHashPolicyConfigured
}

func (s *Server) binaryHashPolicySnapshot() (bool, map[string]bool) {
	s.binaryHashPolicyMu.RLock()
	defer s.binaryHashPolicyMu.RUnlock()

	return s.binaryHashPolicyConfigured, s.knownBinaryHashes
}

// SyncRuntimeManifest builds the runtime manifest from active releases.
// Called after a release is registered to auto-update the expected hashes.
func (s *Server) SyncRuntimeManifest() error {
	releases, err := s.store.ListReleasesWithError()
	if err != nil {
		s.logger.Warn("SyncRuntimeManifest: release inventory unavailable; keeping existing manifest",
			"error", err)
		return fmt.Errorf("sync runtime manifest: %w", err)
	}

	// Minimum provider version is set manually via EIGENINFERENCE_MIN_PROVIDER_VERSION
	// env var. It is NOT auto-derived from the latest release — pushing a new release
	// should not instantly knock all existing providers offline.

	// Every hash — python, runtime, AND each template name including
	// mlx_metallib — is unioned into a SET across ALL active releases.
	// Releases overlap in production for the whole self-update window
	// (providers poll for updates every 30 minutes), so the manifest must
	// accept the runtime facts of every release a connected provider may
	// legitimately be running. Template hashes used to be single-valued per
	// name (newest release wins): registering v0.8.16 replaced the v0.8.15
	// metallib hash and derouted ~1,180 still-current providers at their next
	// challenge (2026-09-03 fleet brownout). Deactivating a release is the
	// mechanism that removes its hashes; iteration order is irrelevant.
	manifest := NewRuntimeManifest()
	hasAny := false
	for _, r := range releases {
		if !r.Active {
			continue
		}
		if r.PythonHash != "" {
			manifest.PythonHashes[r.PythonHash] = true
			hasAny = true
		}
		if r.RuntimeHash != "" {
			manifest.RuntimeHashes[r.RuntimeHash] = true
			hasAny = true
		}
		if manifest.addTemplateHashPairs(r.TemplateHashes) {
			hasAny = true
		}
		if r.MetallibHash != "" {
			normalized, err := normalizeSHA256Hex(r.MetallibHash, "release.metallib_hash")
			if err != nil {
				s.logger.Warn("invalid release metallib hash ignored",
					"version", r.Version,
					"platform", r.Platform,
					"error", err,
				)
			} else if manifest.AddTemplateHash("mlx_metallib", normalized) {
				hasAny = true
			}
		}
	}

	if hasAny {
		s.knownRuntimeManifest = manifest
		s.logger.Info("runtime manifest synced from releases",
			"python_hashes", len(manifest.PythonHashes),
			"runtime_hashes", len(manifest.RuntimeHashes),
			"template_hashes", len(manifest.TemplateHashes),
			"template_hash_sets", manifest.templateHashSetSizes(),
		)
	} else if len(releases) > 0 {
		// Explicit empty: releases exist but none have hashes. Clear manifest.
		s.knownRuntimeManifest = nil
		s.logger.Info("runtime manifest cleared: releases exist but none have runtime hashes")
	} else {
		// Empty releases slice (not nil — nil is handled above). No releases
		// at all, which is only expected on a fresh coordinator. Keep
		// existing manifest if one exists.
		if s.knownRuntimeManifest != nil {
			s.logger.Warn("SyncRuntimeManifest: zero releases returned, keeping existing manifest")
			return nil
		}
		s.knownRuntimeManifest = nil
	}

	s.revalidateConnectedProvidersAgainstRuntimePolicy()
	return nil
}

// convergeRuntimeManifestWithCommittedRelease folds an already-committed
// release registration into the runtime manifest when the post-mutation
// inventory read failed, so a transient store hiccup cannot leave the manifest
// rejecting the runtime facts of the release that /v1/releases/latest is
// already distributing. Every hash set — including each per-template-name
// set — is additive, exactly like a full rebuild (which unions every active
// release): the previous release's fleet keeps passing while the newly saved
// release is accepted too. The next successful sync rebuilds from the exact
// inventory.
func (s *Server) convergeRuntimeManifestWithCommittedRelease(release *store.Release, cause error) {
	merged := s.knownRuntimeManifest.clone()
	contributed := false
	if release.PythonHash != "" {
		merged.PythonHashes[release.PythonHash] = true
		contributed = true
	}
	if release.RuntimeHash != "" {
		merged.RuntimeHashes[release.RuntimeHash] = true
		contributed = true
	}
	if merged.addTemplateHashPairs(release.TemplateHashes) {
		contributed = true
	}
	if release.MetallibHash != "" {
		if normalized, err := normalizeSHA256Hex(release.MetallibHash, "release.metallib_hash"); err == nil &&
			merged.AddTemplateHash("mlx_metallib", normalized) {
			contributed = true
		}
	}
	if !contributed {
		// The committed release carries no runtime facts; a full rebuild would
		// republish the union of the remaining releases — the current manifest.
		return
	}
	s.knownRuntimeManifest = merged
	s.logger.Warn("release inventory unreadable after registration; converged runtime manifest from the committed release",
		"version", release.Version,
		"platform", release.Platform,
		"error", cause,
	)
	s.revalidateConnectedProvidersAgainstRuntimePolicy()
}

// convergeRuntimeManifestWithCommittedDeactivation folds an already-committed
// release deactivation into the runtime manifest when the post-mutation
// inventory read failed. Unlike registration (where the new release's facts
// are simply unioned in), deactivation cannot blindly subtract the pulled
// release's hashes — another active release may share them — so the manifest
// is rebuilt from the live release trust snapshot, which at this point already
// excludes the deactivated version/platform (SyncBinaryHashes either succeeded
// or was converged from the same committed deactivation first). Every hash
// set — including each per-template-name set — is the union of the remaining
// authorized releases, exactly like the full rebuild. Active releases whose
// binary hash failed normalization are absent from the snapshot and thus from
// this approximation; the next successful sync rebuilds from the exact
// inventory.
func (s *Server) convergeRuntimeManifestWithCommittedDeactivation(version, platform string, cause error) {
	merged := NewRuntimeManifest()
	hasAny := false
	if snapshot := s.releaseTrustPolicy.Load(); snapshot != nil {
		for _, policies := range snapshot.ByBinaryHash {
			for _, policy := range policies {
				if policy.PythonHash != "" {
					merged.PythonHashes[policy.PythonHash] = true
					hasAny = true
				}
				if policy.RuntimeHash != "" {
					merged.RuntimeHashes[policy.RuntimeHash] = true
					hasAny = true
				}
				for name, hash := range policy.TemplateHashes {
					if merged.AddTemplateHash(name, hash) {
						hasAny = true
					}
				}
				if policy.MetallibHash != "" {
					if normalized, err := normalizeSHA256Hex(policy.MetallibHash, "release.metallib_hash"); err == nil &&
						merged.AddTemplateHash("mlx_metallib", normalized) {
						hasAny = true
					}
				}
			}
		}
	}
	if !hasAny {
		// The deactivated row committed, so releases exist(ed) but none of the
		// remaining authorized ones carry runtime facts: explicit withdrawal,
		// exactly like the full rebuild's "releases exist but none have
		// hashes" branch. Providers proving the pulled release's facts must
		// not keep passing the manifest gate.
		merged = nil
	}
	s.knownRuntimeManifest = merged
	s.logger.Warn("release inventory unreadable after deactivation; converged runtime manifest from the retained policy snapshot",
		"version", version,
		"platform", platform,
		"error", cause,
	)
	s.revalidateConnectedProvidersAgainstRuntimePolicy()
}

func (s *Server) revalidateConnectedProvidersAgainstRuntimePolicy() {
	// Release-inventory errors are already guarded in SyncRuntimeManifest, which
	// returns the error before reaching this function.
	// A nil manifest here means releases exist but none carry runtime hashes,
	// i.e. an intentional manifest withdrawal. Providers must be derouted.

	for _, providerID := range s.registry.ProviderIDs() {
		provider := s.registry.GetProvider(providerID)
		if provider == nil {
			continue
		}

		provider.Mu().Lock()
		pythonHash := provider.PythonHash
		runtimeHash := provider.RuntimeHash
		templateHashes := registry.CloneStringMap(provider.TemplateHashes)
		version := provider.Version
		backend := provider.Backend

		// Manifest policy is coordinator-owned and can be withdrawn, rotated,
		// or rolled back independently of the connected process. Rebuild all
		// policy-derived state from scratch, but preserve FreshCodeAttested:
		// that proof remains bound to this connection's token, keys, and code.
		// The token/key/code/trust invalidation paths clear it separately.
		provider.RuntimeVerified = false
		provider.RuntimeManifestChecked = false
		provider.MetallibVerified = false
		provider.RuntimeCapabilities = nil

		if s.knownRuntimeManifest == nil {
			// Manifest was withdrawn — keep the process proof, but deroute the
			// provider until policy once again approves its reported runtime.
		} else if s.minProviderVersion != "" &&
			version != "" &&
			semverLess(version, s.minProviderVersion) {
			s.ddIncr("provider_version_below_minimum", []string{"gate:manifest_sync", "version:" + version})
		} else {
			runtimeOK, _ := s.verifyRuntimeHashesForBackend(
				backend,
				pythonHash,
				runtimeHash,
				templateHashes,
			)
			provider.RuntimeVerified = runtimeOK
			provider.RuntimeManifestChecked = runtimeOK
			provider.MetallibVerified = runtimeOK &&
				runtimeManifestApprovesMetallib(
					s.knownRuntimeManifest, templateHashes)
		}
		provider.Mu().Unlock()
		if err := s.registry.ReconcileAttestedRuntimeCapabilities(providerID); err != nil {
			s.logger.Warn("runtime policy capability reconciliation failed",
				"provider_id", providerID, "error", err)
		}
		if cleared := s.registry.ClearIneligiblePendingModelLoads(providerID); cleared > 0 {
			s.logger.Info("cleared pending model loads after runtime policy revocation",
				"provider_id", providerID, "count", cleared)
		}
	}
}

func runtimeManifestApprovesMetallib(
	manifest *RuntimeManifest,
	reported map[string]string,
) bool {
	if manifest == nil {
		return false
	}
	return templateHashAccepted(manifest.TemplateHashes["mlx_metallib"], reported["mlx_metallib"])
}

// RuntimeManifest holds the set of accepted hashes for provider runtime components.
// When configured, the coordinator verifies provider-reported hashes against
// this manifest at registration and during periodic attestation challenges.
//
// Every field is a SET: the manifest is the UNION of every ACTIVE release's
// runtime facts, and a provider passes when its reported value is one of the
// accepted values for that component. TemplateHashes is a set PER template
// name (mlx_metallib included). It must never collapse to a single value per
// name: releases overlap in production for the whole self-update window, and a
// single-valued mlx_metallib entry derouted ~1,180 providers still running the
// previous release the moment the next one was registered (2026-09-03).
// Deactivating a release is the mechanism that removes its values.
type RuntimeManifest struct {
	PythonHashes   map[string]bool            `json:"python_hashes"`   // set of accepted Python runtime hashes
	RuntimeHashes  map[string]bool            `json:"runtime_hashes"`  // set of accepted inference runtime hashes
	TemplateHashes map[string]map[string]bool `json:"template_hashes"` // template_name -> set of accepted hashes
}

// NewRuntimeManifest returns an empty manifest with every set allocated.
func NewRuntimeManifest() *RuntimeManifest {
	return &RuntimeManifest{
		PythonHashes:   make(map[string]bool),
		RuntimeHashes:  make(map[string]bool),
		TemplateHashes: make(map[string]map[string]bool),
	}
}

// AddTemplateHash records value as an accepted hash for template name and
// reports whether anything was recorded. Values are trimmed and lower-cased so
// membership is case-insensitive (SHA-256 hex) and identical on the
// registration, challenge, and revalidation paths; empty names/values are
// ignored.
func (m *RuntimeManifest) AddTemplateHash(name, value string) bool {
	name = strings.TrimSpace(name)
	value = strings.ToLower(strings.TrimSpace(value))
	if name == "" || value == "" {
		return false
	}
	if m.TemplateHashes == nil {
		m.TemplateHashes = make(map[string]map[string]bool)
	}
	accepted := m.TemplateHashes[name]
	if accepted == nil {
		accepted = make(map[string]bool)
		m.TemplateHashes[name] = accepted
	}
	accepted[value] = true
	return true
}

// addTemplateHashPairs unions a release row's "name=hash,name=hash" list into
// the manifest and reports whether any entry was recorded.
func (m *RuntimeManifest) addTemplateHashPairs(raw string) bool {
	added := false
	for _, pair := range strings.Split(raw, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 && m.AddTemplateHash(parts[0], parts[1]) {
			added = true
		}
	}
	return added
}

// clone deep-copies the manifest; a nil receiver yields an empty manifest.
func (m *RuntimeManifest) clone() *RuntimeManifest {
	out := NewRuntimeManifest()
	if m == nil {
		return out
	}
	for hash := range m.PythonHashes {
		out.PythonHashes[hash] = true
	}
	for hash := range m.RuntimeHashes {
		out.RuntimeHashes[hash] = true
	}
	for name, accepted := range m.TemplateHashes {
		for hash := range accepted {
			out.AddTemplateHash(name, hash)
		}
	}
	return out
}

// templateHashSetSizes renders "name=count" pairs (sorted by name) for logs,
// so a sync line shows how many releases' values each template accepts.
func (m *RuntimeManifest) templateHashSetSizes() string {
	names := make([]string, 0, len(m.TemplateHashes))
	for name := range m.TemplateHashes {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", name, len(m.TemplateHashes[name])))
	}
	return strings.Join(parts, ",")
}

// templateHashAccepted reports whether got is one of the accepted hashes for
// a template (case-insensitive; empty values never match).
func templateHashAccepted(accepted map[string]bool, got string) bool {
	got = strings.ToLower(strings.TrimSpace(got))
	return got != "" && accepted[got]
}

// sortedTemplateHashes lists a template's accepted hashes deterministically
// for diagnostics and the public manifest endpoint.
func sortedTemplateHashes(accepted map[string]bool) []string {
	out := make([]string, 0, len(accepted))
	for hash := range accepted {
		out = append(out, hash)
	}
	sort.Strings(out)
	return out
}

// semverGreater returns true when a has higher SemVer precedence than b,
// including the numeric/alphanumeric prerelease identifier rules. Invalid
// non-empty versions sort below valid versions so minimum-version gates fail
// closed.
func semverGreater(a, b string) bool {
	if a == "" {
		return false
	}
	if b == "" {
		return true
	}
	av := a
	if !strings.HasPrefix(av, "v") {
		av = "v" + av
	}
	bv := b
	if !strings.HasPrefix(bv, "v") {
		bv = "v" + bv
	}
	aValid, bValid := semver.IsValid(av), semver.IsValid(bv)
	switch {
	case aValid && bValid:
		return semver.Compare(av, bv) > 0
	case aValid:
		return true
	default:
		return false
	}
}

// semverLess returns true if version a is less than version b.
func semverLess(a, b string) bool {
	return semverGreater(b, a)
}

// SetRuntimeManifest configures the known-good runtime manifest for provider
// verification. Pass nil to disable runtime verification (all providers pass).
func (s *Server) SetRuntimeManifest(m *RuntimeManifest) {
	s.knownRuntimeManifest = m
}

func (s *Server) verifyRuntimeHashesForBackend(backend, pythonHash, runtimeHash string, templateHashes map[string]string) (bool, []protocol.RuntimeMismatch) {
	if s.knownRuntimeManifest == nil {
		return true, nil
	}

	// Only mlx-swift backends are supported. Non-Swift backends (legacy
	// Python/inprocess-mlx) are deprecated and immediately rejected.
	if !registry.BackendUsesSwiftRuntime(backend) {
		return false, []protocol.RuntimeMismatch{{
			Component: "backend",
			Expected:  "mlx-swift",
			Got:       backend,
		}}
	}

	manifest := s.knownRuntimeManifest
	scoped := NewRuntimeManifest()
	scopedReportedTemplates := make(map[string]string)

	if accepted := manifest.TemplateHashes["mlx_metallib"]; len(accepted) > 0 {
		scoped.TemplateHashes["mlx_metallib"] = accepted
	}
	if got := templateHashes["mlx_metallib"]; got != "" {
		scopedReportedTemplates["mlx_metallib"] = got
	}

	return s.verifyRuntimeHashesAgainstManifest(scoped, pythonHash, runtimeHash, scopedReportedTemplates)
}

func (s *Server) verifyRuntimeHashesAgainstManifest(manifest *RuntimeManifest, pythonHash, runtimeHash string, templateHashes map[string]string) (bool, []protocol.RuntimeMismatch) {
	if manifest == nil {
		return true, nil
	}

	var mismatches []protocol.RuntimeMismatch

	requireOneOf := func(component, got string, accepted map[string]bool) {
		if len(accepted) == 0 {
			return
		}
		if got == "" {
			mismatches = append(mismatches, protocol.RuntimeMismatch{
				Component: component,
				Expected:  "reported hash matching one of known-good values",
				Got:       "(missing)",
			})
			return
		}
		if !accepted[got] {
			mismatches = append(mismatches, protocol.RuntimeMismatch{
				Component: component,
				Expected:  "one of known-good hashes",
				Got:       got,
			})
		}
	}

	requireOneOf("python", pythonHash, manifest.PythonHashes)
	requireOneOf("runtime", runtimeHash, manifest.RuntimeHashes)

	if len(manifest.TemplateHashes) > 0 {
		// Each template name maps to the SET of hashes accepted across every
		// active release; the reported value must be one of them.
		for name, accepted := range manifest.TemplateHashes {
			if len(accepted) == 0 {
				continue
			}
			expected := "one of " + strings.Join(sortedTemplateHashes(accepted), ",")
			got, ok := templateHashes[name]
			if !ok || strings.TrimSpace(got) == "" {
				mismatches = append(mismatches, protocol.RuntimeMismatch{
					Component: "template:" + name,
					Expected:  expected,
					Got:       "(missing)",
				})
				continue
			}
			if !templateHashAccepted(accepted, got) {
				mismatches = append(mismatches, protocol.RuntimeMismatch{
					Component: "template:" + name,
					Expected:  expected,
					Got:       got,
				})
			}
		}
		for name, got := range templateHashes {
			if len(manifest.TemplateHashes[name]) == 0 {
				mismatches = append(mismatches, protocol.RuntimeMismatch{
					Component: "template:" + name,
					Expected:  "template listed in runtime manifest",
					Got:       got,
				})
			}
		}
	}

	return len(mismatches) == 0, mismatches
}

// handleRuntimeManifest returns the current runtime manifest as JSON.
// No auth required — hashes are not secrets.
func (s *Server) handleRuntimeManifest(w http.ResponseWriter, r *http.Request) {
	if cached, ok := s.readCache.Get(runtimeManifestCacheKey); ok {
		writeCachedJSON(w, cached)
		return
	}
	var resp map[string]any
	if s.knownRuntimeManifest == nil {
		resp = map[string]any{"configured": false}
	} else {
		// template_hashes is rendered as name -> sorted list of every hash
		// accepted across the active releases: the manifest is a union, not a
		// single expected value per template.
		templates := make(map[string][]string, len(s.knownRuntimeManifest.TemplateHashes))
		for name, accepted := range s.knownRuntimeManifest.TemplateHashes {
			templates[name] = sortedTemplateHashes(accepted)
		}
		resp = map[string]any{
			"configured":      true,
			"python_hashes":   s.knownRuntimeManifest.PythonHashes,
			"runtime_hashes":  s.knownRuntimeManifest.RuntimeHashes,
			"template_hashes": templates,
		}
	}
	body, err := json.Marshal(resp)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse("internal_error", "failed to encode manifest"))
		return
	}
	s.readCache.Set(runtimeManifestCacheKey, body, time.Minute)
	writeCachedJSON(w, body)
}
