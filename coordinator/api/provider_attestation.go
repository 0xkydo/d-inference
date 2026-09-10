package api

// Secure Enclave, MDM, Apple device attestation, and trust status.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"

	"strings"
	"time"

	"github.com/eigeninference/d-inference/coordinator/attestation"
	"github.com/eigeninference/d-inference/coordinator/mdm"
	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/store"
)

// verifyProviderAttestation verifies a provider's Secure Enclave attestation
// if one was included in the registration message. If the attestation is valid,
// the provider is marked as attested. If missing or invalid, the provider is
// accepted in Open Mode only when no binary hash policy is configured.
func (s *Server) verifyProviderAttestation(providerID string, provider *registry.Provider, regMsg *protocol.RegisterMessage) {
	policyConfigured, knownBinaryHashes := s.binaryHashPolicySnapshot()
	if len(regMsg.Attestation) == 0 {
		if policyConfigured {
			s.logger.Warn("provider registered without attestation while binary hash policy is configured",
				"provider_id", providerID,
			)
			provider.SetAttestationResult(&attestation.VerificationResult{
				Valid: false,
				Error: "attestation missing",
			})
			s.registry.MarkUntrusted(providerID)
			return
		}
		s.logger.Info("provider registered without attestation (Open Mode)",
			"provider_id", providerID,
		)
		return
	}

	result, err := attestation.VerifyJSON(regMsg.Attestation)
	if err != nil {
		s.logger.Warn("failed to parse provider attestation",
			"provider_id", providerID,
			"error", err,
		)
		if policyConfigured {
			provider.SetAttestationResult(&attestation.VerificationResult{
				Valid: false,
				Error: "attestation invalid",
			})
			s.registry.MarkUntrusted(providerID)
		}
		return
	}

	provider.SetAttestationResult(&result)

	if !result.Valid {
		s.logger.Warn("provider attestation invalid",
			"provider_id", providerID,
			"error", result.Error,
		)
		if policyConfigured {
			s.registry.MarkUntrusted(providerID)
		}
		return
	}

	enforceReconnectFreshness := regMsg.Version != "" &&
		!semverLess(regMsg.Version, minProviderVersionForReconnectAttestation)
	if enforceReconnectFreshness &&
		!attestation.CheckTimestamp(result, RegistrationAttestationMaxAge) {
		result.Valid = false
		result.Error = "attestation timestamp outside freshness window"
		provider.SetAttestationResult(&result)
		s.registry.MarkUntrusted(providerID)
		s.logger.Warn("provider registration attestation replay rejected",
			"provider_id", providerID)
		return
	}

	if !enforceReconnectFreshness {
		// Pre-0.8.15 providers reuse their signed registration blob across
		// reconnects. Preserve that legacy identity proof, but discard the
		// protected-runtime fields before storing it so no later trust or
		// challenge transition can promote apple_m5/mlx_nax from a replayable
		// claim. Their periodic nonce challenges remain the liveness proof.
		result.ChipFamily = ""
		result.RuntimeCapabilities = nil
		result.MetallibHash = ""
		provider.SetAttestationResult(&result)
	}

	// Bind the WebSocket X25519 key used for E2E text encryption to the
	// attested Secure Enclave identity. If a provider wants to serve private
	// text, the attestation must carry the same encryption public key.
	if regMsg.PublicKey != "" {
		if result.EncryptionPublicKey == "" {
			s.logger.Warn("attestation missing encryption key for registered public key",
				"provider_id", providerID,
			)
			result.Valid = false
			result.Error = "attestation missing encryption public key"
			provider.SetAttestationResult(&result)
			if policyConfigured {
				s.registry.MarkUntrusted(providerID)
			}
			return
		}
		if result.EncryptionPublicKey != regMsg.PublicKey {
			s.logger.Warn("attestation encryption key does not match register public key",
				"provider_id", providerID,
				"attestation_key", result.EncryptionPublicKey,
				"register_key", regMsg.PublicKey,
			)
			result.Valid = false
			result.Error = "encryption key mismatch"
			provider.SetAttestationResult(&result)
			if policyConfigured {
				s.registry.MarkUntrusted(providerID)
			}
			return
		}
	}

	// Verify binary hash against known-good hashes. Once a binary hash policy is
	// configured, omission is a policy violation, not an Open Mode downgrade.
	//
	// v0.6.0: binaryHash is self-reported and demoted to drift telemetry (APNs
	// code-identity attestation is the real signal); this gate deroutes only when
	// enforcement is explicitly enabled (rollback). The attestation-validity and
	// key-binding checks above remain gated on policyConfigured and are unchanged.
	if s.binaryHashEnforce && policyConfigured {
		if result.BinaryHash == "" {
			s.logger.Warn("provider binary hash missing while known-good policy is configured",
				"provider_id", providerID,
			)
			result.Valid = false
			result.Error = "binary hash missing"
			provider.SetAttestationResult(&result)
			s.registry.MarkUntrusted(providerID)
			return
		}
		binaryHash, err := normalizeSHA256Hex(result.BinaryHash, "binary_hash")
		if err != nil || !knownBinaryHashes[binaryHash] {
			s.logger.Warn("provider binary hash not in known-good list",
				"provider_id", providerID,
				"binary_hash", result.BinaryHash,
			)
			result.Valid = false
			result.Error = "binary hash not recognized"
			provider.SetAttestationResult(&result)
			s.registry.MarkUntrusted(providerID)
			return
		}
		s.logger.Info("provider binary hash verified",
			"provider_id", providerID,
			"binary_hash", registry.TruncHash(result.BinaryHash),
		)
	}

	provider.SetAttested(true, registry.TrustSelfSigned)
	s.sendTrustStatus(provider, registry.TrustSelfSigned, "online", "SE attestation verified, awaiting MDM verification")

	// The SE attestation already proves SIP, Secure Boot, and binary hash —
	// the same checks a challenge re-verifies. Set LastChallengeVerified so
	// the provider is immediately routable. The 5-minute challenge cycle will
	// re-verify and add MDM cross-check for defense-in-depth.
	// Without this, a freshly connected provider waits up to 5 minutes before
	// it can serve any requests (until first challenge passes).
	provider.SetLastChallengeVerified(time.Now())

	s.logger.Info("provider attestation verified (self-signed)",
		"provider_id", providerID,
		"hardware_model", result.HardwareModel,
		"chip_name", result.ChipName,
		"serial_number", result.SerialNumber,
		"secure_enclave", result.SecureEnclaveAvailable,
		"sip_enabled", result.SIPEnabled,
		"secure_boot", result.SecureBootEnabled,
		"authenticated_root", result.AuthenticatedRootEnabled,
		"system_volume_hash", result.SystemVolumeHash,
		"binary_hash", result.BinaryHash,
		"trust_level", registry.TrustSelfSigned,
	)

	// Restore persisted state: if this provider was previously known (by serial
	// number or SE key), restore trust level, reputation, and account linkage.
	// Fresh attestation verification still runs (above), but stored reputation
	// is preserved so routing quality is maintained across coordinator restarts.
	if s.storedProviders != nil {
		var storedRec *store.ProviderRecord
		if result.SerialNumber != "" {
			storedRec = s.storedProviders[result.SerialNumber]
		}
		if storedRec == nil && result.PublicKey != "" {
			storedRec = s.storedProviders["sekey:"+result.PublicKey]
		}
		if storedRec != nil {
			s.registry.RestoreProviderState(provider, storedRec)
			s.logger.Info("restored persisted provider state",
				"provider_id", providerID,
				"stored_serial", storedRec.SerialNumber,
				"stored_trust", storedRec.TrustLevel,
			)
		}
	}

	// Stage the durable Apple MDA cert chain from a LIVE store read. storedProviders
	// above is a one-time startup snapshot — empty for the coordinator's whole life
	// under the in-memory store used in prod — so it cannot surface a chain earned
	// during this coordinator's lifetime. The store record survives provider
	// disconnect, so a serial lookup recovers a chain a previous connection earned,
	// letting attachCachedMDAProof reuse it (re-verified + SE-key-bound) instead of
	// forcing a fresh, Apple-rate-limited DevicePropertiesAttestation round-trip.
	s.stageDurableMDAChain(provider, result.SerialNumber)

	// Deduplicate: if another provider connection exists from the same physical
	// device (same serial number), disconnect it. This prevents multiple
	// provider processes on the same machine from registering independently
	// and competing for a single shared vllm-mlx backend.
	if result.SerialNumber != "" && !s.allowDuplicateProviderSerials {
		s.registry.DisconnectDuplicatesBySerial(providerID, result.SerialNumber)
	}

	// Persist provider state after attestation verification.
	// This captures the attestation result, serial number, and trust level.
	s.registry.PersistProvider(provider)

	// MDM verification is not spawned here. Registration binds stable device work
	// to the Server-owned bounded scheduler after attestation has established the
	// Secure Enclave identity and serial.
	if s.mdmClient != nil && result.SerialNumber == "" {
		s.logger.Warn("provider attestation has no serial number — cannot verify via MDM",
			"provider_id", providerID,
		)
	}
}

// mdmVerifyOutcome classifies one scheduler-owned MDM verification attempt.
type mdmVerifyOutcome int

const (
	mdmVerifyGranted   mdmVerifyOutcome = iota // hardware trust granted — stop
	mdmVerifyTransient                         // not-enrolled / not-found / timeout / error — retry
	mdmVerifyTerminal                          // posture mismatch (hard untrust) — stop
)

// verifyProviderViaMDM runs one MDM SecurityInfo attempt and, on success,
// upgrades the live provider to hardware trust. It records a bucketed
// MDMFailureReason and returns a fixed outcome to the scheduler. Transient
// transport/enrollment failures never hard-untrust; proven posture mismatch does.
func (s *Server) verifyProviderViaMDM(ctx context.Context, providerID string, provider *registry.Provider, attestResult attestation.VerificationResult) mdmVerifyOutcome {
	// Never let MDM promote a provider whose Secure Enclave attestation is not
	// valid. verifyProviderAttestation stores an AttestationResult even for an
	// invalid attestation (and, in Open Mode, leaves the provider connected), so
	// without this a later SecurityInfo success could grant hardware to a provider
	// whose SE attestation / encryption-key binding failed. result.Valid==true
	// implies both passed (verifyProviderAttestation returns early otherwise). The
	// scheduler also gates on this; this is the authoritative backstop.
	if !attestResult.Valid {
		s.logger.Warn("refusing MDM verification: SE attestation not valid")
		return mdmVerifyTransient
	}

	s.logger.Info("starting scheduled MDM verification")

	var (
		observeUDID    func(string)
		observeCommand func(string, string)
	)
	if metadata, ok := ctx.Value(mdmSchedulerAttemptContextKey{}).(*mdmSchedulerAttemptMetadata); ok {
		observeUDID = func(udid string) {
			metadata.udid = udid
			if s.mdmScheduler != nil {
				s.mdmScheduler.ObserveAttemptUDID(provider, udid)
			}
		}
		observeCommand = func(udid, commandUUID string) {
			if s.mdmScheduler != nil {
				s.mdmScheduler.ObserveAttemptCommand(
					provider, store.VerificationTaskSecurityInfo,
					udid, commandUUID,
				)
			}
		}
	}
	mdmResult, err := s.mdmClient.VerifyProviderWithUDIDObserver(
		ctx, attestResult.SerialNumber, attestResult.SIPEnabled,
		attestResult.SecureBootEnabled, observeUDID, observeCommand,
	)
	if err != nil {
		s.logger.Error("MDM verification error", "error", err)
		provider.SetMDMFailureReason("error")
		s.ddIncr("mdm.verification", []string{"outcome:error"})
		return mdmVerifyTransient
	}

	if !mdmResult.DeviceEnrolled {
		// A MicroMDM lookup/transport failure (500, network error) also returns
		// DeviceEnrolled=false — but the device may well be enrolled; we just
		// couldn't ask. Bucket that as "error" (MDM-side outage) so the stuck-cohort
		// gauge doesn't point operators at provider enrollment during an MDM outage.
		// Otherwise distinguish "no record of this serial" (profile never installed /
		// check-in never reached the server) from "record exists but enrollment
		// didn't complete" — different provider-side fixes.
		reason := "found-not-enrolled"
		switch {
		case strings.Contains(mdmResult.Error, "lookup failed"):
			reason = "error"
		case strings.Contains(mdmResult.Error, "not found"):
			reason = "device-not-found"
		}
		s.logger.Warn("provider not MDM-verified; retaining current trust",
			"reason", reason,
			"error", mdmResult.Error,
		)
		provider.SetMDMFailureReason(reason)
		s.ddIncr("mdm.verification", []string{"outcome:" + reason})
		return mdmVerifyTransient
	}

	if mdmResult.Error != "" {
		// Hard untrust ONLY for a genuine posture mismatch proven by a received
		// SecurityInfo response (SecurityMismatch). Everything else with a non-empty
		// error — a SecurityInfo timeout, a MicroMDM command-send/transport failure,
		// a decode error, or a context cancellation on disconnect — is a "could not
		// complete the check" condition: keep the provider at its current trust
		// level (self_signed) and let the loop retry. Treating a transient MicroMDM
		// API hiccup as a posture mismatch would wrongly hard-untrust an enrolled,
		// genuinely-secure box.
		if !mdmResult.SecurityMismatch {
			reason := "error"
			if strings.Contains(mdmResult.Error, "timeout") {
				reason = "securityinfo-timeout"
			}
			s.logger.Warn("MDM verification did not complete; retaining current trust",
				"reason", reason,
				"error", mdmResult.Error,
			)
			provider.SetMDMFailureReason(reason)
			s.ddIncr("mdm.verification", []string{"outcome:" + reason})
			return mdmVerifyTransient
		}
		// A real posture mismatch (SIP disabled, Secure Boot not full, attestation
		// disagrees with MDM) IS evidence of a problem — hard untrust, no retry.
		s.logger.Warn("MDM posture verification failed; marking provider untrusted",
			"error", mdmResult.Error,
			"mdm_sip", mdmResult.MDMSIPEnabled,
			"mdm_secure_boot", mdmResult.MDMSecureBootFull,
			"sip_match", mdmResult.SIPMatch,
			"secure_boot_match", mdmResult.SecureBootMatch,
		)
		provider.SetMDMFailureReason("posture-mismatch")
		s.ddIncr("mdm.verification", []string{"outcome:posture-mismatch"})
		s.registry.MarkUntrusted(providerID)
		return mdmVerifyTerminal
	}

	// If the connection went away while we were waiting on SecurityInfo, do NOT
	// mutate/persist trust for a provider that is no longer here — the next
	// connection re-verifies from scratch (RestoreProviderState caps to
	// self_signed). Treat as transient; the loop's ctx.Done will end it.
	if ctx.Err() != nil {
		provider.SetMDMFailureReason("securityinfo-timeout")
		return mdmVerifyTransient
	}
	binaryHash := providerApplicationBinaryHash(
		provider, attestResult.PublicKey, attestResult.BinaryHash,
	)

	// Durable revocation is authoritative. Persist/recover the verified device
	// evidence at the expected generation before touching live hardware trust;
	// then the helper atomically rechecks the provider epoch/status while granting.
	if !s.recordTrustReuse(
		provider,
		attestResult.PublicKey,
		attestResult.SerialNumber,
		binaryHash,
		mdmResult.MDMSIPEnabled,
		mdmResult.MDMSecureBootFull,
		mdmResult.UDID,
	) {
		s.ddIncr("mdm.verification", []string{"outcome:deferred-revocation-cas"})
		return mdmVerifyTransient
	}
	provider.SetMDMFailureReason("")
	s.sendTrustStatus(provider, registry.TrustHardware, "online", "MDM verification passed")
	s.ddIncr("mdm.verification", []string{"outcome:granted"})
	s.logger.Info("MDM verification passed; upgraded live provider to hardware trust",
		"mdm_sip", mdmResult.MDMSIPEnabled,
		"mdm_secure_boot", mdmResult.MDMSecureBootFull,
		"mdm_auth_root_volume", mdmResult.MDMAuthRootVolume,
	)
	s.registry.PersistProvider(provider)

	// Direct attempt-level callers retain the historical synchronous MDA behavior.
	// Scheduler workers enqueue MDA behind the same global budget instead.
	if _, scheduled := ctx.Value(mdmSchedulerAttemptContextKey{}).(*mdmSchedulerAttemptMetadata); !scheduled {
		s.verifyAppleDeviceAttestation(ctx, providerID, provider, attestResult, mdmResult.UDID)
	}
	return mdmVerifyGranted
}

// ApplyLateSecurityInfo accepts a delayed response only for the exact current
// scheduler binding that issued the command and has completed the current
// connection's phase-1 challenge. Unowned or stale-generation callbacks are
// dropped; they are never bearer credentials for a fleet-wide provider lookup.
func (s *Server) ApplyLateSecurityInfo(
	udid, commandUUID string,
	info *mdm.SecurityInfoResponse,
) {
	if s.mdmClient == nil || info == nil || commandUUID == "" {
		return
	}
	securityOK := info.SystemIntegrityProtectionEnabled && info.SecureBootLevel == "full"
	if s.mdmScheduler == nil {
		return
	}
	binding := s.mdmScheduler.ApplyLateSecurityInfo(
		udid, commandUUID, securityOK,
	)
	if binding == nil {
		return
	}
	if !securityOK {
		binding.provider.SetMDMFailureReason("posture-mismatch")
		s.registry.MarkUntrusted(binding.providerID)
		s.mdmScheduler.RejectLateSecurityInfo(
			*binding, udid, commandUUID,
		)
		return
	}
	ar := binding.attestation
	binaryHash := providerApplicationBinaryHash(
		binding.provider, ar.PublicKey, ar.BinaryHash,
	)
	if !s.recordLateTrustReuse(
		binding.provider, ar.PublicKey, ar.SerialNumber, binaryHash,
		true, true, udid,
	) {
		return
	}
	binding.provider.SetMDMFailureReason("")
	s.sendTrustStatus(binding.provider, registry.TrustHardware, "online", "MDM verification passed (late SecurityInfo)")
	s.registry.PersistProvider(binding.provider)
	if s.metrics != nil {
		s.metrics.IncCounter("mdm_late_securityinfo_upgrade_total")
	}
	s.ddIncr("mdm.verification", []string{"outcome:granted-late"})
	s.mdmScheduler.CompleteLateSecurityInfo(
		*binding, udid, commandUUID,
	)
}

// stageDurableMDAChain recovers a previously-earned Apple MDA cert chain from the
// store (by serial) and stages it on the provider as a reuse candidate for this
// reconnect. The store record survives provider disconnect, so this works under
// the in-memory store used in prod — where the startup storedProviders snapshot is
// empty — as well as a durable store. Best-effort: a missing record / chain or a
// read error simply stages nothing, and a fresh attestation is requested.
func (s *Server) stageDurableMDAChain(provider *registry.Provider, serial string) {
	if s.store == nil || serial == "" {
		return
	}
	// Bound the store read: this runs on the attestation path, so a slow or
	// unavailable Postgres must not stall it — on timeout we skip staging and fall
	// back to a fresh attestation.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Newest NON-EMPTY chain for this serial: a reconnect persists a new row that
	// may briefly carry an empty chain (async persists race the reattach), which
	// would shadow a still-valid chain via a plain by-serial lookup. This looks
	// past those empty rows.
	chain, err := s.store.GetMDAChainBySerial(ctx, serial)
	if err != nil || len(chain) == 0 {
		return
	}
	provider.StageMDAChainFromJSON(chain)
}

// attachCachedMDAProof tries to satisfy the Apple Device Attestation (MDA) leg
// from the durable cert chain restored on reconnect, WITHOUT a fresh
// DevicePropertiesAttestation round-trip. Apple rate-limits a fresh attestation to
// ≈1/device/7d and it rides the same throttled MicroMDM→APNs channel as
// SecurityInfo, so re-fetching on every reconnect is the reason restarted
// providers show "Apple Device Attestation incomplete". The cached chain is
// re-verified here against Apple's pinned Enterprise Attestation Root CA (an
// expired or tampered chain is rejected) and re-bound to THIS connection's SE key
// via the FreshnessCode OID (anti-relay). Returns true if a valid, bound proof was
// attached — which requires the provider to already hold hardware trust.
func (s *Server) attachCachedMDAProof(providerID string, provider *registry.Provider, attestResult attestation.VerificationResult) bool {
	chain := provider.StagedMDAChain()
	if len(chain) == 0 {
		return false
	}
	mdaResult, err := attestation.VerifyMDADeviceAttestation(chain)
	if err != nil || mdaResult == nil || !mdaResult.Valid {
		// Chain no longer verifies (expired / not Apple-signed) — fall through to a
		// fresh request.
		return false
	}

	// Cached reuse REQUIRES the strong SE-key binding: the FreshnessCode OID in the
	// Apple-signed chain must equal SHA-256 of THIS connection's SE public key. A
	// serial-only match is deliberately NOT sufficient to reuse a stored chain — if
	// the SE key rotated (re-image / keychain reset) the old chain no longer binds
	// this key, so we fall through to a fresh attestation rather than letting a new
	// key inherit the prior device's Apple proof. (A live challenge has already
	// proven possession of this SE key, so the binding is meaningful.)
	if attestResult.PublicKey == "" || len(mdaResult.FreshnessCode) == 0 {
		return false
	}
	// INVARIANT: this must use the exact same input as the fresh path's nonce
	// (verifyAppleDeviceAttestation computes expectedFreshness = sha256([]byte(
	// attestResult.PublicKey)) and sends its base64 as the DeviceAttestationNonce).
	// Apple echoes the decoded nonce as the FreshnessCode, so a chain earned fresh
	// has FreshnessCode == this digest. Keep the two formulas identical.
	want := sha256.Sum256([]byte(attestResult.PublicKey))
	if !bytes.Equal(mdaResult.FreshnessCode, want[:]) {
		return false
	}
	// Defense in depth: when Apple included a serial, it must match this machine's
	// attested serial (privacy-enrolled chains omit the serial — the SE-key binding
	// above carries the proof in that case).
	if mdaResult.DeviceSerial != "" && mdaResult.DeviceSerial != attestResult.SerialNumber {
		return false
	}

	if !provider.SetMDAProofIfHardwareBound(chain, mdaResult, true) {
		// Not hardware-trusted (yet) — nothing to attach the proof to.
		return false
	}
	// Persist immediately under THIS connection's record. The grant-path
	// PersistProvider ran before this attach, so without this write the new
	// session's row would carry an empty mda_cert_chain until the next throttled
	// heartbeat — and a disconnect in that window would lose the chain (serial now
	// indexes this session's row), forcing a fresh, rate-limited refetch on the
	// next reconnect. Mirrors the fresh-MDA path's immediate persist.
	s.registry.PersistProvider(provider)
	s.logger.Info("MDA reused from durable SE-key-bound certificate chain")
	s.ddIncr("mda.verification", []string{"outcome:reused"})
	return true
}

// verifyAppleDeviceAttestation sends a DeviceInformation command requesting
// DevicePropertiesAttestation and verifies the Apple-signed certificate chain.
func (s *Server) verifyAppleDeviceAttestation(ctx context.Context, providerID string, provider *registry.Provider, attestResult attestation.VerificationResult, udid string) {
	setOutcome := func(outcome string) {
		if metadata, ok := ctx.Value(mdmSchedulerAttemptContextKey{}).(*mdmSchedulerAttemptMetadata); ok {
			metadata.mdaOutcome = outcome
		}
	}
	// Fast path: reuse a still-valid, SE-key-bound Apple attestation recovered from
	// the durable store instead of requesting a fresh one. This skips the
	// rate-limited APNs round-trip entirely on reconnect/restart and is what keeps
	// mda_verified green across a provider restart.
	if s.attachCachedMDAProof(providerID, provider, attestResult) {
		setOutcome("reused")
		return
	}

	if udid == "" {
		setOutcome("invalid")
		s.logger.Warn("no UDID for MDA verification")
		return
	}

	// Compute SE key hash for nonce-based key binding.
	// If the provider has an SE public key, include its hash as the
	// DeviceAttestationNonce (base64-encoded). Apple decodes the nonce and
	// embeds the raw bytes as FreshnessCode (OID 1.2.840.113635.100.8.11.1)
	// in the signed cert, cryptographically binding the SE key to genuine hardware.
	var seKeyNonce string
	var expectedFreshness [32]byte
	if attestResult.PublicKey != "" {
		seKeyHash := sha256.Sum256([]byte(attestResult.PublicKey))
		seKeyNonce = base64.StdEncoding.EncodeToString(seKeyHash[:])
		expectedFreshness = seKeyHash
	}
	s.logger.Info("requesting Apple Device Attestation",
		"se_key_binding_requested", seKeyNonce != "",
	)

	// Install exclusive UDID and exact command ownership before command
	// visibility so an old late response cannot bind to this attempt.
	var observeMDACommand func(string, string)
	if _, scheduled := ctx.Value(mdmSchedulerAttemptContextKey{}).(*mdmSchedulerAttemptMetadata); scheduled &&
		s.mdmScheduler != nil {
		observeMDACommand = func(udid, commandUUID string) {
			s.mdmScheduler.ObserveAttemptCommand(
				provider, store.VerificationTaskMDA,
				udid, commandUUID,
			)
		}
	}
	attestResp, err := s.mdmClient.RequestDeviceAttestation(
		ctx, udid, seKeyNonce, 60*time.Second, observeMDACommand,
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "timeout") {
			setOutcome("timeout")
		} else {
			setOutcome("transient")
		}
		s.logger.Warn("DevicePropertiesAttestation request failed", "error", err)
		return
	}

	// Verify the certificate chain against Apple's Enterprise Attestation Root CA
	mdaResult, err := attestation.VerifyMDADeviceAttestation(attestResp.CertChain)
	if err != nil {
		setOutcome("invalid")
		s.logger.Error("MDA certificate chain parse error", "error", err)
		return
	}

	if !mdaResult.Valid {
		setOutcome("invalid")
		s.logger.Warn("MDA certificate chain verification failed")
		return
	}

	// Cross-check: MDA serial must match the provider's self-reported serial
	if mdaResult.DeviceSerial != "" && mdaResult.DeviceSerial != attestResult.SerialNumber {
		setOutcome("binding_mismatch")
		s.logger.Error("MDA serial binding mismatch")
		s.registry.MarkUntrusted(providerID)
		return
	}

	// Apple Device Attestation verified — store the proof for coordinator-side
	// trust decisions and reuse. Public APIs expose only the redacted verdict.
	// Acquire provider lock since these fields are read by HTTP handlers
	// (handleProviderAttestation, handleChatCompletions) concurrently.
	seKeyBound := false
	if seKeyNonce != "" && len(mdaResult.FreshnessCode) > 0 {
		seKeyBound = bytes.Equal(mdaResult.FreshnessCode, expectedFreshness[:])
	}

	if seKeyNonce != "" && !seKeyBound {
		setOutcome("binding_mismatch")
		s.logger.Warn("MDA FreshnessCode did not bind the current Secure Enclave key")
		return
	}
	if !provider.SetMDAProofIfHardwareBound(attestResp.CertChain, mdaResult, seKeyBound) {
		setOutcome("invalid")
		return
	}
	setOutcome("verified")

	// Persist the freshly-earned chain NOW so it is durable for reuse. The
	// hardware-grant PersistProvider ran before this MDA leg, so without an explicit
	// write here the chain would only reach the store on the next throttled
	// heartbeat persist — and would be lost (and re-fetched, hitting Apple's
	// ~1/device/7d rate limit) if the provider disconnects in that window. With a
	// durable (Postgres) store this is what makes the proof recoverable across a
	// coordinator restart.
	s.registry.PersistProvider(provider)

	s.logger.Info("MDA verified",
		"se_key_bound", seKeyBound,
		"freshness_code_present", len(mdaResult.FreshnessCode) > 0,
	)
}

// sendTrustStatus sends the provider its current trust level and status over
// the WebSocket connection and persist the coordinator's current decision for
// local operator diagnostics. Provider log upload is retired.
func (s *Server) sendTrustStatus(provider *registry.Provider, trustLevel registry.TrustLevel, status string, reason string) {
	if provider == nil || provider.Conn == nil {
		return
	}
	msg := protocol.TrustStatusMessage{
		Type:       protocol.TypeTrustStatus,
		TrustLevel: string(trustLevel),
		Status:     status,
		Reason:     reason,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	if err := provider.EnqueueText(context.Background(), data); err != nil {
		s.logger.Debug("failed to enqueue trust status to provider", "provider_id", provider.ID, "error", err)
		s.ddIncr("provider.enqueue_failed", []string{"msg:trust_status"})
	}
}
