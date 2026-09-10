package api

// Redacted provider attestation status endpoint and cache.

import (
	"net/http"
	"time"

	"github.com/eigeninference/d-inference/coordinator/registry"
)

// providerAttestationCacheTTL bounds staleness of the public trust listing. It
// reflects live connection state (trust level, status, models), so it uses the
// same 2s window as GET /v1/models/capacity. The response is the same for every
// caller (unauthenticated, no query parameters).
const providerAttestationCacheTTL = 2 * time.Second

const providerAttestationCacheKey = "providers:attestation:v1"

// handleProviderAttestation returns privacy-redacted trust status for all providers.
// Device identity and raw MDA certificates stay coordinator-private because
// Apple's leaf certificate embeds the hardware serial number and UDID.
func (s *Server) handleProviderAttestation(w http.ResponseWriter, r *http.Request) {
	if body, ok := s.readCacheGet(providerAttestationCacheKey); ok {
		writeCachedJSON(w, body)
		return
	}
	type providerAttestation struct {
		ProviderID    string `json:"provider_id"`
		ChipName      string `json:"chip_name"`
		HardwareModel string `json:"hardware_model"`
		TrustLevel    string `json:"trust_level"`
		Status        string `json:"status"`

		// Hardware specs
		MemoryGB int      `json:"memory_gb"`
		GPUCores int      `json:"gpu_cores"`
		Models   []string `json:"models"`

		// Secure Enclave attestation (self-signed)
		SecureEnclave     bool   `json:"secure_enclave"`
		SIPEnabled        bool   `json:"sip_enabled"`
		SecureBootEnabled bool   `json:"secure_boot_enabled"`
		AuthenticatedRoot bool   `json:"authenticated_root_enabled"`
		SystemVolumeHash  string `json:"system_volume_hash,omitempty"`
		SEPublicKey       string `json:"se_public_key"`

		// MDM SecurityInfo (verified by Apple's MDM framework)
		MDMVerified bool `json:"mdm_verified"`

		// Deprecated: the ACME device-attest-01 leg was removed (it was never
		// wired end-to-end; hardware trust is earned via MDM SecurityInfo).
		// The key is kept, always false, because shipped provider builds decode
		// it as a required field.
		ACMEVerified bool `json:"acme_verified"`

		// Apple Device Attestation (MDA), verified coordinator-side. The raw
		// certificate chain is intentionally not part of this public DTO.
		MDAVerified   bool   `json:"mda_verified"`
		MDAOSVersion  string `json:"mda_os_version,omitempty"`
		MDASepVersion string `json:"mda_sepos_version,omitempty"`
	}

	var providers []providerAttestation

	publicProviderModels := s.registry.PublicProviderModels()
	s.registry.ForEachProvider(func(p *registry.Provider) {
		// Snapshot mutable fields under provider lock to avoid racing
		// with background MDA verification and challenge goroutines.
		p.Mu().Lock()
		trustLevel := p.TrustLevel
		status := p.Status
		mdaVerified := p.MDAVerified
		attestResult := p.AttestationResult
		mdaResult := p.MDAResult
		p.Mu().Unlock()

		// The public proofs (mdm/mda) are reported true ONLY for a connection
		// that currently holds hardware trust. A hardware proof is meaningful for
		// the connection that earned it live; surfacing mda_verified on a
		// self_signed connection (e.g. a stored flag or a late-arriving MDA
		// webhook) is the misleading "mda_verified=true while self_signed"
		// drift. Gating on the live trust level keeps the endpoint internally
		// consistent.
		isHardware := trustLevel == registry.TrustHardware
		pa := providerAttestation{
			ProviderID:  p.ID,
			TrustLevel:  string(trustLevel),
			Status:      string(status),
			MemoryGB:    p.Hardware.MemoryGB,
			GPUCores:    p.Hardware.GPUCores,
			MDMVerified: isHardware,
			MDAVerified: mdaVerified && isHardware,
		}

		pa.Models = append(pa.Models, publicProviderModels[p.ID].Models...)

		if attestResult != nil {
			pa.ChipName = attestResult.ChipName
			pa.HardwareModel = attestResult.HardwareModel
			pa.SecureEnclave = attestResult.SecureEnclaveAvailable
			pa.SIPEnabled = attestResult.SIPEnabled
			pa.SecureBootEnabled = attestResult.SecureBootEnabled
			pa.AuthenticatedRoot = attestResult.AuthenticatedRootEnabled
			pa.SystemVolumeHash = attestResult.SystemVolumeHash
			pa.SEPublicKey = attestResult.PublicKey
		}

		if isHardware && mdaResult != nil {
			pa.MDAOSVersion = mdaResult.OSVersion
			pa.MDASepVersion = mdaResult.SepOSVersion
		}

		providers = append(providers, pa)
	})

	resp := map[string]any{"providers": providers}
	body, err := encodeCachedJSON(resp)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse("internal_error", "failed to encode attestation"))
		return
	}
	s.readCacheSet(providerAttestationCacheKey, body, providerAttestationCacheTTL)
	writeCachedJSON(w, body)
}
