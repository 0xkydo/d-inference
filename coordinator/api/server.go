// Package api provides the HTTP and WebSocket server for the Darkbloom coordinator.
//
// This package is the network-facing layer of the coordinator. It handles:
//   - Consumer HTTP endpoints (OpenAI-compatible chat completions, model listing)
//   - Provider WebSocket connections (registration, heartbeats, inference relay)
//   - Payment endpoints (deposit, balance, usage)
//   - Authentication via API keys (Bearer token)
//   - CORS middleware for development
//   - Request logging
//
// The coordinator runs in a GCP Confidential VM (AMD SEV). Consumer traffic
// arrives over HTTPS/TLS. The coordinator reads requests for routing but never
// logs prompt content.
package api

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eigeninference/d-inference/coordinator/apns"
	"github.com/eigeninference/d-inference/coordinator/auth"
	"github.com/eigeninference/d-inference/coordinator/billing"
	"github.com/eigeninference/d-inference/coordinator/datadog"
	"github.com/eigeninference/d-inference/coordinator/internal/e2e"
	"github.com/eigeninference/d-inference/coordinator/mdm"
	"github.com/eigeninference/d-inference/coordinator/mediafetch"
	"github.com/eigeninference/d-inference/coordinator/payments"
	"github.com/eigeninference/d-inference/coordinator/payments/baserewards"
	"github.com/eigeninference/d-inference/coordinator/profilesign"
	"github.com/eigeninference/d-inference/coordinator/promptcontract"
	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/ratelimit"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/saferun"
	"github.com/eigeninference/d-inference/coordinator/store"
	"github.com/eigeninference/d-inference/coordinator/telemetry"
	"golang.org/x/sync/singleflight"
)

// LatestProviderVersion is the fallback version returned only when no
// release has been registered in the store (e.g. in-memory dev setups).
// Production reads the latest version from the releases table.
//
// 0.8.1 reverts v0.8.0's fleet default back to the CONTIGUOUS KV backend:
// the paged pool's physical-capacity policy sized fleet KV roughly 10x
// smaller than contiguous, and the resulting token-budget exhaustion
// dominated paged's throughput and prefix-adoption wins. Paged remains
// fully supported behind an explicit `engine_v2_kv_backend = "paged"` (see
// the provider's EngineV2Factory.prepareProductionBackend for the argument).
// 0.8.15 adds the exact Qwen3.8 dense VLM/NAX target and verified inline MTP
// assistant support; model-aware MTP defaults remain provider-side policy.
// Keep this fallback in sync with ProviderCore.version so dev/in-memory
// coordinators advertise the same floor as the Swift binary they expect.
var LatestProviderVersion = "0.8.16"

// minProviderVersionForDesiredModels is the first provider version whose Swift
// runtime understands the desired_models message. The coordinator must NOT send
// desired_models to any provider below this version (or on a non-Swift backend):
// a pre-feature provider's strict decoder throws on unknown message types and
// would disconnect. KEEP THIS IN SYNC with the release that ships Swift
// desired_models support (ProviderCore.version at that cut).
const minProviderVersionForDesiredModels = "0.5.17"

// latestReleasedVersion returns the highest active release version from
// the store, falling back to the hardcoded LatestProviderVersion when
// no release record exists.
func (s *Server) latestReleasedVersion() string {
	if release := s.store.GetLatestRelease(defaultReleasePlatform); release != nil {
		return release.Version
	}
	return LatestProviderVersion
}

type approvedReleasePolicy struct {
	Version        string
	Platform       string
	Backend        string
	BinaryHash     string
	MetallibHash   string
	PythonHash     string
	RuntimeHash    string
	TemplateHashes map[string]string
}

type releaseTrustPolicySnapshot struct {
	Generation   uint64
	Required     bool
	ByBinaryHash map[string][]approvedReleasePolicy
}

// Server is the main HTTP/WS server for the coordinator. It ties together
// the provider registry, key store, payment ledger, billing service, and HTTP routing.
type Server struct {
	registry                      *registry.Registry
	store                         store.Store
	ledger                        *payments.Ledger
	billing                       *billing.Service
	baseRewards                   *baserewards.Engine
	logger                        *slog.Logger
	mux                           *http.ServeMux
	modelAliasMutationMu          sync.Mutex      // serializes cross-endpoint alias validation + persistence
	challengeInterval             time.Duration   // 0 means use DefaultChallengeInterval
	skipChallenge                 bool            // if true, skip attestation challenges entirely (testing only)
	allowDuplicateProviderSerials bool            // in-process multi-provider testbed only
	privyAuth                     *auth.PrivyAuth // Privy JWT authentication (nil if not configured)
	adminEmails                   map[string]bool // emails that have admin access
	adminKey                      string          // EIGENINFERENCE_ADMIN_KEY for admin endpoints
	mdmClient                     *mdm.Client     // MicroMDM client for provider security verification
	mdmScheduler                  *mdmVerificationScheduler
	mdmSchedulerConfig            MDMSchedulerConfig
	mdmWebhookSecret              string              // optional shared secret MicroMDM must present on the webhook
	profileSigner                 *profilesign.Signer // CMS signer for the /v1/enroll .mobileconfig (nil = serve unsigned)
	promptArtifacts               *promptcontract.Provisioner
	promptContract                *promptcontract.Client
	promptSupervisor              *promptcontract.Supervisor
	promptPreloader               *promptcontract.PreloadController
	exactCacheGaugeMu             sync.RWMutex
	exactCacheGaugeStatus         ExactCacheStatus
	exactCacheStatusCacheMu       sync.Mutex
	exactCacheStatusCache         ExactCacheStatus
	exactCacheStatusCacheExpires  time.Time
	codeAttestor                  apns.CodeIdentityAttestor // APNs code-identity attestor (nil = disabled; v0.6.0)
	codeResumeSender              func(string, protocol.CodeAttestationResumeChallenge) error
	codeResumeBeforeIdentityCheck func()              // test seam between cache match and challenge record
	codeResumeFallbackBeforeAPNs  func()              // test seam after nonce consume, before ctx recheck
	codeAttestThrottle            *codeAttestThrottle // per-device APNs push budget + reuse cache (v0.6.0)
	trustReuseCache               *trustReuseCache    // per-device trust-reuse cache: skip a fleet-wide live MDM herd on restart (DAR-326)
	trustReuseJournal             hardUntrustJournal
	trustRevocationMu             sync.Mutex
	trustSafetyMu                 sync.RWMutex
	trustSafetySticky             bool
	trustSafetyReplayBlocked      bool
	pendingHardUntrustKeyHashes   map[string]int
	trustAuthorityMu              sync.Mutex
	trustAuthority                *trustAuthorityLock
	trustReplayCtx                context.Context
	trustReplayCancel             context.CancelFunc
	trustReplayMu                 sync.Mutex
	trustReplayInFlight           map[string]struct{}
	// Connection-continuity coverage tracker: seKey → providerID of the live
	// covered connection. Advanced by the batched trustCoverageLoop and the
	// disconnect/shutdown sweeps (see trust_reuse.go).
	trustCoverageMu     sync.Mutex
	trustCoverage       map[string]string
	trustCoverageCtx    context.Context
	trustCoverageCancel context.CancelFunc

	// Graceful-drain state (DAR-327 Phase 1, zero-downtime upgrades). Set
	// coordinatorDraining=true before a restart/swap so the drain gate rejects
	// NEW inference requests with 429+Retry-After while already-admitted ones run
	// to completion; httpInflight counts requests currently inside the gate so
	// /readyz (and the deploy script) can wait for it to reach 0 before shutdown.
	// Deliberately named to avoid collision with the provider-side drain concepts
	// (protocol.ProviderDrainingForUpdate, registry.drainQueuedRequestsForModels):
	// this is purely the coordinator's own HTTP-ingress drain. See drain.go.
	httpInflight        atomic.Int64
	coordinatorDraining atomic.Bool

	// knownBinaryHashes is the set of accepted provider binary SHA-256 hashes.
	// When binaryHashPolicyConfigured is true, providers whose binary hash is
	// missing or doesn't match are rejected.
	// Auto-populated from active releases via SyncBinaryHashes().
	releasePolicySyncMu               sync.Mutex
	binaryHashPolicyMu                sync.RWMutex
	knownBinaryHashes                 map[string]bool
	manualKnownBinaryHashes           map[string]bool
	releaseKnownBinaryHashes          map[string]bool
	manualBinaryHashPolicyConfigured  bool
	releaseBinaryHashPolicyConfigured bool
	binaryHashPolicyConfigured        bool
	releaseTrustPolicy                atomic.Pointer[releaseTrustPolicySnapshot]
	releaseTrustPolicyGeneration      atomic.Uint64
	releaseInventoryEverConfigured    atomic.Bool

	// binaryHashEnforce gates whether a self-reported binaryHash mismatch actually
	// DEROUTES a provider. Default false as of v0.6.0: binaryHash is self-reported
	// (worthless against a malicious provider) and is demoted to drift telemetry —
	// APNs code-identity attestation is the real code-identity signal. The policy
	// machinery is retained for drift comparison and rollback
	// (EIGENINFERENCE_BINARYHASH_ENFORCE=true).
	binaryHashEnforce bool

	// ttftHardReject controls how the per-request TTFT admission ceiling
	// (configured base + 1ms/token) behaves when the best ESTIMATED
	// time-to-first-token exceeds it. The estimate's prefill term is not
	// provider-measured and runs ~10x
	// pessimistic (see resolvedPrefillTPS), which made the legacy hard gate 429
	// the majority of serveable requests above ~550 prompt tokens. Default false:
	// the ceiling is a SOFT routing preference — when at least one provider passed
	// every routing and capacity gate, the request is served on the best-available
	// provider instead of being rejected. Set true
	// (EIGENINFERENCE_TTFT_HARD_REJECT=true) to restore the legacy hard 429.
	ttftHardReject bool

	// firstContentDeadlineBase is the ordinary-model fixed term in the
	// request-absolute first-content budget. It is immutable after startup and
	// instance-owned; exact-model policy can only tighten it. Concurrent test
	// servers can exercise production and unit-test postures without racing on
	// process-global state.
	firstContentDeadlineBase time.Duration

	// rejectModels are requested aliases or resolved model IDs the coordinator
	// takes out of public/prefer-owner routing: every matching request is answered
	// with 429 + Retry-After at admission instead of being routed. This is a
	// deterministic per-model circuit breaker for unhealthy models (for example,
	// keep Gemma shed while allowing gpt-oss traffic with TTFT_HARD_REJECT=false).
	// Exclusive self-route bypasses this because it never falls back to the public
	// fleet and is useful for owner debugging. nil/empty = none.
	rejectModels map[string]bool

	// minDecodeTPS is the per-request sustained-decode floor (tokens/sec) passed
	// to the scheduler as PendingRequest.MinDecodeTPS. When > 0 the router prefers
	// providers that keep a newly admitted request at >= this rate (avoid
	// overpacking into degraded streams). Soft: never rejects on its own. Default
	// 0 (off). Set via EIGENINFERENCE_MIN_DECODE_TPS.
	minDecodeTPS float64

	// servabilityGate enables the smart early-429 admission gate: when
	// true, a request whose (prompt + max_tokens) cannot fit the model's context
	// window or any provider's structural token budget is rejected with an
	// uptime-NEUTRAL 429 + Retry-After at preflight (OpenRouter fails over)
	// instead of being admitted and failing as an uptime-DAMAGING 5xx. Default
	// false (behavior-neutral). Set via EIGENINFERENCE_SERVABILITY_GATE=true. See
	// registry.PredictServable + servability_gate.go. Independent of (and weaker
	// than) the always-on dispatch-exhausted reclassification of token-budget 5xx
	// → 429, which fixes the same failure on the actual provider-rejection path.
	servabilityGate bool

	// disableClientErrorStop is the kill switch for the C1 StatusCode-driven
	// non-retryable failover stop. Default false = stop ENABLED: a deterministic
	// provider client 4xx (400/413/422/415) returns ONCE instead of failing over up
	// to maxDispatchAttempts. Set EIGENINFERENCE_DISABLE_CLIENT_ERROR_STOP=true to
	// restore the pre-fix behavior (string-only classifyRejection failover).
	disableClientErrorStop bool

	// knownRuntimeManifest holds accepted runtime component hashes.
	// When set, providers whose runtime hashes don't match are marked as
	// unverified and excluded from routing (but not disconnected).
	knownRuntimeManifest *RuntimeManifest

	// settlements parks billing records for requests whose consumer disconnected
	// mid-stream, so a late provider terminal can settle them (or the reservation
	// is refunded on grace expiry). See settlement.go.
	settlements *settlementHolder
	// settleGrace overrides defaultTerminalSettleGrace (tests set it small).
	settleGrace time.Duration
	// zombieCanceller throttles cancels for chunks on abandoned streams. See zombie_stream.go.
	zombieCanceller *zombieStreamCanceller

	// hedgeGov is the fleet-wide hedge admission governor (Routing v2 Phase 4):
	// the mutable half of the speculative-launch verdict — the global
	// concurrent-hedge counter and per-model win-rate EWMAs. One instance per
	// Server; runSpeculative consults it before every backup launch and
	// resolves it exactly once per launched hedge. See hedge_governor.go.
	hedgeGov *hedgeGovernor

	// minProviderVersion is the minimum provider version accepted for routing.
	// Providers below this version are excluded and told to update.
	// Set from EIGENINFERENCE_MIN_PROVIDER_VERSION env var or derived from latest release.
	minProviderVersion string

	// releaseKey is a scoped credential for the GitHub Action to register releases.
	// It can only POST /v1/releases — no admin access.
	releaseKey string

	// consoleURL is the frontend URL (e.g. "https://console.darkbloom.dev").
	// Used for device auth verification_uri so the browser opens the console, not the coordinator.
	consoleURL string

	// baseURL is the public URL clients reach this coordinator at
	// (e.g. "https://api.darkbloom.dev" for prod, "https://api.dev.darkbloom.xyz" for dev).
	// Substituted into the embedded install.sh at serve time so the same binary
	// can serve both environments. Falls back to "https://" + request.Host when empty.
	baseURL string

	// r2CDNURL is the public R2 bucket URL that providers pull release artifacts
	// from (e.g. "https://models.darkbloom.ai").
	// Set from EIGENINFERENCE_R2_CDN_URL env var. Empty disables CDN metadata.
	r2CDNURL string

	// corsOrigin is the allowed CORS origin (e.g. "https://console.darkbloom.dev").
	// Set from CORS_ORIGIN env var. Empty defaults to the production console domain.
	corsOrigin string

	// storedProviders is a lookup table of persisted provider records, indexed
	// by serial number and SE public key. When a provider reconnects after a
	// coordinator restart, this table is checked to restore trust/reputation.
	// Populated once at startup from the store.
	storedProviders map[string]*store.ProviderRecord

	// geoResolver resolves provider and consumer request locations from IP
	// addresses or trusted reverse-proxy headers. Nil when GeoIP is not configured.
	geoResolver providerGeoResolver

	// coordinatorKey is the long-lived X25519 keypair used to receive sealed
	// requests from senders. Set via SetCoordinatorKey. nil disables the
	// /v1/encryption-key endpoint and the sealed-request middleware.
	coordinatorKey *e2e.CoordinatorKey

	// chunkKeys memoizes the per-request NaCl shared key so streaming chunk
	// decryption skips the X25519 scalar multiplication per token. Zero value
	// is ready; entries are dropped on request completion/error and bounded
	// by chunkKeyCacheMax.
	chunkKeys chunkKeyCache

	// metrics is the in-process metrics registry exposed via /v1/admin/metrics
	// and used by internal counters/histograms. Never nil.
	metrics *Metrics

	// telemetryLimiter throttles telemetry ingestion per submitter.
	telemetryLimiter *telemetryLimiter

	// readCache memoizes pre-serialized JSON for read-heavy aggregation
	// endpoints (stats, leaderboard, model catalog, etc.). TTLs are
	// per-key. Never nil.
	readCache *ttlCache
	// statsRefresh owns the stats:v1 readCache entry (stats.go);
	// networkTotalsRefresh owns one network_totals:<window> entry per window
	// (network_totals.go). Both are driven by the refresher machinery in
	// cache_refresher.go.
	summaryWindowsFlights singleflight.Group
	statsRefresh          cacheRefresher
	networkTotalsRefresh  struct {
		queryMu sync.Mutex
		mu      sync.Mutex
		entries map[string]*cacheRefresher
	}

	// emitter writes coordinator-side telemetry events (panics, handler
	// failures, attestation failures, etc.). Set via SetEmitter; nil before
	// main.go wires it up.
	emitter *telemetry.Emitter

	// dd is the Datadog integration client for DogStatsD metrics and
	// Logs API event forwarding. Nil when DD is not configured.
	dd          *datadog.Client
	queueGauges queueGaugeState

	// apiKeyCache memoizes ValidateKeyFull results so repeated requests
	// with the same API key skip the DB round trip. Entries expire after
	// apiKeyCacheTTL. Bounded at apiKeyCacheMaxSize entries.
	apiKeyCacheMu sync.RWMutex
	apiKeyCache   map[string]apiKeyCacheEntry
	// apiKeyCacheGen is bumped on every key mutation. A cached entry is only
	// honored when its gen matches, so a single bump atomically invalidates the
	// whole cache and closes the read-stale-after-mutation race.
	apiKeyCacheGen uint64

	// rateLimiter applies per-account token-bucket rate limits to consumer
	// inference endpoints. Nil means unlimited (compatibility with old call
	// sites and tests). Set via SetRateLimiter.
	rateLimiter *ratelimit.Limiter

	// financialRateLimiter is a separate, stricter limiter for endpoints
	// that touch on-chain state or mutate balances (deposit, withdraw, key
	// creation, referral apply, invite redemption). These are higher-value
	// targets for spam/abuse than inference, so we throttle them harder.
	// Nil means unlimited.
	financialRateLimiter *ratelimit.Limiter

	// serviceRateLimiter applies an elevated per-account limit to trusted
	// service accounts (store.RoleService), e.g. an upstream aggregator like
	// OpenRouter that fans out many end-users behind one key. When nil,
	// service accounts bypass rate limiting entirely.
	serviceRateLimiter *ratelimit.Limiter

	// serviceReservations avoids hot-row pre-router ledger debits for trusted
	// service accounts when enabled. Normal consumers still use ledger debits.
	serviceReservations *serviceReservationManager

	// consumerTokenLimiter / serviceTokenLimiter enforce per-account input
	// (ITPM) and output (OTPM) token-per-minute limits on inference endpoints,
	// the industry-standard token throttle alongside RPM. Nil means no token
	// limiting for that tier. Service accounts use serviceTokenLimiter.
	consumerTokenLimiter *ratelimit.TokenLimiter
	serviceTokenLimiter  *ratelimit.TokenLimiter
	// outputAdmissionEstimator enables service-account expected-output admission
	// for OTPM. Nil means disabled and preserves full max_tokens admission.
	outputAdmissionEstimator *ratelimit.OutputAdmissionEstimator

	// keyRPMLimiter / keyTokenLimiter enforce PER-KEY rate overrides (each key
	// may carry a different ceiling) on top of the per-account limiters above.
	// They only act when a key sets RPMLimit / ITPMLimit / OTPMLimit; otherwise
	// the key inherits the account-level limits. Nil disables per-key limiting.
	keyRPMLimiter   *ratelimit.Limiter
	keyTokenLimiter *ratelimit.KeyTokenLimiter

	// routeTelemetry is the bounded, non-blocking sink that persists
	// best-effort routing telemetry (inference-route records, outcome updates,
	// rejection ledger rows) off the request path. It is set by NewServer; a
	// Server built directly (e.g. &Server{} in tests) leaves it nil, and
	// submitTelemetry falls back to a per-write saferun.Go in that case.
	routeTelemetry *telemetrySink

	// profiler owns the per-request profile records and their dedicated sink
	// (system profiler). Nil on a Server built without NewServer.
	profiler *profiler
	// unknownRequestFrames counts provider frames for requests the coordinator
	// no longer tracks (zombie streams); exported on the fleet coordinator row.
	unknownRequestFrames atomic.Int64

	// mediaResolver fetches remote http(s) image_url/video_url links into
	// inline base64 data: URIs before the request body is E2E-encrypted to a
	// provider, so consumers can pass links instead of pre-encoding media
	// client-side (media_resolve.go). The coordinator is the single SSRF
	// chokepoint; the provider still only ever sees data: URIs. Set by
	// NewServer from env; nil (e.g. a &Server{} built directly in tests)
	// behaves as disabled and falls back to the legacy pre-dispatch rejection.
	mediaResolver *mediafetch.Resolver
	// routingScanSem bounds how many provider-selection scans (the
	// ReserveProviderEx/ReserveProviderWithPlan family — a read-lock walk of
	// ~1,260 providers per attempt) may run concurrently. During the
	// 2026-09-01 congestion collapse, retry-amplified inbound (~100 req/s of
	// retryable 429 traffic) times a fresh full scan per dispatch attempt
	// saturated every coordinator CPU (attempt-0 route p50 40ms → 4.6s,
	// success ~40%, 429s delivered after 11s) — a stable death loop. With the
	// semaphore, excess requests park cheaply on the channel instead of
	// piling onto the scheduler; one that cannot acquire within its remaining
	// first-content budget sheds as a capacity-shaped 429
	// (errRoutingScanSaturated). Capacity defaults to runtime.NumCPU()
	// (min 2); override via EIGENINFERENCE_ROUTING_CONCURRENCY
	// (SetRoutingConcurrency, called before serving starts).
	routingScanSem chan struct{}

	// routeLatencyEWMAMs is an EWMA of attempt-0 route latency (ReceivedAt →
	// RoutedAt, milliseconds), updated where RoutedAt is stamped in
	// dispatchWithReserver. estimateRetryAfter consults it: when routing
	// itself is degraded (EWMA > 1s) the returned Retry-After scales up so
	// upstream backoff actually relieves pressure — during the 2026-09-01
	// collapse the queue-depth heuristic returned 2s on an empty queue and
	// invited 2s retry storms. Guarded by routeLatencyMu (one tiny critical
	// section per request; no allocation).
	routeLatencyMu     sync.Mutex
	routeLatencyEWMAMs float64
}

// NewServer creates a configured Server with all routes mounted.
func NewServer(reg *registry.Registry, st store.Store, cfg ServerConfig, logger *slog.Logger) *Server {
	// Wire the store into the registry for provider fleet persistence.
	reg.SetStore(st)

	// main.go supplies the AppConfig-validated media-fetch config; a nil field
	// (bare ServerConfig{} literals, tests) falls back to the environment.
	mediaFetchCfg := mediafetch.ConfigFromEnv()
	if cfg.MediaFetch != nil {
		mediaFetchCfg = *cfg.MediaFetch
	}
	firstContentDeadlineBase := cfg.FirstContentDeadlineBase
	if firstContentDeadlineBase <= 0 {
		firstContentDeadlineBase = defaultFirstContentDeadlineBase
	}

	s := &Server{
		registry:                 reg,
		store:                    st,
		ledger:                   payments.NewLedger(st),
		logger:                   logger,
		mux:                      http.NewServeMux(),
		knownRuntimeManifest:     &RuntimeManifest{},
		metrics:                  NewMetrics(),
		telemetryLimiter:         newTelemetryLimiter(),
		readCache:                newTTLCache(),
		geoResolver:              newProviderGeoResolverFromEnv(logger),
		apiKeyCache:              make(map[string]apiKeyCacheEntry),
		codeAttestThrottle:       newCodeAttestThrottle(),
		trustReuseCache:          newTrustReuseCache(),
		mdmSchedulerConfig:       cfg.MDMScheduler,
		settlements:              newSettlementHolder(),
		zombieCanceller:          newZombieStreamCanceller(),
		hedgeGov:                 newHedgeGovernor(),
		serviceReservations:      newServiceReservationManager(st, cfg.ServiceReservations),
		routeTelemetry:           newTelemetrySink(logger, defaultTelemetrySinkCapacity, defaultTelemetrySinkWorkers),
		mediaResolver:            mediafetch.NewResolver(mediaFetchCfg, logger),
		firstContentDeadlineBase: firstContentDeadlineBase,
		routingScanSem:           make(chan struct{}, DefaultRoutingConcurrency()),
	}
	if _, clampedDown := trustReuseReconnectGapFromEnv(); clampedDown {
		logger.Warn("EIGENINFERENCE_TRUST_REUSE_RECONNECT_GAP exceeds the 120s security ceiling; clamping DOWN",
			"requested", os.Getenv("EIGENINFERENCE_TRUST_REUSE_RECONNECT_GAP"),
			"allowance", maxTrustReuseReconnectGap,
			"reason", "a contiguous offline gap must stay below the RecoveryOS round-trip floor (Threat-Model T-036)",
		)
	}
	// Registry write-lock wait, by call site. This is the acceptance metric
	// for taking the recorders off the request path: today the wait is only
	// inferable from goroutine dumps.
	reg.SetLockWaitObserver(func(site string, wait time.Duration) {
		s.ddHistogram("registry.mu.write_wait_ms", float64(wait.Microseconds())/1000, []string{"site:" + site})
	})
	s.trustCoverage = make(map[string]string)
	s.trustCoverageCtx, s.trustCoverageCancel = context.WithCancel(context.Background())
	saferun.Go(logger, "trustCoverageLoop", s.trustCoverageLoop)
	if cfg.DurableTrustReuse {
		journalPath := cfg.TrustReuseJournalPath
		if strings.TrimSpace(journalPath) == "" {
			journalPath = resolveTrustReuseRevocationJournalPath()
		}
		s.trustReuseJournal = newFileHardUntrustJournal(journalPath)
		s.pendingHardUntrustKeyHashes = make(map[string]int)
		s.trustReplayCtx, s.trustReplayCancel = context.WithCancel(
			context.Background(),
		)
		s.trustReplayInFlight = make(map[string]struct{})
	}
	reg.SetRuntimeCapabilitiesPromotedHook(s.handleRuntimeCapabilitiesPromoted)
	// The per-identity gate locks that replaced the request-path registry
	// write lock (registry/gate_state.go) report any acquisition wait above
	// 1 ms here, tagged by recorder site, so the new locks stay observable.
	reg.SetGateWaitObserver(func(site string, wait time.Duration) {
		s.ddHistogram("registry.gate.wait_ms", float64(wait.Microseconds())/1000, []string{"site:" + site})
	})
	s.profiler = newProfilerFromEnv(s)
	s.registerDefaultGauges()
	s.routes()

	// Load stored provider records into a lookup table for matching
	// reconnecting providers to their persisted state.
	s.storedProviders = reg.LoadStoredProviders()
	// Apply server configuration from ServerConfig.
	// TODO(auth): storing admin emails in the server struct is an antipattern.
	// Move admin verification to an external auth service (Privy or IDP) so that
	// the server doesn't need to hold email state.
	s.adminKey = cfg.AdminKey
	if len(cfg.AdminEmails) > 0 {
		s.adminEmails = make(map[string]bool)
		for _, e := range cfg.AdminEmails {
			s.adminEmails[strings.ToLower(strings.TrimSpace(e))] = true
		}
	}
	s.consoleURL = cfg.ConsoleURL
	s.corsOrigin = cfg.CORSOrigin
	s.baseURL = strings.TrimRight(cfg.BaseURL, "/")
	s.minProviderVersion = strings.TrimSpace(cfg.MinProviderVersion)
	s.r2CDNURL = strings.TrimRight(cfg.R2CDNURL, "/")
	s.releaseKey = cfg.ReleaseKey

	return s
}

func (s *Server) handleRuntimeCapabilitiesPromoted(providerID string) {
	provider := s.registry.GetProvider(providerID)
	if provider == nil {
		return
	}
	provider.Mu().Lock()
	backend, version := provider.Backend, provider.Version
	provider.Mu().Unlock()
	if !s.providerSupportsDesiredModels(backend, version) {
		return
	}
	entries := s.registry.DesiredModelsForProvider(providerID)
	if err := s.registry.SendDesiredModels(providerID, entries); err != nil {
		s.logger.Warn("failed to refresh desired_models after capability promotion",
			"provider_id", providerID,
			"error", err,
		)
	}
}

// Close releases background resources owned by the Server.
func (s *Server) Close() {
	// Graceful-shutdown continuity sweep: stop the periodic coverage loop,
	// then persist the exact shutdown instant for every covered provider so a
	// short deploy reconnects into the continuity fast-skip on the next
	// coordinator instead of a fleet-wide live MDM herd. A crash skips this —
	// the last periodic write stands and the gap is over-estimated (fail-safe).
	if s.trustCoverageCancel != nil {
		s.trustCoverageCancel()
	}
	s.finalTrustCoverageSweep()
	if s.trustReplayCancel != nil {
		s.trustReplayCancel()
	}
	if s.mdmScheduler != nil {
		s.mdmScheduler.Close()
	}
	if s.promptPreloader != nil {
		s.promptPreloader.Close()
	}
	if s.promptArtifacts != nil {
		s.promptArtifacts.Close()
	}
	if s.routeTelemetry != nil {
		// Bounded flush: buffered route rows are written before main's deferred
		// store Close (registered earlier, so it runs after this) tears down the
		// pool. A stuck store cannot hold shutdown past the deadline; whatever
		// is still unwritten then is counted as dropped by the sink.
		if !s.routeTelemetry.closeAndWait(telemetrySinkShutdownFlush) && s.logger != nil {
			s.logger.Warn("routing telemetry sink did not finish flushing before the shutdown deadline",
				"deadline", telemetrySinkShutdownFlush,
				"dropped_total", s.routeTelemetry.dropped.Load(),
			)
		}
	}
	s.trustAuthorityMu.Lock()
	if s.trustAuthority != nil {
		_ = s.trustAuthority.Close()
		s.trustAuthority = nil
	}
	s.trustAuthorityMu.Unlock()
	if s.profiler != nil {
		s.profiler.close()
	}
}

// SetAdminKey configures the admin API key for admin-only endpoints.
func (s *Server) SetAdminKey(key string) {
	s.adminKey = key
}

// SetMinProviderVersion sets the minimum provider version for routing.
func (s *Server) SetMinProviderVersion(v string) {
	s.minProviderVersion = strings.TrimSpace(v)
}

// SetBaseURL sets the coordinator's public URL (used to template install.sh).
// Pass the canonical origin with no trailing slash, e.g. "https://api.darkbloom.dev".
// If unset, the install.sh handler derives a URL from the request's Host header.
func (s *Server) SetBaseURL(url string) {
	s.baseURL = strings.TrimRight(url, "/")
}

// SetR2CDNURL sets the public R2 bucket URL that install.sh substitutes as
// the model/template/release download origin. If unset, install.sh keeps the
// placeholder — providers will fail to pull artifacts, making the misconfig
// loud instead of silent.
func (s *Server) SetR2CDNURL(url string) {
	s.r2CDNURL = strings.TrimRight(url, "/")
}

// SetProfileSigner configures the CMS signing identity used to sign the
// enrollment .mobileconfig served by /v1/enroll. When unset (nil), profiles are
// served unsigned (the historical behaviour).
func (s *Server) SetProfileSigner(signer *profilesign.Signer) {
	s.profileSigner = signer
}

// SetBilling configures the billing service for multi-chain payments and referrals.
func (s *Server) SetBilling(svc *billing.Service) {
	s.billing = svc
}

func (s *Server) Billing() *billing.Service {
	return s.billing
}

// SetBaseRewards configures the provider base-rewards engine (off unless the
// EIGENINFERENCE_BASE_REWARDS flag is set; nil = disabled).
func (s *Server) SetBaseRewards(e *baserewards.Engine) {
	s.baseRewards = e
}

// BaseRewards returns the base-rewards engine, or nil when disabled.
func (s *Server) BaseRewards() *baserewards.Engine {
	return s.baseRewards
}

func (s *Server) SetChallengeInterval(d time.Duration) {
	s.challengeInterval = d
}

func (s *Server) SetSkipChallenge(skip bool) {
	s.skipChallenge = skip
}

// SetAllowDuplicateProviderSerialsForTesting lets the in-process E2E testbed
// emulate multiple physical providers on one Mac. Production never calls it.
func (s *Server) SetAllowDuplicateProviderSerialsForTesting(allow bool) {
	s.allowDuplicateProviderSerials = allow
}

// SetPrivyAuth configures Privy JWT authentication for consumer endpoints.
func (s *Server) SetPrivyAuth(pa *auth.PrivyAuth) {
	s.privyAuth = pa
}

// SetAdminEmails configures which Privy accounts have admin access.
func (s *Server) SetAdminEmails(emails []string) {
	s.adminEmails = make(map[string]bool, len(emails))
	for _, e := range emails {
		s.adminEmails[strings.ToLower(strings.TrimSpace(e))] = true
	}
}

// SetMDMClient configures the MicroMDM client for provider verification.
// When set, providers are verified against MDM on registration.
func (s *Server) SetMDMClient(client *mdm.Client) {
	s.mdmClient = client
	if client != nil && s.mdmScheduler == nil {
		s.mdmScheduler = newMDMVerificationScheduler(s, s.mdmSchedulerConfig, mdmSchedulerDeps{})
	}
}

// StartMDMScheduler starts the single durable dispatcher and fixed worker pool.
func (s *Server) StartMDMScheduler() {
	if s.mdmScheduler != nil {
		s.mdmScheduler.Start()
	}
}

// SetCodeAttestor wires the APNs code-identity attestor (v0.6.0). When set, the
// coordinator issues code-identity challenges and measures which providers pass —
// but enforcement (derouting un-attested providers) only begins once a deadline
// is reached (SetCodeAttestationDeadline). So configuring the attestor alone is
// SAFE: the fleet stays in grace/observe mode and keeps routing. Passing nil
// leaves the feature disabled. Call once during server setup, before providers
// connect.
func (s *Server) SetCodeAttestor(a apns.CodeIdentityAttestor) {
	s.codeAttestor = a
	s.registry.SetCodeAttestationConfigured(a != nil)
}

// SetCodeAttestationDeadline sets the instant at which code-identity attestation
// becomes mandatory for routing. Before it (or when zero) the coordinator runs in
// grace mode: it challenges providers but still routes un-attested ones, giving
// the fleet time to update to 0.6.0 and attest. Wire it from APNS_ENFORCE_AFTER.
func (s *Server) SetCodeAttestationDeadline(t time.Time) {
	s.registry.SetCodeAttestationDeadline(t)
}

// SetMDMWebhookSecret configures an optional shared secret that MicroMDM must
// present (as ?token= or the X-Webhook-Token header) when calling the webhook.
// When empty, the webhook relies solely on the solicited-command (CommandUUID)
// gate in the MDM client; when set, callers lacking the secret are rejected
// before the body is read. MicroMDM is co-located with the coordinator, so this
// secret never traverses the public network.
func (s *Server) SetMDMWebhookSecret(secret string) {
	s.mdmWebhookSecret = secret
}

// SyncModelCatalog reads active models from the store and updates the
// registry's model catalog. Call this at startup and after admin catalog changes.
func (s *Server) SyncModelCatalog() {
	registryRows, err := s.store.ListActiveModelRegistryWithError()
	if err != nil {
		s.logger.Error("model registry catalog sync failed", "error", err)
		return
	}
	entries := make([]registry.CatalogEntry, 0, len(registryRows))
	for _, row := range registryRows {
		if row.ActiveVersion == nil {
			continue
		}
		entries = append(entries, registry.CatalogEntry{
			ID:         row.ID,
			WeightHash: row.ActiveVersion.AggregateSHA256,
			SizeGB:     float64(row.ActiveVersion.TotalSizeBytes) / 1e9,
			MinRAMGB:   row.MinRAMGB,
			RequiredProviderCapabilities: append(
				[]string{}, row.RequiredProviderCapabilities...),
		})
	}
	// Advance the prompt-artifact generation before publishing new routing
	// hashes. Cache planning also carries and compares the aggregate hash, so
	// either side of this handoff is fail-cold under concurrent requests.
	if err := s.reconcilePromptArtifacts(registryRows); err != nil {
		s.logger.Error("prompt artifact catalog reconcile rejected", "error", err)
	}
	s.registry.SetModelCatalog(entries)
	s.logger.Info("model registry catalog synced to registry", "active_models", len(entries))

	s.syncModelAliases(registryRows)
	// Catalog capability changes can invalidate an in-flight desired-model
	// prefetch even when alias pointers did not change. Re-publish the filtered
	// desired state immediately; newly ineligible providers receive an empty
	// set, which cancels stale reconciliation work.
	s.fanOutDesiredModels()
	s.invalidateCatalogCache()
}

// syncModelAliases loads standard rollout aliases first, then resolves
// OpenRouter-only aliases through either a standard alias or an active concrete
// catalog model. OpenRouter-only targets route requests but do not participate
// in provider convergence or canonical public naming.
func (s *Server) syncModelAliases(registryRows []store.ModelRegistryRecord) {
	aliases, err := s.store.ListModelAliases()
	if err != nil {
		s.logger.Error("model alias sync failed", "error", err)
		return
	}
	resolved := make(map[string]registry.AliasTarget, len(aliases))
	activeConcreteModels := make(map[string]struct{}, len(registryRows))
	for _, row := range registryRows {
		if row.ActiveVersion != nil {
			activeConcreteModels[row.ID] = struct{}{}
		}
	}
	for _, a := range aliases {
		if !a.Active || a.OpenRouterOnly || a.DesiredBuild == "" {
			continue
		}
		resolved[a.AliasID] = registry.AliasTarget{
			Desired:  a.DesiredBuild,
			Previous: a.PreviousBuild,
			Retired:  a.RetiredBuilds,
		}
	}
	for _, a := range aliases {
		if !a.Active || !a.OpenRouterOnly {
			continue
		}
		var target registry.AliasTarget
		var ok bool
		if openRouterAliasUsesConcreteSource(a) {
			if _, ok = activeConcreteModels[a.SourceModel]; ok {
				target = registry.AliasTarget{Desired: a.SourceModel}
			}
		} else {
			target, ok = resolved[a.SourceModel]
		}
		if !ok {
			s.logger.Warn("OpenRouter alias source is unavailable", "alias_id", a.AliasID, "source_model", a.SourceModel)
			continue
		}
		target.OpenRouterOnly = true
		resolved[a.AliasID] = target
	}
	s.registry.SetModelAliases(resolved)
	s.logger.Info("model aliases synced to registry", "active_aliases", len(resolved))
}

// invalidateCatalogCache removes all cached model catalog responses so the
// next request picks up any changes made by admin endpoints.
func (s *Server) invalidateCatalogCache() {
	if s.readCache == nil {
		return
	}
	for _, typeFilter := range []string{"", "text"} {
		for _, includeAliases := range []bool{false, true} {
			s.readCache.Invalidate(modelCatalogCacheKey(typeFilter, includeAliases))
		}
	}
	// /v1/models entry memo + list bodies (both include_builds values) and the
	// OpenRouter feed are derived from the same catalog; drop them too so an
	// admin alias/registry change is visible on the next request instead of
	// after their 2s/5s TTLs (which remain the bound for out-of-band DB edits).
	for _, includeBuilds := range []bool{false, true} {
		s.readCache.Invalidate(modelEntriesCacheKey(includeBuilds))
		s.readCache.Invalidate(modelListBodyCacheKey(includeBuilds))
	}
	s.readCache.Invalidate(openRouterFeedCacheKey)
	// stats:v1 is deliberately NOT evicted here: the stats refresher recomputes
	// it every minute, and evicting it made every concurrent /v1/stats request
	// rerun the multi-second usage analytics statements.
}

// SetTTFTHardReject toggles the per-request TTFT admission ceiling between a
// hard 429 (true, legacy) and a soft routing preference (false, default). See
// the ttftHardReject field for rationale. Call before serving starts.
func (s *Server) SetTTFTHardReject(enabled bool) {
	s.ttftHardReject = enabled
}

// SetRejectModels sets the requested/resolved model IDs to 429 at public
// admission. Call before serving starts.
func (s *Server) SetRejectModels(models map[string]bool) {
	if len(models) == 0 {
		s.rejectModels = nil
		return
	}
	copy := make(map[string]bool, len(models))
	for model, reject := range models {
		if !reject {
			continue
		}
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		copy[model] = true
	}
	if len(copy) == 0 {
		s.rejectModels = nil
		return
	}
	s.rejectModels = copy
}

func (s *Server) modelShed(resolved, requested string) bool {
	if len(s.rejectModels) == 0 {
		return false
	}
	return s.rejectModels[resolved] || s.rejectModels[requested]
}

// SetMinDecodeTPS sets the per-request sustained-decode floor (tokens/sec) the
// scheduler uses as a soft routing preference. <= 0 disables it. See the
// minDecodeTPS field. Call before serving starts.
func (s *Server) SetMinDecodeTPS(tps float64) {
	if tps < 0 {
		tps = 0
	}
	s.minDecodeTPS = tps
}

// SetServabilityGate toggles the smart early-429 admission gate. See the
// servabilityGate field. Call before serving starts.
func (s *Server) SetServabilityGate(enabled bool) {
	s.servabilityGate = enabled
}

// SetDisableClientErrorStop is the kill switch for the C1 client-shape failover
// stop. true restores pre-fix behavior (deterministic provider 4xx fails over up
// to maxDispatchAttempts). Default (false) = stop enabled. Call before serving.
func (s *Server) SetDisableClientErrorStop(disabled bool) {
	s.disableClientErrorStop = disabled
}

// SetLongPromptThreshold configures the estimated-prompt-token count at/above
// which the scheduler applies the long-prompt fastest-tier routing preference.
// 0 disables it (behavior-neutral). It is a package-level scheduler knob (like
// the prefill/decode ratio), so this delegates to the registry. Call before
// serving starts. SOFT bias only — no TTFT 429 is introduced.
func (s *Server) SetLongPromptThreshold(tokens int) {
	registry.SetLongPromptThreshold(tokens)
}

// SetLongPromptPrefillWeight configures the prefill-term multiplier the scheduler
// applies to long prompts. Values < 1 clamp to 1.0 (no amplification).
// Delegates to the registry; call before serving starts.
func (s *Server) SetLongPromptPrefillWeight(weight float64) {
	registry.SetLongPromptPrefillWeight(weight)
}

// SetReleaseKey configures the scoped release key for GitHub Actions.
func (s *Server) SetReleaseKey(key string) {
	s.releaseKey = key
}

// SetCoordinatorKey installs the X25519 keypair the coordinator publishes
// for sender-to-coordinator request encryption. Pass nil to disable.
func (s *Server) SetCoordinatorKey(k *e2e.CoordinatorKey) {
	s.coordinatorKey = k
}

//go:embed install.sh
var installScript []byte

// installScriptPlaceholder is substituted with the coordinator's public URL at
// serve time. coordinator/api/install.sh is generated byte-for-byte from the
// canonical scripts/install.sh by scripts/sync-install-embed.sh.
//
// The legacy install.sh also substituted __DARKBLOOM_R2_CDN_URL__ and
// __DARKBLOOM_R2_SITE_PACKAGES_CDN_URL__ for the Python runtime download.
// Post-Swift-cutover (v0.5.0+) install.sh no longer touches R2 directly --
// model downloads run inside `darkbloom models download` against the public
// R2 CDN -- so only the coordinator URL needs serve-time templating.
const installScriptPlaceholder = "__DARKBLOOM_COORD_URL__"

// resolveBaseURL returns the configured baseURL, or derives one from the
// request's Host header when baseURL is unset. TLS-terminating proxies pass
// through the original scheme via X-Forwarded-Proto; default to https.
func (s *Server) resolveBaseURL(r *http.Request) string {
	if s.baseURL != "" {
		return s.baseURL
	}
	scheme := "https"
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	} else if r.TLS == nil {
		scheme = "http"
	}
	return scheme + "://" + r.Host
}

// routes mounts all HTTP and WebSocket handlers.
func (s *Server) routes() {
	// Install script — served from the generated embed with the coordinator URL
	// substituted per environment.
	s.mux.HandleFunc("GET /install.sh", func(w http.ResponseWriter, r *http.Request) {
		rendered := strings.ReplaceAll(string(installScript), installScriptPlaceholder, s.resolveBaseURL(r))
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		io.WriteString(w, rendered)
	})

	// Health check — no auth required.
	s.mux.HandleFunc("GET /health", s.handleHealth)
	// Aggregate exact-cache rollout health. Contains no provider/model/account
	// identity and is safe for canary automation.
	s.mux.HandleFunc("GET /v1/cache/status", s.handleExactCacheStatus)

	// Readiness probe — no auth required. Reports graceful-drain state so load
	// balancers and the deploy script treat a draining coordinator as not-ready
	// (503) and can wait for inflight==0 before restart. See drain.go (DAR-327).
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)

	// Provider WebSocket — no API key auth (providers authenticate differently).
	s.mux.HandleFunc("GET /ws/provider", s.handleProviderWS)

	// Key management — requires interactive Privy session (API keys rejected
	// to prevent self-replication from a leaked key).
	s.mux.HandleFunc("POST /v1/auth/keys", s.requirePrivyAuth(s.rateLimitFinancial(s.handleCreateKey)))
	s.mux.HandleFunc("DELETE /v1/auth/keys", s.requirePrivyAuth(s.handleRevokeKey))

	// Multi-key management (OpenRouter-shaped CRUD). One account may own many
	// named, individually-limited keys. Management requires an interactive
	// Privy session so a leaked inference key can't enumerate or mint keys.
	s.mux.HandleFunc("GET /v1/keys", s.requirePrivyAuth(s.handleListAPIKeys))
	s.mux.HandleFunc("POST /v1/keys", s.requirePrivyAuth(s.rateLimitFinancial(s.handleCreateAPIKey)))
	s.mux.HandleFunc("GET /v1/keys/{id}", s.requirePrivyAuth(s.handleGetAPIKey))
	s.mux.HandleFunc("PATCH /v1/keys/{id}", s.requirePrivyAuth(s.rateLimitFinancial(s.handleUpdateAPIKey)))
	s.mux.HandleFunc("DELETE /v1/keys/{id}", s.requirePrivyAuth(s.rateLimitFinancial(s.handleDeleteAPIKey)))
	s.mux.HandleFunc("POST /v1/keys/{id}/rotate", s.requirePrivyAuth(s.rateLimitFinancial(s.handleRotateAPIKey)))
	// Metadata for the calling key (OpenRouter parity) — API key auth.
	s.mux.HandleFunc("GET /v1/key", s.requireAuth(s.handleGetCallingKey))

	// Consumer endpoints — API key auth required + per-account rate limit.
	// Inference endpoints are wrapped in sealedTransport so senders can opt into
	// sender→coordinator encryption by setting Content-Type:
	// application/eigeninference-sealed+json (see sender_encryption.go).
	// rateLimitConsumer is chained inside requireAuth so the accountID is in
	// context. Read-only endpoints (GET /v1/models) skip rate limiting since
	// they're cheap and clients poll them.
	// drainGate is the OUTERMOST wrapper: while the coordinator is draining for a
	// restart/upgrade it rejects NEW inference requests with 429+Retry-After
	// before any auth/decrypt work, and otherwise counts the request as in-flight
	// so /readyz can report when it's safe to shut down (DAR-327 Phase 1).
	//
	// IMPORTANT: ANY future provider-routed inference endpoint (e.g.
	// /v1/audio/transcriptions, /v1/images/generations, /v1/embeddings) MUST also
	// be wrapped in s.drainGate(...). An ungated route won't 429 during drain and,
	// because it isn't counted in httpInflight, won't be seen by WaitForInflightZero
	// — so a graceful shutdown could cut it off mid-flight. Add new dispatch routes
	// here, gated, alongside the four below.
	s.mux.HandleFunc("POST /v1/chat/completions", s.drainGate(s.requireAuth(s.rateLimitConsumer(s.sealedTransport(s.handleChatCompletions)))))
	s.mux.HandleFunc("POST /v1/responses", s.drainGate(s.requireAuth(s.rateLimitConsumer(s.sealedTransport(s.handleChatCompletions))))) // Responses API — same handler, auto-detects input vs messages
	s.mux.HandleFunc("POST /v1/completions", s.drainGate(s.requireAuth(s.rateLimitConsumer(s.sealedTransport(s.handleCompletions)))))
	s.mux.HandleFunc("POST /v1/messages", s.drainGate(s.requireAuth(s.rateLimitConsumer(s.sealedTransport(s.handleAnthropicMessages)))))
	s.mux.HandleFunc("GET /v1/models", s.requireAuth(s.handleListModels))
	// Dedicated OpenRouter provider feed — pure OpenRouter schema, no Darkbloom metadata.
	s.mux.HandleFunc("GET /v1/models/openrouter", s.requireAuth(s.handleListModelsOpenRouter))
	// OpenAI "retrieve model" — {id...} matches slashed HuggingFace-style ids;
	// the literal /v1/models/openrouter and /v1/models/capacity routes win.
	s.mux.HandleFunc("GET /v1/models/{id...}", s.requireAuth(s.handleGetModel))

	// Sender encryption — public key publication for sender→coordinator E2E.
	// Optional: senders may use this to encrypt request bodies; plaintext path
	// continues to work unchanged when this header isn't set.
	s.mux.HandleFunc("GET /v1/encryption-key", s.handleEncryptionKey)

	// MDM webhook — MicroMDM sends command responses here.
	s.mux.HandleFunc("POST /v1/mdm/webhook", s.HandleMDMWebhook)

	// Payment endpoints — API key auth required.
	s.mux.HandleFunc("GET /v1/payments/balance", s.requireAuth(s.handleBalance))
	s.mux.HandleFunc("GET /v1/payments/usage", s.requireAuth(s.handleUsage))

	// Provider earnings — no API key auth (providers identify by provider address).
	s.mux.HandleFunc("GET /v1/provider/earnings", s.handleProviderEarnings)

	s.mux.HandleFunc("GET /v1/provider/account-earnings", s.requireAuth(s.handleAccountEarnings))

	// Account-scoped provider dashboard.
	s.mux.HandleFunc("GET /v1/me/providers", s.requirePrivyAuth(s.handleMyProviders))
	s.mux.HandleFunc("GET /v1/me/summary", s.requirePrivyAuth(s.handleMySummary))
	// Alias-aware owned live-model ids for the console's self-route key picker.
	s.mux.HandleFunc("GET /v1/me/self-route-models", s.requirePrivyAuth(s.handleMySelfRouteModels))
	// Ownership-checked hard delete of a retired/offline machine's record(s).
	s.mux.HandleFunc("DELETE /v1/me/providers/{id}", s.requirePrivyAuth(s.rateLimitFinancial(s.handleDeleteMyProvider)))

	// MDM enrollment — generates the per-device .mobileconfig (SCEP + MDM).
	// No auth needed — trust comes from MDM SecurityInfo verification after
	// enrollment, not from possession of the profile.
	s.mux.HandleFunc("POST /v1/enroll", s.handleEnroll)

	// Attestation status — public, no auth needed. Raw device identity and MDA
	// certificates remain coordinator-private because the leaf embeds serial/UDID.
	s.mux.HandleFunc("GET /v1/providers/attestation", s.handleProviderAttestation)

	// Capacity snapshot — no auth needed. Upstream routers poll this.
	s.mux.HandleFunc("GET /v1/models/capacity", s.handleModelsCapacity)

	// Platform stats — no auth needed. Frontend dashboard uses this.
	s.mux.HandleFunc("GET /v1/stats", s.handleStats)

	// Public leaderboard + network totals — no auth, pseudonymized,
	// 5-min/1-min cache.
	s.mux.HandleFunc("GET /v1/leaderboard", s.handleLeaderboard)
	s.mux.HandleFunc("GET /v1/network/totals", s.handleNetworkTotals)
	s.mux.HandleFunc("GET /v1/network/series", s.handleNetworkSeries)

	// Provider version check — no auth needed. Providers call this to check for updates.
	s.mux.HandleFunc("GET /api/version", s.handleVersion)

	// Releases — versioned provider binary distribution.
	s.mux.HandleFunc("POST /v1/releases", s.handleRegisterRelease)     // scoped release key (GitHub Action)
	s.mux.HandleFunc("GET /v1/releases/latest", s.handleLatestRelease) // public (install.sh)

	// Device authorization flow — providers link to user accounts.
	s.mux.HandleFunc("POST /v1/device/code", s.handleDeviceCode)   // no auth — provider not yet authenticated
	s.mux.HandleFunc("POST /v1/device/token", s.handleDeviceToken) // no auth — polls with device_code secret
	// Device approve issues a long-lived provider→account linking token —
	// same risk class as /v1/auth/keys, so financial-tier limit applies.
	// Uses requirePrivyAuth to reject API keys (interactive session only).
	s.mux.HandleFunc("POST /v1/device/approve", s.requirePrivyAuth(s.rateLimitFinancial(s.handleDeviceApprove)))

	// --- Billing endpoints (Stripe payments + referrals) ---

	// Stripe — financial limiter on session creation (creates a checkout
	// intent, hits external API). Read-only status endpoint not throttled.
	s.mux.HandleFunc("POST /v1/billing/stripe/create-session", s.requireAuth(s.rateLimitFinancial(s.handleStripeCreateSession)))
	s.mux.HandleFunc("POST /v1/billing/stripe/webhook", s.handleStripeWebhook) // no auth — Stripe signs it
	s.mux.HandleFunc("GET /v1/billing/stripe/session", s.requireAuth(s.handleStripeSessionStatus))

	// Wallet balance
	s.mux.HandleFunc("GET /v1/billing/wallet/balance", s.requireAuth(s.handleWalletBalance))

	// A single bank withdrawal experience, with separate payout lifecycles.
	s.mux.HandleFunc("POST /v1/billing/stripe/quote", s.requirePrivyAuth(s.rateLimitFinancial(s.handleGlobalPayoutQuote)))
	s.mux.HandleFunc("POST /v1/billing/stripe/global/webhook", s.handleGlobalPayoutWebhook)
	// Stripe Payouts (Connect Express) — bank/card withdrawals.
	s.mux.HandleFunc("POST /v1/billing/stripe/onboard", s.requirePrivyAuth(s.rateLimitFinancial(s.handleStripeOnboard)))
	s.mux.HandleFunc("GET /v1/billing/stripe/status", s.requireAuth(s.handleStripeStatus))
	s.mux.HandleFunc("POST /v1/billing/withdraw/stripe", s.requirePrivyAuth(s.rateLimitFinancial(s.handleStripeWithdraw)))
	s.mux.HandleFunc("GET /v1/billing/stripe/withdrawals", s.requireAuth(s.handleStripeWithdrawals))
	// requirePrivyAuth (not requireAuth): both of these are account-management
	// operations — a leaked inference API key must not be able to detach the
	// user's payout account, nor mint a dashboard session that can point their
	// earnings at a different bank account.
	//
	// The dashboard route additionally carries rateLimitFinancial: every call
	// is a live Stripe POST that mints a credential, so an authenticated
	// session must not be able to loop it and burn the platform's Stripe
	// request capacity. Chained INSIDE requirePrivyAuth because the limiter
	// keys on the account ID the auth middleware puts in the request context.
	s.mux.HandleFunc("POST /v1/billing/stripe/dashboard", s.requirePrivyAuth(s.rateLimitFinancial(s.handleStripeDashboardLink)))
	s.mux.HandleFunc("DELETE /v1/billing/stripe/account", s.requirePrivyAuth(s.handleStripeUnlink))
	s.mux.HandleFunc("POST /v1/billing/stripe/connect/webhook", s.handleStripeConnectWebhook) // no auth — Stripe signs it

	// Pricing — GET is public, PUT/DELETE require auth
	s.mux.HandleFunc("GET /v1/pricing", s.handleGetPricing)                        // public
	s.mux.HandleFunc("PUT /v1/pricing", s.requireAuth(s.handleSetPricing))         // provider sets own prices
	s.mux.HandleFunc("DELETE /v1/pricing", s.requireAuth(s.handleDeletePricing))   // revert to default
	s.mux.HandleFunc("PUT /v1/admin/pricing", s.requireAuth(s.handleAdminPricing)) // platform sets defaults

	// Admin account management (service-role + per-account platform fee)
	s.mux.HandleFunc("PUT /v1/admin/users/role", s.requireAuth(s.handleAdminSetUserRole))
	s.mux.HandleFunc("PUT /v1/admin/users/platform-fee", s.requireAuth(s.handleAdminSetUserPlatformFee))

	// Admin model registry (manifest-backed). The legacy supported_models CRUD
	// (bare GET/POST/DELETE /v1/admin/models) was removed; the model_registry is
	// the single source of truth. Use register + the per-model action endpoints.
	s.mux.HandleFunc("POST /v1/admin/models/register", s.handleRegisterModel)
	// OpenRouter-only feed aliases clone a standard alias while exposing custom
	// provider id, marketplace slug, and Hugging Face identity.
	s.mux.HandleFunc("GET /v1/admin/models/openrouter-aliases", s.handleOpenRouterAliasList)
	s.mux.HandleFunc("POST /v1/admin/models/openrouter-aliases", s.handleOpenRouterAliasUpsert)
	s.mux.HandleFunc("DELETE /v1/admin/models/openrouter-aliases/{aliasID}", s.handleOpenRouterAliasDelete)
	// Public model aliases (stable names → concrete builds). More-specific
	// patterns take precedence over the POST /v1/admin/models/ subtree below.
	s.mux.HandleFunc("GET /v1/admin/models/aliases", s.handleModelAliasList)
	s.mux.HandleFunc("POST /v1/admin/models/aliases", s.handleModelAliasUpsert)
	s.mux.HandleFunc("DELETE /v1/admin/models/aliases/{aliasID}", s.handleModelAliasDelete)
	s.mux.HandleFunc("POST /v1/admin/models/", s.handleAdminModelRegistryAction)
	s.mux.HandleFunc("GET /v1/admin/releases", s.handleAdminListReleases)     // admin key or Privy admin
	s.mux.HandleFunc("DELETE /v1/admin/releases", s.handleAdminDeleteRelease) // admin key or Privy admin

	// Historical admin state export (DAR-70) — streams the TEE-sealed /data
	// archive used for the completed EigenCloud migration. Always registered, but
	// inert (404) unless EIGENINFERENCE_STATE_EXPORT_ENABLED=true; admin-gated;
	// encrypted to an age recipient by default. Auth + output protection are
	// enforced inside the handler.
	s.mux.HandleFunc("GET /v1/admin/state-export", s.handleAdminStateExport)

	// Admin CLI auth — Privy email OTP for getting admin tokens without a browser.
	s.mux.HandleFunc("POST /v1/admin/auth/init", s.handleAdminAuthInit)     // no auth (sends OTP)
	s.mux.HandleFunc("POST /v1/admin/auth/verify", s.handleAdminAuthVerify) // no auth (returns token)

	// Public model catalog — providers and install script fetch this
	s.mux.HandleFunc("GET /v1/models/catalog", s.handleModelCatalog)
	s.mux.HandleFunc("GET /v1/models/catalog/manifest/", s.handleModelCatalogManifest)
	s.mux.HandleFunc("GET /v1/models/catalog/", s.handleModelCatalogItem)

	// Runtime manifest — providers and users can inspect accepted runtime hashes.
	s.mux.HandleFunc("GET /v1/runtime/manifest", s.handleRuntimeManifest)

	// Payment methods info
	s.mux.HandleFunc("GET /v1/billing/methods", s.handleBillingMethods) // no auth needed

	// Referral system — register/apply mutate referral graph (financial
	// limiter); stats/info are read-only.
	s.mux.HandleFunc("POST /v1/referral/register", s.requireAuth(s.rateLimitFinancial(s.handleReferralRegister)))
	s.mux.HandleFunc("POST /v1/referral/apply", s.requireAuth(s.rateLimitFinancial(s.handleReferralApply)))
	s.mux.HandleFunc("GET /v1/referral/stats", s.requireAuth(s.handleReferralStats))
	s.mux.HandleFunc("GET /v1/referral/info", s.requireAuth(s.handleReferralInfo))

	// Invite codes (admin)
	// Invite code creation accepts amount_usd and produces a credit-bearing
	// code; redemption is already financial-tier so the issuance side must
	// match (otherwise an admin-key holder could spam codes anyway, but
	// keeping symmetry).
	s.mux.HandleFunc("POST /v1/admin/invite-codes", s.requireAuth(s.rateLimitFinancial(s.handleAdminCreateInviteCode)))
	s.mux.HandleFunc("GET /v1/admin/invite-codes", s.requireAuth(s.handleAdminListInviteCodes))
	s.mux.HandleFunc("DELETE /v1/admin/invite-codes", s.requireAuth(s.handleAdminDeactivateInviteCode))

	// Invite code redemption (user) — credits the redeemer's balance, so
	// it's a financial-tier endpoint.
	s.mux.HandleFunc("POST /v1/invite/redeem", s.requireAuth(s.rateLimitFinancial(s.handleRedeemInviteCode)))

	// Admin credit & reward
	s.mux.HandleFunc("POST /v1/admin/credit", s.requireAuth(s.handleAdminCredit))
	s.mux.HandleFunc("POST /v1/admin/reward", s.requireAuth(s.handleAdminReward))

	// Retain the client-telemetry route for mixed-version compatibility. The
	// handler returns 410 before reading a request body; coordinator-owned
	// operational telemetry remains separate.
	s.mux.HandleFunc("POST /v1/telemetry/events", s.handleTelemetryIngest)

	// Explicit provider log reports
	s.mux.HandleFunc("POST /v1/provider/log-report", s.requireAuth(s.handleUploadLogReport))
	s.mux.HandleFunc("GET /v1/admin/log-reports/{id}", s.requireAuth(s.handleGetLogReport))

	// Metrics snapshot (admin only)
	s.mux.HandleFunc("GET /v1/admin/metrics", s.handleAdminMetrics)
	s.mux.HandleFunc("GET /v1/admin/base-rewards", s.handleAdminBaseRewards)

	// Network utilization snapshot (admin only) — handler enforces admin auth
	// internally via requireAdminKey.
	s.mux.HandleFunc("GET /v1/admin/utilization", s.handleAdminUtilization)

	// Graceful drain toggle (admin only) — sets the coordinator into drain mode
	// before a restart/upgrade so new inference requests get 429 while in-flight
	// ones finish. Wrapped with requireAuth (the SAME pattern as the other
	// isAdminAuthorized/requireAdminKey endpoints, e.g. invite codes) so a Privy
	// admin JWT is parsed into the request context AND the admin key is accepted
	// as a pseudo-account; handleAdminDrain then authorizes via isAdminAuthorized
	// (admin key OR Privy admin). Registered before the /v1/ catch-all. Note:
	// /readyz stays unauthenticated. See drain.go (DAR-327 Phase 1).
	s.mux.HandleFunc("POST /v1/admin/drain", s.requireAuth(s.handleAdminDrain))

	// Routing telemetry (admin-gated; metadata only — no prompt/response content).
	// Browse as JSON or stream a CSV/NDJSON download for offline analysis.
	// See docs/design/routing-telemetry-and-calibration.md §6. Handlers
	// enforce admin auth internally via requireAdminKey.
	s.mux.HandleFunc("GET /v1/admin/routes", s.handleAdminRoutes)
	s.mux.HandleFunc("GET /v1/admin/routes/export", s.handleAdminRoutesExport)
	s.mux.HandleFunc("GET /v1/admin/profiles", s.handleAdminProfiles)
	s.mux.HandleFunc("GET /v1/admin/profiles/export", s.handleAdminProfilesExport)
	s.mux.HandleFunc("GET /v1/admin/snapshots", s.handleAdminSnapshots)
	s.mux.HandleFunc("GET /v1/admin/snapshots/export", s.handleAdminSnapshotsExport)
	s.mux.HandleFunc("GET /v1/admin/rejections", s.handleAdminRejections)
	s.mux.HandleFunc("GET /v1/admin/rejections/export", s.handleAdminRejectionsExport)

	// Catch-all for unimplemented OpenAI-compatible endpoints.
	// Registered last (old-style pattern) so explicit method+path routes
	// take precedence. Any /v1/* path not handled above gets a structured
	// JSON error instead of the mux default text/plain 404.
	s.mux.HandleFunc("/v1/", s.handleUnimplementedEndpoint)
}

// handleAdminMetrics returns the metrics snapshot in JSON or Prometheus text.
func (s *Server) handleAdminMetrics(w http.ResponseWriter, r *http.Request) {
	if !s.isAdminAuthorized(w, r) {
		return
	}
	snap := s.metrics.Snapshot()
	if r.URL.Query().Get("format") == "prom" {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(snap.RenderProm()))
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// handleUnimplementedEndpoint returns a structured JSON error for any /v1/*
// path not registered as an explicit route. This prevents OpenAI SDK clients
// from crashing on raw text/plain 404s when hitting unimplemented endpoints
// like /v1/embeddings or /v1/moderations.
func (s *Server) handleUnimplementedEndpoint(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusNotFound, errorResponse(
		"invalid_request_error",
		fmt.Sprintf("endpoint %s %s is not implemented", r.Method, r.URL.Path),
	))
}

// Handler returns the root http.Handler with global middleware applied.
// Middleware order (outside-in):
//
//	cors → recover → logging → mux
//
// Recover must sit outside logging so a panic during logging doesn't leak.
func (s *Server) Handler() http.Handler {
	return s.corsMiddleware(s.recoverMiddleware(s.loggingMiddleware(s.bodyLimitMiddleware(s.mux))))
}
