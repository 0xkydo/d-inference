package api

// Provider WebSocket management for the Darkbloom coordinator.
//
// This file handles the provider side of the coordinator: WebSocket connections,
// provider registration, attestation verification, challenge-response loops,
// and inference request/response relay.
//
// Provider lifecycle:
//   1. Provider connects via WebSocket to /ws/provider
//   2. Provider sends a Register message with hardware info, models, and attestation
//   3. Coordinator verifies attestation (Secure Enclave P-256 signature)
//   4. Coordinator starts periodic challenge-response loop to verify liveness
//   5. Coordinator routes inference requests to the provider via WebSocket
//   6. Provider streams response chunks back through the WebSocket
//   7. Coordinator relays chunks to the waiting consumer HTTP handler
//
// Attestation trust levels:
//   - none: No attestation provided (Open Mode, still accepted)
//   - self_signed: Attestation signed by provider's own Secure Enclave key
//   - hardware: MDA certificate chain verified against Apple Root CA (future)

import (
	"context"
	"encoding/json"

	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/saferun"
	"github.com/google/uuid"
	"nhooyr.io/websocket"
)

// handleProviderWS upgrades the connection to WebSocket and manages the
// provider's lifecycle: registration, heartbeats, and inference responses.
func (s *Server) handleProviderWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Allow any origin for provider connections.
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.logger.Error("websocket accept failed", "error", err)
		return
	}

	// Raise the read limit to 10 MB. The default 32 KB is too small for
	// large inference responses.
	conn.SetReadLimit(10 * 1024 * 1024)

	providerID := uuid.New().String()
	s.logger.Info("provider websocket connected", "provider_id", providerID, "remote", r.RemoteAddr)

	// Run the read loop; on return the provider is disconnected.
	s.providerReadLoop(r.Context(), conn, providerID, r)
}

// maxProviderVersionLength bounds the provider-reported binary version accepted
// at registration. The Swift provider sends the compile-time constant
// ProviderCore.version ("0.8.15"; release tags must equal it, dev builds are
// published with the same exact-version contract), and the longest shape the
// coordinator has ever handled is "0.8.15-rc.1+build" (17 bytes). 128 leaves
// an order of magnitude of margin while keeping provider-controlled bytes out
// of the registry's parse memos, metric tags and logs. Raising this must be
// paired with the registry's memo bound (maxMemoizedVersionLen), which stops
// caching above 64 bytes.
const maxProviderVersionLength = 128

// sessionDisconnectReason maps a provider read-loop exit to the disconnect
// reason recorded on its provider_sessions row. Kept to a small, fixed
// vocabulary so the column stays aggregatable:
//   - "oom_suspected"   — abrupt drop under memory pressure with in-flight work
//     (same classification as the provider.oom_suspected metric);
//   - "ws_close_<code>" — the peer sent a WebSocket close frame (1000 = normal
//     shutdown, 1001 = going away, 1006/close codes from intermediaries, ...);
//   - "read_error"      — the socket died without a close frame (TCP reset,
//     NAT/LB teardown, machine went to sleep mid-write);
//   - "read_error_control_frame" — nhooyr failed while handling a peer
//     control frame (see readErrorDisconnectReason).
//
// readReason is the frame-less classification from readErrorDisconnectReason
// and is used only when neither stronger signal applies.
//
// The registry's own generic "disconnect" remains the reason for closes the
// read loop did NOT observe first — in practice the stale-eviction sweep —
// so post-fix, lingering "disconnect" rows ≈ silent drops reaped by eviction.
func sessionDisconnectReason(closeStatus websocket.StatusCode, oomSuspected bool, readReason string) string {
	switch {
	case oomSuspected:
		return string(registry.DisconnectReasonOOMSuspected)
	case closeStatus != -1:
		return "ws_close_" + strconv.Itoa(int(closeStatus))
	default:
		return readReason
	}
}

const (
	readErrorReasonGeneric      = "read_error"
	readErrorReasonControlFrame = "read_error_control_frame"
)

// readErrorDisconnectReason classifies a frame-less provider Read failure
// into a fixed two-value vocabulary shared by the ws_disconnects metric, the
// telemetry event, and the provider_sessions disconnect_reason column.
//
// nhooyr answers peer pings on its READ goroutine, with a 5s budget to take
// the connection's per-frame write lock and put the pong on the wire. When
// that fails, the library fails the Read with "failed to handle control frame
// opPing: failed to write control frame opPong: failed to acquire lock: ..."
// — the peer was alive (it just pinged us), so this is a
// coordinator-side write stall, not a network drop, and must not be counted
// with real drops. The same prefix covers any other control-frame handling
// failure (e.g. a malformed control frame); close frames are never wrapped
// this way (they surface as a CloseError on the peer_close branch).
func readErrorDisconnectReason(err error) string {
	if err != nil && strings.Contains(err.Error(), "failed to handle control frame") {
		return readErrorReasonControlFrame
	}
	return readErrorReasonGeneric
}

// closeSessionWithReason closes this connection's provider_sessions row with a
// specific disconnect reason. Synchronous with a short timeout: it must land
// before the deferred registry.Disconnect issues its generic "disconnect"
// close (first close wins in the store), and a bounded wait means a stalled DB
// delays only this connection's teardown by at most the timeout — the store's
// upsert semantics make the registry's later write a safe fallback if this one
// times out. The caller marks the provider StatusOffline before calling, so
// the wait is never routing-critical: the dead provider cannot be selected
// while the write is in flight.
func (s *Server) closeSessionWithReason(providerID, reason string) {
	if s.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.store.CloseProviderSession(ctx, providerID, reason, time.Now()); err != nil {
		s.logger.Warn("failed to close provider session with reason",
			"provider_id", providerID, "reason", reason, "error", err)
	}
}

// providerReadLoop reads messages from the provider WebSocket and dispatches
// them. It runs until the connection closes or the context is cancelled.
func (s *Server) providerReadLoop(ctx context.Context, conn *websocket.Conn, providerID string, r *http.Request) {
	var provider *registry.Provider
	tracker := newChallengeTracker()
	var schedulerSEKey string
	var schedulerGeneration uint64

	// peerCloseStatus is the close code the peer sent, captured by the read
	// loop so the deferred teardown can flush pending requests with the
	// health-neutral restart cause on a graceful 1000/1001 close and the
	// striking abrupt cause otherwise (registry.ClassifyPeerClose). -1 = no
	// close frame observed.
	peerCloseStatus := websocket.StatusCode(-1)
	// Cancel context for cleanup of the challenge loop goroutine.
	loopCtx, loopCancel := context.WithCancel(ctx)
	defer func() {
		loopCancel()
		if s.mdmScheduler != nil {
			s.mdmScheduler.Unbind(schedulerSEKey, schedulerGeneration)
		}
		if s.codeAttestThrottle != nil {
			s.codeAttestThrottle.clearResumeChallenges(providerID)
		}
		// End connection-continuity coverage with the EXACT coordinator-
		// observed disconnect time (before registry.Disconnect tears the
		// provider down), so the measured reconnect gap starts here rather
		// than at the last periodic coverage pass.
		s.stopTrustCoverageForProvider(providerID)
		s.registry.DisconnectWithReason(providerID, registry.ClassifyPeerClose(peerCloseStatus, false))
		conn.Close(websocket.StatusNormalClosure, "goodbye")
	}()

	for {
		_, data, err := conn.Read(loopCtx)
		if err != nil {
			closeStatus := websocket.CloseStatus(err)
			oomSuspected := false
			readReason := readErrorReasonGeneric
			if closeStatus != -1 {
				peerCloseStatus = closeStatus
				s.logger.Info("provider websocket closed",
					"provider_id", providerID, "close_code", int(closeStatus))
				// Peer-initiated closes were previously unmetered — only
				// read_error incremented ws_disconnects_total — so dashboards
				// could not split graceful closes (update/shutdown) from drops.
				if s.metrics != nil {
					s.metrics.IncCounter("ws_disconnects_total",
						MetricLabel{"reason", "peer_close"},
					)
				}
				s.ddIncr("ws.disconnects", []string{
					"reason:peer_close",
					"code:" + strconv.Itoa(int(closeStatus)),
				})
			} else {
				readReason = readErrorDisconnectReason(err)
				s.logger.Error("provider websocket read error",
					"provider_id", providerID, "error", err, "reason", readReason)
				s.emit(context.Background(), protocol.SeverityWarn, protocol.KindConnectivity,
					"provider websocket read error",
					map[string]any{
						"provider_id": providerID,
						"ws_state":    "read_error",
						"reason":      readReason,
						"last_error":  err.Error(),
					})
				if s.metrics != nil {
					s.metrics.IncCounter("ws_disconnects_total",
						MetricLabel{"reason", readReason},
					)
				}
				s.ddIncr("ws.disconnects", []string{"reason:" + readReason})

				// An abrupt read_error under high last-known memory pressure with
				// active inference is very likely a jetsam OOM (the kill leaves no
				// other trace). Require in-flight > 0: a graceful shutdown/update
				// drains first (and may surface here as a frame-less EOF rather
				// than a clean close), so gating on in-flight avoids misreading a
				// drained going-away close as OOM. Idle-box kills are recovered by
				// the provider's crash-log scrape instead.
				if provider != nil {
					memPressure, inFlight := provider.DisconnectDiagnostics()
					if inFlight > 0 && registry.ClassifyDisconnectReason(true, memPressure, inFlight) == registry.DisconnectReasonOOMSuspected {
						oomSuspected = true
						if s.metrics != nil {
							s.metrics.IncCounter("provider_oom_suspected_total")
						}
						s.ddIncr("provider.oom_suspected", nil)
						s.emit(context.Background(), protocol.SeverityError, protocol.KindOOM,
							"provider disconnected under memory pressure (suspected OOM)",
							map[string]any{
								"provider_id":     providerID,
								"memory_pressure": memPressure,
								"in_flight":       inFlight,
							})
					}
				}
			}

			// Stamp this connection's session row with the observed socket
			// outcome. Every registry.Disconnect path writes the catch-all
			// "disconnect", which made 97% of provider_sessions rows carry a
			// single indistinguishable reason (2026-07-03 churn analysis). The
			// stamp is written synchronously BEFORE the deferred
			// registry.Disconnect so the store's first-close-wins semantics keep
			// the specific reason; the registry's later generic close becomes a
			// no-op. Skipped when:
			//   - provider == nil: never registered, so no session row exists
			//     (writing would fabricate a zero-duration row);
			//   - ctx.Err() != nil: coordinator shutdown — the next instance's
			//     startup reconcile labels these "coordinator_restart";
			//   - the registry no longer has the provider: registry.Disconnect
			//     already ran (stale eviction, duplicate-serial kick) and owns
			//     the reason for that path.
			if provider != nil && ctx.Err() == nil && s.registry.GetProvider(providerID) != nil {
				// The socket is dead, but the deferred registry.Disconnect
				// only runs after the stamp lands (first close wins requires
				// that order). Flip the provider offline first — StatusOffline
				// fails every routing-eligibility gate — so a slow store write
				// can never leave a dead provider selectable. Untrusted stays
				// untrusted: it is equally unroutable, and overwriting it would
				// make Disconnect's status-gated online/model decrements run a
				// second time after markUntrusted already decremented.
				provider.Mu().Lock()
				if provider.Status != registry.StatusUntrusted {
					provider.Status = registry.StatusOffline
				}
				provider.Mu().Unlock()
				s.closeSessionWithReason(providerID, sessionDisconnectReason(closeStatus, oomSuspected, readReason))
			}
			return
		}

		var msg protocol.ProviderMessage
		// DecodeProviderMessage is json.Unmarshal minus its redundant outer
		// validation pass; per-token chunk frames take a hand-written decoder.
		if err := protocol.DecodeProviderMessage(data, &msg); err != nil {
			// Decoder errors may quote provider-controlled fields (notably an
			// unknown message type). Never reflect the detail into logs.
			s.logger.Warn("invalid provider message", "provider_id", providerID)
			continue
		}

		switch msg.Type {
		case protocol.TypeRegister:
			if provider != nil {
				s.logger.Warn("rejecting second register on provider connection",
					"provider_id", providerID)
				_ = conn.Close(websocket.StatusPolicyViolation, "provider already registered")
				return
			}
			regMsg := msg.Payload.(*protocol.RegisterMessage)
			// The version string is provider-controlled and flows into semver
			// parsing memos, metric tags and logs; a legitimate build id is a
			// few dozen bytes. Reject anything larger before it reaches the
			// registry so a hostile client cannot retain multi-MiB keys.
			if len(regMsg.Version) > maxProviderVersionLength {
				s.logger.Warn("rejecting provider registration with oversized version",
					"provider_id", providerID, "version_len", len(regMsg.Version))
				s.ddIncr("providers.registration_rejected", []string{"reason:oversized_version"})
				_ = conn.Close(websocket.StatusPolicyViolation, "version string too long")
				return
			}
			if err := s.registry.ValidatePrefixCacheRegistration(regMsg); err != nil {
				// Validation errors can quote provider-controlled model IDs.
				s.logger.Warn("rejecting malformed provider cache capabilities",
					"provider_id", providerID)
				s.ddIncr("routing.cache_capability_rejected", []string{"source:register"})
				_ = conn.Close(websocket.StatusPolicyViolation, "invalid prefix-cache capabilities")
				return
			}
			provider = s.registry.Register(providerID, conn, regMsg)
			s.attachProviderLocation(providerID, provider, r)
			s.verifyProviderAttestation(providerID, provider, regMsg)

			// Record registration outcome metrics + telemetry.
			if s.metrics != nil {
				s.metrics.IncCounter("provider_registrations_total",
					MetricLabel{"trust_level", string(provider.TrustLevel)},
				)
			}
			s.ddIncr("providers.registrations", []string{"trust_level:" + string(provider.TrustLevel)})
			s.emit(context.Background(), protocol.SeverityInfo, protocol.KindLog,
				"provider registered",
				map[string]any{
					"provider_id":   providerID,
					"trust_level":   string(provider.TrustLevel),
					"hardware_chip": regMsg.Hardware.ChipName,
					"memory_gb":     regMsg.Hardware.MemoryGB,
				})

			// Resolve auth token → account linkage.
			if regMsg.AuthToken != "" {
				pt, err := s.store.GetProviderToken(regMsg.AuthToken)
				if err != nil {
					s.logger.Warn("provider auth token invalid",
						"provider_id", providerID,
						"error", err,
					)
				} else {
					provider.Mu().Lock()
					provider.AccountID = pt.AccountID
					provider.Mu().Unlock()
					// Account linkage can be the provider's ONLY stable identity
					// (Open Mode / invalid attestation → the acct: fallback), and
					// it lands after the attestation-time bind — re-bind so fault
					// state keys by identity instead of the session UUID.
					provider.RebindStableFaultKey()
					s.logger.Info("provider linked to account",
						"provider_id", providerID,
						"account_id", pt.AccountID,
						"token_label", pt.Label,
					)
				}
			}

			// Store provider version. SetVersion also runs the version-changed
			// reconnect reset for the session's stable identity, which the
			// attestation bind above could not (the version was not stored
			// yet) — see registry/version_reset.go.
			if regMsg.Version != "" {
				provider.SetVersion(regMsg.Version)
			}

			// Verify runtime integrity against the known-good manifest. Swift
			// providers omit Python/vllm hashes, but they still report external
			// runtime assets such as mlx.metallib under template_hashes.
			if s.knownRuntimeManifest != nil {
				runtimeOK, mismatches := s.verifyRuntimeHashesForBackend(
					regMsg.Backend, regMsg.PythonHash, regMsg.RuntimeHash, regMsg.TemplateHashes)
				provider.Mu().Lock()
				provider.RuntimeVerified = runtimeOK
				provider.RuntimeManifestChecked = runtimeOK
				provider.MetallibVerified = runtimeOK &&
					runtimeManifestApprovesMetallib(
						s.knownRuntimeManifest, regMsg.TemplateHashes)
				if !runtimeOK || !provider.MetallibVerified {
					provider.RuntimeCapabilities = nil
					provider.FreshCodeAttested = false
				}
				provider.PythonHash = regMsg.PythonHash
				provider.RuntimeHash = regMsg.RuntimeHash
				provider.TemplateHashes = registry.CloneStringMap(regMsg.TemplateHashes)
				provider.Mu().Unlock()

				if !runtimeOK {
					// Send runtime status feedback only on mismatch so the
					// provider can self-heal. Skip the message when everything
					// matches — it would only add noise on the WebSocket.
					statusMsg := protocol.RuntimeStatusMessage{
						Type:       protocol.TypeRuntimeStatus,
						Verified:   false,
						Mismatches: mismatches,
					}
					statusData, err := json.Marshal(statusMsg)
					if err == nil {
						if err := provider.EnqueueText(loopCtx, statusData); err != nil {
							s.logger.Debug("failed to enqueue runtime status to provider", "provider_id", provider.ID, "error", err)
							s.ddIncr("provider.enqueue_failed", []string{"msg:runtime_status"})
						}
					}
					s.logger.Warn("provider runtime integrity mismatch — excluded from routing",
						"provider_id", providerID,
						"mismatches", len(mismatches),
					)
				} else {
					s.logger.Info("provider runtime integrity verified",
						"provider_id", providerID,
						"python_hash", regMsg.PythonHash,
						"runtime_hash", regMsg.RuntimeHash,
					)
				}
			} else {
				// No manifest configured — fail-closed for routing.
				provider.Mu().Lock()
				provider.RuntimeVerified = true
				provider.RuntimeManifestChecked = false
				provider.MetallibVerified = false
				provider.RuntimeCapabilities = nil
				provider.FreshCodeAttested = false
				provider.Mu().Unlock()
			}

			// Version cutoff check — runs AFTER runtime check so it takes precedence.
			// If version is below minimum, override RuntimeVerified to false.
			if s.minProviderVersion != "" && regMsg.Version != "" && semverLess(regMsg.Version, s.minProviderVersion) {
				s.logger.Warn("provider version below minimum — excluded from routing",
					"provider_id", providerID,
					"version", regMsg.Version,
					"min_version", s.minProviderVersion,
				)
				s.ddIncr("provider_version_below_minimum", []string{"gate:registration", "version:" + regMsg.Version})
				provider.Mu().Lock()
				provider.RuntimeVerified = false
				provider.RuntimeManifestChecked = false
				provider.MetallibVerified = false
				provider.RuntimeCapabilities = nil
				provider.FreshCodeAttested = false
				provider.Mu().Unlock()
			}

			if err := s.registry.ReconcileAttestedRuntimeCapabilities(providerID); err != nil {
				s.logger.Warn("provider attested runtime claims rejected",
					"provider_id", providerID,
					"reason", err.Error(),
				)
				s.registry.MarkUntrusted(providerID)
				_ = conn.Close(websocket.StatusPolicyViolation, "attested runtime claims mismatch")
				return
			}

			// Declaratively tell the provider the desired build per alias it
			// already serves, so a fresh/reconnected provider converges without a
			// separate catalog pull. Sent even when EMPTY: a provider that
			// reconnects (same process, prefetch state intact) after the alias it
			// was converging to was deleted/repointed must learn that nothing is
			// desired anymore, or its in-flight prefetch would hard-swap anyway.
			// Gated on Swift backend + feature version: a pre-feature provider's
			// strict decoder throws on unknown types.
			if s.providerSupportsDesiredModels(regMsg.Backend, regMsg.Version) {
				if err := s.registry.SendDesiredModels(providerID, s.registry.DesiredModelsForProvider(providerID)); err != nil {
					s.logger.Warn("failed to send desired_models after register",
						"provider_id", providerID, "error", err)
				}
			}

			// Submit stable device work to the Server-owned bounded scheduler. No
			// goroutine is created for this provider.
			if s.mdmScheduler != nil {
				if ar := provider.GetAttestationResult(); ar != nil && ar.Valid {
					priority := s.verificationSubmitPriority(ar.PublicKey, ar.SerialNumber)
					schedulerSEKey = ar.PublicKey
					schedulerGeneration = s.mdmScheduler.Submit(loopCtx, providerID, provider, priority)
				}
			}
			// Start challenge loop after registration
			saferun.Go(s.logger, "challengeLoop", func() {
				s.challengeLoop(loopCtx, providerID, provider, tracker)
			})

			// v0.6.0: APNs code-identity attestation. Runs only when an attestor is
			// configured; otherwise the provider simply never becomes CodeAttested
			// (fail-closed at the routing chokepoint once enforcement begins). The
			// code-identity proof and the SIP/liveness pillar compose at the routing
			// gate (providerSupportsPrivateTextLocked requires both). The loop pushes
			// (within the per-device budget) and polls; verification of the reply
			// happens in the read-loop delivery path (handleCodeAttestationResponse),
			// so a single dropped/late background push doesn't strand a capable
			// provider, and a reply on a reconnected socket still attests (Fix 1).
			if s.codeAttestor != nil {
				saferun.Go(s.logger, "codeAttest", func() {
					s.codeAttestLoop(loopCtx, providerID, provider)
				})
			}

		case protocol.TypeHeartbeat:
			if provider == nil {
				// Heartbeats are meaningful only after this connection has
				// registered. Reject the protocol violation before touching any
				// provider snapshot or other per-registration state.
				s.logger.Warn("heartbeat from unregistered provider", "provider_id", providerID)
				_ = conn.Close(websocket.StatusPolicyViolation, "register before heartbeat")
				return
			}
			hbMsg := msg.Payload.(*protocol.HeartbeatMessage)
			replaceCacheCapabilities :=
				hbMsg.PrefixCacheProtocol != 0 || hbMsg.PrefixCacheV2Models != nil
			if replaceCacheCapabilities ||
				hbMsg.PrefixCacheStatuses != nil ||
				hbMsg.PrefixCacheDonationOutcomes != nil {
				var capabilities []protocol.PrefixCacheV2Capability
				if hbMsg.PrefixCacheV2Models != nil {
					capabilities = *hbMsg.PrefixCacheV2Models
				}
				_, err := s.registry.UpdatePrefixCacheSnapshot(
					providerID,
					replaceCacheCapabilities,
					hbMsg.PrefixCacheProtocol,
					capabilities,
					hbMsg.PrefixCacheStatuses,
					hbMsg.PrefixCacheDonationOutcomes,
				)
				if err != nil && replaceCacheCapabilities {
					s.logger.Warn("rejecting malformed heartbeat cache capabilities",
						"provider_id", providerID)
					s.ddIncr("routing.cache_capability_rejected", []string{"source:heartbeat"})
					// Malformed refreshes cannot leave stale v2 evidence live.
					_, _ = s.registry.UpdatePrefixCacheSnapshot(
						providerID,
						true,
						1,
						nil,
						hbMsg.PrefixCacheStatuses,
						hbMsg.PrefixCacheDonationOutcomes,
					)
				} else if err != nil {
					s.logger.Warn("failed to apply heartbeat cache telemetry",
						"provider_id", providerID)
					s.ddIncr("routing.cache_telemetry_rejected", []string{"source:heartbeat"})
				}
			}
			s.applyProviderHeartbeat(providerID, provider, hbMsg)
			// W5 Fix 2 (2a): a late/changed APNs token carried in the heartbeat
			// re-arms a code-identity challenge WITHOUT a reconnect.
			s.maybeRearmCodeAttest(loopCtx, providerID, provider, hbMsg)

		case protocol.TypeCapacityQuote:
			if provider == nil {
				// A quote answers a coordinator-sent probe, and probes are only
				// sent to registered providers — a quote on an unregistered
				// connection is a protocol violation, same posture as heartbeat.
				s.logger.Warn("capacity quote from unregistered provider",
					"provider_id", providerID)
				continue
			}
			quoteMsg := msg.Payload.(*protocol.CapacityQuoteMessage)
			// Correlation (quote_id → outstanding probe, provider binding,
			// window expiry) and plan confirm/demote all live registry-side
			// with the probe state; the read loop only delivers. Synchronous
			// like heartbeat ingest — no DB or lock-heavy work on this path.
			s.registry.HandleCapacityQuote(providerID, quoteMsg)

		case protocol.TypeInferenceAccepted:
			acceptMsg := msg.Payload.(*protocol.InferenceAcceptedMessage)
			s.handleInferenceAccepted(provider, acceptMsg)

		case protocol.TypeInferenceResponseChunk:
			chunkMsg := msg.Payload.(*protocol.InferenceResponseChunkMessage)
			s.handleChunk(providerID, provider, chunkMsg)

		case protocol.TypeInferenceComplete:
			completeMsg := msg.Payload.(*protocol.InferenceCompleteMessage)
			_, receivedAt := provider.MarkPendingCompletionIngressNow(completeMsg.RequestID)
			if receivedAt.IsZero() {
				receivedAt = time.Now()
			}
			// Run completion handling (billing settlement) off the read loop.
			// Billing does synchronous DB calls (GetModelPrice, Credit, Charge)
			// that can block for seconds under DB pressure. If the read loop is
			// blocked, attestation challenge responses can't be read from the
			// WebSocket, causing challenge timeouts and provider derouting.
			saferun.Go(s.logger, "handleComplete", func() {
				s.handleCompleteAt(providerID, provider, completeMsg, receivedAt)
			})

		case protocol.TypeInferenceError:
			errMsg := msg.Payload.(*protocol.InferenceErrorMessage)
			s.handleInferenceError(providerID, provider, errMsg)

		case protocol.TypePrefixCacheLookup:
			lookupMsg := msg.Payload.(*protocol.PrefixCacheLookupMessage)
			if s.registry.ApplyPrefixCacheLookup(providerID, lookupMsg) {
				s.ddIncr("routing.cache_lookup_receipt", []string{"outcome:" + lookupMsg.Outcome, "tier:" + lowCardinalityCacheTier(lookupMsg.Tier)})
				s.emitExactCacheSSDLookup("v1", lookupMsg.Outcome, lookupMsg.StageMs)
			} else {
				s.ddIncr("routing.cache_receipt_rejected", []string{"type:lookup"})
			}

		case protocol.TypePrefixCacheReady:
			readyMsg := msg.Payload.(*protocol.PrefixCacheReadyMessage)
			if s.registry.ApplyPrefixCacheReady(providerID, readyMsg) {
				s.ddIncr("routing.cache_ready_receipt", []string{"tier:" + lowCardinalityCacheTier(readyMsg.Tier)})
				s.emitExactCacheSSDDonation("v1", readyMsg.StageMs, readyMsg.ReadyTokens)
			} else {
				s.ddIncr("routing.cache_receipt_rejected", []string{"type:ready"})
			}

		case protocol.TypePrefixCacheLookupV2:
			lookupMsg := msg.Payload.(*protocol.PrefixCacheLookupV2Message)
			if s.registry.ApplyPrefixCacheLookupV2(providerID, lookupMsg) {
				s.ddIncr("routing.cache_lookup_receipt", []string{
					"protocol:v2",
					"outcome:" + lookupMsg.Outcome,
					"tier:" + lowCardinalityCacheTier(lookupMsg.Tier),
				})
				s.emitExactCacheSSDLookup("v2", lookupMsg.Outcome, lookupMsg.StageMs)
			} else {
				s.ddIncr("routing.cache_receipt_rejected", []string{"type:lookup_v2"})
			}

		case protocol.TypePrefixCacheReadyV2:
			readyMsg := msg.Payload.(*protocol.PrefixCacheReadyV2Message)
			if s.registry.ApplyPrefixCacheReadyV2(providerID, readyMsg) {
				s.ddIncr("routing.cache_ready_receipt", []string{
					"protocol:v2",
					"tier:" + lowCardinalityCacheTier(readyMsg.Tier),
				})
				donatedTokens := 0
				if len(readyMsg.ReadyAnchors) > 0 {
					donatedTokens = readyMsg.ReadyAnchors[len(readyMsg.ReadyAnchors)-1].TokenCount
				}
				s.emitExactCacheSSDDonation("v2", readyMsg.StageMs, donatedTokens)
			} else {
				s.ddIncr("routing.cache_receipt_rejected", []string{"type:ready_v2"})
			}

		case protocol.TypeAttestationResponse:
			respMsg := msg.Payload.(*protocol.AttestationResponseMessage)
			s.handleAttestationResponse(providerID, provider, respMsg, tracker)

		case protocol.TypeCodeAttestationResponse:
			respMsg := msg.Payload.(*protocol.CodeAttestationResponseMessage)
			// Verify in the delivery path (Fix 1): a reply attests THIS live
			// connection even if the push round-trip outlived the pushing
			// goroutine or the original connection (reconnect).
			s.handleCodeAttestationResponse(providerID, provider, respMsg)

		case protocol.TypeLoadModelStatus:
			statusMsg := msg.Payload.(*protocol.LoadModelStatusMessage)
			if !validLoadModelStatus(statusMsg.Status) {
				// Both fields are provider-controlled until they pass the closed
				// status vocabulary and pending-command match below.
				s.logger.Warn("rejecting invalid load_model_status", "provider_id", providerID)
				s.ddIncr("provider.load_model_status_rejected", []string{"reason:invalid_status"})
				continue
			}
			if !s.registry.HasPendingModelLoad(providerID, statusMsg.ModelID) {
				s.logger.Warn("rejecting unsolicited load_model_status", "provider_id", providerID)
				s.ddIncr("provider.load_model_status_rejected", []string{"reason:no_pending_command"})
				continue
			}
			// The exact provider/model pair now names a live coordinator-issued
			// command, and Status is one of three fixed constants. Only canonical
			// values may cross into logs, metrics, or registry state.
			s.logger.Info("provider load_model_status",
				"provider_id", providerID,
				"model_id", statusMsg.ModelID,
				"status", statusMsg.Status,
			)
			switch statusMsg.Status {
			case protocol.LoadModelStatusSucceeded:
				// Mark the model warm on this provider BEFORE draining so
				// the scheduler sees it as a candidate. Without this, the
				// provider still looks cold until the next heartbeat.
				s.registry.MarkModelWarm(providerID, statusMsg.ModelID)
				duration := s.registry.ClearPendingModelLoad(providerID, statusMsg.ModelID)
				s.registry.RecordWarmPoolLoadResult(statusMsg.ModelID, true, duration)
				s.registry.DrainQueuedRequestsForModelWithReason(statusMsg.ModelID, registry.DrainTriggerLoad)
			case protocol.LoadModelStatusFailed:
				duration := s.registry.PendingModelLoadDuration(providerID, statusMsg.ModelID)
				s.registry.RecordWarmPoolLoadResult(statusMsg.ModelID, false, duration)
				// Quantify WHY proactive loads are rejected. The reason
				// is derived only from the existing error string (no new wire
				// field). The proactive path's string is often a generic
				// Foundation bridge ("other"), but dashboards still get the
				// draining vs descriptive classes, and the short backoff below
				// does NOT depend on this classification.
				reason := classifyLoadFailure(statusMsg.Error)
				s.ddIncr("routing.load_model_rejects", []string{
					"model:" + statusMsg.ModelID,
					"reason:" + reason,
				})
				switch {
				case statusMsg.Error == protocol.ProviderDrainingForUpdate:
					// Transient: the provider refused only because it is
					// draining ahead of an auto-update restart. Shorten the
					// cooldown so a failed restart (provider resumes serving)
					// becomes loadable again quickly; queued requests are NOT
					// rejected — the provider is back within the queue window
					// and other providers remain plannable.
					s.registry.BackoffPendingModelLoadForDrain(providerID, statusMsg.ModelID)
					s.ddIncr("routing.pending_load_backoff", []string{
						"model:" + statusMsg.ModelID, "kind:drain",
					})
				case loadFailureIsPermanent(reason):
					// Permanent: the provider does not have this model, so a
					// fast retry just re-fails. Keep the full TTL cooldown set
					// when the load was planned (do NOT apply the short memory
					// backoff) so TriggerModelSwaps does not re-attempt the
					// unservable load every ~30s within the 120s queue window.
					// Still reject queued waiters that nothing can serve.
					s.registry.RejectUnservableQueuedRequests(statusMsg.ModelID)
				default:
					// A non-draining, non-permanent load failure is dominated by
					// transient memory pressure that frees in seconds. Re-stamp
					// the pending entry to the short memory backoff (~30s)
					// instead of leaving the full 2-min TTL — that window ≈ the
					// 120s queue timeout, so a request queued right after the
					// failure would time out before this provider (whose memory
					// may already have freed) is reconsidered by
					// TriggerModelSwaps. The ~10s warm-pool sweep reaps the short
					// entry deterministically.
					s.registry.BackoffPendingModelLoadForMemory(providerID, statusMsg.ModelID)
					s.ddIncr("routing.pending_load_backoff", []string{
						"model:" + statusMsg.ModelID, "kind:memory",
					})
					// If no other provider can serve this model, reject queued
					// requests immediately rather than making them wait 120s.
					s.registry.RejectUnservableQueuedRequests(statusMsg.ModelID)
				}
			}
			// "started" status: no action — load is in progress.

		case protocol.TypeModelsUpdate:
			updateMsg := msg.Payload.(*protocol.ModelsUpdateMessage)
			s.handleModelsUpdate(providerID, provider, updateMsg)

		case protocol.TypePrefetchModelStatus:
			// This frame is advisory progress for a provider-autonomous download;
			// it has no coordinator-issued pending-command identity and no state
			// effect. Ignore it entirely. A later catalog-validated models_update
			// remains the authoritative servability signal.
			continue

		default:
			// Provider message types are untrusted strings until explicitly handled.
			s.logger.Warn("unhandled provider message type", "provider_id", providerID)
		}
	}
}

func validLoadModelStatus(status string) bool {
	switch status {
	case protocol.LoadModelStatusStarted,
		protocol.LoadModelStatusSucceeded,
		protocol.LoadModelStatusFailed:
		return true
	default:
		return false
	}
}

// handleModelsUpdate merges a provider's authoritative model inventory update
// (sent after a verified prefetch) into its advertised models in place. Each
// build's weight hash is cross-checked against the catalog before it becomes
// routable, so a bad/buggy prefetch never takes traffic. This closes the loop
// without waiting for the provider to reconnect or resetting trust/reputation.
func (s *Server) handleModelsUpdate(providerID string, provider *registry.Provider, msg *protocol.ModelsUpdateMessage) {
	merged, dropped := s.registry.MergeProviderModelsWithCapabilities(
		providerID,
		msg.Models,
		msg.ToolConstraintProtocol,
		msg.ToolConstraintModels,
	)
	for _, id := range merged {
		s.logger.Info("provider now advertises build (models_update)",
			"provider_id", providerID, "model_id", id)
		// Release any requests queued for this build now that a provider can
		// (cold-)serve it.
		s.registry.DrainQueuedRequestsForModel(id)
	}
	for _, id := range dropped {
		s.logger.Info("provider stopped advertising build (models_update)",
			"provider_id", providerID, "model_id", id)
		// Requests may have queued against the concrete previous build while it
		// was still acceptable. Recheck immediately: drain to another provider if
		// one exists, otherwise fail fast instead of waiting for queue timeout.
		s.registry.DrainQueuedRequestsForModel(id)
		s.registry.RejectUnservableQueuedRequests(id)
	}
}

// attachProviderLocation resolves the provider's approximate geographic
// location from the registration HTTP request. The resolved location is
// stored on the Provider struct for stats aggregation. Raw IP addresses
// are never persisted.
func (s *Server) attachProviderLocation(providerID string, provider *registry.Provider, r *http.Request) {
	if s.geoResolver == nil || provider == nil || r == nil {
		return
	}
	loc := s.geoResolver.Lookup(r)
	if loc == nil {
		return
	}
	provider.Mu().Lock()
	provider.Location = loc
	provider.Mu().Unlock()
	s.registry.PersistProvider(provider)
	// The stats:v1 read-cache entry is owned by the stats refresher (stats.go)
	// and is NOT evicted here. Evicting it on every registration (~1,400/hour
	// in production) turned its 60 s TTL into ~2.6 s and made every /v1/stats
	// request rerun the multi-second usage analytics statements.
	s.logger.Info("provider location resolved",
		"provider_id", providerID,
		"city", loc.City,
		"country", loc.CountryCode,
		"source", loc.Source,
	)
}
