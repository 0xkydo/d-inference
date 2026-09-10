package api

// Request body limits, recovery, CORS, logging, and status writing.

import (
	"bufio"
	"context"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// maxMDMWebhookBodyBytes caps the MicroMDM webhook body. SecurityInfo /
// DevicePropertiesAttestation responses are a few KB; 1 MiB is generous headroom
// while preventing an unauthenticated caller from exhausting memory via an
// unbounded body.
const maxMDMWebhookBodyBytes = 1 << 20 // 1 MiB

// maxRequestBodyBytes is the global ceiling bodyLimitMiddleware applies to every
// request body so no endpoint can be OOM'd by an unbounded POST. It's a coarse
// outer bound that clears every legitimate body with headroom; the hot paths
// self-cap tighter on top (the plaintext-inference path at 16 MiB, sized to the
// provider WS frame budget — see maxInferenceBodyBytes).
const maxRequestBodyBytes = 64 << 20 // 64 MiB

// maxControlPlaneBodyBytes is the tight cap for small unauthenticated
// control-plane JSON (enroll, device token, admin auth) — far below the global
// ceiling so these exposed endpoints buffer at most a few KiB.
const maxControlPlaneBodyBytes = 64 << 10 // 64 KiB

// HandleMDMWebhook processes a MicroMDM webhook callback.
// Mount this on the webhook URL configured in MicroMDM.
//
// Defense layers (the endpoint is reachable but cannot forge trust):
//  1. Body cap — bounds memory for the unauthenticated path.
//  2. Optional shared secret — when configured, rejects callers without it
//     before reading the body.
//  3. Solicited-command gate (in mdm.Client.HandleWebhook) — only responses
//     whose CommandUUID matches a command the coordinator actually issued are
//     acted on, so a forged SecurityInfo can never drive a trust upgrade.
func (s *Server) HandleMDMWebhook(w http.ResponseWriter, r *http.Request) {
	if s.mdmWebhookSecret != "" && !s.mdmWebhookTokenValid(r) {
		s.logger.Warn("mdm webhook rejected: missing/invalid shared secret", "remote_addr", r.RemoteAddr)
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxMDMWebhookBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s.logger.Debug("mdm webhook received", "body_size", len(body), "body_preview", string(body[:min(len(body), 500)]))
	if s.mdmClient != nil {
		s.mdmClient.HandleWebhook(body)
	}
	w.WriteHeader(http.StatusOK)
}

// mdmWebhookTokenValid reports whether the request carries the configured MDM
// webhook secret, via either the X-Webhook-Token header or a ?token= query
// param. Comparison is constant-time. Only called when a secret is configured.
func (s *Server) mdmWebhookTokenValid(r *http.Request) bool {
	token := r.Header.Get("X-Webhook-Token")
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	return token != "" &&
		subtle.ConstantTimeCompare([]byte(token), []byte(s.mdmWebhookSecret)) == 1
}

// bodyLimitMiddleware caps every request body at maxRequestBodyBytes so an
// unbounded POST can't OOM the coordinator (the trusted TEE component).
// Per-handler MaxBytesReader caps (tighter) layer on top. The provider
// WebSocket upgrade is exempt: it hijacks the connection and reads framed
// messages (bounded separately), not r.Body.
func (s *Server) bodyLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.URL.Path != "/ws/provider" {
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

// decodeCappedJSON JSON-decodes the request body under a hard size cap, writing
// a 413 (too large) or 400 (bad JSON) and returning false on failure. For small
// unauthenticated control-plane endpoints that must not buffer an unbounded body.
func decodeCappedJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge,
				errorResponse("invalid_request_error", "request body too large"))
			return false
		}
		writeJSON(w, http.StatusBadRequest,
			errorResponse("invalid_request_error", "invalid JSON"))
		return false
	}
	return true
}

// recoverMiddleware catches panics in any handler, emits a telemetry event
// with the stack trace, and returns 500 to the client. Without this, a single
// nil deref takes down the whole coordinator — panics from tests have hit us
// in production more than once.
func (s *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if recErr, ok := rec.(error); ok && errors.Is(recErr, http.ErrAbortHandler) {
					panic(rec)
				}
				stack := string(debug.Stack())
				s.logger.Error("panic in HTTP handler",
					"error", fmt.Sprintf("%v", rec),
					"path", r.URL.Path,
					"method", r.Method,
					"stack", stack,
				)
				s.emitPanic(r.Context(),
					fmt.Sprintf("panic in handler %s %s: %v", r.Method, r.URL.Path, rec),
					stack,
					map[string]any{
						"handler":  r.URL.Path,
						"endpoint": r.URL.Path,
					},
				)
				// Write a 500 if the response hasn't started yet. If the
				// handler already flushed headers (e.g. streaming SSE), we
				// can't do anything useful — the client will see the stream
				// truncated.
				defer func() { _ = recover() }() // guard against double-write
				writeJSON(w, http.StatusInternalServerError, errorResponse("internal_error", "internal server error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// publicCORSPaths are endpoints whose GET is unauthenticated, read-only public
// data. Their GET is served with a wildcard CORS origin so the marketing site
// (darkbloom.dev) and any third party can read them from the browser. NOTE:
// some of these paths (e.g. /v1/pricing) ALSO serve authenticated PUT/DELETE —
// the wildcard applies only to GET; non-GET methods fall through to the
// credentialed, single-origin CORS below.
var publicCORSPaths = map[string]bool{
	"/v1/models/catalog": true,
	"/v1/pricing":        true,
	"/v1/stats":          true,
	"/v1/network/series": true,
}

// corsMiddleware sets CORS headers. Authenticated/credentialed requests are
// locked to a single origin derived from the CORS_ORIGIN environment variable
// (defaulting to the production console domain); a wildcard is never used for
// those. A GET to a public read-only endpoint (see publicCORSPaths) is readable
// from any origin, without credentials, so a wildcard is safe and intended.
func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	origin := s.corsOrigin
	if origin == "" {
		origin = "https://console.darkbloom.dev"
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Resolve the effective method: for a preflight, the actual request
		// method is in Access-Control-Request-Method (default GET if absent).
		effectiveMethod := r.Method
		if r.Method == http.MethodOptions {
			if reqMethod := r.Header.Get("Access-Control-Request-Method"); reqMethod != "" {
				effectiveMethod = reqMethod
			} else {
				effectiveMethod = http.MethodGet
			}
		}

		if publicCORSPaths[r.URL.Path] && effectiveMethod == http.MethodGet {
			// Public, non-credentialed GET — any origin may read it.
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Vary", "Origin")
		} else {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, "+metadataDetailsHeader)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// loggingMiddleware logs each request using slog and updates HTTP metrics.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

		// Generate (or honor) a request_id and stash it in context +
		// response headers so logs and the client can correlate.
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = newRequestID()
		}
		w.Header().Set("X-Request-ID", reqID)
		ctx := context.WithValue(r.Context(), ctxKeyRequestID, reqID)
		// Profiler correlation id is ALWAYS coordinator-minted (the client-supplied
		// X-Request-ID above is echoed and logged but never persisted).
		if s.profilerEnabled() {
			meta := &requestMeta{coordID: reqID, start: start}
			if r.Header.Get("X-Request-ID") != "" {
				meta.coordID = newRequestID()
			}
			ctx = context.WithValue(ctx, requestMetaKey{}, meta)
		}
		r = r.WithContext(ctx)

		next.ServeHTTP(sw, r)

		dur := time.Since(start)

		// Resolve the route pattern that matched (Go 1.22+ method+path).
		// Falls back to URL.Path when no pattern matched (404).
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}

		// User correlation: if requireAuth attached an account, include
		// it in the access log. Empty for unauthenticated paths.
		userID := consumerKeyFromContext(ctx)

		s.logger.Info("request",
			"request_id", reqID,
			"method", r.Method,
			"path", r.URL.Path,
			"route", route,
			"status", sw.status,
			"duration_ms", dur.Milliseconds(),
			"remote", r.RemoteAddr,
			"user_id", userID,
		)

		pathLabel := httpPathLabel(route)
		statusStr := strconvItoa(sw.status)

		if s.metrics != nil {
			s.metrics.IncCounter("http_requests_total",
				MetricLabel{"method", r.Method},
				MetricLabel{"path", pathLabel},
				MetricLabel{"status", statusStr},
			)
			s.metrics.ObserveHistogram("http_request_duration_ms",
				float64(dur.Milliseconds()),
				MetricLabel{"method", r.Method},
				MetricLabel{"path", pathLabel},
			)
		}

		// DogStatsD — emit request counter and latency histogram.
		if s.dd != nil {
			tags := []string{
				"method:" + r.Method,
				"path:" + pathLabel,
				"status_code:" + statusStr,
			}
			s.dd.Incr("http.requests", tags)
			s.dd.Histogram("http.latency_ms", float64(dur.Milliseconds()), tags)
		}
	})
}

// httpPathLabel returns a bounded label for HTTP metrics.
// We use the mux route pattern (e.g. "POST-/v1/chat/completions")
// instead of URL.Path so attacker-controlled unmatched paths cannot create
// unbounded metric cardinality. Dashes replace spaces so DogStatsD tags
// parse cleanly (spaces break tag parsing).
func httpPathLabel(route string) string {
	if route == "" {
		return "unmatched"
	}
	return strings.ReplaceAll(route, " ", "-")
}

// strconvItoa is a shim to avoid pulling strconv into every middleware file.
func strconvItoa(i int) string { return strconv.Itoa(i) }

// statusWriter wraps http.ResponseWriter to capture the status code
// for logging. It also implements http.Flusher and http.Hijacker by
// delegating to the underlying writer, which is required for SSE
// streaming and WebSocket upgrade respectively.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (sw *statusWriter) WriteHeader(code int) {
	if !sw.wroteHeader {
		sw.status = code
		sw.wroteHeader = true
	}
	sw.ResponseWriter.WriteHeader(code)
}

func (sw *statusWriter) Flush() {
	if f, ok := sw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack implements http.Hijacker by delegating to the underlying writer.
// This is required for WebSocket upgrade to work through middleware.
func (sw *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := sw.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, errors.New("underlying ResponseWriter does not implement http.Hijacker")
}

// Unwrap returns the underlying ResponseWriter, allowing the http package
// and websocket libraries to discover interfaces like http.Hijacker.
func (sw *statusWriter) Unwrap() http.ResponseWriter {
	return sw.ResponseWriter
}
