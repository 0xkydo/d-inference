package api

// Dispatch kill switches and queue TTFT policy.

import (
	"time"
)

// envTTFTTerminalReject is the kill switch for the terminal TTFT-rejection fix.
// A reservation that fails because every candidate exceeds the TTFT ceiling
// (errTTFTTooSlow) is DETERMINISTIC: it is computed from the same fleet-wide
// estimate on every scan, so re-running it within the same request cannot
// succeed. Default true: the dispatch ladder stops on the FIRST such rejection
// at ANY attempt and returns the same 429 the attempt-0 path always produced
// (prod: mid-ladder rejections previously looped to maxDispatchAttempts,
// re-running the doomed scan ~63x per request and writing a ttft_429 route row
// each time — 28% of inference_routes). Set =false to restore the legacy
// attempt-0-only fast path. Read live (not a Server field) following the
// cold_dispatch.go flag pattern, so it stays confined to this file and is
// overridable in tests via t.Setenv.
const envTTFTTerminalReject = "EIGENINFERENCE_TTFT_TERMINAL_REJECT"

// ttftTerminalRejectEnabled reports whether a TTFT-too-slow reservation
// rejection terminates the dispatch ladder on any attempt. Default true.
func ttftTerminalRejectEnabled() bool {
	return envEnabledDefaultTrue(envTTFTTerminalReject)
}

// envJinjaTerminalReject is the kill switch for the deterministic
// template-render rejection stop (E4, 2026-07-15 platform errors deep dive).
// A provider error_reason of jinja_channel_tags / jinja_null_bridge /
// jinja_template means the model's chat template could not render the
// request's tool schemas or message history — the same body renders the same
// way on every provider, so failing over is pure waste (prod: 1.57 dispatch
// rows per jinja request, observed up to 17 attempts, 0% eventual success).
// Default true: the ladder stops on the FIRST jinja_* rejection at any
// attempt and surfaces one 422 model_capability invalid_request_error. Set
// =false to restore the legacy fail-over-on-500 behavior. Read live (not a
// Server field) following the envTTFTTerminalReject pattern, so it stays
// confined to this file and is overridable in tests via t.Setenv.
const envJinjaTerminalReject = "EIGENINFERENCE_JINJA_TERMINAL_REJECT"

// jinjaTerminalRejectEnabled reports whether a jinja_* provider rejection
// terminates the dispatch ladder. Default true.
func jinjaTerminalRejectEnabled() bool {
	return envEnabledDefaultTrue(envJinjaTerminalReject)
}

// jinjaTerminalRejectMessage is the OpenAI-style error body surfaced for a
// latched template-render failure — a curated model_capability message
// instead of the provider's raw Jinja backtrace (which names filters and
// template internals no API consumer can act on).
const jinjaTerminalRejectMessage = "the request's tool schemas or message history cannot be rendered by this model's chat template; simplify the tool parameter schemas or message structure, or use a different model"

// queueMaxTTFTMs returns the TTFT ceiling for queued requests. Public routes
// inherit the prompt-scaled admission threshold; self-route / prefer-owner paths
// are not subject to the public SLA ceiling.
//
// When hardReject is false (the default soft gate), a zero ceiling is returned
// so the scheduler's enforceTTFT path is disabled: candidates over the estimated
// deadline are no longer dropped (and no errTTFTTooSlow is produced). The router
// still ranks by cost (which is TTFT-weighted), so the fastest provider wins, but
// a request is served on the best-available provider instead of being rejected
// on a pessimistic prefill estimate.
func queueMaxTTFTMs(policy selfRoutePolicy, deadline time.Duration, hardReject bool) float64 {
	if policy.enabled || policy.prefer {
		return 0
	}
	if !hardReject {
		return 0
	}
	return float64(deadline.Milliseconds())
}
