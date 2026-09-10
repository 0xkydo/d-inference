package api

// Telemetry emitters, Datadog helpers, and default gauge loop.

import (
	"context"
	_ "embed"
	"time"

	"github.com/eigeninference/d-inference/coordinator/datadog"
	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/saferun"
	"github.com/eigeninference/d-inference/coordinator/telemetry"
)

// submitTelemetry enqueues a best-effort telemetry write onto the non-blocking
// routing-telemetry sink. It never blocks the caller (the inference request
// path): when the sink's buffer is full the write is dropped and counted. name
// identifies the write for panic/drop diagnostics.
//
// Nil-safety: a Server constructed directly (e.g. &Server{} in tests, which
// never runs NewServer) has no sink. In that case it falls back to the previous
// behavior — a per-write panic-safe goroutine — so those tests keep working.
func (s *Server) submitTelemetry(name string, fn func()) {
	if s.routeTelemetry != nil {
		s.routeTelemetry.submit(fn)
		return
	}
	saferun.Go(s.logger, name, fn)
}

// SetEmitter wires the coordinator-side telemetry emitter. Call once at boot.
func (s *Server) SetEmitter(e *telemetry.Emitter) {
	s.emitter = e
}

// SetDatadog wires the Datadog client for DogStatsD metrics and Logs API forwarding.
func (s *Server) SetDatadog(dd *datadog.Client) {
	s.dd = dd
}

// Datadog returns the Datadog client (or nil). Exposed so main.go and the
// telemetry emitter can share the same client.
func (s *Server) Datadog() *datadog.Client {
	return s.dd
}

// Metrics returns the in-process metrics registry so cmd/coordinator can
// expose it to the telemetry emitter and other integrations.
func (s *Server) Metrics() *Metrics {
	return s.metrics
}

// emit is an internal convenience that funnels events through the emitter if
// one has been wired up. No-op otherwise — telemetry must never affect control
// flow.
func (s *Server) emit(ctx context.Context, severity protocol.TelemetrySeverity, kind protocol.TelemetryKind, message string, fields map[string]any) {
	if s.emitter == nil {
		return
	}
	s.emitter.Emit(telemetry.Event{
		Severity: severity,
		Kind:     kind,
		Message:  message,
		Fields:   fields,
	})
}

// emitRequest is like emit but preserves a request_id for correlation.
func (s *Server) emitRequest(ctx context.Context, severity protocol.TelemetrySeverity, requestID, message string, fields map[string]any) {
	if s.emitter == nil {
		return
	}
	s.emitter.Emit(telemetry.Event{
		Severity:  severity,
		Kind:      protocol.KindInferenceError,
		Message:   message,
		Fields:    fields,
		RequestID: requestID,
	})
}

// ddIncr increments a DogStatsD counter. No-op if DD is not configured.
func (s *Server) ddIncr(name string, tags []string) {
	if s.dd != nil {
		s.dd.Incr(name, tags)
	}
}

// ddCount increments a DogStatsD counter by the given value. No-op if DD is not configured.
func (s *Server) ddCount(name string, value int64, tags []string) {
	if s.dd != nil {
		s.dd.Count(name, value, tags)
	}
}

// ddHistogram records a DogStatsD histogram value. No-op if DD is not configured.
func (s *Server) ddHistogram(name string, value float64, tags []string) {
	if s.dd != nil {
		s.dd.Histogram(name, value, tags)
	}
}

// ddGauge sets a DogStatsD gauge value. No-op if DD is not configured.
func (s *Server) ddGauge(name string, value float64, tags []string) {
	if s.dd != nil {
		s.dd.Gauge(name, value, tags)
	}
}

func (s *Server) emitPanic(ctx context.Context, message, stack string, fields map[string]any) {
	if s.emitter == nil {
		return
	}
	s.emitter.Emit(telemetry.Event{
		Severity: protocol.SeverityFatal,
		Kind:     protocol.KindPanic,
		Message:  message,
		Fields:   fields,
		Stack:    stack,
	})
}

// registerDefaultGauges wires live-computed gauges (fleet size, etc.) into
// the metrics registry at construction time.
func (s *Server) registerDefaultGauges() {
	s.metrics.RegisterGauge("providers_online", func() float64 {
		return float64(s.registry.ProviderCount())
	})
	s.metrics.RegisterGauge("min_provider_version_set", func() float64 {
		if s.minProviderVersion != "" {
			return 1
		}
		return 0
	})
	s.registerExactCacheGauges()
}

// StartDDGaugeLoop periodically pushes gauge values to DogStatsD. Gauges
// are point-in-time values and must be pushed regularly (not on-demand like
// counters). Call as a goroutine; stops when ctx is cancelled.
func (s *Server) StartDDGaugeLoop(ctx context.Context) {
	if s.dd == nil {
		return
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.ddGauge("providers.online", float64(s.registry.OnlineCount()), nil)
			// APNs code-identity coverage — watch this climb during the grace
			// window before letting APNS_ENFORCE_AFTER pass.
			codeAttested, _ := s.registry.CodeAttestationCoverage()
			s.ddGauge("attestation.code_attested", float64(codeAttested), nil)
			enforced := 0.0
			if s.registry.CodeAttestationEnforced() {
				enforced = 1.0
			}
			s.ddGauge("attestation.code_enforced", enforced, nil)
			perModel := s.registry.ModelProviderSnapshot()
			for model, count := range perModel {
				s.ddGauge("providers.per_model", float64(count), []string{"model:" + model})
			}
			// Per-model queue depth/age (fleet_gauges.go).
			s.emitPerModelQueueGauges(perModel)
			for ver, count := range s.registry.ProviderCountByVersion() {
				s.ddGauge("providers.per_version", float64(count), []string{"version:" + ver})
			}
			// Trust-state cohort gauges — alert when self_signed/untrusted grows.
			for _, b := range s.registry.ProviderCountByTrustStatus() {
				s.ddGauge("providers.by_trust_status", float64(b.Count),
					[]string{"trust_level:" + b.TrustLevel, "status:" + b.Status})
			}
			// Stuck-cohort breakdown — distinguishes never-enrolled from
			// enrolled-but-SecurityInfo-timing-out so we know if the problem is
			// provider-side enrollment or APNs/MDM delivery.
			for reason, count := range s.registry.ProviderCountByMDMFailure() {
				s.ddGauge("providers.by_mdm_failure", float64(count), []string{"reason:" + reason})
			}
			if s.minProviderVersion != "" {
				s.ddGauge("coordinator.min_provider_version_set", 1, []string{"min_version:" + s.minProviderVersion})
			}
			if q := s.registry.Queue(); q != nil {
				s.ddGauge("request_queue.depth", float64(q.TotalSize()), nil)
			}
			s.emitExactCacheDDGauges()
			s.emitStoreCacheGauges()
			// Network utilization — demand/capacity across the warm-serving and
			// token-budget axes, plus a per-model breakdown.
			util := s.registry.NetworkUtilizationSnapshot()
			s.ddGauge("utilization.network", util.Utilization, nil)
			s.ddGauge("utilization.warm", util.WarmUtilization, nil)
			s.ddGauge("utilization.token_budget", util.TokenBudgetUtilization, nil)
			s.ddGauge("utilization.bottleneck", util.BottleneckUtilization, nil)
			s.ddGauge("capacity.tps", util.CapacityTPS, nil)
			s.ddGauge("capacity.demand_concurrency", util.DemandConcurrency, nil)
			s.ddGauge("capacity.serving_capacity", util.ServingCapacity, nil)
			s.ddGauge("capacity.spill_arrival_rate", util.SpillArrivalRate, nil)
			for _, m := range util.Models {
				s.ddGauge("utilization.model", m.Utilization, []string{"model:" + m.Model})
			}
		}
	}
}
