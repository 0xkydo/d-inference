package api

// Speculative first-chunk waiting, race arms, and committed response writing.

import (
	"net/http"
	"time"

	"github.com/eigeninference/d-inference/coordinator/protocol"
	"github.com/eigeninference/d-inference/coordinator/registry"
	"github.com/eigeninference/d-inference/coordinator/saferun"
)

// waitFirstChunk runs the speculative TTFT-aware first-chunk wait (the former
// `firstChunkWait` labeled loop). It holds preamble chunks, commits on first
// content, ignores AcceptedCh for race/timer decisions, may proceed to
// waitAccepted only for legacy preamble liveness, retries invisibly on provider
// error/timeout, and launches the speculative backup race when the primary is
// slow. Returns outcomeCommitted (content / clean close), outcomeAccepted
// (legacy preamble liveness — proceed to waitAccepted), outcomeRetry
// (advance to the next attempt), or outcomeClientGone (context cancelled, refunded).
func (d *dispatchState) waitFirstChunk() (outcome dispatchOutcome) {
	s := d.s
	r := d.r
	provider, pr := d.provider, d.pr
	captured := routingAttempt(provider, pr, pr.RequestID, pr.Attempt)

	defer func() {
		target := d.currentOrCapturedRoutingAttempt(captured)
		switch outcome {
		case outcomeCommitted:
			d.updateRoutingOutcomeForAttempt(target, d.successRoutingOutcomeFor(target.pending))
		case outcomeRetry:
			// A 504 here is a coordinator-synthesized first-chunk timeout
			// unless it carries a KNOWN typed 504 cause (safety_deadline /
			// backpressure_timeout) — those are real provider terminals and
			// keep their provider-error route class and attempt usage.
			// setLastError clears the cause for synthetic timeouts (so the
			// discriminator cannot go stale), and an UNKNOWN cause value
			// stays on this legacy timeout path, mirroring
			// classifyTerminalCause's unknown→legacy rule for mixed-version
			// rollouts.
			if d.lastErrCode == http.StatusGatewayTimeout && !isTypedTimeout504Cause(d.lastErrTerminalCause) {
				d.updateRoutingOutcomeForAttempt(target, d.errorRoutingOutcomeFor(target.pending, "timeout", "first_chunk_timeout", d.lastErrCode))
			} else {
				// Post-dispatch provider failure (incl. OOM/model-load): admitted but failed.
				d.updateRoutingOutcomeForAttempt(target, d.providerFailedRoutingOutcomeFor(target.pending))
			}
		case outcomeClientGone:
			d.emitClientGone(phaseBeforeFirstToken)
			d.updateRoutingOutcomeForAttempt(target, d.errorRoutingOutcomeFor(target.pending, "cancelled", "client_gone", 0))
		}
	}()

	deadlineWait := d.firstTokenWait(d.deadline)
	speculativeTimer := time.NewTimer(d.firstTokenSpeculativeWait())
	deadlineTimer := time.NewTimer(deadlineWait)
	// Routing v2 W2: the probe round may deliver ONE refined (strictly
	// earlier) absolute speculative launch instant. Read through a local so
	// the arm disarms itself after its single use; a nil channel (no probe
	// round) never fires.
	hedgeAdvance := d.hedgeAdvanceCh
	// preambleLiveness records that held boilerplate earned a legacy bounded
	// extension. AcceptedCh never earns or resets a content wait.
	// A preamble-then-stall with leftover budget is still bounded by
	// preambleContentTimeout so a role-then-stall zombie fails over.
	d.preambleLiveness = false

	for {
		select {
		case chunk, ok := <-pr.ChunkCh:
			if ok && holdPreContentBoilerplate(pr, chunk, &d.heldChunks) {
				if d.firstTokenSpeculativeWait() <= 0 {
					speculativeTimer.Stop()
					deadlineTimer.Stop()
					return d.runSpeculative()
				}
				continue
			}
			speculativeTimer.Stop()
			deadlineTimer.Stop()
			if ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
			} else {
				select {
				case errMsg := <-pr.ErrorCh:
					d.excludeProviders[provider.ID] = struct{}{}
					s.cancelDispatchAfterTerminal(provider, pr)
					d.setLastInferenceError(provider, errMsg)
					d.lastFailedVersion = failedProviderVersion(provider)
					d.noteDispatchRetry(provider, pr, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &d.heldChunks, errMsg.CoordinatorCause)
					d.provider = nil
					d.pr = nil
					return outcomeRetry
				default:
					// Closed without error — commit (held chunks only is
					// fine: a preamble-then-complete stream is empty output).
					d.committed = true
				}
			}
			return outcomeCommitted

		case <-pr.AcceptedCh:
			// Acceptance is not content and must not suppress either the
			// speculative launch point or the absolute first-content timer.
			continue

		case errMsg := <-pr.ErrorCh:
			speculativeTimer.Stop()
			deadlineTimer.Stop()
			if d.commitReadyFirstContent(pr, &d.heldChunks, errMsg) {
				return outcomeCommitted
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.cancelDispatchAfterTerminal(provider, pr)
			d.setLastInferenceError(provider, errMsg)
			d.lastFailedVersion = failedProviderVersion(provider)
			s.logger.Warn("provider failed, retrying",
				"request_id", d.requestID,
				"provider_id", provider.ID,
				"attempt", d.attempt+1,
				"failure_code", errMsg.FailureCode,
			)
			s.emitRequest(r.Context(), protocol.SeverityWarn, d.requestID,
				"provider failed, retrying",
				map[string]any{
					"provider_id": provider.ID,
					"attempt":     d.attempt + 1,
					"reason":      "provider_error",
					"status_code": errMsg.StatusCode,
				})
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "retry"})
			}
			d.noteDispatchRetry(provider, pr, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &d.heldChunks, errMsg.CoordinatorCause)
			d.provider = nil
			d.pr = nil
			return outcomeRetry

		case at := <-hedgeAdvance:
			// One-shot re-arm of the speculative timer to the probe round's
			// refined launch instant. Guards, in order: only once (the local
			// disarms), only with the absolute clock stamped (mirrors
			// first_token_clock.go invariant 5), only strictly EARLIER than
			// the armed point, and never after the timer fired — Stop()
			// reports whether the timer was still pending; a spent fire stays
			// buffered in C for its own arm and must not be re-armed over.
			// Never past the deadline by construction: hedgeLaunchAt's
			// ceiling is deadline/2. speculativeAt is updated so every
			// downstream remaining-window computation, the launch-now check
			// above, and telemetry agree with the re-armed timer; without a
			// delivered value it stays the 50% default — exact legacy timing.
			hedgeAdvance = nil
			receivedAt := timingReceivedAt(d.timing)
			if receivedAt.IsZero() || !at.Before(receivedAt.Add(d.speculativeAt)) {
				continue
			}
			if !speculativeTimer.Stop() {
				continue
			}
			d.speculativeAt = at.Sub(receivedAt)
			if d.speculativeAt < 0 {
				d.speculativeAt = 0
			}
			speculativeTimer.Reset(d.firstTokenSpeculativeWait())
			continue

		case <-speculativeTimer.C:
			if pr.FirstContentIngressArrivedByDeadline() {
				if d.onSpeculativeDeferral != nil {
					d.onSpeculativeDeferral()
				}
				continue
			}
			deadlineTimer.Stop()
			return d.runSpeculative()

		case <-deadlineTimer.C:
			speculativeTimer.Stop()
			if chunk, ok := drainReadyFirstContent(pr, &d.heldChunks); ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
				return outcomeCommitted
			}
			if pr.FirstContentIngressArrivedByDeadline() {
				continue
			}
			if len(d.heldChunks) > 0 && d.canExtendPreambleLiveness() {
				// Preamble liveness — the provider is alive but still in its
				// pre-content phase. Fall through to waitAccepted, still
				// bounded by leftover request-absolute first-token budget.
				d.preambleLiveness = true
				return outcomeAccepted
			}
			if !s.cancelDispatchForFirstContentTimeout(provider, pr) {
				continue
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.registry.RecordWarmPoolTTFTMiss(d.model, d.deadline)
			if providerAttemptAttributableStall(pr, d.deadline) {
				s.noteInferenceError(provider.ID, pr, http.StatusGatewayTimeout, "", "", "")
			}
			d.setLastError("timeout waiting for first response", http.StatusGatewayTimeout)
			s.logger.Warn("provider timeout (full deadline), retrying",
				"request_id", d.requestID,
				"provider_id", provider.ID,
				"attempt", d.attempt+1,
			)
			s.emitRequest(r.Context(), protocol.SeverityWarn, d.requestID,
				"provider first-chunk timeout",
				map[string]any{
					"provider_id": provider.ID,
					"attempt":     d.attempt + 1,
					"reason":      "first_chunk_timeout",
				})
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "timeout"})
			}
			s.ddIncr("inference.dispatches", []string{"status:timeout"})
			d.provider = nil
			d.pr = nil
			return outcomeRetry

		case <-r.Context().Done():
			speculativeTimer.Stop()
			deadlineTimer.Stop()
			s.cancelDispatch(provider, pr, cancelCauseClientGonePre)
			d.refundReservation()
			return outcomeClientGone
		}
	}
}

// runSpeculative is the speculativeTimer.C arm of waitFirstChunk: the primary is
// slow, so dispatch a speculative backup (unless this is a prefer request being
// served by the caller's own machine) and either keep waiting for the primary
// alone (no backup available) or race primary vs backup. Returns the same outcome
// set as waitFirstChunk.
func (d *dispatchState) runSpeculative() dispatchOutcome {
	s := d.s
	r := d.r
	provider := d.provider
	if d.onSpeculativeDispatch != nil {
		d.onSpeculativeDispatch()
	}
	if _, empty := d.pr.OnTimeEmptyCompletionIngress(); empty {
		return d.waitAccepted()
	}

	// Primary is slow. Attempt speculative backup dispatch.
	s.ddIncr("inference.speculative_dispatch", []string{"model:" + d.model})
	s.registry.RecordWarmPoolSpeculativeStarted(d.model)

	var backupProvider *registry.Provider
	var attemptedBackupProvider *registry.Provider
	var backupPR *registry.PendingRequest
	var backupErr string
	var backupErrCode int
	backupRouteRecorded := false
	backupRouteRequestID := ""
	backupRouteAttempt := d.attempt

	// Do NOT speculatively race a paid PUBLIC backup against a prefer
	// request that is being served by the caller's OWN machine: the user
	// opted into "prefer my machine (free)", so a slow owned machine must
	// be waited on, not raced (and billed) by the public fleet. (Exclusive
	// self-route is already safe — its backup selection is owned-only and
	// returns nil when there's no other owned machine.) When the prefer
	// primary is itself a public provider (the owner owns nothing / fell
	// back), normal speculative behaviour applies.
	skipBackup := false
	if d.policy.prefer {
		provider.Mu().Lock()
		skipBackup = d.policy.ownerAccountID != "" && provider.AccountID == d.policy.ownerAccountID
		provider.Mu().Unlock()
	}

	// Hedge governor (Routing v2 Phase 4): insurance must never amplify an
	// overload. A non-allow verdict suppresses the backup entirely and falls
	// through the nil-backup branch below — byte-identical to today's
	// "no backup available" path. The owner-served prefer skip above stays
	// governor-blind: it is a product rule, not a capacity decision.
	//
	// The verdict and the budget-slot increment are ONE atomic governor
	// operation (tryAcquireHedge): concurrent slow requests can no longer
	// each read the last free slot and all launch past the fleet-wide cap.
	// An acquired slot is released exactly once — below when no backup
	// actually dispatches, at race resolution otherwise.
	hedgeLaunched := false
	if !skipBackup && s.hedgeGov != nil {
		verdict, acquired := d.tryAcquireBackupHedge(provider.ID)
		d.hedgeGovernorVerdict = verdict.String()
		hedgeLaunched = acquired
		if verdict != hedgeAllow {
			s.ddIncr("routing.hedge_governor_suppressed", []string{"model:" + d.model, "verdict:" + verdict.String()})
			s.logger.Info("speculative_backup_suppressed",
				"request_id", d.requestID,
				"primary_provider", provider.ID,
				"verdict", verdict.String(),
			)
			skipBackup = true
		}
	}

	if !skipBackup {
		d.pr.EnableSpeculativeEmptyCompletionArbitration()
		backupExclude := make(map[string]struct{}, len(d.excludeProviders)+1)
		for id := range d.excludeProviders {
			backupExclude[id] = struct{}{}
		}
		backupExclude[provider.ID] = struct{}{}

		recordBackupRoute := func(provider *registry.Provider, pr *registry.PendingRequest, decision registry.RoutingDecision) {
			attemptedBackupProvider = provider
			if pr != nil {
				pr.EnableSpeculativeEmptyCompletionArbitration()
				d.configurePending(pr)
				backupRouteRecorded = true
				backupRouteRequestID = pr.RequestID
				backupRouteAttempt = pr.Attempt
			}
			d.recordRoutingDecisionFor(provider, pr, "", d.attempt, decision, "", "")
		}
		// Routing v2 W2: the backup consumes the retained plan first — the
		// next confirmed/revalidated entry, then the request's single refresh
		// — falling back to the legacy full scan only when the plan machinery
		// yields nothing (prefer-owner and legacy fleets keep their exact
		// selection behavior). The backup shares only ReceivedAt with the
		// primary's clock, as before.
		backupTiming := &registry.RequestTiming{ReceivedAt: d.timing.ReceivedAt}
		planTried := false
		backupProvider, backupPR, _, backupErr, backupErrCode, planTried =
			d.dispatchFromPlanMachinery(backupTiming, backupExclude, d.requestID, recordBackupRoute)
		if !planTried {
			backupProvider, backupPR, _, _, backupErr, backupErrCode = s.dispatchOneProvider(
				r, d.model, d.publicModel, d.rawBody, d.consumerKey, d.consumerLocation, d.reservedMicroUSD,
				d.estimatedPromptTokens, d.deadline, d.requestedMaxTokens, d.tokenAdmission, d.requiresVision,
				d.traits(),
				d.allowedProviderSerials, d.isResponsesAPI, d.policy,
				backupTiming,
				d.serviceReservation,
				d.cachePlan,
				backupExclude,
				d.attempt, d.profile, d.requestID,
				recordBackupRoute,
				d.noteProviderDispatched,
			)
		}
	}

	if backupProvider == nil {
		if hedgeLaunched {
			// The governor admitted a hedge that never dispatched — release
			// its budget slot immediately. No outcome is recorded: no race
			// ran, so there is nothing to fold into the win-rate EWMA.
			s.hedgeGov.noteHedgeResolved()
		}
		if d.pr != nil {
			d.pr.ResolveSpeculativeEmptyCompletion(true)
		}
		if backupErrCode == http.StatusRequestEntityTooLarge && attemptedBackupProvider != nil {
			d.noteProviderBodyTooLargeFor(attemptedBackupProvider, backupErr)
		}
		if backupRouteRecorded {
			d.s.updateInferenceRouteOutcomeWithModel(backupRouteRequestID, backupRouteAttempt, d.model, d.errorRoutingOutcome("error", dispatchErrorClass(backupErr), backupErrCode))
		}
		// No backup available. Keep waiting for primary with remaining deadline.
		s.logger.Info("speculative_dispatch_no_backup",
			"request_id", d.requestID,
			"primary_provider", provider.ID,
		)
		return d.waitNoBackup()
	}
	// Backup dispatched — race primary vs backup.
	if d.pr != nil {
		d.pr.UsedBackup = true
		if ap := d.pr.Profile; ap != nil {
			ap.BackupLaunched.Store(true)
		}
	}
	if backupPR != nil {
		backupPR.UsedBackup = true
	}
	s.logger.Info("speculative_dispatch",
		"request_id", d.requestID,
		"primary_provider", provider.ID,
		"backup_provider", backupProvider.ID,
		"ttft_deadline_ms", d.deadline.Milliseconds(),
		"speculative_at_ms", d.speculativeAt.Milliseconds(),
	)
	outcome := d.runRace(backupProvider, backupPR)
	if hedgeLaunched {
		// Exactly-once hedge accounting: every runRace exit — win, loss,
		// retry, client-gone, empty-completion promotion, and the failed-racer
		// sub-waits — returns through here, and BackupWon is the winner marker
		// every backup-win path sets before committing.
		s.hedgeGov.noteHedgeResolved()
		s.hedgeGov.recordHedgeOutcome(d.model, backupPR.BackupWon)
	}
	return outcome
}

// waitNoBackup is the speculative-no-backup branch (`noBackupWait`): keep waiting
// for the primary alone with the remaining deadline. d.provider / d.pr are the primary.
func (d *dispatchState) waitNoBackup() dispatchOutcome {
	s := d.s
	r := d.r
	provider, pr := d.provider, d.pr

	remainingDeadline := time.NewTimer(d.firstTokenWait(d.deadline - d.speculativeAt))
	for {
		select {
		case chunk, ok := <-pr.ChunkCh:
			if ok && holdPreContentBoilerplate(pr, chunk, &d.heldChunks) {
				continue
			}
			remainingDeadline.Stop()
			if ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
			} else {
				select {
				case errMsg := <-pr.ErrorCh:
					d.excludeProviders[provider.ID] = struct{}{}
					s.cancelDispatchAfterTerminal(provider, pr)
					d.setLastInferenceError(provider, errMsg)
					d.lastFailedVersion = failedProviderVersion(provider)
					d.noteDispatchRetry(provider, pr, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &d.heldChunks, errMsg.CoordinatorCause)
					d.provider = nil
					d.pr = nil
					return outcomeRetry
				default:
					d.committed = true
				}
			}
			return outcomeCommitted
		case <-pr.AcceptedCh:
			continue
		case errMsg := <-pr.ErrorCh:
			remainingDeadline.Stop()
			if d.commitReadyFirstContent(pr, &d.heldChunks, errMsg) {
				return outcomeCommitted
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.cancelDispatchAfterTerminal(provider, pr)
			d.setLastInferenceError(provider, errMsg)
			d.lastFailedVersion = failedProviderVersion(provider)
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "retry"})
			}
			d.noteDispatchRetry(provider, pr, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &d.heldChunks, errMsg.CoordinatorCause)
			d.provider = nil
			d.pr = nil
			return outcomeRetry
		case <-remainingDeadline.C:
			if chunk, ok := drainReadyFirstContent(pr, &d.heldChunks); ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
				return outcomeCommitted
			}
			if pr.FirstContentIngressArrivedByDeadline() {
				continue
			}
			if len(d.heldChunks) > 0 && d.canExtendPreambleLiveness() {
				// Liveness: the provider already produced its preamble.
				// Fall through to waitAccepted, still bounded by leftover
				// request-absolute first-token budget.
				d.preambleLiveness = true
				return outcomeAccepted
			}
			if !s.cancelDispatchForFirstContentTimeout(provider, pr) {
				continue
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.registry.RecordWarmPoolTTFTMiss(d.model, d.deadline)
			if providerAttemptAttributableStall(pr, d.deadline) {
				s.noteInferenceError(provider.ID, pr, http.StatusGatewayTimeout, "", "", "")
			}
			d.setLastError("timeout waiting for first response", http.StatusGatewayTimeout)
			s.logger.Warn("provider timeout (no backup), retrying",
				"request_id", d.requestID,
				"provider_id", provider.ID,
				"attempt", d.attempt+1,
			)
			s.emitRequest(r.Context(), protocol.SeverityWarn, d.requestID,
				"provider first-chunk timeout",
				map[string]any{
					"provider_id": provider.ID,
					"attempt":     d.attempt + 1,
					"reason":      "first_chunk_timeout",
				})
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "timeout"})
			}
			s.ddIncr("inference.dispatches", []string{"status:timeout"})
			d.provider = nil
			d.pr = nil
			return outcomeRetry
		case <-r.Context().Done():
			remainingDeadline.Stop()
			s.cancelDispatch(provider, pr, cancelCauseClientGonePre)
			d.refundReservation()
			return outcomeClientGone
		}
	}
}

func emptyCompletionPrecedesChunk(
	empty *registry.PendingRequest,
	chunk registry.ProviderChunk,
) bool {
	completedAt, ok := empty.OnTimeEmptyCompletionIngress()
	return ok &&
		!chunk.ReceivedAt.IsZero() &&
		!completedAt.After(chunk.ReceivedAt)
}

func (d *dispatchState) awaitPrimaryEmptyCompletion(
	backupProvider *registry.Provider,
	backupPR *registry.PendingRequest,
) dispatchOutcome {
	d.pr.ResolveSpeculativeEmptyCompletion(true)
	d.s.cancelDispatch(backupProvider, backupPR, cancelCauseHedgeLoser)
	d.markSpeculativeLoser(backupPR)
	return d.waitAccepted()
}

func (d *dispatchState) awaitBackupEmptyCompletion(
	primaryProvider *registry.Provider,
	primaryPR *registry.PendingRequest,
	backupProvider *registry.Provider,
	backupPR *registry.PendingRequest,
	backupHeld []string,
) dispatchOutcome {
	backupPR.ResolveSpeculativeEmptyCompletion(true)
	d.s.cancelDispatch(primaryProvider, primaryPR, cancelCauseHedgeLoser)
	d.s.ddIncr("inference.speculative_win", []string{"model:" + d.model})
	d.s.registry.RecordWarmPoolSpeculativeWon(d.model)
	d.markSpeculativeLoser(primaryPR)
	backupPR.BackupWon = true
	if ap := backupPR.Profile; ap != nil {
		ap.BackupWon.Store(true)
		if primaryPR != nil {
			ap.CopyPreDispatchFrom(primaryPR.Profile)
		}
	}
	d.provider = backupProvider
	d.pr = backupPR
	d.requestID = backupPR.RequestID
	d.heldChunks = backupHeld
	d.noteServingSlot()
	return d.waitAccepted()
}

// runRace is the speculative `race` loop: primary (d.provider/d.pr) vs backup,
// first CONTENT chunk wins; the loser is cancelled. Preamble from each racer is
// buffered separately (held chunks must never mix providers). On a racer error the
// surviving racer is waited on via a sub-loop. Returns the waitFirstChunk outcome
// set; on a backup win d.provider/d.pr/d.requestID/d.heldChunks are swapped to the backup.
func (d *dispatchState) runRace(backupProvider *registry.Provider, backupPR *registry.PendingRequest) dispatchOutcome {
	s := d.s
	r := d.r
	provider, pr := d.provider, d.pr

	raceDeadline := time.NewTimer(d.firstTokenWait(d.deadline - d.speculativeAt))
	// One-shot extension: when the race deadline expires but a racer
	// has shown liveness (preamble received), the race continues up to
	// leftover first-token budget (capped by preambleContentTimeout).
	raceExtended := false
	// Preamble chunks from the backup are buffered separately —
	// held chunks must never mix providers.
	var backupHeld []string
	primaryCompletion := pr.CompletionIngressSignal()
	backupCompletion := backupPR.CompletionIngressSignal()

	for {
		select {
		case chunk, ok := <-pr.ChunkCh:
			if ok && holdPreContentBoilerplate(pr, chunk, &d.heldChunks) {
				// Preamble only — the primary hasn't proven it can
				// generate; keep the backup racing for first content.
				if completedAt, empty := backupPR.OnTimeEmptyCompletionIngress(); empty &&
					!pr.ContentIngressAtOrBefore(completedAt) {
					raceDeadline.Stop()
					return d.awaitBackupEmptyCompletion(
						provider, pr, backupProvider, backupPR, backupHeld)
				}
				continue
			}
			if ok && emptyCompletionPrecedesChunk(backupPR, chunk) {
				raceDeadline.Stop()
				return d.awaitBackupEmptyCompletion(
					provider, pr, backupProvider, backupPR, backupHeld)
			}
			if !ok {
				primaryAt, primaryEmpty := pr.OnTimeEmptyCompletionIngress()
				backupAt, backupEmpty := backupPR.OnTimeEmptyCompletionIngress()
				if backupEmpty && (!primaryEmpty || backupAt.Before(primaryAt)) {
					raceDeadline.Stop()
					return d.awaitBackupEmptyCompletion(
						provider, pr, backupProvider, backupPR, backupHeld)
				}
			}
			// Primary wins!
			raceDeadline.Stop()
			s.cancelDispatch(backupProvider, backupPR, cancelCauseHedgeLoser)
			if ok {
				d.markSpeculativeLoser(backupPR)
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
			} else {
				select {
				case errMsg := <-pr.ErrorCh:
					// Primary failed but we already cancelled backup.
					d.markSpeculativeLoser(backupPR)
					d.excludeProviders[provider.ID] = struct{}{}
					s.cancelDispatchAfterTerminal(provider, pr)
					d.setLastInferenceError(provider, errMsg)
					d.lastFailedVersion = failedProviderVersion(provider)
					d.noteDispatchRetry(provider, pr, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &d.heldChunks, errMsg.CoordinatorCause)
					d.provider = nil
					d.pr = nil
					return outcomeRetry
				default:
					d.markSpeculativeLoser(backupPR)
					d.committed = true
				}
			}
			return outcomeCommitted

		case chunk, ok := <-backupPR.ChunkCh:
			if ok && holdPreContentBoilerplate(backupPR, chunk, &backupHeld) {
				// Backup preamble doesn't win the race — first CONTENT does.
				if completedAt, empty := pr.OnTimeEmptyCompletionIngress(); empty &&
					!backupPR.ContentIngressAtOrBefore(completedAt) {
					raceDeadline.Stop()
					return d.awaitPrimaryEmptyCompletion(backupProvider, backupPR)
				}
				continue
			}
			if ok && emptyCompletionPrecedesChunk(pr, chunk) {
				raceDeadline.Stop()
				return d.awaitPrimaryEmptyCompletion(backupProvider, backupPR)
			}
			if !ok {
				primaryAt, primaryEmpty := pr.OnTimeEmptyCompletionIngress()
				backupAt, backupEmpty := backupPR.OnTimeEmptyCompletionIngress()
				if primaryEmpty && (!backupEmpty || primaryAt.Before(backupAt)) {
					raceDeadline.Stop()
					return d.awaitPrimaryEmptyCompletion(backupProvider, backupPR)
				}
			}
			// Backup wins!
			raceDeadline.Stop()
			s.cancelDispatch(provider, pr, cancelCauseHedgeLoser)
			s.ddIncr("inference.speculative_win", []string{"model:" + d.model})
			s.registry.RecordWarmPoolSpeculativeWon(d.model)
			if ok {
				d.markSpeculativeLoser(pr)
				backupPR.BackupWon = true
				d.provider = backupProvider
				d.pr = backupPR
				d.requestID = d.pr.RequestID
				d.heldChunks = backupHeld
				// The backup is now the serving slot; re-latch so a
				// post-commit failure books under ITS backend, not the
				// cancelled primary's.
				d.noteServingSlot()
				d.commitFirstContent(d.pr, chunk.Data)
				d.committed = true
			} else {
				select {
				case errMsg := <-backupPR.ErrorCh:
					// Backup failed too. Keep primary context for retry.
					d.excludeProviders[backupProvider.ID] = struct{}{}
					d.lastFailedVersion = failedProviderVersion(backupProvider)
					d.updateSpeculativeFailure(backupPR, errMsg)
					d.noteProviderError(backupProvider, backupPR, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &backupHeld, errMsg.CoordinatorCause)
					// Preserve a deterministic-unservable verdict from this loser so the
					// surviving primary's error can't mask it (see latchDeterministicLoser).
					d.latchDeterministicLoser(backupProvider, errMsg)
					// Wait remaining deadline for primary.
					return d.raceBackupChunkClosedWaitPrimary(provider, pr)
				default:
					// Backup channel closed with no error — treat as committed.
					s.cancelDispatch(provider, pr, cancelCauseHedgeLoser)
					d.markSpeculativeLoser(pr)
					backupPR.BackupWon = true
					d.provider = backupProvider
					d.pr = backupPR
					d.requestID = d.pr.RequestID
					d.heldChunks = backupHeld
					d.noteServingSlot()
					d.committed = true
				}
			}
			return outcomeCommitted

		case <-primaryCompletion:
			primaryCompletion = nil
			completedAt, empty := pr.OnTimeEmptyCompletionIngress()
			if !empty || backupPR.ContentIngressAtOrBefore(completedAt) {
				continue
			}
			if backupAt, backupEmpty := backupPR.OnTimeEmptyCompletionIngress(); backupEmpty && backupAt.Before(completedAt) {
				raceDeadline.Stop()
				return d.awaitBackupEmptyCompletion(
					provider, pr, backupProvider, backupPR, backupHeld)
			}
			raceDeadline.Stop()
			return d.awaitPrimaryEmptyCompletion(backupProvider, backupPR)

		case <-backupCompletion:
			backupCompletion = nil
			completedAt, empty := backupPR.OnTimeEmptyCompletionIngress()
			if !empty || pr.ContentIngressAtOrBefore(completedAt) {
				continue
			}
			if primaryAt, primaryEmpty := pr.OnTimeEmptyCompletionIngress(); primaryEmpty && primaryAt.Before(completedAt) {
				raceDeadline.Stop()
				return d.awaitPrimaryEmptyCompletion(backupProvider, backupPR)
			}
			raceDeadline.Stop()
			return d.awaitBackupEmptyCompletion(
				provider, pr, backupProvider, backupPR, backupHeld)

		case <-pr.AcceptedCh:
			// Acceptance never wins a race. Both providers keep racing until
			// real content, error, or the absolute deadline.
			continue

		case <-backupPR.AcceptedCh:
			continue

		case errMsg := <-pr.ErrorCh:
			// Primary failed. Keep waiting for backup.
			raceDeadline.Stop()
			if chunk, ok := drainReadyFirstContent(pr, &d.heldChunks); ok {
				if emptyCompletionPrecedesChunk(backupPR, chunk) {
					return d.awaitBackupEmptyCompletion(
						provider, pr, backupProvider, backupPR, backupHeld)
				}
				s.cancelDispatch(backupProvider, backupPR, cancelCauseHedgeLoser)
				d.markSpeculativeLoser(backupPR)
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
				d.initialError = &errMsg
				return outcomeCommitted
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.cancelDispatchAfterTerminal(provider, pr)
			d.lastFailedVersion = failedProviderVersion(provider)
			d.updateSpeculativeFailure(pr, errMsg)
			d.noteProviderError(provider, pr, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &d.heldChunks, errMsg.CoordinatorCause)
			// Preserve a deterministic-unservable verdict from this loser so the
			// surviving backup's error can't mask it (see latchDeterministicLoser).
			d.latchDeterministicLoser(provider, errMsg)
			d.requestID = ""
			d.provider = nil
			d.pr = nil
			backupPR.ResolveSpeculativeEmptyCompletion(true)
			return d.racePrimaryFailedWaitBackup(backupProvider, backupPR, backupHeld)

		case errMsg := <-backupPR.ErrorCh:
			// Backup failed. Keep waiting for primary.
			raceDeadline.Stop()
			if chunk, ok := drainReadyFirstContent(backupPR, &backupHeld); ok {
				if emptyCompletionPrecedesChunk(pr, chunk) {
					return d.awaitPrimaryEmptyCompletion(backupProvider, backupPR)
				}
				s.cancelDispatch(provider, pr, cancelCauseHedgeLoser)
				d.markSpeculativeLoser(pr)
				backupPR.BackupWon = true
				d.provider = backupProvider
				d.pr = backupPR
				d.requestID = backupPR.RequestID
				d.heldChunks = backupHeld
				d.noteServingSlot()
				d.commitFirstContent(backupPR, chunk.Data)
				d.committed = true
				d.initialError = &errMsg
				return outcomeCommitted
			}
			d.excludeProviders[backupProvider.ID] = struct{}{}
			s.cancelDispatchAfterTerminal(backupProvider, backupPR)
			d.lastFailedVersion = failedProviderVersion(backupProvider)
			d.updateSpeculativeFailure(backupPR, errMsg)
			d.noteProviderError(backupProvider, backupPR, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &backupHeld, errMsg.CoordinatorCause)
			// Preserve a deterministic-unservable verdict from this loser so the
			// surviving primary's error can't mask it (see latchDeterministicLoser).
			d.latchDeterministicLoser(backupProvider, errMsg)
			pr.ResolveSpeculativeEmptyCompletion(true)
			return d.raceBackupErrWaitPrimary(provider, pr)

		case <-raceDeadline.C:
			// A token that is already buffered beats the timer: the backup is
			// dispatched synchronously in runSpeculative, so an on-time primary
			// token can be sitting in ChunkCh when a zero-leftover timer fires.
			if chunk, ok := drainReadyFirstContent(pr, &d.heldChunks); ok {
				if emptyCompletionPrecedesChunk(backupPR, chunk) {
					return d.awaitBackupEmptyCompletion(
						provider, pr, backupProvider, backupPR, backupHeld)
				}
				s.cancelDispatch(backupProvider, backupPR, cancelCauseHedgeLoser)
				d.markSpeculativeLoser(backupPR)
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
				return outcomeCommitted
			}
			if chunk, ok := drainReadyFirstContent(backupPR, &backupHeld); ok {
				if emptyCompletionPrecedesChunk(pr, chunk) {
					return d.awaitPrimaryEmptyCompletion(backupProvider, backupPR)
				}
				s.cancelDispatch(provider, pr, cancelCauseHedgeLoser)
				s.ddIncr("inference.speculative_win", []string{"model:" + d.model})
				s.registry.RecordWarmPoolSpeculativeWon(d.model)
				d.markSpeculativeLoser(pr)
				backupPR.BackupWon = true
				d.provider = backupProvider
				d.pr = backupPR
				d.requestID = d.pr.RequestID
				d.heldChunks = backupHeld
				d.noteServingSlot()
				d.commitFirstContent(d.pr, chunk.Data)
				d.committed = true
				return outcomeCommitted
			}
			if pr.FirstContentIngressArrivedByDeadline() ||
				backupPR.FirstContentIngressArrivedByDeadline() {
				continue
			}
			if !raceExtended && (len(d.heldChunks) > 0 || len(backupHeld) > 0) {
				// Liveness from at least one racer: don't fail at the
				// relative TTFT slice — extend once by leftover
				// request-absolute first-token budget, capped by
				// preambleContentTimeout (zero bytes have reached the
				// client; a genuine cold load would have signalled
				// AcceptedCh).
				ext := d.firstTokenWait(preambleContentTimeout)
				if ext > preambleContentTimeout {
					ext = preambleContentTimeout
				}
				if ext > 0 {
					raceExtended = true
					raceDeadline = time.NewTimer(ext)
					continue
				}
			}
			if !s.cancelDispatchForFirstContentTimeout(provider, pr) {
				continue
			}
			if !s.cancelDispatchForFirstContentTimeout(backupProvider, backupPR) {
				// The primary was cancelled for the timeout but the backup
				// won its ingress race: record the primary's timeout (route
				// outcome + attempt profile) before its identity is cleared.
				d.updateSpeculativeTimeout(pr, "first_chunk_timeout")
				d.excludeProviders[provider.ID] = struct{}{}
				d.setLastError("timeout waiting for first response", http.StatusGatewayTimeout)
				d.provider = nil
				d.pr = nil
				d.requestID = ""
				backupPR.ResolveSpeculativeEmptyCompletion(true)
				return d.racePrimaryFailedWaitBackup(
					backupProvider, backupPR, backupHeld)
			}
			// Both missed deadline. A racer that held preamble (role
			// then stall) is a 504-shaped sickness — feed the breaker
			// before cancelling, mirroring the single-provider
			// acceptedWait timeout path so a stalling provider/model
			// (shape-keyed) trips its cooldown.
			// Attribute each provider's complete initial+racing interval. The
			// prior extension-only check missed stalls split across phases.
			if providerAttemptAttributableStall(pr, d.deadline) {
				s.noteInferenceError(provider.ID, pr, http.StatusGatewayTimeout, "", "", "")
			}
			if providerAttemptAttributableStall(
				backupPR, d.deadline-d.speculativeAt) {
				s.noteInferenceError(backupProvider.ID, backupPR, http.StatusGatewayTimeout, "", "", "")
			}
			s.registry.RecordWarmPoolTTFTMiss(d.model, d.deadline)
			d.updateSpeculativeTimeout(backupPR, "first_chunk_timeout")
			d.excludeProviders[provider.ID] = struct{}{}
			d.excludeProviders[backupProvider.ID] = struct{}{}
			d.setLastError("timeout waiting for first response (both providers)", http.StatusGatewayTimeout)
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "timeout"})
			}
			s.ddIncr("inference.dispatches", []string{"status:timeout"})
			d.provider = nil
			d.pr = nil
			return outcomeRetry

		case <-r.Context().Done():
			raceDeadline.Stop()
			d.updateSpeculativeClientGone(backupPR)
			s.cancelDispatch(provider, pr, cancelCauseClientGonePre)
			s.cancelDispatch(backupProvider, backupPR, cancelCauseClientGonePre)
			d.refundReservation()
			return outcomeClientGone
		}
	}
}

// raceBackupChunkClosedWaitPrimary handles the race sub-case where the backup's
// ChunkCh closed with an error (already recorded by the caller): wait the
// remaining deadline for the primary. This is the former `backupFailedPrimaryWait`
// loop. d.provider/d.pr remain the primary throughout (the backup already lost).
func (d *dispatchState) raceBackupChunkClosedWaitPrimary(provider *registry.Provider, pr *registry.PendingRequest) dispatchOutcome {
	s := d.s
	r := d.r
	remainingPrimary := time.NewTimer(d.firstTokenWait(d.deadline - d.speculativeAt))
	for {
		select {
		case chunk, ok := <-pr.ChunkCh:
			if ok && holdPreContentBoilerplate(pr, chunk, &d.heldChunks) {
				continue
			}
			remainingPrimary.Stop()
			if ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
			} else {
				select {
				case errMsg2 := <-pr.ErrorCh:
					d.excludeProviders[provider.ID] = struct{}{}
					s.cancelDispatchAfterTerminal(provider, pr)
					d.setLastInferenceError(provider, errMsg2)
					d.lastFailedVersion = failedProviderVersion(provider)
					d.updateSpeculativeFailure(pr, errMsg2)
					d.noteDispatchRetry(provider, pr, errMsg2.StatusCode, errMsg2.Error, errMsg2.ErrorReason, errMsg2.TerminalCause, &d.heldChunks, errMsg2.CoordinatorCause)
					d.provider = nil
					d.pr = nil
					d.requestID = ""
					return outcomeRetry
				default:
					d.committed = true
				}
			}
			return outcomeCommitted
		case <-pr.AcceptedCh:
			continue
		case errMsg2 := <-pr.ErrorCh:
			// Defensive: both ErrorCh senders currently send before
			// closing ChunkCh (the closed-ChunkCh check above catches
			// them), but a direct arm keeps this loop correct if that
			// ordering ever changes — mirroring its sibling wait loops.
			remainingPrimary.Stop()
			if d.commitReadyFirstContent(pr, &d.heldChunks, errMsg2) {
				return outcomeCommitted
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.cancelDispatchAfterTerminal(provider, pr)
			d.setLastInferenceError(provider, errMsg2)
			d.lastFailedVersion = failedProviderVersion(provider)
			d.updateSpeculativeFailure(pr, errMsg2)
			d.noteDispatchRetry(provider, pr, errMsg2.StatusCode, errMsg2.Error, errMsg2.ErrorReason, errMsg2.TerminalCause, &d.heldChunks, errMsg2.CoordinatorCause)
			d.provider = nil
			d.pr = nil
			d.requestID = ""
			return outcomeRetry
		case <-remainingPrimary.C:
			if chunk, ok := drainReadyFirstContent(pr, &d.heldChunks); ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
				return outcomeCommitted
			}
			if pr.FirstContentIngressArrivedByDeadline() {
				continue
			}
			if len(d.heldChunks) > 0 && d.canExtendPreambleLiveness() {
				// Primary preamble liveness — continue in waitAccepted
				// on leftover request-absolute first-token budget.
				d.preambleLiveness = true
				return outcomeAccepted
			}
			if !s.cancelDispatchForFirstContentTimeout(provider, pr) {
				continue
			}
			// The PRIMARY timed out here (the backup's earlier error
			// is already recorded); report the timeout, not the
			// backup's stale error text.
			d.excludeProviders[provider.ID] = struct{}{}
			s.registry.RecordWarmPoolTTFTMiss(d.model, d.deadline)
			if providerAttemptAttributableStall(pr, d.deadline) {
				s.noteInferenceError(provider.ID, pr, http.StatusGatewayTimeout, "", "", "")
			}
			d.updateSpeculativeTimeout(pr, "first_chunk_timeout")
			d.setLastError("timeout waiting for first response", http.StatusGatewayTimeout)
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "timeout"})
			}
			s.ddIncr("inference.dispatches", []string{"status:timeout"})
			d.provider = nil
			d.pr = nil
			d.requestID = ""
			return outcomeRetry
		case <-r.Context().Done():
			remainingPrimary.Stop()
			d.updateSpeculativeClientGone(pr)
			s.cancelDispatch(provider, pr, cancelCauseClientGonePre)
			d.refundReservation()
			return outcomeClientGone
		}
	}
}

// racePrimaryFailedWaitBackup handles the race sub-case where the primary errored
// (already recorded): wait the remaining deadline for the backup, promoting it to
// the committed/accepted provider on success. This is the former
// `primaryFailedBackupWait` loop.
func (d *dispatchState) racePrimaryFailedWaitBackup(backupProvider *registry.Provider, backupPR *registry.PendingRequest, backupHeld []string) dispatchOutcome {
	s := d.s
	r := d.r
	// The primary already failed and d.pr is cleared: the BACKUP is the only
	// racer left, so every failure or timeout below is the backup's. Re-latch
	// now so the terminal outcome names the backup's backend rather than
	// falling back to the dead primary's latch. When the primary's failure
	// latched a DETERMINISTIC verdict (latchDeterministicLoser just ran), the
	// re-latch is a no-op by design: the terminal response will be the
	// primary's 4xx/422/429, so the primary keeps the attribution even
	// though the backup keeps racing (noteServingSlotFor's freeze rule).
	d.noteServingSlotFor(backupPR)
	backupDeadline := time.NewTimer(d.firstTokenWait(d.deadline - d.speculativeAt))
	for {
		select {
		case chunk, ok := <-backupPR.ChunkCh:
			if ok && holdPreContentBoilerplate(backupPR, chunk, &backupHeld) {
				continue
			}
			backupDeadline.Stop()
			if ok {
				backupPR.BackupWon = true
				d.provider = backupProvider
				d.pr = backupPR
				d.requestID = d.pr.RequestID
				d.heldChunks = backupHeld
				d.commitFirstContent(d.pr, chunk.Data)
				d.committed = true
			} else {
				select {
				case errMsg2 := <-backupPR.ErrorCh:
					d.excludeProviders[backupProvider.ID] = struct{}{}
					s.cancelDispatchAfterTerminal(backupProvider, backupPR)
					d.setLastInferenceError(backupProvider, errMsg2)
					d.lastFailedVersion = failedProviderVersion(backupProvider)
					d.updateSpeculativeFailure(backupPR, errMsg2)
					d.noteDispatchRetry(backupProvider, backupPR, errMsg2.StatusCode, errMsg2.Error, errMsg2.ErrorReason, errMsg2.TerminalCause, &backupHeld, errMsg2.CoordinatorCause)
					d.provider = nil
					d.pr = nil
					return outcomeRetry
				default:
					backupPR.BackupWon = true
					d.provider = backupProvider
					d.pr = backupPR
					d.requestID = d.pr.RequestID
					d.heldChunks = backupHeld
					d.committed = true
				}
			}
			return outcomeCommitted
		case <-backupPR.AcceptedCh:
			continue
		case errMsg2 := <-backupPR.ErrorCh:
			backupDeadline.Stop()
			if chunk, ok := drainReadyFirstContent(backupPR, &backupHeld); ok {
				backupPR.BackupWon = true
				d.provider = backupProvider
				d.pr = backupPR
				d.requestID = backupPR.RequestID
				d.heldChunks = backupHeld
				d.noteServingSlot()
				d.commitFirstContent(backupPR, chunk.Data)
				d.committed = true
				d.initialError = &errMsg2
				return outcomeCommitted
			}
			d.excludeProviders[backupProvider.ID] = struct{}{}
			s.cancelDispatchAfterTerminal(backupProvider, backupPR)
			d.setLastInferenceError(backupProvider, errMsg2)
			d.lastFailedVersion = failedProviderVersion(backupProvider)
			d.updateSpeculativeFailure(backupPR, errMsg2)
			d.noteProviderError(backupProvider, backupPR, errMsg2.StatusCode, errMsg2.Error, errMsg2.ErrorReason, errMsg2.TerminalCause, &backupHeld, errMsg2.CoordinatorCause)
			d.provider = nil
			d.pr = nil
			return outcomeRetry
		case <-backupDeadline.C:
			if chunk, ok := drainReadyFirstContent(backupPR, &backupHeld); ok {
				backupPR.BackupWon = true
				d.provider = backupProvider
				d.pr = backupPR
				d.requestID = d.pr.RequestID
				d.heldChunks = backupHeld
				d.commitFirstContent(d.pr, chunk.Data)
				d.committed = true
				return outcomeCommitted
			}
			if backupPR.FirstContentIngressArrivedByDeadline() {
				continue
			}
			if len(backupHeld) > 0 && d.canExtendPreambleLiveness() {
				// Backup preamble liveness — promote it and continue
				// in waitAccepted on leftover first-token budget.
				backupPR.BackupWon = true
				d.provider = backupProvider
				d.pr = backupPR
				d.requestID = d.pr.RequestID
				d.heldChunks = backupHeld
				d.preambleLiveness = true
				return outcomeAccepted
			}
			if !s.cancelDispatchForFirstContentTimeout(backupProvider, backupPR) {
				continue
			}
			d.excludeProviders[backupProvider.ID] = struct{}{}
			s.registry.RecordWarmPoolTTFTMiss(d.model, d.deadline)
			if providerAttemptAttributableStall(
				backupPR, d.deadline-d.speculativeAt) {
				s.noteInferenceError(backupProvider.ID, backupPR, http.StatusGatewayTimeout, "", "", "")
			}
			d.updateSpeculativeTimeout(backupPR, "first_chunk_timeout")
			d.setLastError("timeout waiting for first response (backup)", http.StatusGatewayTimeout)
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "timeout"})
			}
			s.ddIncr("inference.dispatches", []string{"status:timeout"})
			d.provider = nil
			d.pr = nil
			return outcomeRetry
		case <-r.Context().Done():
			backupDeadline.Stop()
			d.updateSpeculativeClientGone(backupPR)
			s.cancelDispatch(backupProvider, backupPR, cancelCauseClientGonePre)
			d.refundReservation()
			return outcomeClientGone
		}
	}
}

// raceBackupErrWaitPrimary handles the race sub-case where the backup errored
// (already recorded): wait the remaining deadline for the primary. This is the
// former `backupFailedWaitPrimary` loop. d.provider/d.pr remain the primary.
func (d *dispatchState) raceBackupErrWaitPrimary(provider *registry.Provider, pr *registry.PendingRequest) dispatchOutcome {
	s := d.s
	r := d.r
	primaryDeadline := time.NewTimer(d.firstTokenWait(d.deadline - d.speculativeAt))
	for {
		select {
		case chunk, ok := <-pr.ChunkCh:
			if ok && holdPreContentBoilerplate(pr, chunk, &d.heldChunks) {
				continue
			}
			primaryDeadline.Stop()
			if ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
			} else {
				select {
				case errMsg2 := <-pr.ErrorCh:
					d.excludeProviders[provider.ID] = struct{}{}
					s.cancelDispatchAfterTerminal(provider, pr)
					d.setLastInferenceError(provider, errMsg2)
					d.lastFailedVersion = failedProviderVersion(provider)
					d.noteDispatchRetry(provider, pr, errMsg2.StatusCode, errMsg2.Error, errMsg2.ErrorReason, errMsg2.TerminalCause, &d.heldChunks, errMsg2.CoordinatorCause)
					d.provider = nil
					d.pr = nil
					return outcomeRetry
				default:
					d.committed = true
				}
			}
			return outcomeCommitted
		case <-pr.AcceptedCh:
			continue
		case errMsg2 := <-pr.ErrorCh:
			primaryDeadline.Stop()
			if d.commitReadyFirstContent(pr, &d.heldChunks, errMsg2) {
				return outcomeCommitted
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.cancelDispatchAfterTerminal(provider, pr)
			d.setLastInferenceError(provider, errMsg2)
			d.lastFailedVersion = failedProviderVersion(provider)
			d.updateSpeculativeFailure(pr, errMsg2)
			d.noteProviderError(provider, pr, errMsg2.StatusCode, errMsg2.Error, errMsg2.ErrorReason, errMsg2.TerminalCause, &d.heldChunks, errMsg2.CoordinatorCause)
			d.provider = nil
			d.pr = nil
			d.requestID = ""
			return outcomeRetry
		case <-primaryDeadline.C:
			if chunk, ok := drainReadyFirstContent(pr, &d.heldChunks); ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
				return outcomeCommitted
			}
			if pr.FirstContentIngressArrivedByDeadline() {
				continue
			}
			if len(d.heldChunks) > 0 && d.canExtendPreambleLiveness() {
				// Primary preamble liveness — continue in waitAccepted
				// on leftover request-absolute first-token budget.
				d.preambleLiveness = true
				return outcomeAccepted
			}
			if !s.cancelDispatchForFirstContentTimeout(provider, pr) {
				continue
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.registry.RecordWarmPoolTTFTMiss(d.model, d.deadline)
			if providerAttemptAttributableStall(pr, d.deadline) {
				s.noteInferenceError(provider.ID, pr, http.StatusGatewayTimeout, "", "", "")
			}
			d.updateSpeculativeTimeout(pr, "first_chunk_timeout")
			d.setLastError("timeout waiting for first response", http.StatusGatewayTimeout)
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "timeout"})
			}
			s.ddIncr("inference.dispatches", []string{"status:timeout"})
			d.provider = nil
			d.pr = nil
			d.requestID = ""
			return outcomeRetry
		case <-r.Context().Done():
			primaryDeadline.Stop()
			d.updateSpeculativeClientGone(pr)
			s.cancelDispatch(provider, pr, cancelCauseClientGonePre)
			d.refundReservation()
			return outcomeClientGone
		}
	}
}

// waitAccepted runs the post-accept wait for first content (the former
// `acceptedWait` loop). It is entered when the committed provider accepted or held
// preamble but hasn't produced content yet. Accept is not a completion token:
// the request-absolute first-token clock keeps running. preambleLiveness still
// caps the wait at preambleContentTimeout so a role-then-stall zombie fails
// over instead of pinning, but that cap cannot exceed leftover SLA.
func (d *dispatchState) waitAccepted() (outcome dispatchOutcome) {
	s := d.s
	r := d.r
	provider, pr := d.provider, d.pr
	captured := routingAttempt(provider, pr, pr.RequestID, pr.Attempt)

	defer func() {
		target := d.currentOrCapturedRoutingAttempt(captured)
		switch outcome {
		case outcomeCommitted:
			d.updateRoutingOutcomeForAttempt(target, d.successRoutingOutcomeFor(target.pending))
		case outcomeRetry:
			// Synthetic-timeout 504s unless a KNOWN typed 504 cause — a typed
			// provider 504 keeps its provider-error class + usage; unknown
			// causes stay legacy (see waitFirstChunk).
			if d.lastErrCode == http.StatusGatewayTimeout && !isTypedTimeout504Cause(d.lastErrTerminalCause) {
				if d.preambleLiveness {
					d.updateRoutingOutcomeForAttempt(target, d.errorRoutingOutcomeFor(target.pending, "timeout", "preamble_liveness_timeout", d.lastErrCode))
				} else {
					d.updateRoutingOutcomeForAttempt(target, d.errorRoutingOutcomeFor(target.pending, "timeout", "accepted_timeout", d.lastErrCode))
				}
			} else {
				// Post-dispatch provider failure (incl. OOM/model-load): admitted but failed.
				d.updateRoutingOutcomeForAttempt(target, d.providerFailedRoutingOutcomeFor(target.pending))
			}
		case outcomeClientGone:
			d.emitClientGone(phaseBeforeFirstToken)
			d.updateRoutingOutcomeForAttempt(target, d.errorRoutingOutcomeFor(target.pending, "cancelled", "client_gone", 0))
		}
	}()

	firstContentBudget := inferenceTimeout
	if d.preambleLiveness {
		firstContentBudget = preambleContentTimeout
	}
	if remaining, ok := d.firstTokenRemaining(); ok && remaining < firstContentBudget {
		firstContentBudget = remaining
	}
	chunkTimer := time.NewTimer(firstContentBudget)
	for {
		select {
		case chunk, ok := <-pr.ChunkCh:
			if ok && holdPreContentBoilerplate(pr, chunk, &d.heldChunks) {
				continue
			}
			chunkTimer.Stop()
			if ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
			} else {
				// Closed — check for error. Use a short grace
				// period instead of a non-blocking default to
				// close the race where Go's select picks the
				// ChunkCh close before the ErrorCh value (sent
				// by the provider handler before closing ChunkCh).
				select {
				case errMsg := <-pr.ErrorCh:
					d.excludeProviders[provider.ID] = struct{}{}
					s.cancelDispatchAfterTerminal(provider, pr)
					d.setLastInferenceError(provider, errMsg)
					d.lastFailedVersion = failedProviderVersion(provider)
					s.logger.Warn("provider failed after accepting request, retrying",
						"request_id", d.requestID,
						"provider_id", provider.ID,
						"attempt", d.attempt+1,
						"failure_code", errMsg.FailureCode,
					)
					s.emitRequest(r.Context(), protocol.SeverityWarn, d.requestID,
						"provider failed after accepting request, retrying",
						map[string]any{
							"provider_id": provider.ID,
							"attempt":     d.attempt + 1,
							"reason":      "provider_error",
							"status_code": errMsg.StatusCode,
						})
					if s.metrics != nil {
						s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "retry"})
					}
					d.noteDispatchRetry(provider, pr, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &d.heldChunks, errMsg.CoordinatorCause)
					d.provider = nil
					d.pr = nil
					return outcomeRetry
				case <-time.After(50 * time.Millisecond):
					d.committed = true
				}
			}
			return outcomeCommitted
		case errMsg := <-pr.ErrorCh:
			chunkTimer.Stop()
			if d.commitReadyFirstContent(pr, &d.heldChunks, errMsg) {
				return outcomeCommitted
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.cancelDispatchAfterTerminal(provider, pr)
			d.setLastInferenceError(provider, errMsg)
			d.lastFailedVersion = failedProviderVersion(provider)
			s.logger.Warn("provider failed after accepting request, retrying",
				"request_id", d.requestID,
				"provider_id", provider.ID,
				"attempt", d.attempt+1,
				"failure_code", errMsg.FailureCode,
			)
			s.emitRequest(r.Context(), protocol.SeverityWarn, d.requestID,
				"provider failed after accepting request, retrying",
				map[string]any{
					"provider_id": provider.ID,
					"attempt":     d.attempt + 1,
					"reason":      "provider_error",
					"status_code": errMsg.StatusCode,
				})
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "retry"})
			}
			d.noteDispatchRetry(provider, pr, errMsg.StatusCode, errMsg.Error, errMsg.ErrorReason, errMsg.TerminalCause, &d.heldChunks, errMsg.CoordinatorCause)
			d.provider = nil
			d.pr = nil
			return outcomeRetry
		case <-chunkTimer.C:
			if chunk, ok := drainReadyFirstContent(pr, &d.heldChunks); ok {
				d.commitFirstContent(pr, chunk.Data)
				d.committed = true
				return outcomeCommitted
			}
			if pr.FirstContentIngressArrivedByDeadline() {
				continue
			}
			if !s.cancelDispatchForFirstContentTimeout(provider, pr) {
				continue
			}
			d.excludeProviders[provider.ID] = struct{}{}
			s.registry.RecordWarmPoolTTFTMiss(d.model, firstContentBudget)
			// Accepted-then-silent (or preamble-then-stall) feeds the
			// breaker so a provider that repeatedly acks and stalls enters
			// cooldown — but ONLY when the provider was actually granted a
			// provider-attributable window. A budget capped short by the
			// request-absolute first-token clock is OUR deadline (queueing,
			// admission), not provider sickness.
			if providerAttemptAttributableStall(pr, firstContentBudget) {
				s.noteInferenceError(provider.ID, pr, http.StatusGatewayTimeout, "", "", "")
			}
			d.setLastError("provider accepted but timed out before first chunk", http.StatusGatewayTimeout)
			if d.preambleLiveness {
				d.setLastError("provider sent preamble but stalled before first content", http.StatusGatewayTimeout)
			}
			s.logger.Warn("provider timed out after accepting request, retrying",
				"request_id", d.requestID,
				"provider_id", provider.ID,
				"attempt", d.attempt+1,
				"preamble_liveness", d.preambleLiveness,
			)
			s.emitRequest(r.Context(), protocol.SeverityWarn, d.requestID,
				"provider accepted timeout",
				map[string]any{
					"provider_id": provider.ID,
					"attempt":     d.attempt + 1,
					"reason":      "accepted_timeout",
				})
			if s.metrics != nil {
				s.metrics.IncCounter("inference_dispatches_total", MetricLabel{"result", "timeout"})
			}
			s.ddIncr("inference.dispatches", []string{"status:timeout"})
			d.provider = nil
			d.pr = nil
			return outcomeRetry
		case <-r.Context().Done():
			s.cancelDispatch(provider, pr, cancelCauseClientGonePre)
			d.refundReservation()
			return outcomeClientGone
		}
	}
}

// writeCommittedResponse writes the provider attestation + timing headers, installs
// the park-before-remove settlement defer, and hands off to the streaming /
// non-streaming response writer. Extracted verbatim from the committed tail of the
// original handler.
// contentLatency is the time from dispatch to the first CONTENT chunk delivered
// to the client (FirstContentAt). It deliberately does NOT fall back to
// FirstChunkAt — that timestamp is also stamped on held role-only / lifecycle
// preamble, so using it would let a fast-preamble-then-stall provider (or a
// preamble-only clean close that produced no content) look artificially
// responsive. Returns 0 when no content was delivered or the timing is
// incomplete, which the caller treats as "no sample".
func contentLatency(t *registry.RequestTiming) time.Duration {
	if t == nil || t.DispatchedAt.IsZero() || t.FirstContentAt.IsZero() {
		return 0
	}
	if d := t.FirstContentAt.Sub(t.DispatchedAt); d > 0 {
		return d
	}
	return 0
}

// adjustLatencyForPrefill turns a raw time-to-first-content into the reputation
// latency sample by removing the prompt-size-dependent prefill. Time-to-first-
// token grows with the input length, so a provider serving long prompts would
// otherwise look slow purely because of its workload. Using the provider's own
// benchmarked prefill rate keeps the correction per-provider and free of
// hard-coded constants; what remains approximates queueing, scheduling,
// model-load and first-decode overhead. Returns 0 when there is no usable sample
// (which RecordLatency ignores), including when the prefill estimate exceeds the
// measured latency.
func adjustLatencyForPrefill(raw time.Duration, promptTokens int, prefillTPS float64) time.Duration {
	if raw <= 0 {
		return 0
	}
	if promptTokens > 0 && prefillTPS > 0 {
		raw -= time.Duration(float64(promptTokens) / prefillTPS * float64(time.Second))
	}
	if raw <= 0 {
		return 0
	}
	return raw
}

func shouldRecordReputationLatency(pr *registry.PendingRequest, firstChunk string) bool {
	return pr != nil && pr.Timing != nil && firstChunk != "" && !pr.CacheRoutingParticipates()
}

func (d *dispatchState) writeCommittedResponse() {
	s := d.s
	w, r := d.w, d.r
	provider, pr, requestID := d.provider, d.pr, d.requestID

	// Record the provider responsiveness sample here, in the goroutine that OWNS
	// pr.Timing. handleComplete runs in the provider read-loop goroutine and could
	// race this goroutine's timing writes, so the latency must be recorded from
	// here rather than handed across. d.firstChunk is non-empty only when an actual
	// content chunk was received — a preamble-then-clean-close commits with no
	// content, so FirstContentAt stays zero and no sample is recorded. The
	// prompt-size prefill is removed using the coordinator-side prompt estimate
	// (known up front, adequate for normalization) and the provider's benchmarked
	// PrefillTPS (set once at registration, read-only thereafter).
	if shouldRecordReputationLatency(pr, d.firstChunk) {
		// FirstContentAt was already stamped at the content-commit site
		// (commitFirstContent), earlier in THIS goroutine, so contentLatency reads
		// a set value here. No re-stamp needed; just read it for the reputation
		// latency sample.
		sample := adjustLatencyForPrefill(contentLatency(pr.Timing), pr.EstimatedPromptTokens, provider.PrefillTPS)
		// Provider-level: p.mu only. The registry-level form looks the
		// provider up under r.mu, and this runs before the first client write.
		provider.RecordLatency(sample)
	}

	// Write provider attestation headers now that we're committed. When the
	// caller opted into metadata_details, snapshot the same consumer-safe
	// fields onto the pending request so chat-completions writers can attach
	// them to the JSON body (OpenAI SDKs often hide custom headers).
	info := collectCommittedProviderInfo(provider)
	writeCommittedProviderHeaders(w, info)
	d.writeTimingHeaderWithProfile(w, pr)
	d.stampCommitted(pr)
	writeInferenceJobIDHeader(w, pr.RequestID)
	snapshotChatCompletionMetadata(pr, info)

	// On return (disconnect/timeout/completion): free the slot, tell the
	// provider to stop if it may still be generating, and preserve billing for
	// a mid-stream disconnect.
	// Park BEFORE RemovePending so a racing provider terminal always finds the
	// record in pending or the holder — never neither (which would drop it and
	// mis-refund). GetPending is nil if a terminal already settled it (normal
	// completion), so nothing is parked then. Both settle paths are
	// FinalizeReservation-guarded, so the park-then-remove overlap can't double-bill.
	//
	// The cancel is sent only when a pending record still existed — no terminal
	// seen, so the provider may still be running (consumer gone mid-stream, idle
	// stream timeout). After a clean completion or a provider error terminal the
	// record is already gone and a cancel would only cost the provider a no-op
	// frame per request (~one per dispatch fleet-wide before this rule).
	defer func() {
		abandoned := false
		cause := cancelCauseStreamTimeout
		if r.Context().Err() != nil {
			cause = cancelCauseClientGonePost
		}
		if stale := provider.GetPending(requestID); stale != nil {
			// Record the abandon BEFORE parking so a terminal racing this
			// defer is correlated with the cancel rather than logged as unknown.
			_, expired := s.zombieCanceller.record(requestID, pr.Model, cause, time.Now())
			s.emitExpiredCancelEntries(expired)
			s.holdForSettlement(stale)
			abandoned = true
		} else {
			// A terminal already claimed the pending. In every normal path the
			// reservation is finalized by now (completion billed it, the relay
			// error/timeout branches refunded it) and this is a no-op. The one
			// exception is a provider error landing in the gap between this
			// handler abandoning its channels and this defer running: that
			// terminal pushed into an unread ErrorCh and nobody settled — sweep
			// it here. Post-commit only, so it can never finalize a reservation
			// the dispatch loop still needs for a retry attempt.
			refundPr := pr
			saferun.Go(s.logger, "api.postTerminalSweep", func() {
				s.refundReservedBalance(refundPr, "post_terminal_sweep:"+requestID)
			})
		}
		removed := provider.RemovePending(requestID) // then remove so SetProviderIdle frees the slot
		s.registry.SetProviderIdle(provider.ID)
		if !abandoned {
			return
		}
		if removed == nil {
			// A terminal claimed the record between GetPending and
			// RemovePending: it settles via the parked copy and nothing is
			// running provider-side.
			s.zombieCanceller.forget(requestID)
			return
		}
		// The provider is still generating for a client that is gone: this
		// cancel is the one that stops real work, so stamp it.
		pr.Profile.Mark(registry.StampCancelSent)
		s.sendRecordedCancel(provider, requestID, pr.Model, cause)
	}()

	// The committed provider's held preamble chunks stream out first, in
	// arrival order, ahead of the content chunk that committed the dispatch.
	firstChunks := d.heldChunks
	if d.firstChunk != "" {
		firstChunks = append(firstChunks, d.firstChunk)
	}
	if d.stream {
		s.handleStreamingResponseWithFirstChunkAndError(
			w, r, pr, firstChunks, d.initialError)
	} else {
		// Record the OR-uptime outcome from the status the non-streaming writer
		// actually emits: it can still return a 5xx/504 after commit, and a
		// client-gone exit writes no status (0 → not counted, cancelled is excluded).
		// statusWriter (server.go) captures the WriteHeader code and transparently
		// delegates Flush/Hijack/Unwrap, so wrapping preserves the writer's
		// capabilities; zero-valued status starts at 0 (uncounted).
		sw := &statusWriter{ResponseWriter: w}
		s.handleNonStreamingResponseWithFirstChunkAndError(
			sw, r, pr, firstChunks, d.initialError)
		switch {
		case sw.status == http.StatusOK:
			d.recordDispatchedRequestOutcome(d.kvBackendAttribution(), orClassSuccess)
		case sw.status > 0:
			d.recordDispatchedRequestOutcome(
				d.kvBackendAttribution(), classifyOutcomeByCode(sw.status))
		}
	}
}
