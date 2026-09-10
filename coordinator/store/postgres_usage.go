package store

// Usage records, routes, rejections, payments, and totals.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// RecordUsage inserts a usage record into PostgreSQL.
func (s *PostgresStore) RecordUsage(providerID, consumerKey, model string, promptTokens, completionTokens int) {
	h := hashKey(consumerKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = s.pool.Exec(ctx,
		`WITH ins AS (
			INSERT INTO usage (provider_id, consumer_key_hash, model, prompt_tokens, completion_tokens)
			VALUES ($1, $2, $3, $4, $5)
		)
		UPDATE usage_totals SET
			total_requests = total_requests + 1,
			total_prompt_tokens = total_prompt_tokens + $4,
			total_completion_tokens = total_completion_tokens + $5
		WHERE id = 1`,
		providerID, h, model, promptTokens, completionTokens,
	)
}

// UsageByConsumer returns usage records for a specific consumer key.
func (s *PostgresStore) UsageByConsumer(consumerKey string) []UsageRecord {
	h := hashKey(consumerKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT provider_id, consumer_key_hash, model, public_model, prompt_tokens, completion_tokens, created_at, request_id, cost_micro_usd
			 FROM usage WHERE consumer_key_hash = $1 ORDER BY created_at DESC LIMIT 100`, h)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var records []UsageRecord
	for rows.Next() {
		var r UsageRecord
		if err := rows.Scan(&r.ProviderID, &r.ConsumerKey, &r.Model, &r.PublicModel, &r.PromptTokens, &r.CompletionTokens, &r.CreatedAt, &r.RequestID, &r.CostMicroUSD); err != nil {
			continue
		}
		records = append(records, r)
	}
	return records
}

// RecordUsageWithCost inserts a usage record with request ID and cost.
func (s *PostgresStore) RecordUsageWithCost(providerID, consumerKey, model, requestID string, promptTokens, completionTokens int, costMicroUSD int64) {
	s.RecordUsageWithCostAndLocation(providerID, consumerKey, model, requestID, promptTokens, completionTokens, costMicroUSD, nil)
}

// RecordUsageWithCostAndLocation inserts a usage record with request ID, cost,
// and approximate request-origin location.
func (s *PostgresStore) RecordUsageWithCostAndLocation(providerID, consumerKey, model, requestID string, promptTokens, completionTokens int, costMicroUSD int64, requestLocation *ProviderLocation) {
	s.RecordUsageFull(providerID, consumerKey, "", model, requestID, promptTokens, completionTokens, costMicroUSD, requestLocation)
}

// RecordUsageFull inserts a usage record with full attribution including the
// originating API key ID for per-key usage and spend tracking.
func (s *PostgresStore) RecordUsageFull(providerID, consumerKey, keyID, model, requestID string, promptTokens, completionTokens int, costMicroUSD int64, requestLocation *ProviderLocation) {
	s.RecordUsageFullWithPublicModel(providerID, consumerKey, keyID, model, "", requestID, promptTokens, completionTokens, costMicroUSD, requestLocation)
}

// RecordUsageFullWithPublicModel inserts a usage record with full attribution,
// storing both the concrete billing model and optional public display model.
func (s *PostgresStore) RecordUsageFullWithPublicModel(providerID, consumerKey, keyID, model, publicModel, requestID string, promptTokens, completionTokens int, costMicroUSD int64, requestLocation *ProviderLocation) {
	h := hashKey(consumerKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = s.pool.Exec(ctx,
		`WITH ins AS (
			INSERT INTO usage (provider_id, consumer_key_hash, key_id, model, public_model, prompt_tokens, completion_tokens, request_id, cost_micro_usd, request_location)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		)
		UPDATE usage_totals SET
			total_requests = total_requests + 1,
			total_prompt_tokens = total_prompt_tokens + $6,
			total_completion_tokens = total_completion_tokens + $7
		WHERE id = 1`,
		providerID, h, keyID, model, publicModel, promptTokens, completionTokens, requestID, costMicroUSD, marshalProviderLocation(requestLocation),
	)
}

const inferenceRouteSelectColumns = `
			id,
			request_id, attempt, provider_id, model, public_model, consumer_key_hash, key_id, outcome,
			cost_ms, state_ms, queue_ms, pending_ms, backlog_ms, this_req_ms, health_ms, ttft_ms, best_ttft_ms,
			effective_queue, candidate_count, capacity_rejections, model_too_large_rejections, vision_rejections, ttft_rejections,
			effective_tps, static_tps, provider_status, provider_trust_level, provider_version,
			hardware_chip, hardware_chip_family, hardware_tier, memory_gb, gpu_cores, cpu_cores,
			system_memory_pressure, system_cpu_usage, system_thermal_state,
			gpu_memory_active_gb, gpu_memory_peak_gb, gpu_memory_cache_gb,
			slot_state, backend_running, backend_waiting,
			active_token_budget_used, active_token_budget_max, queued_token_budget,
			estimated_prompt_tokens, requested_max_tokens,
			requires_vision, has_tools, self_route_only, prefer_owner,
			final_status, error_code, error_class, prompt_tokens, completion_tokens, reasoning_tokens, cost_micro_usd,
			actual_ttft_ms, dispatch_to_first_chunk_ms, total_duration_ms,
			created_at, updated_at,
			provider_region, consumer_region,
			parse_ms, reserve_ms, route_ms, encrypt_ms, queue_wait_ms, dispatch_ms, actual_decode_tps,
			admitted_but_failed, used_backup, backup_won, error_reason`

// InferenceRouteRecordsSince returns routing records created at or after the
// given time. Zero since returns all records.
func (s *PostgresStore) InferenceRouteRecordsSince(since time.Time) []InferenceRouteRecord {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT `+inferenceRouteSelectColumns+` FROM inference_routes WHERE created_at >= $1 ORDER BY created_at DESC LIMIT $2`,
		since, maxTelemetryReadRows)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var records []InferenceRouteRecord
	for rows.Next() {
		var r InferenceRouteRecord
		var id int64
		var finalStatus string
		var errorCode *int
		var errorClass *string
		var errorReason *string
		var promptTokens *int
		var completionTokens *int
		var reasoningTokens *int
		var costMicroUSD *int64
		var actualTTFTMs *float64
		var dispatchToFirstChunkMs *float64
		var totalDurationMs *float64
		var providerRegion *string
		var consumerRegion *string
		var parseMs *float64
		var reserveMs *float64
		var routeMs *float64
		var encryptMs *float64
		var queueWaitMs *float64
		var dispatchMs *float64
		var actualDecodeTPS *float64
		var admittedButFailed *bool
		var usedBackup *bool
		var backupWon *bool

		if err := rows.Scan(
			&id,
			&r.RequestID, &r.Attempt, &r.ProviderID, &r.Model, &r.PublicModel, &r.ConsumerKeyHash, &r.KeyID, &r.Outcome,
			&r.CostMs, &r.StateMs, &r.QueueMs, &r.PendingMs, &r.BacklogMs, &r.ThisReqMs, &r.HealthMs, &r.TTFTMs, &r.BestTTFTMs,
			&r.EffectiveQueue, &r.CandidateCount, &r.CapacityRejections, &r.ModelTooLargeRejections, &r.VisionRejections, &r.TTFTRejections,
			&r.EffectiveTPS, &r.StaticTPS, &r.ProviderStatus, &r.ProviderTrustLevel, &r.ProviderVersion,
			&r.HardwareChip, &r.HardwareChipFamily, &r.HardwareTier, &r.MemoryGB, &r.GPUCores, &r.CPUCores,
			&r.SystemMemoryPressure, &r.SystemCPUUsage, &r.SystemThermalState,
			&r.GPUMemoryActiveGB, &r.GPUMemoryPeakGB, &r.GPUMemoryCacheGB,
			&r.SlotState, &r.BackendRunning, &r.BackendWaiting,
			&r.ActiveTokenBudgetUsed, &r.ActiveTokenBudgetMax, &r.QueuedTokenBudget,
			&r.EstimatedPromptTokens, &r.RequestedMaxTokens,
			&r.RequiresVision, &r.HasTools, &r.SelfRouteOnly, &r.PreferOwner,
			&finalStatus, &errorCode, &errorClass, &promptTokens, &completionTokens, &reasoningTokens, &costMicroUSD,
			&actualTTFTMs, &dispatchToFirstChunkMs, &totalDurationMs,
			&r.CreatedAt, &r.UpdatedAt,
			&providerRegion, &consumerRegion,
			&parseMs, &reserveMs, &routeMs, &encryptMs, &queueWaitMs, &dispatchMs, &actualDecodeTPS,
			&admittedButFailed, &usedBackup, &backupWon, &errorReason,
		); err != nil {
			continue
		}
		if providerRegion != nil {
			r.ProviderRegion = *providerRegion
		}
		if consumerRegion != nil {
			r.ConsumerRegion = *consumerRegion
		}
		outcome := InferenceRouteOutcome{FinalStatus: finalStatus}
		if errorCode != nil {
			outcome.ErrorCode = *errorCode
		}
		if errorClass != nil {
			outcome.ErrorClass = *errorClass
		}
		if errorReason != nil {
			outcome.ErrorReason = *errorReason
		}
		if promptTokens != nil {
			outcome.PromptTokens = *promptTokens
		}
		if completionTokens != nil {
			outcome.CompletionTokens = *completionTokens
		}
		if reasoningTokens != nil {
			outcome.ReasoningTokens = *reasoningTokens
		}
		if costMicroUSD != nil {
			outcome.CostMicroUSD = *costMicroUSD
		}
		if actualTTFTMs != nil {
			outcome.ActualTTFTMs = *actualTTFTMs
		}
		if dispatchToFirstChunkMs != nil {
			outcome.DispatchToFirstChunkMs = *dispatchToFirstChunkMs
		}
		if totalDurationMs != nil {
			outcome.TotalDurationMs = *totalDurationMs
		}
		if parseMs != nil {
			outcome.ParseMs = *parseMs
		}
		if reserveMs != nil {
			outcome.ReserveMs = *reserveMs
		}
		if routeMs != nil {
			outcome.RouteMs = *routeMs
		}
		if encryptMs != nil {
			outcome.EncryptMs = *encryptMs
		}
		if queueWaitMs != nil {
			outcome.QueueWaitMs = *queueWaitMs
		}
		if dispatchMs != nil {
			outcome.DispatchMs = *dispatchMs
		}
		if actualDecodeTPS != nil {
			outcome.ActualDecodeTPS = *actualDecodeTPS
		}
		if admittedButFailed != nil {
			outcome.AdmittedButFailed = *admittedButFailed
		}
		if usedBackup != nil {
			outcome.UsedBackup = *usedBackup
		}
		if backupWon != nil {
			outcome.BackupWon = *backupWon
		}
		applyInferenceRouteOutcomeToRecord(&r, outcome)
		records = append(records, r)
	}
	return records
}

// RecordRejection writes a rejected-request record with its counterfactual
// servability snapshot. Best-effort; failures are discarded and never block
// the request path.
func (s *PostgresStore) RecordRejection(record *RejectionRecord) error {
	if record == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createdAt := record.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	// Mirror marshalProviderLocation's JSONB handling: pass nil (→ SQL NULL)
	// when there are no params so we never write an invalid empty JSONB value.
	var params json.RawMessage
	if len(record.Params) > 0 {
		params = record.Params
	}

	_, _ = s.pool.Exec(ctx,
		`INSERT INTO request_rejections (
			request_id, endpoint, stage, reason_code, http_status, consumer_key_hash, key_id, client_class,
			requested_model, resolved_model, stream, n, estimated_prompt_tokens, requested_max_tokens,
			requires_vision, has_image, has_audio, has_tools, tool_count, response_format, self_route_only, prefer_owner,
			params, request_body_bytes, retry_after_ms,
			could_have_served, candidate_count, capacity_rejections, model_too_large_rejections, vision_rejections,
			warm_provider_existed, best_ttft_ms, shortfall_micro_usd, limit_kind, over_by,
			created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20, $21, $22,
			$23, $24, $25,
			$26, $27, $28, $29, $30,
			$31, $32, $33, $34, $35,
			$36
		)`,
		record.RequestID, record.Endpoint, record.Stage, record.ReasonCode, record.HTTPStatus, record.ConsumerKeyHash, record.KeyID, record.ClientClass,
		record.RequestedModel, record.ResolvedModel, record.Stream, record.N, record.EstimatedPromptTokens, record.RequestedMaxTokens,
		record.RequiresVision, record.HasImage, record.HasAudio, record.HasTools, record.ToolCount, record.ResponseFormat, record.SelfRouteOnly, record.PreferOwner,
		params, record.RequestBodyBytes, record.RetryAfterMs,
		record.CouldHaveServed, record.CandidateCount, record.CapacityRejections, record.ModelTooLargeRejections, record.VisionRejections,
		record.WarmProviderExisted, record.BestTTFTMs, record.ShortfallMicroUSD, record.LimitKind, record.OverBy,
		createdAt,
	)
	return nil
}

// RejectionRecordsSince returns rejection records created at or after the given
// time. Zero since returns all records.
func (s *PostgresStore) RejectionRecordsSince(since time.Time) []RejectionRecord {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT * FROM request_rejections WHERE created_at >= $1 ORDER BY created_at DESC LIMIT $2`,
		since, maxTelemetryReadRows)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var records []RejectionRecord
	for rows.Next() {
		var r RejectionRecord
		var id int64
		var paramsRaw []byte

		if err := rows.Scan(
			&id,
			&r.RequestID, &r.Endpoint, &r.Stage, &r.ReasonCode, &r.HTTPStatus, &r.ConsumerKeyHash, &r.KeyID, &r.ClientClass,
			&r.RequestedModel, &r.ResolvedModel, &r.Stream, &r.N, &r.EstimatedPromptTokens, &r.RequestedMaxTokens,
			&r.RequiresVision, &r.HasImage, &r.HasAudio, &r.HasTools, &r.ToolCount, &r.ResponseFormat, &r.SelfRouteOnly, &r.PreferOwner,
			&paramsRaw, &r.RequestBodyBytes, &r.RetryAfterMs,
			&r.CouldHaveServed, &r.CandidateCount, &r.CapacityRejections, &r.ModelTooLargeRejections, &r.VisionRejections,
			&r.WarmProviderExisted, &r.BestTTFTMs, &r.ShortfallMicroUSD, &r.LimitKind, &r.OverBy,
			&r.CreatedAt,
		); err != nil {
			continue
		}
		if len(paramsRaw) > 0 {
			r.Params = paramsRaw
		}
		records = append(records, r)
	}
	return records
}

// RecordPayment inserts a payment record into PostgreSQL.
func (s *PostgresStore) RecordPayment(txHash, consumerAddr, providerAddr, amountUSD, model string, promptTokens, completionTokens int, memo string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO payments (tx_hash, consumer_address, provider_address, amount_usd, model, prompt_tokens, completion_tokens, memo)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		txHash, consumerAddr, providerAddr, amountUSD, model, promptTokens, completionTokens, memo,
	)
	if err != nil {
		return fmt.Errorf("store: insert payment: %w", err)
	}
	return nil
}

// UsageCountSince returns the number of usage records created at or after the
// given time. Uses idx_usage_created for an index-only count. A statement that
// cannot complete is reported as an error, never as a zero count.
func (s *PostgresStore) UsageCountSince(since time.Time) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int64
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM usage
		 WHERE ($1::timestamptz IS NULL OR created_at >= $1)`,
		nullSince(since),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("store: usage count: %w", err)
	}
	return count, nil
}

// UsageTotals returns aggregated lifetime totals from the materialized
// usage_totals counter row. This is a single PK lookup — O(1) regardless
// of how many rows exist in the usage table. A statement that cannot complete
// is reported as an error, never as zero totals; a database with no counter
// row yet (before the usage_totals migration) genuinely has zero totals.
func (s *PostgresStore) UsageTotals() (UsageTotals, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var t UsageTotals
	err := s.pool.QueryRow(ctx,
		`SELECT total_requests, total_prompt_tokens, total_completion_tokens
		 FROM usage_totals WHERE id = 1`,
	).Scan(&t.Requests, &t.PromptTokens, &t.CompletionTokens)
	if errors.Is(err, pgx.ErrNoRows) {
		return UsageTotals{}, nil
	}
	if err != nil {
		return UsageTotals{}, fmt.Errorf("store: usage totals: %w", err)
	}
	return t, nil
}

// UsageTotalsSince returns aggregate usage at or after `since`. A statement
// that cannot complete is reported as an error, never as zero totals.
func (s *PostgresStore) UsageTotalsSince(since time.Time) (UsageTotals, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var t UsageTotals
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*),
		        COALESCE(SUM(prompt_tokens), 0),
		        COALESCE(SUM(completion_tokens), 0)
		 FROM usage
		 WHERE created_at >= $1`,
		since,
	).Scan(&t.Requests, &t.PromptTokens, &t.CompletionTokens); err != nil {
		return UsageTotals{}, fmt.Errorf("store: usage totals since: %w", err)
	}
	return t, nil
}

// UsageTimeSeries returns usage buckets at or after `since` using a bounded,
// caller-selected interval so long windows do not return tens of thousands of
// minute rows. A statement that cannot complete — including one that times
// out mid-iteration — is reported as an error, never as a partial series.
func (s *PostgresStore) UsageTimeSeries(since, until time.Time, bucketSize time.Duration) ([]UsageBucket, error) {
	since, until, bucketSize = normalizeUsageTimeSeriesRequest(since, until, bucketSize, time.Now())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`WITH bounded AS (
		   SELECT to_timestamp(
		            floor(extract(epoch FROM created_at) / $3::double precision) * $3::double precision
		          ) AS bucket_start,
		          COUNT(*) AS requests,
		          COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
		          COALESCE(SUM(completion_tokens), 0) AS completion_tokens
		   FROM usage
		   WHERE created_at >= $1 AND created_at < $2
		   GROUP BY 1
		   ORDER BY 1 DESC
		   LIMIT $4
		 )
		 SELECT bucket_start, requests, prompt_tokens, completion_tokens
		 FROM bounded
		 ORDER BY bucket_start ASC`,
		since,
		until,
		bucketSize.Seconds(),
		usageTimeSeriesMaxBuckets,
	)
	if err != nil {
		return nil, fmt.Errorf("store: usage time series: %w", err)
	}
	defer rows.Close()

	var buckets []UsageBucket
	for rows.Next() {
		var b UsageBucket
		if err := rows.Scan(&b.Minute, &b.Requests, &b.PromptTokens, &b.CompletionTokens); err != nil {
			return nil, fmt.Errorf("store: usage time series: scan: %w", err)
		}
		buckets = append(buckets, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: usage time series: %w", err)
	}
	return limitUsageTimeSeriesBuckets(buckets), nil
}

// rewardLedgerTypesSQLList renders RewardLedgerTypes as a comma-separated list
// of single-quoted SQL string literals (e.g. "'referral_reward','admin_reward'")
// for use in an IN (...) clause. The values are package constants, never user
// input, so literal interpolation here is safe from SQL injection.
func rewardLedgerTypesSQLList() string {
	out := ""
	for i, t := range RewardLedgerTypes {
		if i > 0 {
			out += ","
		}
		out += "'" + string(t) + "'"
	}
	return out
}

// Leaderboard returns the top N accounts ranked by the given metric over the
// given time window. Base-reward rows live in provider_earnings for
// provider-facing history, but count as reward earnings here so they do not
// inflate inference work/jobs/tokens. Ledger reward-only accounts (e.g.
// consumer-only referrers) never appear on the provider leaderboard.
func (s *PostgresStore) Leaderboard(metric LeaderboardMetric, since time.Time, limit int) []LeaderboardRow {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if limit <= 0 || limit > 200 {
		limit = 50
	}

	orderCol := "earnings_micro_usd"
	switch metric {
	case LeaderboardTokens:
		orderCol = "tokens"
	case LeaderboardJobs:
		orderCol = "jobs"
	}

	// `since` is bound once as $1 and referenced in both CTEs; `limit` is the
	// final positional arg. account_id != '' filters out unassigned earnings.
	args := []any{}
	workWhere := ` WHERE account_id != '' AND model <> 'base_reward'`
	baseRewardWhere := ` WHERE account_id != '' AND model = 'base_reward'`
	rewardSince := ""
	if !since.IsZero() {
		args = append(args, since)
		workWhere += ` AND created_at >= $1`
		baseRewardWhere += ` AND created_at >= $1`
		rewardSince = ` AND created_at >= $1`
	}

	q := `WITH work AS (
	          SELECT account_id,
		                 SUM(amount_micro_usd)                  AS work_micro,
		                 SUM(prompt_tokens + completion_tokens) AS tokens,
		                 COUNT(*)                               AS jobs
		          FROM provider_earnings` + workWhere + `
		          GROUP BY account_id
		      ),
		      base_reward AS (
	          SELECT account_id,
	                 SUM(amount_micro_usd) AS reward_micro
	          FROM provider_earnings` + baseRewardWhere + `
	          GROUP BY account_id
	      ),
	      reward AS (
	          SELECT account_id,
	                 SUM(amount_micro_usd) AS reward_micro
	          FROM ledger_entries
	          WHERE account_id != '' AND entry_type IN (` + rewardLedgerTypesSQLList() + `)` + rewardSince + `
	          GROUP BY account_id
	      )
	      SELECT COALESCE(w.account_id, br.account_id)  AS account_id,
	             COALESCE(w.work_micro,0) + COALESCE(br.reward_micro,0) + COALESCE(r.reward_micro,0) AS earnings_micro_usd,
	             COALESCE(w.work_micro,0)                AS work_micro_usd,
	             COALESCE(br.reward_micro,0) + COALESCE(r.reward_micro,0) AS reward_micro_usd,
	             COALESCE(w.tokens,0)                    AS tokens,
	             COALESCE(w.jobs,0)                      AS jobs
	      FROM work w
	      FULL OUTER JOIN base_reward br ON br.account_id = w.account_id
	      LEFT JOIN reward r ON r.account_id = COALESCE(w.account_id, br.account_id)
	      WHERE COALESCE(w.account_id, br.account_id) IS NOT NULL
	      ORDER BY ` + orderCol + ` DESC, account_id ASC
	      LIMIT $` + strconv.Itoa(len(args)+1)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()

	out := make([]LeaderboardRow, 0, limit)
	for rows.Next() {
		var r LeaderboardRow
		if err := rows.Scan(&r.AccountID, &r.EarningsMicroUSD, &r.WorkEarningsMicroUSD, &r.RewardEarningsMicroUSD, &r.Tokens, &r.Jobs); err != nil {
			continue
		}
		out = append(out, r)
	}
	return out
}

// UsageRecords returns usage records from the database, ordered by creation time.
// Limited to the most recent 10000 rows as a safety guard against unbounded reads.
func (s *PostgresStore) UsageRecords() []UsageRecord {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT provider_id, consumer_key_hash, model, public_model, prompt_tokens, completion_tokens, created_at, request_id, cost_micro_usd, request_location
			 FROM usage ORDER BY created_at DESC LIMIT 10000`,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var records []UsageRecord
	for rows.Next() {
		var r UsageRecord
		var locationRaw []byte
		if err := rows.Scan(
			&r.ProviderID,
			&r.ConsumerKey,
			&r.Model,
			&r.PublicModel,
			&r.PromptTokens,
			&r.CompletionTokens,
			&r.Timestamp,
			&r.RequestID,
			&r.CostMicroUSD,
			&locationRaw,
		); err != nil {
			continue
		}
		r.CreatedAt = r.Timestamp
		r.RequestLocation = unmarshalProviderLocation(locationRaw)
		records = append(records, r)
	}
	if records == nil {
		records = make([]UsageRecord, 0)
	}
	return records
}

// UsageRecordsSince returns usage records created at or after the given time.
func (s *PostgresStore) UsageRecordsSince(since time.Time) []UsageRecord {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT provider_id, consumer_key_hash, model, public_model, prompt_tokens, completion_tokens, created_at, request_id, cost_micro_usd, request_location
		 FROM usage
		 WHERE ($1::timestamptz IS NULL OR created_at >= $1)
		 ORDER BY created_at ASC`,
		nullSince(since),
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var records []UsageRecord
	for rows.Next() {
		var r UsageRecord
		var locationRaw []byte
		if err := rows.Scan(
			&r.ProviderID,
			&r.ConsumerKey,
			&r.Model,
			&r.PublicModel,
			&r.PromptTokens,
			&r.CompletionTokens,
			&r.Timestamp,
			&r.RequestID,
			&r.CostMicroUSD,
			&locationRaw,
		); err != nil {
			continue
		}
		r.CreatedAt = r.Timestamp
		r.RequestLocation = unmarshalProviderLocation(locationRaw)
		records = append(records, r)
	}
	if records == nil {
		return []UsageRecord{}
	}
	return records
}
