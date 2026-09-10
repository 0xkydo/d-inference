package store

// Provider records, trust state, budgets, and sessions.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func marshalProviderLocation(loc *ProviderLocation) json.RawMessage {
	if loc == nil {
		return nil
	}
	b, err := json.Marshal(loc)
	if err != nil {
		return nil
	}
	return b
}

func unmarshalProviderLocation(raw []byte) *ProviderLocation {
	if len(raw) == 0 {
		return nil
	}
	var loc ProviderLocation
	if err := json.Unmarshal(raw, &loc); err != nil {
		return nil
	}
	return &loc
}

func providerStatsJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}

func (s *PostgresStore) UpsertProvider(ctx context.Context, p ProviderRecord) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO providers (
			id, hardware, models, backend, location, trust_level, attested,
			attestation_result, se_public_key, serial_number,
			mda_verified, mda_cert_chain,
			version, runtime_verified, python_hash, runtime_hash,
			last_challenge_verified, failed_challenges, account_id,
			lifetime_requests_served, lifetime_tokens_generated,
			last_session_requests_served, last_session_tokens_generated,
			lifetime_stats, last_session_stats,
			registered_at, last_seen, public_key
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7,
			$8, $9, $10,
			$11, $12,
			$13, $14, $15, $16,
			$17, $18, $19,
			$20, $21, $22, $23,
			$24, $25,
			$26, $27, $28
		)
		ON CONFLICT (id) DO UPDATE SET
			hardware = $2, models = $3, backend = $4, location = $5,
			trust_level = $6, attested = $7,
			attestation_result = $8, se_public_key = $9, serial_number = $10,
			mda_verified = $11, mda_cert_chain = $12,
			version = $13, runtime_verified = $14, python_hash = $15, runtime_hash = $16,
			last_challenge_verified = $17, failed_challenges = $18, account_id = $19,
			lifetime_requests_served = $20, lifetime_tokens_generated = $21,
			last_session_requests_served = $22, last_session_tokens_generated = $23,
			lifetime_stats = $24, last_session_stats = $25,
			last_seen = $27, public_key = $28`,
		p.ID, p.Hardware, p.Models, p.Backend,
		marshalProviderLocation(p.Location),
		p.TrustLevel, p.Attested,
		p.AttestationResult, p.SEPublicKey, p.SerialNumber,
		p.MDAVerified, p.MDACertChain,
		p.Version, p.RuntimeVerified, p.PythonHash, p.RuntimeHash,
		p.LastChallengeVerified, p.FailedChallenges, p.AccountID,
		p.LifetimeRequestsServed, p.LifetimeTokensGenerated,
		p.LastSessionRequestsServed, p.LastSessionTokensGenerated,
		providerStatsJSON(p.LifetimeStats), providerStatsJSON(p.LastSessionStats),
		p.RegisteredAt, p.LastSeen, p.PublicKey,
	)
	if err != nil {
		return fmt.Errorf("store: upsert provider: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetProviderRecord(ctx context.Context, id string) (*ProviderRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var p ProviderRecord
	var locationRaw []byte
	err := s.pool.QueryRow(ctx,
		`SELECT id, hardware, models, backend, location, trust_level, attested,
			attestation_result, se_public_key, serial_number,
			mda_verified, mda_cert_chain,
			version, runtime_verified, python_hash, runtime_hash,
			last_challenge_verified, failed_challenges, account_id,
			lifetime_requests_served, lifetime_tokens_generated,
			last_session_requests_served, last_session_tokens_generated,
			lifetime_stats, last_session_stats,
			registered_at, last_seen, public_key
		 FROM providers WHERE id = $1`, id,
	).Scan(
		&p.ID, &p.Hardware, &p.Models, &p.Backend,
		&locationRaw,
		&p.TrustLevel, &p.Attested,
		&p.AttestationResult, &p.SEPublicKey, &p.SerialNumber,
		&p.MDAVerified, &p.MDACertChain,
		&p.Version, &p.RuntimeVerified, &p.PythonHash, &p.RuntimeHash,
		&p.LastChallengeVerified, &p.FailedChallenges, &p.AccountID,
		&p.LifetimeRequestsServed, &p.LifetimeTokensGenerated,
		&p.LastSessionRequestsServed, &p.LastSessionTokensGenerated,
		&p.LifetimeStats, &p.LastSessionStats,
		&p.RegisteredAt, &p.LastSeen, &p.PublicKey,
	)
	if err != nil {
		return nil, fmt.Errorf("store: provider not found: %w", err)
	}
	p.Location = unmarshalProviderLocation(locationRaw)
	return &p, nil
}

func (s *PostgresStore) GetProviderBySerial(ctx context.Context, serial string) (*ProviderRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var p ProviderRecord
	var locationRaw []byte
	err := s.pool.QueryRow(ctx,
		`SELECT id, hardware, models, backend, location, trust_level, attested,
			attestation_result, se_public_key, serial_number,
			mda_verified, mda_cert_chain,
			version, runtime_verified, python_hash, runtime_hash,
			last_challenge_verified, failed_challenges, account_id,
			lifetime_requests_served, lifetime_tokens_generated,
			last_session_requests_served, last_session_tokens_generated,
			lifetime_stats, last_session_stats,
			registered_at, last_seen, public_key
		 FROM providers WHERE serial_number = $1 AND serial_number != ''
		 ORDER BY last_seen DESC LIMIT 1`, serial,
	).Scan(
		&p.ID, &p.Hardware, &p.Models, &p.Backend,
		&locationRaw,
		&p.TrustLevel, &p.Attested,
		&p.AttestationResult, &p.SEPublicKey, &p.SerialNumber,
		&p.MDAVerified, &p.MDACertChain,
		&p.Version, &p.RuntimeVerified, &p.PythonHash, &p.RuntimeHash,
		&p.LastChallengeVerified, &p.FailedChallenges, &p.AccountID,
		&p.LifetimeRequestsServed, &p.LifetimeTokensGenerated,
		&p.LastSessionRequestsServed, &p.LastSessionTokensGenerated,
		&p.LifetimeStats, &p.LastSessionStats,
		&p.RegisteredAt, &p.LastSeen, &p.PublicKey,
	)
	if err != nil {
		return nil, fmt.Errorf("store: provider with serial not found: %w", err)
	}
	p.Location = unmarshalProviderLocation(locationRaw)
	return &p, nil
}

func (s *PostgresStore) GetMDAChainBySerial(ctx context.Context, serial string) (json.RawMessage, error) {
	if serial == "" {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Newest NON-EMPTY chain for the serial — skips a reconnect's empty row that
	// would otherwise shadow a still-valid chain from a prior connection.
	var chain json.RawMessage
	err := s.pool.QueryRow(ctx,
		`SELECT mda_cert_chain FROM providers
		 WHERE serial_number = $1 AND serial_number != '' AND mda_cert_chain IS NOT NULL
		 ORDER BY last_seen DESC LIMIT 1`, serial,
	).Scan(&chain)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: get mda chain by serial: %w", err)
	}
	return chain, nil
}

func (s *PostgresStore) ListProviderRecords(ctx context.Context) ([]ProviderRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT id, hardware, models, backend, location, trust_level, attested,
			attestation_result, se_public_key, serial_number,
			mda_verified, mda_cert_chain,
			version, runtime_verified, python_hash, runtime_hash,
			last_challenge_verified, failed_challenges, account_id,
			lifetime_requests_served, lifetime_tokens_generated,
			last_session_requests_served, last_session_tokens_generated,
			lifetime_stats, last_session_stats,
			registered_at, last_seen, public_key
		 FROM providers ORDER BY last_seen DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list providers: %w", err)
	}
	defer rows.Close()

	var records []ProviderRecord
	for rows.Next() {
		var p ProviderRecord
		var locationRaw []byte
		if err := rows.Scan(
			&p.ID, &p.Hardware, &p.Models, &p.Backend,
			&locationRaw,
			&p.TrustLevel, &p.Attested,
			&p.AttestationResult, &p.SEPublicKey, &p.SerialNumber,
			&p.MDAVerified, &p.MDACertChain,
			&p.Version, &p.RuntimeVerified, &p.PythonHash, &p.RuntimeHash,
			&p.LastChallengeVerified, &p.FailedChallenges, &p.AccountID,
			&p.LifetimeRequestsServed, &p.LifetimeTokensGenerated,
			&p.LastSessionRequestsServed, &p.LastSessionTokensGenerated,
			&p.LifetimeStats, &p.LastSessionStats,
			&p.RegisteredAt, &p.LastSeen, &p.PublicKey,
		); err != nil {
			continue
		}
		p.Location = unmarshalProviderLocation(locationRaw)
		records = append(records, p)
	}
	if records == nil {
		return []ProviderRecord{}, nil
	}
	return records, nil
}

func (s *PostgresStore) ListProvidersByAccount(ctx context.Context, accountID string) ([]ProviderRecord, error) {
	if accountID == "" {
		return []ProviderRecord{}, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Dedupe in SQL: many session UUIDs can map to the same physical
	// machine (one row per reconnect). Pick the most-recent row per
	// stable identity (serial → SE key → id) so we don't return tens
	// of thousands of historical rows for accounts with churny providers.
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT ON (
			COALESCE(NULLIF(serial_number, ''),
			         NULLIF(se_public_key, ''),
			         id)
		 )
		 id, hardware, models, backend, location, trust_level, attested,
			attestation_result, se_public_key, serial_number,
			mda_verified, mda_cert_chain,
			version, runtime_verified, python_hash, runtime_hash,
			last_challenge_verified, failed_challenges, account_id,
			lifetime_requests_served, lifetime_tokens_generated,
			last_session_requests_served, last_session_tokens_generated,
			lifetime_stats, last_session_stats,
			registered_at, last_seen, public_key
		 FROM providers
		 WHERE account_id = $1
		 ORDER BY COALESCE(NULLIF(serial_number, ''),
		                   NULLIF(se_public_key, ''),
		                   id),
		          last_seen DESC`,
		accountID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list providers by account: %w", err)
	}
	defer rows.Close()

	records := make([]ProviderRecord, 0)
	for rows.Next() {
		var p ProviderRecord
		var locationRaw []byte
		if err := rows.Scan(
			&p.ID, &p.Hardware, &p.Models, &p.Backend,
			&locationRaw,
			&p.TrustLevel, &p.Attested,
			&p.AttestationResult, &p.SEPublicKey, &p.SerialNumber,
			&p.MDAVerified, &p.MDACertChain,
			&p.Version, &p.RuntimeVerified, &p.PythonHash, &p.RuntimeHash,
			&p.LastChallengeVerified, &p.FailedChallenges, &p.AccountID,
			&p.LifetimeRequestsServed, &p.LifetimeTokensGenerated,
			&p.LastSessionRequestsServed, &p.LastSessionTokensGenerated,
			&p.LifetimeStats, &p.LastSessionStats,
			&p.RegisteredAt, &p.LastSeen, &p.PublicKey,
		); err != nil {
			continue
		}
		p.Location = unmarshalProviderLocation(locationRaw)
		records = append(records, p)
	}
	return records, nil
}

func (s *PostgresStore) DeleteProvidersBySerial(ctx context.Context, ownerAccountID, serialOrID string) (int, error) {
	if ownerAccountID == "" || serialOrID == "" {
		return 0, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("store: delete providers begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// Resolve all provider rows for this owner matching the stable identity
	// (serial OR session id). Postgres keeps one row per session UUID, so a
	// serial can map to many ids — delete them all.
	rows, err := tx.Query(ctx,
		`SELECT id FROM providers
		 WHERE account_id = $1
		   AND ((serial_number = $2 AND serial_number <> '') OR id = $2)`,
		ownerAccountID, serialOrID,
	)
	if err != nil {
		return 0, fmt.Errorf("store: delete providers select: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: delete providers scan: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: delete providers iterate: %w", err)
	}
	if len(ids) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, fmt.Errorf("store: delete providers commit: %w", err)
		}
		return 0, nil
	}

	// provider_reputation.provider_id has a FK to providers(id) with NO
	// ON DELETE CASCADE — delete the reputation rows FIRST or the providers
	// delete fails. usage / provider_earnings / provider_sessions hold
	// money/uptime history and have no FK; they are intentionally preserved.
	if _, err := tx.Exec(ctx,
		`DELETE FROM provider_reputation WHERE provider_id = ANY($1)`, ids,
	); err != nil {
		return 0, fmt.Errorf("store: delete provider reputation: %w", err)
	}

	tag, err := tx.Exec(ctx,
		`DELETE FROM providers WHERE id = ANY($1) AND account_id = $2`,
		ids, ownerAccountID,
	)
	if err != nil {
		return 0, fmt.Errorf("store: delete providers: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("store: delete providers commit: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStore) UpdateProviderLastSeen(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`UPDATE providers SET last_seen = NOW() WHERE id = $1`, id,
	)
	if err != nil {
		return fmt.Errorf("store: update provider last_seen: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateProviderTrust(ctx context.Context, id string, trustLevel string, attested bool, attestationResult json.RawMessage) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`UPDATE providers SET trust_level = $2, attested = $3, attestation_result = $4
		 WHERE id = $1`,
		id, trustLevel, attested, attestationResult,
	)
	if err != nil {
		return fmt.Errorf("store: update provider trust: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateProviderChallenge(ctx context.Context, id string, lastVerified time.Time, failedCount int) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`UPDATE providers SET last_challenge_verified = $2, failed_challenges = $3
		 WHERE id = $1`,
		id, lastVerified, failedCount,
	)
	if err != nil {
		return fmt.Errorf("store: update provider challenge: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpdateProviderRuntime(ctx context.Context, id string, verified bool, pythonHash, runtimeHash string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`UPDATE providers SET runtime_verified = $2, python_hash = $3, runtime_hash = $4
		 WHERE id = $1`,
		id, verified, pythonHash, runtimeHash,
	)
	if err != nil {
		return fmt.Errorf("store: update provider runtime: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpsertReputation(ctx context.Context, providerID string, rep ReputationRecord) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO provider_reputation (
			provider_id, total_jobs, successful_jobs, failed_jobs,
			total_uptime_seconds, avg_response_time_ms,
			challenges_passed, challenges_failed, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
		ON CONFLICT (provider_id) DO UPDATE SET
			total_jobs = $2, successful_jobs = $3, failed_jobs = $4,
			total_uptime_seconds = $5, avg_response_time_ms = $6,
			challenges_passed = $7, challenges_failed = $8,
			updated_at = NOW()`,
		providerID, rep.TotalJobs, rep.SuccessfulJobs, rep.FailedJobs,
		rep.TotalUptimeSeconds, rep.AvgResponseTimeMs,
		rep.ChallengesPassed, rep.ChallengesFailed,
	)
	if err != nil {
		return fmt.Errorf("store: upsert reputation: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetReputation(ctx context.Context, providerID string) (*ReputationRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var rep ReputationRecord
	err := s.pool.QueryRow(ctx,
		`SELECT total_jobs, successful_jobs, failed_jobs,
			total_uptime_seconds, avg_response_time_ms,
			challenges_passed, challenges_failed
		 FROM provider_reputation WHERE provider_id = $1`, providerID,
	).Scan(
		&rep.TotalJobs, &rep.SuccessfulJobs, &rep.FailedJobs,
		&rep.TotalUptimeSeconds, &rep.AvgResponseTimeMs,
		&rep.ChallengesPassed, &rep.ChallengesFailed,
	)
	if err != nil {
		return nil, fmt.Errorf("store: reputation not found: %w", err)
	}
	return &rep, nil
}

func (s *PostgresStore) ListCodeAttestations(ctx context.Context) ([]CodeAttestation, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT se_pubkey, version, attested_at, apns_token, node_public_key, binary_hash FROM code_attestations`)
	if err != nil {
		return nil, fmt.Errorf("store: list code attestations: %w", err)
	}
	defer rows.Close()

	var out []CodeAttestation
	for rows.Next() {
		var rec CodeAttestation
		if err := rows.Scan(
			&rec.SEPubKey, &rec.Version, &rec.AttestedAt,
			&rec.APNsToken, &rec.NodePublicKey, &rec.BinaryHash,
		); err != nil {
			return nil, fmt.Errorf("store: scan code attestation: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate code attestations: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) UpsertCodeAttestation(ctx context.Context, rec CodeAttestation) error {
	if rec.SEPubKey == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO code_attestations (
			se_pubkey, version, attested_at, apns_token, node_public_key, binary_hash
		 ) VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (se_pubkey) DO UPDATE SET
			version = $2, attested_at = $3,
			apns_token = $4, node_public_key = $5, binary_hash = $6`,
		rec.SEPubKey, rec.Version, rec.AttestedAt,
		rec.APNsToken, rec.NodePublicKey, rec.BinaryHash,
	)
	if err != nil {
		return fmt.Errorf("store: upsert code attestation: %w", err)
	}
	return nil
}

func (s *PostgresStore) DeleteCodeAttestation(ctx context.Context, seKey string) error {
	if seKey == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := s.pool.Exec(ctx, `DELETE FROM code_attestations WHERE se_pubkey = $1`, seKey); err != nil {
		return fmt.Errorf("store: delete code attestation: %w", err)
	}
	return nil
}

func (s *PostgresStore) ListCodeAttestPushBudgets(ctx context.Context) ([]CodeAttestPushBudget, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT se_pubkey, token_hash, next_push_at, updated_at, last_clear_at
		   FROM code_attest_push_budgets`)
	if err != nil {
		return nil, fmt.Errorf("store: list code attest push budgets: %w", err)
	}
	defer rows.Close()
	var out []CodeAttestPushBudget
	for rows.Next() {
		var rec CodeAttestPushBudget
		var lastClear *time.Time
		if err := rows.Scan(
			&rec.SEPubKey, &rec.TokenHash, &rec.NextPushAt, &rec.UpdatedAt,
			&lastClear,
		); err != nil {
			return nil, fmt.Errorf("store: scan code attest push budget: %w", err)
		}
		if lastClear != nil {
			rec.LastClearAt = *lastClear
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate code attest push budgets: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) UpsertCodeAttestPushBudget(ctx context.Context, rec CodeAttestPushBudget) error {
	if rec.SEPubKey == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO code_attest_push_budgets (
			se_pubkey, token_hash, next_push_at, updated_at
		 ) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (se_pubkey, token_hash) DO UPDATE SET
			next_push_at = GREATEST(
				code_attest_push_budgets.next_push_at,
				EXCLUDED.next_push_at
			),
			updated_at = EXCLUDED.updated_at`,
		rec.SEPubKey, rec.TokenHash, rec.NextPushAt, rec.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("store: upsert code attest push budget: %w", err)
	}
	return nil
}

func (s *PostgresStore) DeleteCodeAttestPushBudget(ctx context.Context, seKey string) error {
	if seKey == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM code_attest_push_budgets WHERE se_pubkey = $1`, seKey,
	); err != nil {
		return fmt.Errorf("store: delete code attest push budget: %w", err)
	}
	return nil
}

func (s *PostgresStore) ReserveCodeAttestPushBudget(
	ctx context.Context,
	seKey, tokenHash string,
	now, nextPushAt time.Time,
) (bool, error) {
	if seKey == "" || tokenHash == "" || !nextPushAt.After(now) {
		return false, errors.New("store: invalid code attest push reservation")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Token row = per-token cooldown (A-B-A retention). Sentinel row
	// (token_hash = '') = per-SE-key admission floor: a NOVEL token (no row) is
	// only admitted once the floor has elapsed, so fabricated fresh tokens
	// cannot mint fresh budgets (Codex P1).
	//
	// Novel-token admission is serialized on the sentinel row itself: the floor
	// is created-or-advanced FIRST, and only the statement whose ON CONFLICT
	// guard passes against the row's latest committed version proceeds to
	// insert the token row. Two blue-green coordinators racing distinct novel
	// tokens for one SE key therefore cannot both admit — the loser re-checks
	// the winner's freshly raised floor and returns floor-blocked, even when
	// neither snapshot saw a sentinel (or a floor block) at statement start.
	var admitted bool
	err := s.pool.QueryRow(ctx,
		`WITH known AS (
			SELECT 1 FROM code_attest_push_budgets
			 WHERE se_pubkey = $1 AND token_hash = $2
		),
		floor_acquired AS (
			INSERT INTO code_attest_push_budgets (
				se_pubkey, token_hash, next_push_at, updated_at
			)
			SELECT $1, '', $4, $3
			 WHERE NOT EXISTS (SELECT 1 FROM known)
			ON CONFLICT (se_pubkey, token_hash) DO UPDATE SET
				next_push_at = EXCLUDED.next_push_at,
				updated_at = EXCLUDED.updated_at
			WHERE code_attest_push_budgets.next_push_at <= $3
			RETURNING 1
		),
		admitted AS (
			INSERT INTO code_attest_push_budgets (
				se_pubkey, token_hash, next_push_at, updated_at
			)
			SELECT $1, $2, $4, $3
			 WHERE EXISTS (SELECT 1 FROM known)
			    OR EXISTS (SELECT 1 FROM floor_acquired)
			ON CONFLICT (se_pubkey, token_hash) DO UPDATE SET
				next_push_at = EXCLUDED.next_push_at,
				updated_at = EXCLUDED.updated_at
			WHERE code_attest_push_budgets.next_push_at <= $3
			RETURNING 1
		),
		floor_raised AS (
			-- Known-token admissions raise the floor too; novel admissions
			-- already set it in floor_acquired. The two paths are mutually
			-- exclusive, so the sentinel row is written at most once here.
			INSERT INTO code_attest_push_budgets (
				se_pubkey, token_hash, next_push_at, updated_at
			)
			SELECT $1, '', $4, $3
			  FROM admitted
			 WHERE EXISTS (SELECT 1 FROM known)
			ON CONFLICT (se_pubkey, token_hash) DO UPDATE SET
				next_push_at = GREATEST(
					code_attest_push_budgets.next_push_at,
					EXCLUDED.next_push_at
				),
				updated_at = EXCLUDED.updated_at
		)
		SELECT EXISTS (SELECT 1 FROM admitted)`,
		seKey, tokenHash, now, nextPushAt,
	).Scan(&admitted)
	if err != nil {
		return false, fmt.Errorf("store: reserve code attest push budget: %w", err)
	}
	if admitted {
		// Bound rows per SE key: keep the newest token rows plus the floor
		// sentinel. Best-effort — a failure only delays GC to the next push.
		if _, err := s.pool.Exec(ctx,
			`DELETE FROM code_attest_push_budgets
			  WHERE se_pubkey = $1 AND token_hash <> ''
			    AND token_hash NOT IN (
				SELECT token_hash FROM code_attest_push_budgets
				 WHERE se_pubkey = $1 AND token_hash <> ''
				 ORDER BY updated_at DESC, token_hash DESC
				 LIMIT $2
			    )`,
			seKey, CodeAttestPushBudgetMaxTokenRows,
		); err != nil {
			return true, nil
		}
	}
	return admitted, nil
}

// ClearCodeAttestPushFloor drops the per-SE-key novel-token admission floor so
// a genuinely rotated token can be challenged promptly. Per-token cooldown rows
// are untouched (A-B-A retention). The clear is compare-and-set on the
// sentinel's durable last_clear_at: it is honored only when the previous
// durable clear is at least cooldown old (NULL = never cleared → honored), so
// the anti-abuse spacing between rotation clears holds across coordinator
// restarts and blue-green peers — not just within one process. The sentinel row
// is kept (next_push_at=now lifts the floor; last_clear_at=now starts the next
// cooldown). Returns the durable last-clear instant (now when honored, the
// pre-statement one when throttled) and whether the clear was honored.
func (s *PostgresStore) ClearCodeAttestPushFloor(
	ctx context.Context, seKey string, now time.Time, cooldown time.Duration,
) (time.Time, bool, error) {
	if seKey == "" {
		return time.Time{}, false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var (
		cleared   bool
		lastClear time.Time
	)
	// The outer SELECT sees the pre-statement snapshot (data-modifying CTE
	// semantics); when the CAS wins the caller's last-clear is `now`, so the
	// snapshot value is only reported on the throttled path. COALESCE covers
	// the no-sentinel / never-cleared cases conservatively with `now`.
	err := s.pool.QueryRow(ctx,
		`WITH cleared AS (
			INSERT INTO code_attest_push_budgets (
				se_pubkey, token_hash, next_push_at, updated_at, last_clear_at
			) VALUES ($1, '', $2, $2, $2)
			ON CONFLICT (se_pubkey, token_hash) DO UPDATE SET
				next_push_at = EXCLUDED.next_push_at,
				updated_at = EXCLUDED.updated_at,
				last_clear_at = EXCLUDED.last_clear_at
			WHERE code_attest_push_budgets.last_clear_at IS NULL
			   OR code_attest_push_budgets.last_clear_at <= $3
			RETURNING 1
		)
		SELECT EXISTS (SELECT 1 FROM cleared),
		       COALESCE((
			SELECT last_clear_at FROM code_attest_push_budgets
			 WHERE se_pubkey = $1 AND token_hash = ''
		       ), $2)`,
		seKey, now, now.Add(-cooldown),
	).Scan(&cleared, &lastClear)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("store: clear code attest push floor: %w", err)
	}
	if cleared {
		lastClear = now
	}
	return lastClear, cleared, nil
}

func (s *PostgresStore) ListProviderTrustReuse(ctx context.Context) ([]ProviderTrustReuse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT se_pubkey, serial, trust_level, last_verified_binary_hash,
		        sip_enabled, secure_boot_full, mda_udid,
		        hardware_proof_verified_at, application_proof_verified_at,
		        continuous_coverage_until,
		        evidence_generation, revocation_generation,
		        revocation_event_id, revoked_at
		   FROM provider_trust_reuse`)
	if err != nil {
		return nil, fmt.Errorf("store: list provider trust reuse: %w", err)
	}
	defer rows.Close()

	var out []ProviderTrustReuse
	for rows.Next() {
		var rec ProviderTrustReuse
		if err := rows.Scan(
			&rec.SEPubKey, &rec.Serial, &rec.TrustLevel,
			&rec.LastVerifiedBinaryHash, &rec.SIPEnabled,
			&rec.SecureBootFull, &rec.MDAUDID,
			&rec.HardwareProofVerifiedAt, &rec.ApplicationProofVerifiedAt,
			&rec.ContinuousCoverageUntil,
			&rec.EvidenceGeneration, &rec.RevocationGeneration,
			&rec.RevocationEventID, &rec.RevokedAt,
		); err != nil {
			return nil, fmt.Errorf("store: scan provider trust reuse: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate provider trust reuse: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) UpsertProviderTrustReuse(ctx context.Context, rec ProviderTrustReuse, expectedRevocationGeneration uint64) (ProviderTrustReuseWriteResult, error) {
	if rec.SEPubKey == "" {
		return ProviderTrustReuseWriteResult{}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result ProviderTrustReuseWriteResult
	err := s.pool.QueryRow(ctx,
		`WITH written AS (
			INSERT INTO provider_trust_reuse (
				se_pubkey, serial, trust_level, binary_hash,
				last_verified_binary_hash, sip_enabled, secure_boot_full, mda_udid,
				verified_at, hardware_proof_verified_at,
				application_proof_verified_at, continuous_coverage_until,
				evidence_generation,
				revocation_generation, revocation_event_id, revoked_at)
			 VALUES ($1,$2,$3,$4,$4,$5,$6,$7,$8,$8,$9,$12,1,$10,$11,NULL)
			 ON CONFLICT (se_pubkey) DO UPDATE SET
				serial = EXCLUDED.serial,
				trust_level = EXCLUDED.trust_level,
				binary_hash = EXCLUDED.binary_hash,
				last_verified_binary_hash = EXCLUDED.last_verified_binary_hash,
				sip_enabled = EXCLUDED.sip_enabled,
				secure_boot_full = EXCLUDED.secure_boot_full,
				mda_udid = EXCLUDED.mda_udid,
				verified_at = EXCLUDED.verified_at,
				hardware_proof_verified_at = EXCLUDED.hardware_proof_verified_at,
				application_proof_verified_at = EXCLUDED.application_proof_verified_at,
				continuous_coverage_until = GREATEST(
					provider_trust_reuse.continuous_coverage_until,
					EXCLUDED.continuous_coverage_until),
				evidence_generation = provider_trust_reuse.evidence_generation + 1
			 WHERE provider_trust_reuse.revoked_at IS NULL
			   AND provider_trust_reuse.revocation_generation = EXCLUDED.revocation_generation
			 RETURNING evidence_generation, revocation_generation
		)
		SELECT TRUE, evidence_generation, revocation_generation FROM written
		UNION ALL
		SELECT FALSE, evidence_generation, revocation_generation
		  FROM provider_trust_reuse
		 WHERE se_pubkey = $1 AND NOT EXISTS (SELECT 1 FROM written)
		LIMIT 1`,
		rec.SEPubKey, rec.Serial, rec.TrustLevel,
		rec.LastVerifiedBinaryHash, rec.SIPEnabled, rec.SecureBootFull,
		rec.MDAUDID, rec.HardwareProofVerifiedAt,
		rec.ApplicationProofVerifiedAt, expectedRevocationGeneration,
		rec.RevocationEventID, rec.ContinuousCoverageUntil,
	).Scan(&result.Applied, &result.EvidenceGeneration, &result.RevocationGeneration)
	if err != nil {
		return ProviderTrustReuseWriteResult{}, fmt.Errorf("store: upsert provider trust reuse: %w", err)
	}
	return result, nil
}

func (s *PostgresStore) RecoverProviderTrustReuse(ctx context.Context, rec ProviderTrustReuse, expectedRevocationGeneration uint64) (ProviderTrustReuseWriteResult, error) {
	if rec.SEPubKey == "" {
		return ProviderTrustReuseWriteResult{}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var result ProviderTrustReuseWriteResult
	err := s.pool.QueryRow(ctx,
		`WITH written AS (
			INSERT INTO provider_trust_reuse (
				se_pubkey, serial, trust_level, binary_hash,
				last_verified_binary_hash, sip_enabled, secure_boot_full, mda_udid,
				verified_at, hardware_proof_verified_at,
				application_proof_verified_at, continuous_coverage_until,
				evidence_generation,
				revocation_generation, revocation_event_id, revoked_at)
			 VALUES ($1,$2,$3,$4,$4,$5,$6,$7,$8,$8,$9,$12,1,$10,$11,NULL)
			 ON CONFLICT (se_pubkey) DO UPDATE SET
				serial = EXCLUDED.serial,
				trust_level = EXCLUDED.trust_level,
				binary_hash = EXCLUDED.binary_hash,
				last_verified_binary_hash = EXCLUDED.last_verified_binary_hash,
				sip_enabled = EXCLUDED.sip_enabled,
				secure_boot_full = EXCLUDED.secure_boot_full,
				mda_udid = EXCLUDED.mda_udid,
				verified_at = EXCLUDED.verified_at,
				hardware_proof_verified_at = EXCLUDED.hardware_proof_verified_at,
				application_proof_verified_at = EXCLUDED.application_proof_verified_at,
				continuous_coverage_until = GREATEST(
					provider_trust_reuse.continuous_coverage_until,
					EXCLUDED.continuous_coverage_until),
				evidence_generation = provider_trust_reuse.evidence_generation + 1,
				revoked_at = NULL
			 WHERE provider_trust_reuse.revocation_generation = EXCLUDED.revocation_generation
			 RETURNING evidence_generation, revocation_generation
		)
		SELECT TRUE, evidence_generation, revocation_generation FROM written
		UNION ALL
		SELECT FALSE, evidence_generation, revocation_generation
		  FROM provider_trust_reuse
		 WHERE se_pubkey = $1 AND NOT EXISTS (SELECT 1 FROM written)
		LIMIT 1`,
		rec.SEPubKey, rec.Serial, rec.TrustLevel,
		rec.LastVerifiedBinaryHash, rec.SIPEnabled, rec.SecureBootFull,
		rec.MDAUDID, rec.HardwareProofVerifiedAt,
		rec.ApplicationProofVerifiedAt, expectedRevocationGeneration,
		rec.RevocationEventID, rec.ContinuousCoverageUntil,
	).Scan(&result.Applied, &result.EvidenceGeneration, &result.RevocationGeneration)
	if err != nil {
		return ProviderTrustReuseWriteResult{}, fmt.Errorf("store: recover provider trust reuse: %w", err)
	}
	return result, nil
}

func (s *PostgresStore) RevokeProviderTrustReuse(ctx context.Context, seKey, revocationEventID string) (ProviderTrustReuse, error) {
	if seKey == "" || revocationEventID == "" {
		return ProviderTrustReuse{}, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var rec ProviderTrustReuse
	err := s.pool.QueryRow(ctx,
		`WITH revoked AS (
			INSERT INTO provider_trust_reuse (
				se_pubkey, trust_level, revoked_at, revocation_generation,
				revocation_event_id)
			 VALUES ($1, '', NOW(), 1, $2)
			 ON CONFLICT (se_pubkey) DO UPDATE SET
				trust_level = '',
				revoked_at = NOW(),
				continuous_coverage_until = NULL,
				revocation_generation = provider_trust_reuse.revocation_generation + 1,
				revocation_event_id = EXCLUDED.revocation_event_id
			 WHERE provider_trust_reuse.revocation_event_id
			       IS DISTINCT FROM EXCLUDED.revocation_event_id
			 RETURNING se_pubkey, serial, trust_level,
				last_verified_binary_hash, sip_enabled, secure_boot_full,
				mda_udid, hardware_proof_verified_at,
				application_proof_verified_at, continuous_coverage_until,
				evidence_generation,
				revocation_generation, revocation_event_id, revoked_at
		)
		SELECT se_pubkey, serial, trust_level, last_verified_binary_hash,
		       sip_enabled, secure_boot_full, mda_udid,
		       hardware_proof_verified_at, application_proof_verified_at,
		       continuous_coverage_until,
		       evidence_generation, revocation_generation,
		       revocation_event_id, revoked_at
		  FROM revoked
		UNION ALL
		SELECT se_pubkey, serial, trust_level, last_verified_binary_hash,
		       sip_enabled, secure_boot_full, mda_udid,
		       hardware_proof_verified_at, application_proof_verified_at,
		       continuous_coverage_until,
		       evidence_generation, revocation_generation,
		       revocation_event_id, revoked_at
		  FROM provider_trust_reuse
		 WHERE se_pubkey = $1 AND NOT EXISTS (SELECT 1 FROM revoked)
		LIMIT 1`,
		seKey, revocationEventID,
	).Scan(
		&rec.SEPubKey, &rec.Serial, &rec.TrustLevel,
		&rec.LastVerifiedBinaryHash, &rec.SIPEnabled,
		&rec.SecureBootFull, &rec.MDAUDID,
		&rec.HardwareProofVerifiedAt, &rec.ApplicationProofVerifiedAt,
		&rec.ContinuousCoverageUntil,
		&rec.EvidenceGeneration, &rec.RevocationGeneration,
		&rec.RevocationEventID, &rec.RevokedAt,
	)
	if err != nil {
		return ProviderTrustReuse{}, fmt.Errorf("store: revoke provider trust reuse: %w", err)
	}
	return rec, nil
}

func (s *PostgresStore) AdvanceProviderTrustReuseCoverage(ctx context.Context, seKeys []string, until time.Time) error {
	if len(seKeys) == 0 || until.IsZero() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// One batched pass; GREATEST keeps the watermark monotonic and a
	// tombstoned/non-hardware row is never touched (revocation wins).
	_, err := s.pool.Exec(ctx,
		`UPDATE provider_trust_reuse
		    SET continuous_coverage_until = GREATEST(continuous_coverage_until, $2::timestamptz)
		  WHERE se_pubkey = ANY($1)
		    AND revoked_at IS NULL
		    AND trust_level = 'hardware'`,
		seKeys, until)
	if err != nil {
		return fmt.Errorf("store: advance provider trust reuse coverage: %w", err)
	}
	return nil
}

// OpenProviderSession records the start of a provider connection. Idempotent:
// ON CONFLICT DO NOTHING so a duplicate register, or an open that races behind a
// close (fast connect→disconnect), never creates a second or reopened row.
func (s *PostgresStore) OpenProviderSession(ctx context.Context, sessionID, serial, accountID string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO provider_sessions (session_id, serial_number, account_id)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (session_id) DO NOTHING`,
		sessionID, serial, accountID,
	)
	if err != nil {
		return fmt.Errorf("store: open provider session: %w", err)
	}
	return nil
}

// TouchProviderSession updates the open session's last_seen and backfills
// serial/account/provider_key if they were unknown at open time.
func (s *PostgresStore) TouchProviderSession(ctx context.Context, sessionID, serial, accountID, providerKey string, lastSeen time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE provider_sessions
		    SET last_seen = $2,
		        serial_number = CASE WHEN serial_number = '' THEN $3 ELSE serial_number END,
		        account_id    = CASE WHEN account_id = ''    THEN $4 ELSE account_id    END,
		        provider_key  = CASE WHEN provider_key = ''  THEN $5 ELSE provider_key  END
		  WHERE session_id = $1 AND disconnected_at IS NULL`,
		sessionID, lastSeen, serial, accountID, providerKey,
	)
	if err != nil {
		return fmt.Errorf("store: touch provider session: %w", err)
	}
	return nil
}

// CloseProviderSession marks the session for sessionID as ended. Implemented as
// an upsert so it is correct regardless of whether the async OpenProviderSession
// has landed yet: if the row is missing (close raced ahead of open on a fast
// connect→disconnect) it inserts an already-closed row; if open, it closes it;
// if already closed, it leaves the original disconnect timestamp/reason intact.
func (s *PostgresStore) CloseProviderSession(ctx context.Context, sessionID, reason string, when time.Time) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO provider_sessions (session_id, connected_at, last_seen, disconnected_at, disconnect_reason)
		 VALUES ($1, $3, $3, $3, $2)
		 ON CONFLICT (session_id) DO UPDATE
		    SET disconnected_at = COALESCE(provider_sessions.disconnected_at, EXCLUDED.disconnected_at),
		        disconnect_reason = CASE WHEN provider_sessions.disconnected_at IS NULL
		                                 THEN EXCLUDED.disconnect_reason
		                                 ELSE provider_sessions.disconnect_reason END`,
		sessionID, reason, when,
	)
	if err != nil {
		return fmt.Errorf("store: close provider session: %w", err)
	}
	return nil
}

// CloseOpenProviderSessions closes open sessions whose last heartbeat predates
// staleBefore (orphaned by a prior coordinator process), setting disconnected_at
// to the last heartbeat seen. The last_seen < staleBefore fence prevents a
// blue-green deploy from truncating a session still live (and being touched) on
// the old instance over the shared DB — its last_seen stays fresh.
//
// Note: crash-path disconnected_at granularity is bounded by how often last_seen
// advances. Heartbeats touch it (TouchProviderSession), so the recorded
// disconnect can lag the true last-seen by at most the heartbeat interval.
func (s *PostgresStore) CloseOpenProviderSessions(ctx context.Context, staleBefore time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE provider_sessions
		    SET disconnected_at = last_seen, disconnect_reason = 'coordinator_restart'
		  WHERE disconnected_at IS NULL AND last_seen < $1`,
		staleBefore,
	)
	if err != nil {
		return 0, fmt.Errorf("store: close open provider sessions: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
