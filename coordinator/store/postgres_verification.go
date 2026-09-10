package store

// Verification jobs and stored log reports.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const verificationJobColumns = `se_pubkey, serial, udid, task_kind, task_state,
	priority, retry_stage, previous_delay_ns, next_attempt_at, last_outcome,
	reopen_pending, updated_at, claim_owner, claim_expires_at`

type verificationJobScanner interface {
	Scan(dest ...any) error
}

func scanVerificationJob(row verificationJobScanner) (VerificationJob, error) {
	var rec VerificationJob
	var previousDelayNS int64
	var nextAttemptAt *time.Time
	if err := row.Scan(
		&rec.SEPubKey, &rec.Serial, &rec.UDID, &rec.Kind, &rec.State,
		&rec.Priority, &rec.RetryStage, &previousDelayNS, &nextAttemptAt,
		&rec.LastOutcome, &rec.ReopenPending, &rec.UpdatedAt,
		&rec.ClaimOwner, &rec.ClaimExpiresAt,
	); err != nil {
		return VerificationJob{}, err
	}
	rec.PreviousDelay = time.Duration(previousDelayNS)
	if nextAttemptAt != nil {
		rec.NextAttemptAt = *nextAttemptAt
	}
	return rec, nil
}

func (s *PostgresStore) UpsertVerificationJob(ctx context.Context, rec VerificationJob) (VerificationJob, error) {
	if rec.SEPubKey == "" || rec.Kind == "" {
		return VerificationJob{}, errors.New("store: verification job requires SE key and kind")
	}
	if rec.LastOutcome == "" {
		rec.LastOutcome = VerificationOutcomeNone
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	row := s.pool.QueryRow(ctx,
		`INSERT INTO provider_verification_jobs (
			se_pubkey, serial, udid, task_kind, task_state, priority,
			retry_stage, previous_delay_ns, next_attempt_at, last_outcome,
			updated_at, claim_owner, claim_expires_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'',NULL)
		 ON CONFLICT (se_pubkey, task_kind) DO UPDATE SET
			serial = EXCLUDED.serial,
			udid = CASE WHEN EXCLUDED.udid <> '' THEN EXCLUDED.udid ELSE provider_verification_jobs.udid END,
			task_state = CASE
				WHEN provider_verification_jobs.task_state = 'completed' THEN EXCLUDED.task_state
				WHEN provider_verification_jobs.task_state = 'waiting_challenge'
				 AND EXCLUDED.task_state = 'pending' THEN 'pending'
				ELSE provider_verification_jobs.task_state END,
			priority = CASE
				WHEN provider_verification_jobs.task_state = 'completed'
					THEN EXCLUDED.priority
				ELSE LEAST(provider_verification_jobs.priority, EXCLUDED.priority) END,
			retry_stage = CASE WHEN provider_verification_jobs.task_state = 'completed'
				THEN EXCLUDED.retry_stage ELSE provider_verification_jobs.retry_stage END,
			previous_delay_ns = CASE WHEN provider_verification_jobs.task_state = 'completed'
				THEN EXCLUDED.previous_delay_ns ELSE provider_verification_jobs.previous_delay_ns END,
			next_attempt_at = CASE
				WHEN provider_verification_jobs.task_state = 'completed' THEN EXCLUDED.next_attempt_at
				WHEN provider_verification_jobs.task_state IN ('waiting_challenge', 'running')
				 AND EXCLUDED.task_state = 'pending' THEN EXCLUDED.next_attempt_at
				ELSE provider_verification_jobs.next_attempt_at END,
			last_outcome = CASE WHEN provider_verification_jobs.task_state = 'completed'
				THEN EXCLUDED.last_outcome ELSE provider_verification_jobs.last_outcome END,
			reopen_pending = CASE
				WHEN provider_verification_jobs.task_state = 'completed' THEN FALSE
				WHEN provider_verification_jobs.task_state = 'running'
				 AND EXCLUDED.task_state = 'pending' THEN TRUE
				ELSE provider_verification_jobs.reopen_pending END,
			updated_at = EXCLUDED.updated_at,
			claim_owner = CASE WHEN provider_verification_jobs.task_state = 'completed'
				THEN '' ELSE provider_verification_jobs.claim_owner END,
			claim_expires_at = CASE WHEN provider_verification_jobs.task_state = 'completed'
				THEN NULL ELSE provider_verification_jobs.claim_expires_at END
		 RETURNING `+verificationJobColumns,
		rec.SEPubKey, rec.Serial, rec.UDID, rec.Kind, rec.State, rec.Priority,
		rec.RetryStage, int64(rec.PreviousDelay), nullableVerificationTime(rec.NextAttemptAt),
		rec.LastOutcome, rec.UpdatedAt,
	)
	out, err := scanVerificationJob(row)
	if err != nil {
		return VerificationJob{}, fmt.Errorf("store: upsert verification job: %w", err)
	}
	return out, nil
}

func nullableVerificationTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func (s *PostgresStore) GetVerificationJob(ctx context.Context, seKey string, kind VerificationTaskKind) (*VerificationJob, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rec, err := scanVerificationJob(s.pool.QueryRow(ctx,
		`SELECT `+verificationJobColumns+`
		   FROM provider_verification_jobs
		  WHERE se_pubkey = $1 AND task_kind = $2`, seKey, kind))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: get verification job: %w", err)
	}
	return &rec, nil
}

func (s *PostgresStore) ListDueVerificationJobs(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]VerificationJob, error) {
	return s.ListDueVerificationJobsPage(ctx, now, limit, 0)
}

// verificationDuePageHint caps the initial capacity of a due-rows page.
const verificationDuePageHint = 256

func (s *PostgresStore) ListDueVerificationJobsPage(
	ctx context.Context,
	now time.Time,
	limit, offset int,
) ([]VerificationJob, error) {
	if limit <= 0 || offset < 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT `+verificationJobColumns+`
		   FROM provider_verification_jobs
		  WHERE (task_state IN ('pending','backoff')
		         OR (task_state = 'running' AND claim_expires_at IS NOT NULL
		             AND claim_expires_at <= $1))
		    AND next_attempt_at <= $1
		    AND (claim_owner = '' OR claim_expires_at IS NULL OR claim_expires_at <= $1)
		  ORDER BY priority, next_attempt_at, se_pubkey, task_kind
		  LIMIT $2 OFFSET $3`, now, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("store: list due verification jobs: %w", err)
	}
	defer rows.Close()
	// The page is sized for the common case, not the limit: the caller asks
	// for its whole queue capacity (4,096) every poll while only a few dozen
	// rows are usually due, and a 4,096-row pre-allocation per poll was 16 %
	// of all bytes the coordinator allocated. append grows it when needed.
	out := make([]VerificationJob, 0, min(limit, verificationDuePageHint))
	for rows.Next() {
		rec, scanErr := scanVerificationJob(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("store: scan due verification job: %w", scanErr)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate due verification jobs: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ClaimVerificationJob(ctx context.Context, seKey string, kind VerificationTaskKind, owner string, now, expiresAt time.Time) (VerificationJob, bool, error) {
	if owner == "" || !expiresAt.After(now) {
		return VerificationJob{}, false, errors.New("store: invalid verification claim")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rec, err := scanVerificationJob(s.pool.QueryRow(ctx,
		`UPDATE provider_verification_jobs
		    SET task_state = 'running', reopen_pending = FALSE, claim_owner = $3,
		        claim_expires_at = $5, updated_at = $4
		  WHERE se_pubkey = $1 AND task_kind = $2
		    AND (task_state IN ('pending','backoff')
		         OR (task_state = 'running' AND claim_expires_at IS NOT NULL
		             AND claim_expires_at <= $4))
		    AND next_attempt_at <= $4
		    AND (claim_owner = '' OR claim_expires_at IS NULL OR claim_expires_at <= $4)
		  RETURNING `+verificationJobColumns,
		seKey, kind, owner, now, expiresAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return VerificationJob{}, false, nil
	}
	if err != nil {
		return VerificationJob{}, false, fmt.Errorf("store: claim verification job: %w", err)
	}
	return rec, true, nil
}

func (s *PostgresStore) ReleaseVerificationJob(ctx context.Context, seKey string, kind VerificationTaskKind, owner string, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`UPDATE provider_verification_jobs
		    SET task_state = 'pending', reopen_pending = FALSE,
		        claim_owner = '', claim_expires_at = NULL, updated_at = $4
		  WHERE se_pubkey = $1 AND task_kind = $2 AND claim_owner = $3`,
		seKey, kind, owner, now)
	if err != nil {
		return fmt.Errorf("store: release verification job: %w", err)
	}
	return nil
}

func (s *PostgresStore) CompleteVerificationJob(ctx context.Context, seKey string, kind VerificationTaskKind, owner string, outcome VerificationOutcome, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`UPDATE provider_verification_jobs
		    SET task_state = CASE WHEN reopen_pending THEN 'pending' ELSE 'completed' END,
		        retry_stage = CASE WHEN reopen_pending THEN retry_stage ELSE 0 END,
		        previous_delay_ns = CASE WHEN reopen_pending THEN previous_delay_ns ELSE 0 END,
		        next_attempt_at = CASE WHEN reopen_pending THEN next_attempt_at ELSE NULL END,
		        last_outcome = CASE WHEN reopen_pending THEN last_outcome ELSE $4 END,
		        reopen_pending = FALSE, updated_at = $5,
		        claim_owner = '', claim_expires_at = NULL
		  WHERE se_pubkey = $1 AND task_kind = $2
		    AND (claim_owner = '' OR claim_owner = $3)`,
		seKey, kind, owner, outcome, now)
	if err != nil {
		return fmt.Errorf("store: complete verification job: %w", err)
	}
	return nil
}

func (s *PostgresStore) RescheduleVerificationJob(ctx context.Context, seKey string, kind VerificationTaskKind, owner string, priority VerificationPriority, retryStage int, previousDelay time.Duration, nextAttemptAt time.Time, outcome VerificationOutcome, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := s.pool.Exec(ctx,
		`UPDATE provider_verification_jobs
		    SET task_state = CASE WHEN reopen_pending THEN 'pending' ELSE 'backoff' END,
		        priority = CASE WHEN reopen_pending THEN priority ELSE $4 END,
		        retry_stage = CASE WHEN reopen_pending THEN retry_stage ELSE $5 END,
		        previous_delay_ns = CASE WHEN reopen_pending THEN previous_delay_ns ELSE $6 END,
		        next_attempt_at = CASE WHEN reopen_pending THEN next_attempt_at ELSE $7 END,
		        last_outcome = CASE WHEN reopen_pending THEN last_outcome ELSE $8 END,
		        reopen_pending = FALSE, updated_at = $9,
		        claim_owner = '', claim_expires_at = NULL
		  WHERE se_pubkey = $1 AND task_kind = $2 AND claim_owner = $3`,
		seKey, kind, owner, priority, retryStage, int64(previousDelay),
		nextAttemptAt, outcome, now)
	if err != nil {
		return fmt.Errorf("store: reschedule verification job: %w", err)
	}
	return nil
}

const maxLogReportSize = 10 << 20 // 10 MB

func (s *PostgresStore) StoreLogReport(accountID string, logData []byte) (int64, error) {
	if len(logData) > maxLogReportSize {
		logData = logData[:maxLogReportSize]
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var reportID int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO provider_log_reports (account_id, log_data, log_size_bytes)
		 VALUES ($1, $2, $3)
		 RETURNING id`,
		accountID, logData, int64(len(logData)),
	).Scan(&reportID)
	if err != nil {
		return 0, fmt.Errorf("store: insert log report: %w", err)
	}
	return reportID, nil
}

func (s *PostgresStore) GetLogReport(id int64) (*LogReport, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var r LogReport
	err := s.pool.QueryRow(ctx,
		`SELECT id, account_id, log_data, log_size_bytes, created_at
		 FROM provider_log_reports WHERE id = $1`, id,
	).Scan(&r.ID, &r.AccountID, &r.LogData, &r.LogSizeBytes, &r.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("store: log report %d not found: %w", id, err)
	}
	return &r, nil
}
