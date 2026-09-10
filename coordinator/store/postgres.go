package store

// PostgreSQL-backed implementation of the Store interface.
//
// PostgresStore provides persistent storage with proper transactional
// guarantees. It stores API key hashes (SHA-256) rather than raw keys,
// so even if the database is compromised, API keys cannot be recovered.
//
// Balance operations (Credit/Debit) use PostgreSQL transactions to ensure
// atomicity — the balance update and ledger entry are committed together
// or not at all. The Debit operation uses a conditional UPDATE that only
// succeeds if the balance is sufficient, preventing negative balances.
//
// Schema migrations run automatically on startup via the migrate() method.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Compile-time check that PostgresStore implements Store.
var _ Store = (*PostgresStore)(nil)

// PostgresStore is a PostgreSQL-backed implementation of Store.
type PostgresStore struct {
	pool *pgxpool.Pool

	// In-memory cache for model prices. Keyed by "accountID:model".
	// Eliminates a DB round trip on every inference request for
	// platform pricing lookups (which change rarely).
	priceCacheMu sync.RWMutex
	priceCache   map[string]cachedPrice
}

type cachedPrice struct {
	input, output int64
	at            time.Time
}

// NewPostgres creates a new PostgresStore connected to the given database URL.
// It runs schema migrations on startup.
func NewPostgres(ctx context.Context, scfg Config) (*PostgresStore, error) {
	return newPostgresWithPoolConfig(ctx, scfg, nil)
}

// newPostgresWithPoolConfig is NewPostgres with a hook that may adjust the
// parsed pool configuration before the pool is created. Production passes nil;
// package tests use it to attach a pgx query tracer.
func newPostgresWithPoolConfig(ctx context.Context, scfg Config, tune func(*pgxpool.Config)) (*PostgresStore, error) {
	cfg, err := pgxpool.ParseConfig(scfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("store: parse postgres config: %w", err)
	}

	// Pool was previously capped at 20, causing connection starvation under
	// load. The stats endpoint holds connections for up to 10s (full-table
	// scans on usage), billing settlement takes 5-7 sequential operations,
	// and heartbeat upserts fire every 30s per provider. 20 connections is
	// exhausted by 3-4 concurrent inference completions + a single stats
	// cache miss.
	if cfg.MaxConns < 80 {
		cfg.MaxConns = 80
	}
	cfg.MinConns = 10
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	if tune != nil {
		tune(cfg)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("store: connect to postgres: %w", err)
	}

	// Verify connectivity.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping postgres: %w", err)
	}

	s := &PostgresStore{
		pool:       pool,
		priceCache: make(map[string]cachedPrice),
	}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: run migrations: %w", err)
	}

	return s, nil
}

// Close shuts down the connection pool.
func (s *PostgresStore) Close() {
	s.pool.Close()
}

const legacyCacheAffinityGuardFunction = `CREATE OR REPLACE FUNCTION clear_legacy_cache_affinity_key()
RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
	NEW.cache_affinity_key := '';
	RETURN NEW;
END $$`

const legacyCacheAffinityGuardTrigger = `DO $$ BEGIN
	IF NOT EXISTS (
		SELECT 1
		FROM pg_trigger tg
		JOIN pg_class target ON target.oid = tg.tgrelid
		JOIN pg_namespace ns ON ns.oid = target.relnamespace
		WHERE tg.tgname = 'clear_legacy_cache_affinity_key'
		  AND NOT tg.tgisinternal
		  AND target.relname = 'inference_routes'
		  AND ns.nspname = current_schema()
	) THEN
		CREATE TRIGGER clear_legacy_cache_affinity_key
		BEFORE INSERT OR UPDATE OF cache_affinity_key ON inference_routes
		FOR EACH ROW EXECUTE FUNCTION clear_legacy_cache_affinity_key();
	END IF;
END $$`

const legacyCacheAffinityScrubMigration = `DO $$ BEGIN
	IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE id = 'scrub_inference_route_cache_affinity_v1') THEN
		IF EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'inference_routes'
			  AND column_name = 'cache_affinity_key'
		) THEN
			UPDATE inference_routes SET cache_affinity_key = '' WHERE cache_affinity_key <> '';
		END IF;
		INSERT INTO schema_migrations (id) VALUES ('scrub_inference_route_cache_affinity_v1');
	END IF;
END $$`

// migrate runs the schema creation statements.
func (s *PostgresStore) migrate(ctx context.Context) error {
	migrations := []string{
		globalPayoutSchema,
		// schema_migrations records one-time data migrations that must run at most
		// once rather than on every boot. Idempotent DDL (CREATE/ALTER ... IF [NOT]
		// EXISTS) does not need this; it exists to gate destructive one-shot DML
		// cleanups (see the model_prices cleanup below) behind a marker id.
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			id TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,

		`CREATE TABLE IF NOT EXISTS providers (
			id TEXT PRIMARY KEY,
			hardware JSONB NOT NULL,
			models JSONB NOT NULL,
			backend TEXT NOT NULL,
			location JSONB,
			registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			trust_level TEXT NOT NULL DEFAULT 'none',
			attested BOOLEAN NOT NULL DEFAULT FALSE,
			attestation_result JSONB,
			se_public_key TEXT NOT NULL DEFAULT '',
			public_key TEXT NOT NULL DEFAULT '',
			serial_number TEXT NOT NULL DEFAULT '',
			mda_verified BOOLEAN NOT NULL DEFAULT FALSE,
			mda_cert_chain JSONB,
			version TEXT NOT NULL DEFAULT '',
			runtime_verified BOOLEAN NOT NULL DEFAULT FALSE,
			python_hash TEXT NOT NULL DEFAULT '',
			runtime_hash TEXT NOT NULL DEFAULT '',
			last_challenge_verified TIMESTAMPTZ,
			failed_challenges INT NOT NULL DEFAULT 0,
			account_id TEXT NOT NULL DEFAULT '',
			lifetime_requests_served BIGINT NOT NULL DEFAULT 0,
			lifetime_tokens_generated BIGINT NOT NULL DEFAULT 0,
			last_session_requests_served BIGINT NOT NULL DEFAULT 0,
			last_session_tokens_generated BIGINT NOT NULL DEFAULT 0,
			lifetime_stats JSONB NOT NULL DEFAULT '{}'::jsonb,
			last_session_stats JSONB NOT NULL DEFAULT '{}'::jsonb
		)`,
		// Migrate existing providers table: add new columns if upgrading from previous schema
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS location JSONB; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS trust_level TEXT NOT NULL DEFAULT 'none'; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS attested BOOLEAN NOT NULL DEFAULT FALSE; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS attestation_result JSONB; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS se_public_key TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS public_key TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS serial_number TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS mda_verified BOOLEAN NOT NULL DEFAULT FALSE; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS mda_cert_chain JSONB; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS version TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS runtime_verified BOOLEAN NOT NULL DEFAULT FALSE; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS python_hash TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS runtime_hash TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS last_challenge_verified TIMESTAMPTZ; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS failed_challenges INT NOT NULL DEFAULT 0; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS account_id TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS lifetime_requests_served BIGINT NOT NULL DEFAULT 0; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS lifetime_tokens_generated BIGINT NOT NULL DEFAULT 0; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS last_session_requests_served BIGINT NOT NULL DEFAULT 0; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS last_session_tokens_generated BIGINT NOT NULL DEFAULT 0; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS lifetime_stats JSONB NOT NULL DEFAULT '{}'::jsonb; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE providers ADD COLUMN IF NOT EXISTS last_session_stats JSONB NOT NULL DEFAULT '{}'::jsonb; EXCEPTION WHEN others THEN NULL; END $$`,
		`CREATE INDEX IF NOT EXISTS idx_providers_serial ON providers(serial_number) WHERE serial_number != ''`,
		`CREATE INDEX IF NOT EXISTS idx_providers_account ON providers(account_id, last_seen DESC) WHERE account_id != ''`,

		// Migrate usage table: add request_id and cost columns
		`DO $$ BEGIN ALTER TABLE usage ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE usage ADD COLUMN IF NOT EXISTS cost_micro_usd BIGINT NOT NULL DEFAULT 0; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE usage ADD COLUMN IF NOT EXISTS request_location JSONB; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE usage ADD COLUMN IF NOT EXISTS public_model TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,

		// Provider reputation — persistent reputation tracking
		`CREATE TABLE IF NOT EXISTS provider_reputation (
			provider_id TEXT PRIMARY KEY REFERENCES providers(id),
			total_jobs INT NOT NULL DEFAULT 0,
			successful_jobs INT NOT NULL DEFAULT 0,
			failed_jobs INT NOT NULL DEFAULT 0,
			total_uptime_seconds BIGINT NOT NULL DEFAULT 0,
			avg_response_time_ms BIGINT NOT NULL DEFAULT 0,
			challenges_passed INT NOT NULL DEFAULT 0,
			challenges_failed INT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS api_keys (
			key_hash TEXT PRIMARY KEY,
			raw_prefix TEXT NOT NULL,
			owner_account_id TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			active BOOLEAN NOT NULL DEFAULT TRUE
		)`,
		`DO $$ BEGIN
			ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS owner_account_id TEXT NOT NULL DEFAULT '';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		// Multi-key support: per-key id, name, limits, expiry, last-used.
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS id TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS limit_micro_usd BIGINT; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS limit_reset TEXT NOT NULL DEFAULT 'none'; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS rpm_limit BIGINT; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS itpm_limit BIGINT; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS otpm_limit BIGINT; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS allowed_models TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS self_route_only BOOLEAN NOT NULL DEFAULT FALSE; EXCEPTION WHEN others THEN NULL; END $$`,
		// Backfill stable IDs for legacy rows (deterministic from the hash so
		// it is stable across restarts and idempotent).
		`UPDATE api_keys SET id = 'key_' || substr(md5(key_hash), 1, 24) WHERE id IS NULL OR id = ''`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_id ON api_keys(id) WHERE id <> ''`,
		`CREATE INDEX IF NOT EXISTS idx_api_keys_owner ON api_keys(owner_account_id) WHERE owner_account_id <> ''`,
		`CREATE TABLE IF NOT EXISTS usage (
			id BIGSERIAL PRIMARY KEY,
			provider_id TEXT NOT NULL,
			consumer_key_hash TEXT NOT NULL,
				key_id TEXT NOT NULL DEFAULT '',
				model TEXT NOT NULL,
				public_model TEXT NOT NULL DEFAULT '',
				prompt_tokens INTEGER NOT NULL,
				completion_tokens INTEGER NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			request_id TEXT NOT NULL DEFAULT '',
			cost_micro_usd BIGINT NOT NULL DEFAULT 0,
			request_location JSONB
		)`,
		// Per-key usage attribution — ALTER for DBs upgrading from a usage
		// table created before key_id existed. Must run AFTER CREATE TABLE usage.
		`DO $$ BEGIN ALTER TABLE usage ADD COLUMN IF NOT EXISTS key_id TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE usage ADD COLUMN IF NOT EXISTS public_model TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		// Indexes for usage queries (stats, billing, per-consumer history).
		`CREATE INDEX IF NOT EXISTS idx_usage_created ON usage(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_consumer ON usage(consumer_key_hash, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_provider ON usage(provider_id, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_usage_key ON usage(key_id, created_at DESC) WHERE key_id <> ''`,

		`CREATE TABLE IF NOT EXISTS payments (
			id BIGSERIAL PRIMARY KEY,
			tx_hash TEXT UNIQUE,
			consumer_address TEXT NOT NULL,
			provider_address TEXT NOT NULL,
			amount_usd TEXT NOT NULL,
			model TEXT NOT NULL,
			prompt_tokens INTEGER NOT NULL,
			completion_tokens INTEGER NOT NULL,
			memo TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS balances (
			account_id TEXT PRIMARY KEY,
			balance_micro_usd BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS ledger_entries (
			id BIGSERIAL PRIMARY KEY,
			account_id TEXT NOT NULL,
			entry_type TEXT NOT NULL,
			amount_micro_usd BIGINT NOT NULL,
			balance_after BIGINT NOT NULL,
			reference TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_ledger_account ON ledger_entries(account_id, created_at DESC)`,
		// Partial index for the public leaderboard/network-totals reward scans,
		// which filter ledger_entries by reward entry_type across all accounts.
		// Without it, each cache miss seq-scans the whole (multi-million-row)
		// ledger to find the handful of reward rows. Predicate is derived from
		// RewardLedgerTypes so it matches the query's IN-list exactly.
		`CREATE INDEX IF NOT EXISTS idx_ledger_reward ON ledger_entries(account_id, created_at DESC) WHERE entry_type IN (` + rewardLedgerTypesSQLList() + `)`,

		// Referral system tables
		`CREATE TABLE IF NOT EXISTS referrers (
			account_id TEXT PRIMARY KEY,
			code TEXT UNIQUE NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_referrers_code ON referrers(code)`,

		`CREATE TABLE IF NOT EXISTS referrals (
			referred_account TEXT PRIMARY KEY,
			referrer_code TEXT NOT NULL REFERENCES referrers(code),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_referrals_code ON referrals(referrer_code)`,

		// Billing sessions table
		`CREATE TABLE IF NOT EXISTS billing_sessions (
			id TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			payment_method TEXT NOT NULL,
			amount_micro_usd BIGINT NOT NULL,
			external_id TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'pending',
			referral_code TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			completed_at TIMESTAMPTZ
		)`,
		`CREATE INDEX IF NOT EXISTS idx_billing_sessions_account ON billing_sessions(account_id)`,
		`CREATE INDEX IF NOT EXISTS idx_billing_sessions_external ON billing_sessions(external_id)`,
		`DO $$ BEGIN
			ALTER TABLE billing_sessions DROP COLUMN IF EXISTS chain;
		EXCEPTION WHEN others THEN NULL;
		END $$`,

		// Custom pricing — per-account model price overrides
		`CREATE TABLE IF NOT EXISTS model_prices (
			account_id TEXT NOT NULL,
			model TEXT NOT NULL,
			input_price BIGINT NOT NULL,
			output_price BIGINT NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (account_id, model)
		)`,

		// Clean up wallet-keyed custom prices: with the removal of wallet-based
		// payouts, model_prices rows keyed by Solana wallet addresses are
		// unreachable. Providers must re-enter custom prices under their Stripe
		// Connect account ID.
		//
		// This is a one-time, destructive cleanup, so it is gated on a
		// schema_migrations marker and runs at most once instead of on every boot.
		// Two further guards:
		//   - Exclude the synthetic "platform" account. Platform-default per-model
		//     pricing (set via PUT /v1/admin/pricing and at model registration) is
		//     stored under account_id='platform', which is NEVER a row in users.
		//     Without this guard the cleanup would wipe all platform pricing,
		//     silently reverting billing to the fallback defaults.
		//   - The marker is written only after a successful DELETE within the same
		//     block, so a run that errors (e.g. users not yet created on a brand-new
		//     DB) rolls back and is retried on the next boot.
		`DO $$ BEGIN
			IF NOT EXISTS (SELECT 1 FROM schema_migrations WHERE id = 'cleanup_wallet_model_prices_v1') THEN
				DELETE FROM model_prices
				WHERE account_id NOT IN (SELECT account_id FROM users)
				  AND account_id <> 'platform';
				INSERT INTO schema_migrations (id) VALUES ('cleanup_wallet_model_prices_v1');
			END IF;
		EXCEPTION WHEN others THEN NULL;
		END $$`,

		// Users — Privy identity → internal account mapping
		`CREATE TABLE IF NOT EXISTS users (
			account_id TEXT PRIMARY KEY,
			privy_user_id TEXT UNIQUE NOT NULL,
			email TEXT NOT NULL DEFAULT '',
			role TEXT NOT NULL DEFAULT '',
			platform_fee_percent BIGINT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`DO $$ BEGIN
			ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT NOT NULL DEFAULT '';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE users DROP COLUMN IF EXISTS solana_wallet_address;
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE users DROP COLUMN IF EXISTS solana_wallet_id;
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_privy ON users(privy_user_id)`,

		// The legacy admin-managed supported_models catalog was replaced by the
		// manifest-backed model_registry below. Drop the stale duplicate table if
		// it is still present from an older deployment.
		`DROP TABLE IF EXISTS supported_models`,

		`CREATE TABLE IF NOT EXISTS model_registry (
			id TEXT PRIMARY KEY,
			display_name TEXT NOT NULL,
			family TEXT NOT NULL DEFAULT '',
			architecture TEXT NOT NULL DEFAULT '',
			quantization TEXT NOT NULL DEFAULT '',
			max_context_length INTEGER NOT NULL DEFAULT 0,
			max_output_length INTEGER NOT NULL DEFAULT 0,
			min_ram_gb INTEGER NOT NULL DEFAULT 0,
			capabilities TEXT[] NOT NULL DEFAULT '{}',
			required_provider_capabilities TEXT[] NOT NULL DEFAULT '{}',
			status TEXT NOT NULL DEFAULT 'beta',
			description TEXT NOT NULL DEFAULT '',
			runtime_parameters JSONB NOT NULL DEFAULT '{}',
			metadata JSONB NOT NULL DEFAULT '{}',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_model_registry_status ON model_registry(status)`,
		`CREATE TABLE IF NOT EXISTS model_versions (
			id BIGSERIAL PRIMARY KEY,
			model_id TEXT NOT NULL REFERENCES model_registry(id) ON DELETE CASCADE,
			version TEXT NOT NULL,
			r2_prefix TEXT NOT NULL,
			aggregate_sha256 TEXT NOT NULL,
			total_size_bytes BIGINT NOT NULL,
			file_count INTEGER NOT NULL,
			status TEXT NOT NULL DEFAULT 'ready',
			uploaded_by TEXT NOT NULL DEFAULT '',
			uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			promoted_at TIMESTAMPTZ,
			metadata JSONB NOT NULL DEFAULT '{}',
			UNIQUE(model_id, version)
		)`,
		`ALTER TABLE model_versions ADD COLUMN IF NOT EXISTS hugging_face_artifact JSONB`,
		`DO $$ BEGIN
			ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS max_context_length INTEGER NOT NULL DEFAULT 0;
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS max_output_length INTEGER NOT NULL DEFAULT 0;
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS runtime_parameters JSONB NOT NULL DEFAULT '{}';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE model_registry ADD COLUMN IF NOT EXISTS required_provider_capabilities TEXT[] NOT NULL DEFAULT '{}';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`UPDATE model_registry
		 SET required_provider_capabilities = (
		   SELECT ARRAY_AGG(DISTINCT capability ORDER BY capability)
		   FROM UNNEST(required_provider_capabilities ||
		     ARRAY['apple_m5', 'mlx_nax']::TEXT[]) AS capability
		 )
		 WHERE id = 'EigenLabs/Qwen3.8-27B-4bit'
		   AND NOT (required_provider_capabilities @>
		     ARRAY['apple_m5', 'mlx_nax']::TEXT[])`,
		`CREATE INDEX IF NOT EXISTS idx_model_versions_model ON model_versions(model_id)`,
		`CREATE TABLE IF NOT EXISTS model_version_files (
			id BIGSERIAL PRIMARY KEY,
			model_version_id BIGINT NOT NULL REFERENCES model_versions(id) ON DELETE CASCADE,
			path TEXT NOT NULL,
			size_bytes BIGINT NOT NULL,
			sha256 TEXT NOT NULL,
			role TEXT NOT NULL,
			UNIQUE(model_version_id, path)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_model_version_files_version ON model_version_files(model_version_id)`,
		`CREATE TABLE IF NOT EXISTS model_active_versions (
			model_id TEXT PRIMARY KEY REFERENCES model_registry(id) ON DELETE CASCADE,
			model_version_id BIGINT NOT NULL REFERENCES model_versions(id) ON DELETE RESTRICT,
			activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS publishing_api_keys (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			key_hash TEXT NOT NULL,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_used_at TIMESTAMPTZ
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_publishing_api_keys_hash ON publishing_api_keys(key_hash)`,

		// Model aliases (public-facing names → a desired concrete build). An alias
		// resolves to a single desired_build (the build providers converge to) with
		// an optional previous_build that stays acceptable during a rollout. Lets us
		// swap the underlying quant (fp8 → qat-4bit) behind a stable consumer-facing
		// model name. The legacy `builds` JSONB column is kept (nullable, default
		// '[]') only so an older coordinator binary doesn't choke on the table; it
		// is no longer read or written — drop it in a follow-up release.
		`CREATE TABLE IF NOT EXISTS model_aliases (
			alias_id TEXT PRIMARY KEY,
			display_name TEXT NOT NULL DEFAULT '',
			builds JSONB NOT NULL DEFAULT '[]'::jsonb,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		// Declarative desired/previous build pointers (additive migration).
		`DO $$ BEGIN ALTER TABLE model_aliases ADD COLUMN IF NOT EXISTS desired_build TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE model_aliases ADD COLUMN IF NOT EXISTS previous_build TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		// Alias lineage: former desired/previous builds rotated out by later
		// upserts, so a provider returning from a long offline period is still
		// recognized as part of the alias's fleet.
		`DO $$ BEGIN ALTER TABLE model_aliases ADD COLUMN IF NOT EXISTS retired_builds JSONB NOT NULL DEFAULT '[]'::jsonb; EXCEPTION WHEN others THEN NULL; END $$`,
		// OpenRouter-only aliases clone an existing public alias or concrete model
		// while keeping an independent API id and marketplace identities. Existing
		// rows predate source_kind and therefore retain standard-alias semantics.
		`DO $$ BEGIN ALTER TABLE model_aliases ADD COLUMN IF NOT EXISTS openrouter_only BOOLEAN NOT NULL DEFAULT FALSE; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE model_aliases ADD COLUMN IF NOT EXISTS source_model TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE model_aliases ADD COLUMN IF NOT EXISTS source_kind TEXT NOT NULL DEFAULT 'standard_alias'; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE model_aliases ADD COLUMN IF NOT EXISTS openrouter_slug TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE model_aliases ADD COLUMN IF NOT EXISTS hugging_face_id TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,

		// Backfill desired_build from the old `builds` JSON: pick the highest-weight
		// active build of each alias that hasn't been migrated yet. DISTINCT ON keeps
		// exactly one (highest-weight) build per alias so the UPDATE...FROM join is
		// deterministic. One-shot; safe to re-run because it only touches rows still
		// on the empty default.
		`DO $$ BEGIN
			UPDATE model_aliases a
			SET desired_build = sub.build_id
			FROM (
				SELECT DISTINCT ON (alias_id) alias_id, (b->>'build_id') AS build_id
				FROM model_aliases, jsonb_array_elements(builds) AS b
				WHERE COALESCE((b->>'active')::boolean, true)
				  AND COALESCE((b->>'weight')::int, 0) > 0
				ORDER BY alias_id, COALESCE((b->>'weight')::int, 0) DESC
			) sub
			WHERE a.alias_id = sub.alias_id AND a.desired_build = '';
		EXCEPTION WHEN others THEN NULL; END $$`,
		// The weighted-ramp migration controller is gone; drop its table.
		`DROP TABLE IF EXISTS model_migrations`,

		// Releases (provider binary versioning)
		`CREATE TABLE IF NOT EXISTS releases (
			version TEXT NOT NULL,
			platform TEXT NOT NULL,
			backend TEXT NOT NULL DEFAULT '',
			binary_hash TEXT NOT NULL DEFAULT '',
			bundle_hash TEXT NOT NULL DEFAULT '',
			metallib_hash TEXT NOT NULL DEFAULT '',
			python_hash TEXT NOT NULL DEFAULT '',
			runtime_hash TEXT NOT NULL DEFAULT '',
			template_hashes TEXT NOT NULL DEFAULT '',
			grpc_binary_hash TEXT NOT NULL DEFAULT '',
			url TEXT NOT NULL DEFAULT '',
			changelog TEXT NOT NULL DEFAULT '',
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (version, platform)
		)`,
		`DO $$ BEGIN
			ALTER TABLE releases ADD COLUMN IF NOT EXISTS backend TEXT NOT NULL DEFAULT '';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE releases ADD COLUMN IF NOT EXISTS metallib_hash TEXT NOT NULL DEFAULT '';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE releases ADD COLUMN IF NOT EXISTS changelog TEXT NOT NULL DEFAULT '';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE releases ADD COLUMN IF NOT EXISTS python_hash TEXT NOT NULL DEFAULT '';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE releases ADD COLUMN IF NOT EXISTS runtime_hash TEXT NOT NULL DEFAULT '';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE releases ADD COLUMN IF NOT EXISTS template_hashes TEXT NOT NULL DEFAULT '';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		`DO $$ BEGIN
			ALTER TABLE releases ADD COLUMN IF NOT EXISTS grpc_binary_hash TEXT NOT NULL DEFAULT '';
		EXCEPTION WHEN others THEN NULL;
		END $$`,
		// Drop deprecated image_bridge_hash column. Image generation is no longer
		// a first-class capability; the hash is meaningless. The DROP is wrapped
		// in a DO block so it's safe to re-run on databases that already lack it.
		`DO $$ BEGIN
			ALTER TABLE releases DROP COLUMN IF EXISTS image_bridge_hash;
		EXCEPTION WHEN others THEN NULL;
		END $$`,

		// Device authorization (RFC 8628-style)
		`CREATE TABLE IF NOT EXISTS device_codes (
			device_code TEXT PRIMARY KEY,
			user_code TEXT UNIQUE NOT NULL,
			account_id TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'pending',
			expires_at TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_device_codes_user ON device_codes(user_code)`,

		// Provider tokens — long-lived auth linking provider machines to accounts
		`CREATE TABLE IF NOT EXISTS provider_tokens (
			token_hash TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			label TEXT NOT NULL DEFAULT '',
			active BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_tokens_account ON provider_tokens(account_id)`,

		// Invite codes
		`CREATE TABLE IF NOT EXISTS invite_codes (
			code TEXT PRIMARY KEY,
			amount_micro_usd BIGINT NOT NULL,
			max_uses INTEGER NOT NULL DEFAULT 1,
			used_count INTEGER NOT NULL DEFAULT 0,
			active BOOLEAN NOT NULL DEFAULT TRUE,
			expires_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS invite_redemptions (
			code TEXT NOT NULL REFERENCES invite_codes(code),
			account_id TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (code, account_id)
		)`,

		// Provider earnings — per-node tracking
		`CREATE TABLE IF NOT EXISTS provider_earnings (
			id BIGSERIAL PRIMARY KEY,
			account_id TEXT NOT NULL,
			provider_id TEXT NOT NULL,
			provider_key TEXT NOT NULL DEFAULT '',
			job_id TEXT NOT NULL,
			model TEXT NOT NULL,
			amount_micro_usd BIGINT NOT NULL,
			prompt_tokens INTEGER NOT NULL DEFAULT 0,
			completion_tokens INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_earnings_account ON provider_earnings(account_id, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_earnings_provider ON provider_earnings(provider_key, created_at DESC)`,

		// Materialized earnings summaries — atomically maintained by CreditProviderAccount.
		// Eliminates full-table SUM scans on /v1/provider/account-earnings.
		`CREATE TABLE IF NOT EXISTS earnings_summary (
			key TEXT NOT NULL,
			key_type TEXT NOT NULL,
			total_count BIGINT NOT NULL DEFAULT 0,
			total_micro_usd BIGINT NOT NULL DEFAULT 0,
			total_prompt_tokens BIGINT NOT NULL DEFAULT 0,
			total_completion_tokens BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (key, key_type)
		)`,

		// Backfill earnings_summary from existing provider_earnings rows.
		// The INSERT ... ON CONFLICT DO NOTHING ensures this only runs once per key.
		`INSERT INTO earnings_summary (key, key_type, total_count, total_micro_usd, total_prompt_tokens, total_completion_tokens, updated_at)
		 SELECT account_id, 'account', COUNT(*), COALESCE(SUM(amount_micro_usd), 0),
		        COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0), NOW()
		 FROM provider_earnings
		 WHERE account_id != ''
		 GROUP BY account_id
		 ON CONFLICT (key, key_type) DO NOTHING`,

		`INSERT INTO earnings_summary (key, key_type, total_count, total_micro_usd, total_prompt_tokens, total_completion_tokens, updated_at)
		 SELECT provider_key, 'provider', COUNT(*), COALESCE(SUM(amount_micro_usd), 0),
		        COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0), NOW()
		 FROM provider_earnings
		 WHERE provider_key != ''
		 GROUP BY provider_key
		 ON CONFLICT (key, key_type) DO NOTHING`,

		// Provider payouts — wallet-based payout history for unlinked providers
		`CREATE TABLE IF NOT EXISTS provider_payouts (
			id BIGSERIAL PRIMARY KEY,
			provider_address TEXT NOT NULL,
			amount_micro_usd BIGINT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			job_id TEXT NOT NULL DEFAULT '',
			settled BOOLEAN NOT NULL DEFAULT FALSE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_payouts_address ON provider_payouts(provider_address, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_payouts_settled ON provider_payouts(settled, created_at DESC)`,

		// Stripe Connect — bank/card payouts
		`DO $$ BEGIN ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_account_id TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_account_status TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_account_country TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_destination_type TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_destination_last4 TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_instant_eligible BOOLEAN NOT NULL DEFAULT FALSE; EXCEPTION WHEN others THEN NULL; END $$`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_stripe_account ON users(stripe_account_id) WHERE stripe_account_id != ''`,

		// Account role + per-account platform fee override (service accounts, e.g. OpenRouter).
		`DO $$ BEGIN ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE users ADD COLUMN IF NOT EXISTS platform_fee_percent BIGINT; EXCEPTION WHEN others THEN NULL; END $$`,

		`CREATE TABLE IF NOT EXISTS stripe_withdrawals (
			id TEXT PRIMARY KEY,
			account_id TEXT NOT NULL,
			stripe_account_id TEXT NOT NULL,
			transfer_id TEXT NOT NULL DEFAULT '',
			payout_id TEXT NOT NULL DEFAULT '',
			amount_micro_usd BIGINT NOT NULL,
			fee_micro_usd BIGINT NOT NULL DEFAULT 0,
			net_micro_usd BIGINT NOT NULL,
			method TEXT NOT NULL,
			status TEXT NOT NULL,
			failure_reason TEXT NOT NULL DEFAULT '',
			refunded BOOLEAN NOT NULL DEFAULT FALSE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_stripe_withdrawals_account ON stripe_withdrawals(account_id, created_at DESC)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_stripe_withdrawals_transfer ON stripe_withdrawals(transfer_id) WHERE transfer_id != ''`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_stripe_withdrawals_payout ON stripe_withdrawals(payout_id) WHERE payout_id != ''`,
		`CREATE INDEX IF NOT EXISTS idx_stripe_withdrawals_status ON stripe_withdrawals(status, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_stripe_withdrawals_stripe_account ON stripe_withdrawals(stripe_account_id, status)`,
		`DO $$ BEGIN ALTER TABLE stripe_withdrawals ADD COLUMN IF NOT EXISTS fee_refunded BOOLEAN NOT NULL DEFAULT FALSE; EXCEPTION WHEN others THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE stripe_withdrawals ADD COLUMN IF NOT EXISTS sweep_payout_id TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`CREATE INDEX IF NOT EXISTS idx_stripe_withdrawals_sweep_payout ON stripe_withdrawals(sweep_payout_id) WHERE sweep_payout_id != ''`,

		// Telemetry events table + indices removed.
		// Datadog is the sole durable sink for telemetry — the Postgres table
		// was the single largest source of DB write pressure under provider load
		// (60 providers × batch/10s × 50 rows × 5 indexes = ~30-40% of the
		// connection pool). No read endpoints consumed this table.',

		// Materialized usage totals — eliminates full-table scan of usage
		// on every stats cache miss.  Single counter row incremented
		// atomically by RecordUsage / RecordUsageWithCostAndLocation.
		`CREATE TABLE IF NOT EXISTS usage_totals (
			id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
			total_requests BIGINT NOT NULL DEFAULT 0,
			total_prompt_tokens BIGINT NOT NULL DEFAULT 0,
			total_completion_tokens BIGINT NOT NULL DEFAULT 0
		)`,

		// Partial index for UsageLocationBuckets — only rows with a
		// non-null request_location are ever queried.
		`CREATE INDEX IF NOT EXISTS idx_usage_request_location_notnull ON usage(created_at DESC) WHERE request_location IS NOT NULL`,

		// Provider log reports — providers upload 24h unified logs for debugging.
		// serial_number is retained only as a rollback-compatible legacy column.
		// The write guard and one-time scrub keep it empty.
		`CREATE TABLE IF NOT EXISTS provider_log_reports (
			id BIGSERIAL PRIMARY KEY,
			serial_number TEXT NOT NULL DEFAULT '',
			provider_id TEXT NOT NULL DEFAULT '',
			account_id TEXT NOT NULL DEFAULT '',
			log_data BYTEA NOT NULL,
			log_size_bytes BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`ALTER TABLE provider_log_reports ALTER COLUMN serial_number SET DEFAULT ''`,
		providerLogReportSerialGuardFunction,
		providerLogReportSerialGuardTrigger,
		providerLogReportSerialScrubMigration,
		`DROP INDEX IF EXISTS idx_log_reports_serial`,

		// Provider sessions — durable connect→disconnect history for uptime/downtime.
		// One row per websocket connection; disconnected_at IS NULL while open.
		// session_id is UNIQUE so the async open/close paths are order-independent
		// (open = INSERT ON CONFLICT DO NOTHING; close = upsert) — a fast
		// connect→disconnect where close races ahead of open cannot leave a
		// permanently-open row.
		`CREATE TABLE IF NOT EXISTS provider_sessions (
			id BIGSERIAL PRIMARY KEY,
			session_id TEXT NOT NULL UNIQUE,
			serial_number TEXT NOT NULL DEFAULT '',
			account_id TEXT NOT NULL DEFAULT '',
			connected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			disconnected_at TIMESTAMPTZ,
			disconnect_reason TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_sessions_serial ON provider_sessions(serial_number, connected_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_sessions_connected ON provider_sessions(connected_at DESC)`,
		// Partial index over still-open sessions — speeds the online-now count and
		// the startup reconcile. (session_id lookups use the UNIQUE index.)
		`CREATE INDEX IF NOT EXISTS idx_provider_sessions_open ON provider_sessions(connected_at) WHERE disconnected_at IS NULL`,

		// Inference routing telemetry — per-request scheduler decisions and outcomes.
		// Contains no prompt or response content.
		`CREATE TABLE IF NOT EXISTS inference_routes (
			id BIGSERIAL PRIMARY KEY,
			request_id TEXT NOT NULL,
			attempt INTEGER NOT NULL DEFAULT 0,
			provider_id TEXT NOT NULL DEFAULT '',
			model TEXT NOT NULL,
			public_model TEXT NOT NULL DEFAULT '',
			consumer_key_hash TEXT NOT NULL DEFAULT '',
			key_id TEXT NOT NULL DEFAULT '',
			outcome TEXT NOT NULL DEFAULT '',
			cost_ms DOUBLE PRECISION,
			state_ms DOUBLE PRECISION,
			queue_ms DOUBLE PRECISION,
			pending_ms DOUBLE PRECISION,
			backlog_ms DOUBLE PRECISION,
			this_req_ms DOUBLE PRECISION,
			health_ms DOUBLE PRECISION,
			ttft_ms DOUBLE PRECISION,
			best_ttft_ms DOUBLE PRECISION,
			effective_queue INTEGER,
			candidate_count INTEGER,
			capacity_rejections INTEGER,
			model_too_large_rejections INTEGER,
			vision_rejections INTEGER,
			ttft_rejections INTEGER,
			effective_tps DOUBLE PRECISION,
			static_tps DOUBLE PRECISION,
			provider_status TEXT,
			provider_trust_level TEXT,
			provider_version TEXT,
			hardware_chip TEXT,
			hardware_chip_family TEXT,
			hardware_tier TEXT,
			memory_gb INTEGER,
			gpu_cores INTEGER,
			cpu_cores INTEGER,
			system_memory_pressure DOUBLE PRECISION,
			system_cpu_usage DOUBLE PRECISION,
			system_thermal_state TEXT,
			gpu_memory_active_gb DOUBLE PRECISION,
			gpu_memory_peak_gb DOUBLE PRECISION,
			gpu_memory_cache_gb DOUBLE PRECISION,
			slot_state TEXT,
			backend_running INTEGER,
			backend_waiting INTEGER,
			active_token_budget_used BIGINT,
			active_token_budget_max BIGINT,
			queued_token_budget BIGINT,
			estimated_prompt_tokens INTEGER,
			requested_max_tokens INTEGER,
			requires_vision BOOLEAN NOT NULL DEFAULT FALSE,
			has_tools BOOLEAN NOT NULL DEFAULT FALSE,
			self_route_only BOOLEAN NOT NULL DEFAULT FALSE,
			prefer_owner BOOLEAN NOT NULL DEFAULT FALSE,
			cache_affinity_key TEXT NOT NULL DEFAULT '',
			final_status TEXT NOT NULL DEFAULT '',
			error_code INTEGER,
			error_class TEXT,
			prompt_tokens INTEGER,
			completion_tokens INTEGER,
			reasoning_tokens INTEGER,
			cost_micro_usd BIGINT,
			actual_ttft_ms DOUBLE PRECISION,
			dispatch_to_first_chunk_ms DOUBLE PRECISION,
			total_duration_ms DOUBLE PRECISION,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			provider_region TEXT,
			consumer_region TEXT,
			parse_ms DOUBLE PRECISION,
			reserve_ms DOUBLE PRECISION,
			route_ms DOUBLE PRECISION,
			encrypt_ms DOUBLE PRECISION,
			queue_wait_ms DOUBLE PRECISION,
			dispatch_ms DOUBLE PRECISION,
			actual_decode_tps DOUBLE PRECISION,
			admitted_but_failed BOOL,
			used_backup BOOL,
			backup_won BOOL,
			error_reason TEXT,
			UNIQUE(request_id, attempt)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_inference_routes_created ON inference_routes(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_inference_routes_provider ON inference_routes(provider_id, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_inference_routes_model ON inference_routes(model, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_inference_routes_request ON inference_routes(request_id)`,
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1
				FROM pg_index i
				JOIN pg_class t ON t.oid = i.indrelid
				WHERE t.oid = 'inference_routes'::regclass
				  AND i.indisunique
				  AND ARRAY(
					SELECT a.attname::text
					FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
					JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
					ORDER BY k.ord
				  ) = ARRAY['request_id', 'attempt']
			) THEN
				CREATE UNIQUE INDEX idx_inference_routes_request_attempt_unique ON inference_routes(request_id, attempt);
			END IF;
		END $$`,
		// Phase 1 additions to inference_routes: coarse geo, coordinator-side
		// latency decomposition, measured decode TPS, and admission/backup-race
		// outcome flags. Added idempotently so a dev DB that already created the
		// Phase 0 table picks them up. New columns are appended AFTER updated_at
		// in the CREATE TABLE above so fresh and ALTER'd DBs share one column
		// order (InferenceRouteRecordsSince scans `SELECT *` positionally).
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS provider_region TEXT`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS consumer_region TEXT`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS parse_ms DOUBLE PRECISION`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS reserve_ms DOUBLE PRECISION`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS route_ms DOUBLE PRECISION`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS encrypt_ms DOUBLE PRECISION`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS queue_wait_ms DOUBLE PRECISION`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS dispatch_ms DOUBLE PRECISION`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS actual_decode_tps DOUBLE PRECISION`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS admitted_but_failed BOOL`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS used_backup BOOL`,
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS backup_won BOOL`,
		// DAR-341: normalized provider/coordinator error reason. Nullable and
		// appended so fresh DBs match upgraded DB column order for SELECT * scans.
		`ALTER TABLE inference_routes ADD COLUMN IF NOT EXISTS error_reason TEXT`,
		// Route keys are memory-only HMACs. Scrub legacy persisted SHA-256
		// prompt-cache identifiers once. The trigger also clears writes from an
		// older coordinator during blue-green overlap or emergency rollback while
		// retaining that binary's expected SQL shape.
		legacyCacheAffinityGuardFunction,
		legacyCacheAffinityGuardTrigger,
		legacyCacheAffinityScrubMigration,

		// Rejected inbound inference requests (4xx/5xx) at any pipeline stage,
		// with the request shape and a counterfactual servability snapshot
		// ("could the fleet have served it?"). Contains no prompt or response
		// content.
		`CREATE TABLE IF NOT EXISTS request_rejections (
			id BIGSERIAL PRIMARY KEY,
			request_id TEXT,
			endpoint TEXT,
			stage TEXT,
			reason_code TEXT,
			http_status INT,
			consumer_key_hash TEXT,
			key_id TEXT,
			client_class TEXT,
			requested_model TEXT,
			resolved_model TEXT,
			stream BOOL,
			n INT,
			estimated_prompt_tokens INT,
			requested_max_tokens INT,
			requires_vision BOOL,
			has_image BOOL,
			has_audio BOOL,
			has_tools BOOL,
			tool_count INT,
			response_format TEXT,
			self_route_only BOOL,
			prefer_owner BOOL,
			params JSONB,
			request_body_bytes INT,
			retry_after_ms INT,
			could_have_served BOOL,
			candidate_count INT,
			capacity_rejections INT,
			model_too_large_rejections INT,
			vision_rejections INT,
			warm_provider_existed BOOL,
			best_ttft_ms DOUBLE PRECISION,
			shortfall_micro_usd BIGINT,
			limit_kind TEXT,
			over_by BIGINT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_request_rejections_created ON request_rejections(created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_request_rejections_reason ON request_rejections(reason_code, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_request_rejections_model ON request_rejections(resolved_model, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_request_rejections_status ON request_rejections(http_status, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_request_rejections_servable ON request_rejections(could_have_served, created_at DESC) WHERE could_have_served = true`,

		// APNs code-identity attestation reuse cache (W5 Fix 2). Persists the
		// in-memory reuse cache so a blue-green deploy / restart does not wipe it
		// and provoke a fleet-wide push storm against Apple's ~3/hour/device push
		// budget. One row per device (keyed by Secure Enclave public key). The
		// row records that the device completed a FULL code-identity round-trip at
		// attested_at on binary version; the freshness + version gate is applied on
		// READ (in the coordinator), so a stale/wrong-version row never extends
		// trust — it only lets the coordinator skip a redundant push.
		`CREATE TABLE IF NOT EXISTS code_attestations (
			se_pubkey TEXT PRIMARY KEY,
			version TEXT NOT NULL DEFAULT '',
			attested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			apns_token TEXT NOT NULL DEFAULT '',
			node_public_key TEXT NOT NULL DEFAULT '',
			binary_hash TEXT NOT NULL DEFAULT ''
		)`,
		// Token-binding column for reuse (Codex #7): additive for DBs whose
		// code_attestations table predates it (the CREATE above is a no-op there).
		`ALTER TABLE code_attestations ADD COLUMN IF NOT EXISTS apns_token TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE code_attestations ADD COLUMN IF NOT EXISTS node_public_key TEXT NOT NULL DEFAULT ''`,
		// Attested binary identity (Codex 05:55Z P1): additive; a pre-existing
		// row's empty hash marks a legacy identity-less proof, which never
		// authorizes a release-transition resume (real APNs challenge instead).
		`ALTER TABLE code_attestations ADD COLUMN IF NOT EXISTS binary_hash TEXT NOT NULL DEFAULT ''`,
		// Durable APNs admission state is deliberately separate from successful
		// attestation evidence. Spending a push budget never creates trust.
		`CREATE TABLE IF NOT EXISTS code_attest_push_budgets (
			se_pubkey TEXT NOT NULL,
			token_hash TEXT NOT NULL DEFAULT '',
			next_push_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (se_pubkey, token_hash)
		)`,
		`DO $$
		DECLARE key_columns TEXT[];
		BEGIN
			SELECT array_agg(a.attname::TEXT ORDER BY k.ordinality)
			  INTO key_columns
			  FROM pg_constraint c
			  CROSS JOIN LATERAL unnest(c.conkey)
			    WITH ORDINALITY AS k(attnum, ordinality)
			  JOIN pg_attribute a
			    ON a.attrelid = c.conrelid AND a.attnum = k.attnum
			 WHERE c.conrelid = 'code_attest_push_budgets'::regclass
			   AND c.contype = 'p';
			IF key_columns IS DISTINCT FROM
			   ARRAY['se_pubkey', 'token_hash']::TEXT[] THEN
				ALTER TABLE code_attest_push_budgets
					DROP CONSTRAINT IF EXISTS code_attest_push_budgets_pkey;
				ALTER TABLE code_attest_push_budgets
					ADD CONSTRAINT code_attest_push_budgets_pkey
					PRIMARY KEY (se_pubkey, token_hash);
			END IF;
		END $$`,
		`CREATE INDEX IF NOT EXISTS idx_code_attest_push_budgets_due
			ON code_attest_push_budgets(next_push_at)`,
		// Durable rotation-clear cooldown (Codex 06:36Z P1): the sentinel row
		// records the last honored floor clear so restart/blue-green peers
		// share one anti-abuse clear budget. NULL = never cleared (legacy rows
		// and fresh sentinels clear immediately, preserving genuine-rotation UX).
		`ALTER TABLE code_attest_push_budgets
			ADD COLUMN IF NOT EXISTS last_clear_at TIMESTAMPTZ`,

		// Durable provider device evidence. The legacy binary_hash/verified_at
		// columns remain accepted during migration, but application proof is never
		// fabricated from them.
		`CREATE TABLE IF NOT EXISTS provider_trust_reuse (
			se_pubkey TEXT PRIMARY KEY,
			serial TEXT NOT NULL DEFAULT '',
			trust_level TEXT NOT NULL DEFAULT '',
			binary_hash TEXT NOT NULL DEFAULT '',
			sip_enabled BOOL NOT NULL DEFAULT FALSE,
			secure_boot_full BOOL NOT NULL DEFAULT FALSE,
			mda_udid TEXT NOT NULL DEFAULT '',
			verified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			last_verified_binary_hash TEXT NOT NULL DEFAULT '',
			hardware_proof_verified_at TIMESTAMPTZ,
			application_proof_verified_at TIMESTAMPTZ,
			evidence_generation BIGINT NOT NULL DEFAULT 1,
			revocation_generation BIGINT NOT NULL DEFAULT 0,
			revocation_event_id TEXT NOT NULL DEFAULT '',
			revoked_at TIMESTAMPTZ
		)`,
		`ALTER TABLE provider_trust_reuse ADD COLUMN IF NOT EXISTS last_verified_binary_hash TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE provider_trust_reuse ADD COLUMN IF NOT EXISTS hardware_proof_verified_at TIMESTAMPTZ`,
		`ALTER TABLE provider_trust_reuse ADD COLUMN IF NOT EXISTS application_proof_verified_at TIMESTAMPTZ`,
		`ALTER TABLE provider_trust_reuse ADD COLUMN IF NOT EXISTS evidence_generation BIGINT NOT NULL DEFAULT 1`,
		`ALTER TABLE provider_trust_reuse ADD COLUMN IF NOT EXISTS revocation_generation BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE provider_trust_reuse ADD COLUMN IF NOT EXISTS revocation_event_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE provider_trust_reuse ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ`,
		// Coordinator-measured continuous-liveness watermark (connection
		// continuity trust reuse). Written only by the coordinator; NULL for
		// pre-migration rows means "no continuity evidence" (fail-safe).
		`ALTER TABLE provider_trust_reuse ADD COLUMN IF NOT EXISTS continuous_coverage_until TIMESTAMPTZ`,
		`UPDATE provider_trust_reuse
		 SET hardware_proof_verified_at = verified_at
		 WHERE hardware_proof_verified_at IS NULL`,
		`UPDATE provider_trust_reuse
		 SET last_verified_binary_hash = binary_hash
		 WHERE last_verified_binary_hash = '' AND binary_hash <> ''`,
		`ALTER TABLE provider_trust_reuse ALTER COLUMN hardware_proof_verified_at SET DEFAULT NOW()`,
		`ALTER TABLE provider_trust_reuse ALTER COLUMN hardware_proof_verified_at SET NOT NULL`,

		// Durable bounded verification scheduler. This table contains retry and
		// short claim metadata only; provider/session IDs are intentionally absent
		// and a row is never trust authority.
		`CREATE TABLE IF NOT EXISTS provider_verification_jobs (
			se_pubkey TEXT NOT NULL,
			serial TEXT NOT NULL DEFAULT '',
			udid TEXT NOT NULL DEFAULT '',
			task_kind TEXT NOT NULL,
			task_state TEXT NOT NULL,
			priority SMALLINT NOT NULL,
			retry_stage INTEGER NOT NULL DEFAULT 0,
			previous_delay_ns BIGINT NOT NULL DEFAULT 0,
			next_attempt_at TIMESTAMPTZ,
			last_outcome TEXT NOT NULL DEFAULT 'none',
			reopen_pending BOOLEAN NOT NULL DEFAULT FALSE,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			claim_owner TEXT NOT NULL DEFAULT '',
			claim_expires_at TIMESTAMPTZ,
			PRIMARY KEY (se_pubkey, task_kind)
		)`,
		`ALTER TABLE provider_verification_jobs
		 ADD COLUMN IF NOT EXISTS reopen_pending BOOLEAN NOT NULL DEFAULT FALSE`,
		`CREATE INDEX IF NOT EXISTS idx_provider_verification_jobs_due
		 ON provider_verification_jobs(priority, next_attempt_at)
		 WHERE task_state IN ('pending', 'backoff')`,
		`CREATE INDEX IF NOT EXISTS idx_provider_verification_jobs_claim
		 ON provider_verification_jobs(claim_expires_at)
		 WHERE claim_owner <> ''`,

		// Base-rewards per-job settlement idempotency relies on a partial UNIQUE
		// index on provider_earnings(job_id). DAR-349: that index is built AFTER
		// this migration loop, CONCURRENTLY and at most once, by
		// ensureProviderEarningsJobIndex — NEVER with a boot-time dedupe DELETE.
		// The old `DELETE ... GROUP BY job_id` here full-scanned and locked this
		// hot table (~900k rows / ~443MB) for ~15m on deploy, blocking the
		// coordinator from binding :8080 and causing a production outage, while
		// doing no useful work (prod duplicate count is 0). Offline dedupe, if it
		// is ever needed, lives in coordinator/store/migrations/dedupe_provider_earnings.sql.

		// Base-rewards: unify sessions↔earnings identity (design §8).
		`DO $$ BEGIN ALTER TABLE provider_sessions ADD COLUMN IF NOT EXISTS provider_key TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN others THEN NULL; END $$`,
		`CREATE INDEX IF NOT EXISTS idx_provider_sessions_key ON provider_sessions(provider_key, connected_at) WHERE provider_key <> ''`,

		// Base-rewards: idempotent epoch settlement, one row per (provider_key, epoch_id).
		`CREATE TABLE IF NOT EXISTS provider_floor_draws (
			id BIGSERIAL PRIMARY KEY,
			provider_key TEXT NOT NULL,
			account_id TEXT NOT NULL DEFAULT '',
			epoch_id TEXT NOT NULL,
			amount_micro_usd BIGINT NOT NULL,
			floor_micro_usd BIGINT NOT NULL DEFAULT 0,
			earned_micro_usd BIGINT NOT NULL DEFAULT 0,
			uptime_frac DOUBLE PRECISION NOT NULL DEFAULT 0,
			memory_gb INTEGER NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (provider_key, epoch_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_floor_draws_epoch ON provider_floor_draws(epoch_id)`,
		`CREATE INDEX IF NOT EXISTS idx_floor_draws_account ON provider_floor_draws(account_id, epoch_id)`,

		// System profiler (docs/reports request-profiler plan §2.1/§2.4): two NEW
		// append-only telemetry tables, no ALTER on any existing table. The DDL
		// text lives in the request/fleet constants below so tests can pin the Go
		// column lists to it. Both tables are created empty on first boot, so the
		// plain CREATE INDEX statements never lock a populated table; any FUTURE
		// index on these tables must be built CONCURRENTLY outside this loop
		// (see ensureProviderEarningsJobIndex). The request_waterfall view is NOT
		// here — it is applied by hand from store/migrations/request_waterfall.sql.
		requestProfilesTableDDL,
		requestProfilesCreatedIndexDDL,
		requestProfilesCoordIndexDDL,
		requestProfilesProviderIndexDDL,
		fleetSnapshotsTableDDL,
		fleetSnapshotsSampledIndexDDL,
		// request_profiles / fleet_snapshots are profiler-only and cold (never
		// hot-path locked), so idempotent ADD COLUMN IF NOT EXISTS is safe here;
		// it upgrades a database that first booted at 02832be21 (before the
		// request-shape columns) or before the fleet capability columns. Only
		// duplicate_column (two coordinators racing the same ALTER) is swallowed;
		// any other failure aborts boot so a half-migrated schema is never served.
		`DO $$ BEGIN ALTER TABLE request_profiles ADD COLUMN IF NOT EXISTS estimated_prompt_tokens INT NOT NULL DEFAULT 0; EXCEPTION WHEN duplicate_column THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE request_profiles ADD COLUMN IF NOT EXISTS requested_max_tokens INT NOT NULL DEFAULT 0; EXCEPTION WHEN duplicate_column THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE request_profiles ADD COLUMN IF NOT EXISTS requires_vision BOOL NOT NULL DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE request_profiles ADD COLUMN IF NOT EXISTS has_tools BOOL NOT NULL DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE fleet_snapshots ADD COLUMN IF NOT EXISTS provider_version TEXT NOT NULL DEFAULT ''; EXCEPTION WHEN duplicate_column THEN NULL; END $$`,
		// free_for_load_gb became nullable (nil = provider did not report it); idempotent.
		`ALTER TABLE fleet_snapshots ALTER COLUMN free_for_load_gb DROP NOT NULL`,
		`DO $$ BEGIN ALTER TABLE fleet_snapshots ADD COLUMN IF NOT EXISTS model_vision BOOL NOT NULL DEFAULT FALSE; EXCEPTION WHEN duplicate_column THEN NULL; END $$`,
		`DO $$ BEGIN ALTER TABLE fleet_snapshots ADD COLUMN IF NOT EXISTS template_render_ok BOOL; EXCEPTION WHEN duplicate_column THEN NULL; END $$`,
		fleetSnapshotsProviderIndexDDL,
	}

	for _, m := range migrations {
		if _, err := s.pool.Exec(ctx, m); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	if err := s.migrateUsageTotals(ctx); err != nil {
		return err
	}

	if err := s.migrateWithdrawableBalance(ctx); err != nil {
		return err
	}

	// DAR-349: build the provider_earnings(job_id) partial unique index outside
	// the loop — CONCURRENTLY, duplicate-checked, and at most once — so coordinator
	// startup never runs a long, lock-holding data migration on this hot table.
	if err := s.ensureProviderEarningsJobIndex(ctx); err != nil {
		return err
	}
	return nil
}

// ensureProviderEarningsJobIndex creates the partial UNIQUE index that backs the
// `ON CONFLICT (job_id) WHERE job_id <> ” DO NOTHING` idempotency used by
// RecordProviderEarning and CreditProviderAccount.
//
// DAR-349: this MUST stay cheap and non-blocking on the serving startup path.
//   - Fast path: if a valid index already exists, return immediately (every boot
//     after the first does no work here).
//   - It NEVER deletes rows. If existing data would violate uniqueness it fails
//     loudly with an actionable message rather than running a destructive,
//     table-locking cleanup at boot (the original outage).
//   - The build is CONCURRENTLY so a blue-green old coordinator still writing to
//     provider_earnings is never lock-blocked, and uses the simple query protocol
//     because CREATE INDEX CONCURRENTLY cannot run inside the extended protocol's
//     implicit transaction.
func (s *PostgresStore) ensureProviderEarningsJobIndex(ctx context.Context) error {
	const idxName = "idx_provider_earnings_job"

	// Already present AND valid? No-op fast path for every boot after the first.
	var valid bool
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE((
			SELECT i.indisvalid
			FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
			WHERE c.relname = $1
		), false)`, idxName).Scan(&valid); err != nil {
		return fmt.Errorf("store: check %s: %w", idxName, err)
	}
	if valid {
		return nil
	}

	// A leftover *invalid* index from a previously interrupted CONCURRENTLY build
	// would make CREATE ... IF NOT EXISTS a silent no-op, so drop it first.
	if _, err := s.pool.Exec(ctx, `DROP INDEX IF EXISTS `+idxName); err != nil {
		return fmt.Errorf("store: drop invalid %s: %w", idxName, err)
	}

	// Verify the data can support a UNIQUE index. We do NOT dedupe at boot.
	var dupGroups int64
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT 1 FROM provider_earnings
			WHERE job_id <> '' GROUP BY job_id HAVING count(*) > 1
		) d`).Scan(&dupGroups); err != nil {
		return fmt.Errorf("store: count duplicate provider_earnings job_ids: %w", err)
	}
	if dupGroups > 0 {
		return fmt.Errorf("store: %d duplicate provider_earnings.job_id group(s) block unique index %s; "+
			"run the offline dedupe (coordinator/store/migrations/dedupe_provider_earnings.sql) before deploying "+
			"— boot does NOT auto-dedupe (DAR-349)", dupGroups, idxName)
	}

	// Build CONCURRENTLY on a dedicated connection via the simple query protocol.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("store: acquire conn for %s: %w", idxName, err)
	}
	defer conn.Release()
	mrr := conn.Conn().PgConn().Exec(ctx,
		`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_provider_earnings_job ON provider_earnings(job_id) WHERE job_id <> ''`)
	if _, err := mrr.ReadAll(); err != nil {
		return fmt.Errorf("store: create %s concurrently: %w", idxName, err)
	}
	return nil
}

// hashKey returns the SHA-256 hex digest of the given API key.
func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// HashKey returns the SHA-256 hex digest of the given API key.
func HashKey(key string) string { return hashKey(key) }

// rowScanner is satisfied by both pgx.Row and pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// encodeModelList serializes a model allow-list for storage. Empty → "".
func encodeModelList(models []string) string {
	if len(models) == 0 {
		return ""
	}
	b, err := json.Marshal(models)
	if err != nil {
		return ""
	}
	return string(b)
}

// decodeModelList parses a stored model allow-list. "" / invalid → nil.
func decodeModelList(s string) []string {
	if s == "" || s == "[]" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func nullSince(since time.Time) any {
	if since.IsZero() {
		return nil
	}
	return since
}

func nullableCreatedAt(ts time.Time) any {
	if ts.IsZero() {
		return nil
	}
	return ts
}

// pgQuerier is the subset of *pgxpool.Pool and pgx.Tx the single-statement
// ledger helpers need, so one helper serves both a standalone call (pool: one
// round trip in an implicit transaction) and a caller's open transaction.
type pgQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// System profiler DDL (boot slice). Column names are the snake_case of the
// RequestProfileRecord / FleetSnapshotRow field names; nullability mirrors Go
// pointer-ness (pointer and json.RawMessage fields are nullable, everything
// else is NOT NULL with a zero default so zero values — 0, empty string,
// false — round-trip as themselves).
// Per-table autovacuum thresholds are tightened because both tables are
// insert-heavy with a rolling retention DELETE.
const (
	requestProfilesTableDDL = `CREATE TABLE IF NOT EXISTS request_profiles (
			id BIGSERIAL PRIMARY KEY,
			coord_request_id TEXT NOT NULL,
			request_id TEXT NOT NULL,
			attempt INT NOT NULL,
			backup_of TEXT NOT NULL DEFAULT '',
			winning BOOL NOT NULL DEFAULT FALSE,
			endpoint TEXT NOT NULL DEFAULT '',
			stream BOOL NOT NULL DEFAULT FALSE,
			model TEXT NOT NULL DEFAULT '',
			public_model TEXT NOT NULL DEFAULT '',
			provider_id TEXT NOT NULL DEFAULT '',
			provider_version TEXT NOT NULL DEFAULT '',
			chip_family TEXT NOT NULL DEFAULT '',
			kv_backend TEXT NOT NULL DEFAULT '',
			final_status TEXT NOT NULL DEFAULT '',
			error_reason TEXT NOT NULL DEFAULT '',
			terminal_cause TEXT NOT NULL DEFAULT '',
			client_outcome TEXT NOT NULL DEFAULT '',
			provider_outcome TEXT NOT NULL DEFAULT '',
			client_gone_phase TEXT NOT NULL DEFAULT '',
			first_content_budget_ms INT NOT NULL DEFAULT 0,
			admission_mode TEXT NOT NULL DEFAULT '',
			estimated_prompt_tokens INT NOT NULL DEFAULT 0,
			requested_max_tokens INT NOT NULL DEFAULT 0,
			requires_vision BOOL NOT NULL DEFAULT FALSE,
			has_tools BOOL NOT NULL DEFAULT FALSE,
			received_at TIMESTAMPTZ NOT NULL,

			auth_done_us BIGINT,
			ratelimit_done_us BIGINT,
			sealed_open_us BIGINT,
			handler_entry_us BIGINT,
			parsed_us BIGINT,
			reserved_us BIGINT,
			media_fetched_us BIGINT,
			preflight_done_us BIGINT,
			plan_done_us BIGINT,
			attempt_start_us BIGINT,
			reserve_lock_acquired_us BIGINT,
			reserve_done_us BIGINT,
			queued_us BIGINT,
			dequeued_us BIGINT,
			topup_done_us BIGINT,
			encrypted_us BIGINT,
			write_submitted_us BIGINT,
			write_dequeued_us BIGINT,
			write_done_us BIGINT,
			accepted_us BIGINT,
			first_chunk_ingress_us BIGINT,
			first_chunk_dequeued_us BIGINT,
			first_content_ingress_us BIGINT,
			first_content_us BIGINT,
			headers_written_us BIGINT,
			first_flush_us BIGINT,
			last_flush_us BIGINT,
			client_gone_us BIGINT,
			cancel_sent_us BIGINT,
			complete_ingress_us BIGINT,
			done_flushed_us BIGINT,
			finalized_us BIGINT,
			settle_db_us BIGINT,
			db_us BIGINT,
			db_calls INT NOT NULL DEFAULT 0,

			body_bytes INT NOT NULL DEFAULT 0,
			sealed_body_bytes INT NOT NULL DEFAULT 0,
			auth_kind TEXT NOT NULL DEFAULT '',
			auth_db_read BOOL NOT NULL DEFAULT FALSE,
			reserve_mode TEXT NOT NULL DEFAULT '',
			media_items INT NOT NULL DEFAULT 0,
			media_bytes BIGINT NOT NULL DEFAULT 0,
			preflight_outcome TEXT NOT NULL DEFAULT '',
			plan_outcome TEXT NOT NULL DEFAULT '',
			chunks_in INT NOT NULL DEFAULT 0,
			chunks_out INT NOT NULL DEFAULT 0,
			bytes_out BIGINT NOT NULL DEFAULT 0,
			decrypt_us_total BIGINT NOT NULL DEFAULT 0,
			max_chunk_gap_us BIGINT NOT NULL DEFAULT 0,
			held_preamble_chunks INT NOT NULL DEFAULT 0,
			client_write_err BOOL NOT NULL DEFAULT FALSE,
			attempts_total INT NOT NULL DEFAULT 0,
			failed_attempts INT NOT NULL DEFAULT 0,
			failed_attempts_us BIGINT NOT NULL DEFAULT 0,
			backup_launched BOOL NOT NULL DEFAULT FALSE,
			backup_won BOOL NOT NULL DEFAULT FALSE,
			transport_est_us BIGINT,
			slept_us BIGINT,
			timing_anomaly BOOL NOT NULL DEFAULT FALSE,

			candidate_set_size INT NOT NULL DEFAULT 0,
			scanned INT NOT NULL DEFAULT 0,
			gate_rejections JSONB,
			runner_up_provider_id TEXT NOT NULL DEFAULT '',
			runner_up_cost_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
			near_tie_pool_size INT NOT NULL DEFAULT 0,
			selection_path TEXT NOT NULL DEFAULT '',
			best_idle_provider_id TEXT NOT NULL DEFAULT '',
			best_idle_ttft_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
			predicted_ttft_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
			raw_ttft_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
			predicted_decode_tps DOUBLE PRECISION NOT NULL DEFAULT 0,
			snapshot_age_ms INT NOT NULL DEFAULT 0,
			pending_for_model INT NOT NULL DEFAULT 0,
			total_pending INT NOT NULL DEFAULT 0,
			capacity_rate_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
			cache_discount_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
			shadow_would_shed BOOL,
			shadow_idle_alternative BOOL,
			lock_wait_us BIGINT NOT NULL DEFAULT 0,
			scan_us BIGINT NOT NULL DEFAULT 0,
			admit_us BIGINT NOT NULL DEFAULT 0,
			preflight_us BIGINT NOT NULL DEFAULT 0,
			ttft_calibration_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
			prefill_decode_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
			queue_position_at_enqueue INT NOT NULL DEFAULT 0,
			queue_depth_at_enqueue INT NOT NULL DEFAULT 0,
			drain_trigger TEXT NOT NULL DEFAULT '',
			candidates JSONB,

			prov_total_us BIGINT,
			prov_first_delta_us BIGINT,
			prov_engine_submit_us BIGINT,
			prov_engine_admitted_us BIGINT,
			prov_prompt_prep_us BIGINT,
			prov_load_wait_us BIGINT,
			prov_load_cold BOOL,
			prov_running_at_admit INT,
			prov_waiting_at_admit INT,
			prov_kv_bytes_in_use_at_admit BIGINT,
			prov_cancel_stage TEXT NOT NULL DEFAULT '',
			eng_queue_wait_ns BIGINT,
			eng_first_token_ns BIGINT,
			eng_prompt_computed_ns BIGINT,
			eng_prefill_chunks INT,
			eng_decode_steps INT,
			eng_mtp_accepted INT,
			eng_finish_reason TEXT NOT NULL DEFAULT '',
			provider_profile JSONB,
			provider_profile_valid BOOL NOT NULL DEFAULT FALSE,
			provider_profile_invalid_reason TEXT NOT NULL DEFAULT '',
			provider_profile_consistent BOOL,

			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			UNIQUE (request_id, attempt)
		) WITH (autovacuum_vacuum_scale_factor=0.02, autovacuum_analyze_scale_factor=0.01)`
	requestProfilesCreatedIndexDDL  = `CREATE INDEX IF NOT EXISTS idx_request_profiles_created ON request_profiles(created_at DESC)`
	requestProfilesCoordIndexDDL    = `CREATE INDEX IF NOT EXISTS idx_request_profiles_coord ON request_profiles(coord_request_id)`
	requestProfilesProviderIndexDDL = `CREATE INDEX IF NOT EXISTS idx_request_profiles_provider ON request_profiles(provider_id, created_at DESC)`

	fleetSnapshotsTableDDL = `CREATE TABLE IF NOT EXISTS fleet_snapshots (
			id BIGSERIAL PRIMARY KEY,
			sampled_at TIMESTAMPTZ NOT NULL,
			provider_id TEXT NOT NULL,
			model TEXT NOT NULL DEFAULT '',
			eligibility_reason TEXT NOT NULL DEFAULT '',
			slot_state TEXT NOT NULL DEFAULT '',
			num_running INT NOT NULL DEFAULT 0,
			num_waiting INT NOT NULL DEFAULT 0,
			queued_prefill_tokens INT NOT NULL DEFAULT 0,
			partial_prefill_rows INT NOT NULL DEFAULT 0,
			active_token_budget_used BIGINT NOT NULL DEFAULT 0,
			active_token_budget_max BIGINT NOT NULL DEFAULT 0,
			kv_bytes_in_use BIGINT NOT NULL DEFAULT 0,
			kv_bytes_capacity BIGINT NOT NULL DEFAULT 0,
			observed_decode_tps DOUBLE PRECISION NOT NULL DEFAULT 0,
			observed_prefill_tps DOUBLE PRECISION NOT NULL DEFAULT 0,
			isolated_prefill_tps DOUBLE PRECISION NOT NULL DEFAULT 0,
			ewma_initialized BOOL,
			max_concurrency INT NOT NULL DEFAULT 0,
			pending_count INT NOT NULL DEFAULT 0,
			effective_cap INT NOT NULL DEFAULT 0,
			cooldown_active BOOL NOT NULL DEFAULT FALSE,
			breaker_open BOOL NOT NULL DEFAULT FALSE,
			clamp_active BOOL NOT NULL DEFAULT FALSE,
			ejected BOOL NOT NULL DEFAULT FALSE,
			gpu_memory_active_gb DOUBLE PRECISION NOT NULL DEFAULT 0,
			gpu_memory_peak_gb DOUBLE PRECISION NOT NULL DEFAULT 0,
			free_for_load_gb DOUBLE PRECISION,
			memory_pressure DOUBLE PRECISION NOT NULL DEFAULT 0,
			cpu_usage DOUBLE PRECISION NOT NULL DEFAULT 0,
			thermal_state TEXT NOT NULL DEFAULT '',
			low_power_mode BOOL,
			memory_pressure_level TEXT NOT NULL DEFAULT '',
			steps_executed BIGINT NOT NULL DEFAULT 0,
			step_wall_ns_total BIGINT NOT NULL DEFAULT 0,
			decode_rows_total BIGINT NOT NULL DEFAULT 0,
			prefill_tokens_total BIGINT NOT NULL DEFAULT 0,
			mtp_rounds_total BIGINT NOT NULL DEFAULT 0,
			mtp_proposed_total BIGINT NOT NULL DEFAULT 0,
			mtp_accepted_total BIGINT NOT NULL DEFAULT 0,
			heartbeat_age_ms INT NOT NULL DEFAULT 0,
			wedge_suspected BOOL NOT NULL DEFAULT FALSE,
			eval_in_flight_ms BIGINT NOT NULL DEFAULT 0,
			requests_served BIGINT NOT NULL DEFAULT 0,
			tokens_generated BIGINT NOT NULL DEFAULT 0,
			cancellations_received BIGINT NOT NULL DEFAULT 0,
			cancellations_before_output BIGINT NOT NULL DEFAULT 0,
			cancellations_partial_complete BIGINT NOT NULL DEFAULT 0,
			generation_errors_after_output BIGINT NOT NULL DEFAULT 0,
			chunk_encryption_errors BIGINT NOT NULL DEFAULT 0,
			stream_closed_without_terminal BIGINT NOT NULL DEFAULT 0,
			cancel_during_model_load BIGINT NOT NULL DEFAULT 0,
			usage_gaps BIGINT NOT NULL DEFAULT 0,
			cancel_stage_pre_accept_total BIGINT NOT NULL DEFAULT 0,
			cancel_stage_pre_engine_total BIGINT NOT NULL DEFAULT 0,
			cancel_stage_prefill_total BIGINT NOT NULL DEFAULT 0,
			cancel_stage_decode_total BIGINT NOT NULL DEFAULT 0,
			cancel_stage_post_terminal_total BIGINT NOT NULL DEFAULT 0,
			tokens_after_cancel_total BIGINT NOT NULL DEFAULT 0,
			cancel_abort_ns_sum BIGINT NOT NULL DEFAULT 0,
			queue_depth_total INT NOT NULL DEFAULT 0,
			queue_depth_by_model JSONB,
			inflight_requests INT NOT NULL DEFAULT 0,
			reserve_lock_wait_p95_us BIGINT NOT NULL DEFAULT 0,
			profile_sink_depth INT NOT NULL DEFAULT 0,
			profile_sink_dropped_total BIGINT NOT NULL DEFAULT 0,
			route_sink_dropped_total BIGINT NOT NULL DEFAULT 0,
			unknown_request_frames_total BIGINT NOT NULL DEFAULT 0,
			goroutines INT NOT NULL DEFAULT 0,
			provider_version TEXT NOT NULL DEFAULT '',
			model_vision BOOL NOT NULL DEFAULT FALSE,
			template_render_ok BOOL
		) WITH (autovacuum_vacuum_scale_factor=0.02, autovacuum_analyze_scale_factor=0.01)`
	fleetSnapshotsSampledIndexDDL  = `CREATE INDEX IF NOT EXISTS idx_fleet_snapshots_sampled ON fleet_snapshots(sampled_at DESC)`
	fleetSnapshotsProviderIndexDDL = `CREATE INDEX IF NOT EXISTS idx_fleet_snapshots_provider ON fleet_snapshots(provider_id, sampled_at DESC)`
)
