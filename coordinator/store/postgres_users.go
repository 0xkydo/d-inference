package store

// Users, roles, device codes, tokens, and invite codes.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// CreateUser creates a new user record linked to a Privy identity.
func (s *PostgresStore) CreateUser(user *User) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO users (account_id, privy_user_id, email, role, platform_fee_percent)
		 VALUES ($1, $2, $3, $4, $5)`,
		user.AccountID, user.PrivyUserID, user.Email, user.Role, user.PlatformFeePercent,
	)
	if err != nil {
		return fmt.Errorf("store: create user: %w", err)
	}
	return nil
}

const userSelectColumns = `account_id, privy_user_id, email, role, platform_fee_percent,
	stripe_account_id, stripe_account_status, stripe_account_country,
	stripe_destination_type, stripe_destination_last4, stripe_instant_eligible, created_at`

func scanUser(row interface {
	Scan(...any) error
}) (*User, error) {
	var u User
	if err := row.Scan(&u.AccountID, &u.PrivyUserID, &u.Email, &u.Role, &u.PlatformFeePercent,
		&u.StripeAccountID, &u.StripeAccountStatus, &u.StripeAccountCountry,
		&u.StripeDestinationType, &u.StripeDestinationLast4, &u.StripeInstantEligible, &u.CreatedAt); err != nil {
		return nil, err
	}
	return &u, nil
}

// wrapUserScanError preserves the historical "store: user not found: ..."
// message for every scan failure, and additionally tags a true miss
// (pgx.ErrNoRows) with ErrNotFound so callers -- including the read-through
// cache -- can distinguish "no such user" from a transient DB error with
// errors.Is. ErrNotFound.Error() is exactly "not found", so the rendered
// string is byte-for-byte unchanged.
func wrapUserScanError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("store: user %w: %w", ErrNotFound, err)
	}
	return fmt.Errorf("store: user not found: %w", err)
}

// GetUserByPrivyID returns the user for a Privy DID.
func (s *PostgresStore) GetUserByPrivyID(privyUserID string) (*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	row := s.pool.QueryRow(ctx,
		`SELECT `+userSelectColumns+` FROM users WHERE privy_user_id = $1`, privyUserID,
	)
	u, err := scanUser(row)
	if err != nil {
		return nil, wrapUserScanError(err)
	}
	return u, nil
}

// GetUserByAccountID returns the user for an internal account ID.
func (s *PostgresStore) GetUserByAccountID(accountID string) (*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	row := s.pool.QueryRow(ctx,
		`SELECT `+userSelectColumns+` FROM users WHERE account_id = $1`, accountID,
	)
	u, err := scanUser(row)
	if err != nil {
		return nil, wrapUserScanError(err)
	}
	return u, nil
}

// SetUserStripeAccount upserts the Stripe Connect fields on a user record.
// stripeAccountCountry is the ISO country the Express account is locked to.
// Pass an empty string to leave the existing country value unchanged.
func (s *PostgresStore) SetUserStripeAccount(accountID, stripeAccountID, status, stripeAccountCountry, destinationType, destinationLast4 string, instantEligible bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	countryClause := ""
	args := []any{accountID, stripeAccountID, status, destinationType, destinationLast4, instantEligible}
	switch {
	case stripeAccountCountry != "":
		countryClause = ", stripe_account_country = $7"
		args = append(args, stripeAccountCountry)
	case stripeAccountID == "":
		// Unlinking: empty country normally means "keep existing", but with
		// no account there is no country — a stale value would leak into the
		// next onboarding attempt.
		countryClause = ", stripe_account_country = ''"
	}

	tag, err := s.pool.Exec(ctx,
		fmt.Sprintf(`UPDATE users SET
			stripe_account_id = $2,
			stripe_account_status = $3,
			stripe_destination_type = $4,
			stripe_destination_last4 = $5,
			stripe_instant_eligible = $6%s
		 WHERE account_id = $1`, countryClause),
		args...,
	)
	if err != nil {
		return fmt.Errorf("store: set stripe account: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("user with account ID %q not found", accountID)
	}
	return nil
}

// GetUserByStripeAccount finds a user by their Stripe connected account ID.
func (s *PostgresStore) GetUserByStripeAccount(stripeAccountID string) (*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	row := s.pool.QueryRow(ctx,
		`SELECT `+userSelectColumns+` FROM users WHERE stripe_account_id = $1`, stripeAccountID,
	)
	u, err := scanUser(row)
	if err != nil {
		return nil, fmt.Errorf("store: user with Stripe account %q not found: %w", stripeAccountID, err)
	}
	return u, nil
}

// SetUserRole sets the account role on a user record.
func (s *PostgresStore) SetUserRole(accountID, role string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tag, err := s.pool.Exec(ctx,
		`UPDATE users SET role = $2 WHERE account_id = $1`,
		accountID, role,
	)
	if err != nil {
		return fmt.Errorf("store: set user role: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("user with account ID %q not found", accountID)
	}
	return nil
}

// SetUserPlatformFeePercent sets (or clears, when nil) the per-account platform
// fee override.
func (s *PostgresStore) SetUserPlatformFeePercent(accountID string, feePercent *int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tag, err := s.pool.Exec(ctx,
		`UPDATE users SET platform_fee_percent = $2 WHERE account_id = $1`,
		accountID, feePercent,
	)
	if err != nil {
		return fmt.Errorf("store: set user platform fee: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("user with account ID %q not found", accountID)
	}
	return nil
}

// GetUserByEmail returns the user for an email address.
func (s *PostgresStore) GetUserByEmail(email string) (*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	row := s.pool.QueryRow(ctx,
		`SELECT `+userSelectColumns+` FROM users WHERE LOWER(email) = LOWER($1)`, email,
	)
	u, err := scanUser(row)
	if err != nil {
		return nil, fmt.Errorf("user with email %q not found", email)
	}
	return u, nil
}

func (s *PostgresStore) CreateDeviceCode(dc *DeviceCode) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO device_codes (device_code, user_code, account_id, status, expires_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		dc.DeviceCode, dc.UserCode, dc.AccountID, dc.Status, dc.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("store: create device code: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetDeviceCode(deviceCode string) (*DeviceCode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var dc DeviceCode
	err := s.pool.QueryRow(ctx,
		`SELECT device_code, user_code, account_id, status, expires_at, created_at
		 FROM device_codes WHERE device_code = $1`, deviceCode,
	).Scan(&dc.DeviceCode, &dc.UserCode, &dc.AccountID, &dc.Status, &dc.ExpiresAt, &dc.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("store: device code not found: %w", err)
	}
	return &dc, nil
}

func (s *PostgresStore) GetDeviceCodeByUserCode(userCode string) (*DeviceCode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var dc DeviceCode
	err := s.pool.QueryRow(ctx,
		`SELECT device_code, user_code, account_id, status, expires_at, created_at
		 FROM device_codes WHERE user_code = $1`, userCode,
	).Scan(&dc.DeviceCode, &dc.UserCode, &dc.AccountID, &dc.Status, &dc.ExpiresAt, &dc.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("store: user code not found: %w", err)
	}
	return &dc, nil
}

func (s *PostgresStore) ApproveDeviceCode(deviceCode, accountID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tag, err := s.pool.Exec(ctx,
		`UPDATE device_codes SET status = 'approved', account_id = $2
		 WHERE device_code = $1 AND status = 'pending' AND expires_at > NOW()`,
		deviceCode, accountID,
	)
	if err != nil {
		return fmt.Errorf("store: approve device code: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("device code not found, not pending, or expired")
	}
	return nil
}

func (s *PostgresStore) DeleteExpiredDeviceCodes() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx, `DELETE FROM device_codes WHERE expires_at < NOW()`)
	if err != nil {
		return fmt.Errorf("store: delete expired device codes: %w", err)
	}
	return nil
}

func (s *PostgresStore) CreateProviderToken(pt *ProviderToken) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO provider_tokens (token_hash, account_id, label, active)
		 VALUES ($1, $2, $3, $4)`,
		pt.TokenHash, pt.AccountID, pt.Label, pt.Active,
	)
	if err != nil {
		return fmt.Errorf("store: create provider token: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetProviderToken(token string) (*ProviderToken, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	h := hashKey(token)
	var pt ProviderToken
	err := s.pool.QueryRow(ctx,
		`SELECT token_hash, account_id, label, active, created_at
		 FROM provider_tokens WHERE token_hash = $1 AND active = TRUE`, h,
	).Scan(&pt.TokenHash, &pt.AccountID, &pt.Label, &pt.Active, &pt.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("store: provider token not found: %w", err)
	}
	return &pt, nil
}

func (s *PostgresStore) RevokeProviderToken(token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	h := hashKey(token)
	tag, err := s.pool.Exec(ctx,
		`UPDATE provider_tokens SET active = FALSE WHERE token_hash = $1`, h,
	)
	if err != nil {
		return fmt.Errorf("store: revoke provider token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("provider token not found")
	}
	return nil
}

func (s *PostgresStore) CreateInviteCode(code *InviteCode) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.pool.Exec(ctx,
		`INSERT INTO invite_codes (code, amount_micro_usd, max_uses, used_count, active, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		code.Code, code.AmountMicroUSD, code.MaxUses, code.UsedCount, code.Active, code.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("store: create invite code: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetInviteCode(code string) (*InviteCode, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var ic InviteCode
	err := s.pool.QueryRow(ctx,
		`SELECT code, amount_micro_usd, max_uses, used_count, active, expires_at, created_at
		 FROM invite_codes WHERE code = $1`, code,
	).Scan(&ic.Code, &ic.AmountMicroUSD, &ic.MaxUses, &ic.UsedCount, &ic.Active, &ic.ExpiresAt, &ic.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("store: invite code not found: %w", err)
	}
	return &ic, nil
}

func (s *PostgresStore) ListInviteCodes() []InviteCode {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := s.pool.Query(ctx,
		`SELECT code, amount_micro_usd, max_uses, used_count, active, expires_at, created_at
		 FROM invite_codes ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var codes []InviteCode
	for rows.Next() {
		var ic InviteCode
		if err := rows.Scan(&ic.Code, &ic.AmountMicroUSD, &ic.MaxUses, &ic.UsedCount, &ic.Active, &ic.ExpiresAt, &ic.CreatedAt); err != nil {
			continue
		}
		codes = append(codes, ic)
	}
	return codes
}

func (s *PostgresStore) DeactivateInviteCode(code string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tag, err := s.pool.Exec(ctx,
		`UPDATE invite_codes SET active = FALSE WHERE code = $1`, code,
	)
	if err != nil {
		return fmt.Errorf("store: deactivate invite code: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("invite code %q not found", code)
	}
	return nil
}

func (s *PostgresStore) RedeemInviteCode(code string, accountID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("store: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// Lock the invite code row
	var ic InviteCode
	err = tx.QueryRow(ctx,
		`SELECT code, amount_micro_usd, max_uses, used_count, active, expires_at
		 FROM invite_codes WHERE code = $1 FOR UPDATE`, code,
	).Scan(&ic.Code, &ic.AmountMicroUSD, &ic.MaxUses, &ic.UsedCount, &ic.Active, &ic.ExpiresAt)
	if err != nil {
		return fmt.Errorf("invite code %q not found", code)
	}
	if !ic.Active {
		return fmt.Errorf("invite code %q is inactive", code)
	}
	if ic.ExpiresAt != nil && time.Now().After(*ic.ExpiresAt) {
		return fmt.Errorf("invite code %q has expired", code)
	}
	if ic.MaxUses > 0 && ic.UsedCount >= ic.MaxUses {
		return fmt.Errorf("invite code %q has reached max uses", code)
	}

	// Insert redemption (PK constraint prevents double-redemption)
	_, err = tx.Exec(ctx,
		`INSERT INTO invite_redemptions (code, account_id) VALUES ($1, $2)`,
		code, accountID,
	)
	if err != nil {
		return fmt.Errorf("account has already redeemed code %q", code)
	}

	// Increment used_count
	_, err = tx.Exec(ctx,
		`UPDATE invite_codes SET used_count = used_count + 1 WHERE code = $1`, code,
	)
	if err != nil {
		return fmt.Errorf("store: update invite code: %w", err)
	}

	return tx.Commit(ctx)
}

func (s *PostgresStore) HasRedeemedInviteCode(code, accountID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	_ = s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM invite_redemptions WHERE code = $1 AND account_id = $2`,
		code, accountID,
	).Scan(&count)
	return count > 0
}
