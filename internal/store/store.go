// Package store owns all database reads and writes. It talks to Postgres/
// TimescaleDB via a pgx connection pool.
package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/saimuu1/uptime-monitor/internal/check"
)

// Store wraps the connection pool. Construct one with New and Close it on exit.
type Store struct {
	pool *pgxpool.Pool
}

// --- users & sessions (auth) ---

// ErrEmailTaken is returned by CreateUser when the email already exists.
var ErrEmailTaken = fmt.Errorf("email already registered")

// CreateUser inserts a user and returns its id. Duplicate email => ErrEmailTaken.
func (s *Store) CreateUser(ctx context.Context, email, passwordHash string) (int64, error) {
	const q = `INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id`
	var id int64
	err := s.pool.QueryRow(ctx, q, email, passwordHash).Scan(&id)
	if err != nil {
		if strings.Contains(err.Error(), "users_email_key") {
			return 0, ErrEmailTaken
		}
		return 0, fmt.Errorf("create user: %w", err)
	}
	return id, nil
}

// UserByEmail returns a user's id and password hash for login. Missing user
// returns ok=false (no error) so callers can't distinguish it by error type.
func (s *Store) UserByEmail(ctx context.Context, email string) (id int64, hash string, ok bool, err error) {
	const q = `SELECT id, password_hash FROM users WHERE email = $1`
	err = s.pool.QueryRow(ctx, q, email).Scan(&id, &hash)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, "", false, nil
		}
		return 0, "", false, fmt.Errorf("user by email: %w", err)
	}
	return id, hash, true, nil
}

// CreateSession stores a session token for a user until expiry.
func (s *Store) CreateSession(ctx context.Context, token string, userID int64, expires time.Time) error {
	const q = `INSERT INTO sessions (token, user_id, expires_at) VALUES ($1, $2, $3)`
	_, err := s.pool.Exec(ctx, q, token, userID, expires)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	return nil
}

// UserBySession resolves a non-expired session token to a user id.
func (s *Store) UserBySession(ctx context.Context, token string) (userID int64, ok bool, err error) {
	const q = `SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()`
	err = s.pool.QueryRow(ctx, q, token).Scan(&userID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("user by session: %w", err)
	}
	return userID, true, nil
}

// DeleteSession removes a session (logout).
func (s *Store) DeleteSession(ctx context.Context, token string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM sessions WHERE token = $1`, token)
	return err
}

// Monitor is the full monitor record as persisted.
type Monitor struct {
	ID              int64
	Name            string
	URL             string
	Method          string
	IntervalSeconds int
	TimeoutMs       int
	ExpectedStatus  int
	Enabled         bool
	NotifyEmails    []string // addresses to alert on this monitor's outages
	ExpectedKeyword string   // if set, page body must contain this to be "up"
	SlowThresholdMs int      // if > 0, alert when response time exceeds this
}

// New opens a connection pool against the given postgres URL and verifies it
// with a ping.
func New(ctx context.Context, dbURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

// UpsertMonitor seeds a *system* monitor from config (user_id NULL), inserting
// or updating the row with the same name. Uniqueness is per the partial index
// on system monitors, so it doesn't collide with users' monitors.
func (s *Store) UpsertMonitor(ctx context.Context, m Monitor) (Monitor, error) {
	// notify_emails is NOT NULL; a nil slice would encode as NULL, so normalize.
	if m.NotifyEmails == nil {
		m.NotifyEmails = []string{}
	}
	const q = `
		INSERT INTO monitors (name, url, method, interval_seconds, timeout_ms, expected_status, enabled, notify_emails, expected_keyword, slow_threshold_ms, user_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL)
		ON CONFLICT (name) WHERE user_id IS NULL DO UPDATE SET
			url = EXCLUDED.url,
			method = EXCLUDED.method,
			interval_seconds = EXCLUDED.interval_seconds,
			timeout_ms = EXCLUDED.timeout_ms,
			expected_status = EXCLUDED.expected_status,
			enabled = EXCLUDED.enabled,
			notify_emails = EXCLUDED.notify_emails,
			expected_keyword = EXCLUDED.expected_keyword,
			slow_threshold_ms = EXCLUDED.slow_threshold_ms
		RETURNING id`
	err := s.pool.QueryRow(ctx, q,
		m.Name, m.URL, m.Method, m.IntervalSeconds, m.TimeoutMs, m.ExpectedStatus, m.Enabled, m.NotifyEmails, m.ExpectedKeyword, m.SlowThresholdMs,
	).Scan(&m.ID)
	if err != nil {
		return Monitor{}, fmt.Errorf("upsert monitor %q: %w", m.Name, err)
	}
	return m, nil
}

// CreateMonitor adds a monitor owned by a user (from the web form).
func (s *Store) CreateMonitor(ctx context.Context, m Monitor, userID int64) (int64, error) {
	if m.NotifyEmails == nil {
		m.NotifyEmails = []string{}
	}
	const q = `
		INSERT INTO monitors (name, url, method, interval_seconds, timeout_ms, expected_status, enabled, notify_emails, expected_keyword, slow_threshold_ms, user_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id`
	var id int64
	err := s.pool.QueryRow(ctx, q,
		m.Name, m.URL, m.Method, m.IntervalSeconds, m.TimeoutMs, m.ExpectedStatus, m.Enabled, m.NotifyEmails, m.ExpectedKeyword, m.SlowThresholdMs, userID,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("create monitor %q: %w", m.Name, err)
	}
	return id, nil
}

// UserEmail returns a user's email (for the page header).
func (s *Store) UserEmail(ctx context.Context, userID int64) (string, error) {
	var email string
	err := s.pool.QueryRow(ctx, `SELECT email FROM users WHERE id = $1`, userID).Scan(&email)
	return email, err
}

// EnabledMonitors returns every enabled monitor.
func (s *Store) EnabledMonitors(ctx context.Context) ([]Monitor, error) {
	const q = `
		SELECT id, name, url, method, interval_seconds, timeout_ms, expected_status, enabled, notify_emails, expected_keyword, slow_threshold_ms
		FROM monitors WHERE enabled = TRUE ORDER BY id`
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query monitors: %w", err)
	}
	defer rows.Close()

	var out []Monitor
	for rows.Next() {
		var m Monitor
		if err := rows.Scan(&m.ID, &m.Name, &m.URL, &m.Method,
			&m.IntervalSeconds, &m.TimeoutMs, &m.ExpectedStatus, &m.Enabled, &m.NotifyEmails, &m.ExpectedKeyword, &m.SlowThresholdMs); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteMonitor removes a monitor and its history, but only if it belongs to
// userID. checks has no FK, incidents does, so clear children first.
func (s *Store) DeleteMonitor(ctx context.Context, id, userID int64) error {
	// Ownership check: refuse to touch a monitor the user doesn't own.
	var owner *int64
	err := s.pool.QueryRow(ctx, `SELECT user_id FROM monitors WHERE id = $1`, id).Scan(&owner)
	if err == pgx.ErrNoRows {
		return nil // already gone
	}
	if err != nil {
		return fmt.Errorf("delete monitor: %w", err)
	}
	if owner == nil || *owner != userID {
		return fmt.Errorf("not your monitor")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)
	for _, q := range []string{
		`DELETE FROM checks WHERE monitor_id = $1`,
		`DELETE FROM incidents WHERE monitor_id = $1`,
		`DELETE FROM monitors WHERE id = $1`,
	} {
		if _, err := tx.Exec(ctx, q, id); err != nil {
			return fmt.Errorf("delete monitor: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// AlertRecipients returns who to email for a monitor's alerts, looked up fresh
// at alert time: the monitor's explicit notify_emails, or — if it has none — the
// owner's account email. Empty means "fall back to the global default".
func (s *Store) AlertRecipients(ctx context.Context, monitorID int64) ([]string, error) {
	const q = `
		SELECT m.notify_emails, u.email
		FROM monitors m LEFT JOIN users u ON u.id = m.user_id
		WHERE m.id = $1`
	var emails []string
	var ownerEmail *string
	if err := s.pool.QueryRow(ctx, q, monitorID).Scan(&emails, &ownerEmail); err != nil {
		return nil, fmt.Errorf("alert recipients: %w", err)
	}
	if len(emails) > 0 {
		return emails, nil
	}
	if ownerEmail != nil && *ownerEmail != "" {
		return []string{*ownerEmail}, nil
	}
	return nil, nil
}

// InsertCheck records one check result in the time-series hypertable at the
// time the check actually ran.
func (s *Store) InsertCheck(ctx context.Context, monitorID int64, region string, at time.Time, r check.Result) error {
	const q = `
		INSERT INTO checks (time, monitor_id, region, up, status_code, latency_ms, error)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`
	// Pass NULL for status/error when they carry no signal.
	var status *int
	if r.StatusCode != 0 {
		status = &r.StatusCode
	}
	var errStr *string
	if r.Err != "" {
		errStr = &r.Err
	}
	_, err := s.pool.Exec(ctx, q, at, monitorID, region, r.Up, status, r.LatencyMs, errStr)
	if err != nil {
		return fmt.Errorf("insert check: %w", err)
	}
	return nil
}

// HasOpenIncident reports whether the monitor currently has an unresolved
// incident. Used at startup to seed in-memory state so a restart mid-outage
// doesn't open a duplicate incident.
func (s *Store) HasOpenIncident(ctx context.Context, monitorID int64) (bool, error) {
	const q = `SELECT EXISTS (
		SELECT 1 FROM incidents WHERE monitor_id = $1 AND resolved_at IS NULL)`
	var exists bool
	err := s.pool.QueryRow(ctx, q, monitorID).Scan(&exists)
	return exists, err
}

// OpenIncident records the start of an outage.
func (s *Store) OpenIncident(ctx context.Context, monitorID int64, cause string) error {
	const q = `INSERT INTO incidents (monitor_id, started_at, cause) VALUES ($1, $2, $3)`
	_, err := s.pool.Exec(ctx, q, monitorID, time.Now(), cause)
	if err != nil {
		return fmt.Errorf("open incident: %w", err)
	}
	return nil
}

// Status is a monitor's current state for the status page.
type Status struct {
	ID         int64
	Name       string
	URL        string
	Down       bool       // has an open (unresolved) incident
	Uptime24h  float64    // fraction 0..1 of checks up in the last 24h
	Checks24h  int        // number of checks in the last 24h (0 => "no data")
	LastCheck  *time.Time // most recent check time, nil if none in 24h
	CertExpiry *time.Time // TLS cert expiry (HTTPS monitors), nil if unknown
	P50ms      *float64   // median response time (24h, up checks), nil if none
	P95ms      *float64   // 95th-percentile response time (24h, up checks)
}

// SetCertExpiry records a monitor's TLS certificate expiry (from a check).
func (s *Store) SetCertExpiry(ctx context.Context, monitorID int64, expiry time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE monitors SET cert_expiry = $2 WHERE id = $1`, monitorID, expiry)
	if err != nil {
		return fmt.Errorf("set cert expiry: %w", err)
	}
	return nil
}

// DayUptime is one day's up-ratio for a monitor.
type DayUptime struct {
	MonitorID int64
	Day       time.Time
	Ratio     float64 // 0..1 fraction of checks that were up that day
	Count     int
}

// UptimeHistory returns per-day up-ratios for every monitor over the last `days`
// days. Only days that actually had checks appear; the caller fills the gaps as
// "no data" (that's the grey at the start of a status-page history strip).
func (s *Store) UptimeHistory(ctx context.Context, days int) ([]DayUptime, error) {
	const q = `
		SELECT monitor_id,
		       date_trunc('day', time) AS day,
		       avg(CASE WHEN up THEN 1.0 ELSE 0.0 END) AS ratio,
		       count(*) AS n
		FROM checks
		WHERE time > now() - make_interval(days => $1)
		GROUP BY monitor_id, day
		ORDER BY day`
	rows, err := s.pool.Query(ctx, q, days)
	if err != nil {
		return nil, fmt.Errorf("uptime history: %w", err)
	}
	defer rows.Close()

	var out []DayUptime
	for rows.Next() {
		var d DayUptime
		if err := rows.Scan(&d.MonitorID, &d.Day, &d.Ratio, &d.Count); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MonitorStatusesForUser returns the current state of the given user's enabled
// monitors: whether each has an open incident, its 24h uptime, and last check.
func (s *Store) MonitorStatusesForUser(ctx context.Context, userID int64) ([]Status, error) {
	const q = `
		SELECT m.id, m.name, m.url,
			EXISTS (SELECT 1 FROM incidents i
				WHERE i.monitor_id = m.id AND i.resolved_at IS NULL) AS down,
			COALESCE(AVG(CASE WHEN c.up THEN 1.0 ELSE 0.0 END), 1.0) AS uptime,
			COUNT(c.*) AS checks,
			MAX(c.time) AS last_check,
			m.cert_expiry,
			percentile_cont(0.5)  WITHIN GROUP (ORDER BY c.latency_ms) FILTER (WHERE c.up) AS p50,
			percentile_cont(0.95) WITHIN GROUP (ORDER BY c.latency_ms) FILTER (WHERE c.up) AS p95
		FROM monitors m
		LEFT JOIN checks c
			ON c.monitor_id = m.id AND c.time > now() - interval '24 hours'
		WHERE m.enabled AND m.user_id = $1
		GROUP BY m.id, m.name, m.url, m.cert_expiry
		ORDER BY m.id`
	rows, err := s.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, fmt.Errorf("query statuses: %w", err)
	}
	defer rows.Close()

	var out []Status
	for rows.Next() {
		var st Status
		if err := rows.Scan(&st.ID, &st.Name, &st.URL, &st.Down,
			&st.Uptime24h, &st.Checks24h, &st.LastCheck, &st.CertExpiry, &st.P50ms, &st.P95ms); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// Incident is a past or ongoing outage, with the monitor's name joined in.
type Incident struct {
	MonitorName string
	StartedAt   time.Time
	ResolvedAt  *time.Time // nil = still ongoing
	Cause       string
}

// RecentIncidentsForUser returns a user's most recent incidents (newest first).
func (s *Store) RecentIncidentsForUser(ctx context.Context, userID int64, limit int) ([]Incident, error) {
	const q = `
		SELECT m.name, i.started_at, i.resolved_at, COALESCE(i.cause, '')
		FROM incidents i JOIN monitors m ON m.id = i.monitor_id
		WHERE m.user_id = $1
		ORDER BY i.started_at DESC
		LIMIT $2`
	rows, err := s.pool.Query(ctx, q, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("recent incidents: %w", err)
	}
	defer rows.Close()

	var out []Incident
	for rows.Next() {
		var in Incident
		if err := rows.Scan(&in.MonitorName, &in.StartedAt, &in.ResolvedAt, &in.Cause); err != nil {
			return nil, err
		}
		out = append(out, in)
	}
	return out, rows.Err()
}

// ResolveIncident closes the currently-open incident for a monitor.
func (s *Store) ResolveIncident(ctx context.Context, monitorID int64) error {
	const q = `
		UPDATE incidents SET resolved_at = now()
		WHERE monitor_id = $1 AND resolved_at IS NULL`
	_, err := s.pool.Exec(ctx, q, monitorID)
	if err != nil {
		return fmt.Errorf("resolve incident: %w", err)
	}
	return nil
}
