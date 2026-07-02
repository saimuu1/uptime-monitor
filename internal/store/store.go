// Package store owns all database reads and writes. It talks to Postgres/
// TimescaleDB via a pgx connection pool.
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/saimuu1/uptime-monitor/internal/check"
)

// Store wraps the connection pool. Construct one with New and Close it on exit.
type Store struct {
	pool *pgxpool.Pool
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

// UpsertMonitor inserts a monitor from config, or updates the existing row with
// the same name. Config is the source of truth on (re)start; the returned row
// carries the DB-assigned id.
func (s *Store) UpsertMonitor(ctx context.Context, m Monitor) (Monitor, error) {
	const q = `
		INSERT INTO monitors (name, url, method, interval_seconds, timeout_ms, expected_status, enabled)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (name) DO UPDATE SET
			url = EXCLUDED.url,
			method = EXCLUDED.method,
			interval_seconds = EXCLUDED.interval_seconds,
			timeout_ms = EXCLUDED.timeout_ms,
			expected_status = EXCLUDED.expected_status,
			enabled = EXCLUDED.enabled
		RETURNING id`
	err := s.pool.QueryRow(ctx, q,
		m.Name, m.URL, m.Method, m.IntervalSeconds, m.TimeoutMs, m.ExpectedStatus, m.Enabled,
	).Scan(&m.ID)
	if err != nil {
		return Monitor{}, fmt.Errorf("upsert monitor %q: %w", m.Name, err)
	}
	return m, nil
}

// EnabledMonitors returns every enabled monitor.
func (s *Store) EnabledMonitors(ctx context.Context) ([]Monitor, error) {
	const q = `
		SELECT id, name, url, method, interval_seconds, timeout_ms, expected_status, enabled
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
			&m.IntervalSeconds, &m.TimeoutMs, &m.ExpectedStatus, &m.Enabled); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// InsertCheck records one check result in the time-series hypertable.
func (s *Store) InsertCheck(ctx context.Context, monitorID int64, region string, r check.Result) error {
	const q = `
		INSERT INTO checks (time, monitor_id, region, up, status_code, latency_ms, error)
		VALUES (now(), $1, $2, $3, $4, $5, $6)`
	// Pass NULL for status/error when they carry no signal.
	var status *int
	if r.StatusCode != 0 {
		status = &r.StatusCode
	}
	var errStr *string
	if r.Err != "" {
		errStr = &r.Err
	}
	_, err := s.pool.Exec(ctx, q, monitorID, region, r.Up, status, r.LatencyMs, errStr)
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
