// Package check performs a single HTTP health check against a monitor and
// reports whether it was up, how long it took, and any error.
package check

import (
	"context"
	"io"
	"net/http"
	"time"
)

// Monitor is the subset of a monitor's config that a check needs. The store
// package defines the full record; we keep this narrow so check has no DB deps.
type Monitor struct {
	ID             int64
	Name           string
	URL            string
	Method         string
	Timeout        time.Duration
	ExpectedStatus int
}

// Result is the outcome of one check.
type Result struct {
	Up         bool
	StatusCode int    // 0 if the request never completed (e.g. timeout).
	LatencyMs  int    // wall-clock time for the request.
	Err        string // human-readable error, empty on success.
}

// Do performs one HTTP request against m, bounded by m.Timeout via a derived
// context. "Up" means: the request completed AND the status matched
// ExpectedStatus. A timeout or a connection refused yields Up=false.
//
// The parent ctx lets a caller cancel in-flight checks on shutdown; the
// per-check timeout is layered on top with context.WithTimeout.
func Do(ctx context.Context, m Monitor) Result {
	method := m.Method
	if method == "" {
		method = http.MethodGet
	}

	ctx, cancel := context.WithTimeout(ctx, m.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, m.URL, nil)
	if err != nil {
		return Result{Up: false, Err: err.Error()}
	}

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	latency := int(time.Since(start).Milliseconds())
	if err != nil {
		return Result{Up: false, LatencyMs: latency, Err: err.Error()}
	}
	defer resp.Body.Close()
	// Drain the body so the connection can be reused by the transport.
	_, _ = io.Copy(io.Discard, resp.Body)

	return Result{
		Up:         resp.StatusCode == m.ExpectedStatus,
		StatusCode: resp.StatusCode,
		LatencyMs:  latency,
	}
}
