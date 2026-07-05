// Package check performs a single HTTP health check against a monitor and
// reports whether it was up, how long it took, and any error.
package check

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// maxBodyScan caps how much of a response body we read for keyword matching.
const maxBodyScan = 1 << 20 // 1 MiB

// Monitor is the subset of a monitor's config that a check needs. The store
// package defines the full record; we keep this narrow so check has no DB deps.
type Monitor struct {
	ID              int64
	Name            string
	URL             string
	Method          string
	Timeout         time.Duration
	ExpectedStatus  int
	ExpectedKeyword string // if set, body must contain this text to count as up
}

// Result is the outcome of one check.
type Result struct {
	Up         bool
	StatusCode int       // 0 if the request never completed (e.g. timeout).
	LatencyMs  int       // wall-clock time for the request.
	Err        string    // human-readable error, empty on success.
	CertExpiry time.Time // TLS cert NotAfter for HTTPS; zero if not applicable.
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

	res := Result{
		Up:         resp.StatusCode == m.ExpectedStatus,
		StatusCode: resp.StatusCode,
		LatencyMs:  latency,
	}
	// Keyword check: if the status is right and a keyword is configured, the body
	// must contain it. Otherwise just drain the body so the connection is reused.
	if res.Up && m.ExpectedKeyword != "" {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyScan))
		if !strings.Contains(string(body), m.ExpectedKeyword) {
			res.Up = false
			res.Err = fmt.Sprintf("keyword %q not found in page", m.ExpectedKeyword)
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	// For HTTPS, note when the server's certificate expires.
	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		res.CertExpiry = resp.TLS.PeerCertificates[0].NotAfter
	}
	return res
}
