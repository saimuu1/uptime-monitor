// Package metrics defines the Prometheus counters/gauges the services expose and
// a small HTTP server to publish them at /metrics. This is how we "monitor the
// monitor": check volume, alert counts, and each monitor's own health.
package metrics

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// ChecksTotal — checks performed, split by region and up/down (checker).
	ChecksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "uptime_checks_total",
		Help: "Checks performed, by region and result.",
	}, []string{"region", "result"})

	// CheckLatency — how long checks took, by region (checker).
	CheckLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "uptime_check_latency_ms",
		Help:    "Check latency in milliseconds, by region.",
		Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
	}, []string{"region"})

	// JobsPublished — check jobs the scheduler put on the queue.
	JobsPublished = promauto.NewCounter(prometheus.CounterOpts{
		Name: "uptime_jobs_published_total",
		Help: "Check jobs published by the scheduler.",
	})

	// ResultsProcessed — results the evaluator consumed.
	ResultsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "uptime_results_processed_total",
		Help: "Check results processed by the evaluator.",
	})

	// IncidentsOpened — incidents the evaluator opened.
	IncidentsOpened = promauto.NewCounter(prometheus.CounterOpts{
		Name: "uptime_incidents_opened_total",
		Help: "Incidents opened by the evaluator.",
	})

	// AlertsSent — alerts fired, by kind (down/recovered).
	AlertsSent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "uptime_alerts_sent_total",
		Help: "Alerts sent, by kind.",
	}, []string{"kind"})

	// MonitorUp — committed state per monitor (1 = up, 0 = down).
	MonitorUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "uptime_monitor_up",
		Help: "Committed up/down state per monitor (1=up, 0=down).",
	}, []string{"monitor"})
)

// Serve runs an HTTP server exposing /metrics (and /healthz) until ctx is done.
// Call it in a goroutine from a service's main.
func Serve(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("metrics server: %v", err)
	}
}

// Handler returns the /metrics handler, for services that already run an HTTP
// server (the web status page) and just want to add the route.
func Handler() http.Handler { return promhttp.Handler() }
