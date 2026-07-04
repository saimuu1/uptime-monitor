// Command scheduler owns the monitor list. On each monitor's interval it
// publishes a "check this" job to NATS; it does no checking itself. This is the
// half of the v1 monitor that decided *what* to check.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/saimuu1/uptime-monitor/internal/config"
	"github.com/saimuu1/uptime-monitor/internal/env"
	"github.com/saimuu1/uptime-monitor/internal/message"
	"github.com/saimuu1/uptime-monitor/internal/metrics"
	"github.com/saimuu1/uptime-monitor/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, env.DBURL())
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer st.Close()

	nc, err := nats.Connect(env.NatsURL(), nats.Name("scheduler"))
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	defer nc.Drain()

	// Seed monitors from config (source of truth on start), then read them back.
	// A missing config is tolerated so you can also run from what's in the DB.
	if cfg, err := config.Load(env.ConfigPath()); err != nil {
		log.Printf("config: %v (using monitors already in the DB)", err)
	} else {
		for _, m := range cfg.StoreMonitors() {
			if _, err := st.UpsertMonitor(ctx, m); err != nil {
				log.Fatalf("seed monitor %q: %v", m.Name, err)
			}
		}
	}

	go metrics.Serve(ctx, env.MetricsAddr())

	// Reconcile the monitor list on a timer so sites added/removed via the web
	// form (or config) start/stop being watched without a restart.
	log.Printf("scheduler up; reconciling every %s", reconcileInterval)
	reconcile(ctx, st, nc)
	log.Print("scheduler shutdown complete")
}

const reconcileInterval = 15 * time.Second

// active is one running per-monitor publish loop.
type active struct {
	cancel  context.CancelFunc
	monitor store.Monitor
}

// changed reports whether anything the publish loop bakes into its job changed.
func (a active) changed(m store.Monitor) bool {
	o := a.monitor
	return o.URL != m.URL || o.Method != m.Method || o.Name != m.Name ||
		o.IntervalSeconds != m.IntervalSeconds || o.TimeoutMs != m.TimeoutMs ||
		o.ExpectedStatus != m.ExpectedStatus
}

// reconcile loops until ctx is done, keeping a publish loop running per enabled
// monitor: it starts loops for new monitors, restarts ones whose config
// changed, and stops loops for monitors that were removed or disabled.
func reconcile(ctx context.Context, st *store.Store, nc *nats.Conn) {
	running := map[int64]active{}
	start := func(m store.Monitor) {
		loopCtx, cancel := context.WithCancel(ctx)
		running[m.ID] = active{cancel: cancel, monitor: m}
		go publishLoop(loopCtx, nc, m)
	}

	syncOnce := func() {
		monitors, err := st.EnabledMonitors(ctx)
		if err != nil {
			log.Printf("reconcile: %v", err)
			return
		}
		seen := make(map[int64]bool, len(monitors))
		for _, m := range monitors {
			seen[m.ID] = true
			if a, ok := running[m.ID]; !ok {
				log.Printf("now watching [%s] %s", m.Name, m.URL)
				start(m)
			} else if a.changed(m) {
				a.cancel()
				start(m)
			}
		}
		for id, a := range running {
			if !seen[id] {
				log.Printf("stopped watching [%s]", a.monitor.Name)
				a.cancel()
				delete(running, id)
			}
		}
	}

	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	syncOnce() // reconcile immediately, then on each tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			syncOnce()
		}
	}
}

func publishLoop(ctx context.Context, nc *nats.Conn, m store.Monitor) {
	job := message.CheckJob{
		MonitorID:      m.ID,
		Name:           m.Name,
		URL:            m.URL,
		Method:         m.Method,
		TimeoutMs:      m.TimeoutMs,
		ExpectedStatus: m.ExpectedStatus,
	}
	payload, err := json.Marshal(job)
	if err != nil {
		log.Printf("[%s] marshal job: %v", m.Name, err)
		return
	}

	publish := func() {
		if err := nc.Publish(message.SubjectCheckRequest, payload); err != nil {
			log.Printf("[%s] publish: %v", m.Name, err)
			return
		}
		metrics.JobsPublished.Inc()
	}

	ticker := time.NewTicker(time.Duration(m.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	publish() // fire once immediately, then on every tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publish()
		}
	}
}
