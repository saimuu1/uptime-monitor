// Command evaluator is the single owner of check storage and incident state.
// It subscribes to check results from every region, stores each one, and feeds
// them into a per-monitor consensus engine (internal/evaluate). A monitor is
// only declared down when a majority of regions agree AND the state holds long
// enough to rule out flapping — at which point it opens/resolves an incident
// and fires an alert. Run exactly one evaluator (it holds authoritative state).
package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/saimuu1/uptime-monitor/internal/alert"
	"github.com/saimuu1/uptime-monitor/internal/check"
	"github.com/saimuu1/uptime-monitor/internal/env"
	"github.com/saimuu1/uptime-monitor/internal/evaluate"
	"github.com/saimuu1/uptime-monitor/internal/message"
	"github.com/saimuu1/uptime-monitor/internal/metrics"
	"github.com/saimuu1/uptime-monitor/internal/store"
)

// entry is the evaluator's per-monitor bookkeeping around the pure engine.
type entry struct {
	id         int64
	name       string
	engine     *evaluate.Monitor
	lastRegion string // region of the most recent result (for alert context)
	lastMs     int    // latency of the most recent result
	downCause  string // cause from the most recent down result
}

type evaluator struct {
	st       *store.Store
	notifier alert.Notifier
	cfg      evaluate.Config
	mu       sync.Mutex
	monitors map[int64]*entry
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, env.DBURL())
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer st.Close()

	var notifier alert.Notifier = alert.Noop{}
	if url := env.AlertWebhookURL(); url != "" {
		notifier = alert.NewWebhook(url)
		log.Print("alerts: webhook enabled")
	} else {
		log.Print("alerts: no ALERT_WEBHOOK_URL set, logging only")
	}

	e := &evaluator{
		st:       st,
		notifier: notifier,
		cfg:      evaluate.Config{Freshness: env.ConsensusFreshness(), Stability: env.ConsensusStability()},
		monitors: make(map[int64]*entry),
	}

	// Seed engines from the DB so a restart mid-outage doesn't reopen incidents.
	monitors, err := st.EnabledMonitors(ctx)
	if err != nil {
		log.Fatalf("load monitors: %v", err)
	}
	for _, m := range monitors {
		open, err := st.HasOpenIncident(ctx, m.ID)
		if err != nil {
			log.Printf("[%s] seed state: %v", m.Name, err)
		}
		e.monitors[m.ID] = &entry{
			id:     m.ID,
			name:   m.Name,
			engine: evaluate.NewMonitor(e.cfg, !open),
		}
	}

	nc, err := nats.Connect(env.NatsURL(), nats.Name("evaluator"))
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	defer nc.Drain()

	sub, err := nc.Subscribe(message.SubjectCheckResult, func(msg *nats.Msg) {
		e.handleResult(ctx, msg.Data)
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	log.Printf("evaluator up, seeded %d monitor(s) (freshness=%s stability=%s)",
		len(monitors), e.cfg.Freshness, e.cfg.Stability)

	go metrics.Serve(ctx, env.MetricsAddr())

	// A slow tick re-evaluates every monitor so a pending change still commits
	// once its stability window elapses, even between results.
	go e.tickLoop(ctx)

	<-ctx.Done()
	log.Print("evaluator shutdown complete")
}

func (e *evaluator) tickLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			e.mu.Lock()
			for _, en := range e.monitors {
				e.commit(ctx, en, en.engine.Evaluate(now))
			}
			e.mu.Unlock()
		}
	}
}

func (e *evaluator) handleResult(ctx context.Context, data []byte) {
	var r message.CheckResult
	if err := json.Unmarshal(data, &r); err != nil {
		log.Printf("bad result: %v", err)
		return
	}

	// Persist the raw check at the time it actually ran (outside the lock).
	if err := e.st.InsertCheck(ctx, r.MonitorID, r.Region, r.CheckedAt, check.Result{
		Up:         r.Up,
		StatusCode: r.StatusCode,
		LatencyMs:  r.LatencyMs,
		Err:        r.Error,
	}); err != nil {
		log.Printf("[%s] store check: %v", r.Name, err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	en, ok := e.monitors[r.MonitorID]
	if !ok { // monitor added after startup: assume up as baseline
		en = &entry{id: r.MonitorID, name: r.Name, engine: evaluate.NewMonitor(e.cfg, true)}
		e.monitors[r.MonitorID] = en
	}
	en.lastRegion = r.Region
	en.lastMs = r.LatencyMs
	if !r.Up {
		en.downCause = causeOf(r)
	}

	metrics.ResultsProcessed.Inc()
	en.engine.Observe(r.Region, evaluate.Sample{Up: r.Up, At: r.CheckedAt})
	e.commit(ctx, en, en.engine.Evaluate(time.Now()))
}

// commit turns a committed engine event into DB writes, a log line, and an
// alert. Caller must hold e.mu.
func (e *evaluator) commit(ctx context.Context, en *entry, ev evaluate.Event) {
	switch ev {
	case evaluate.Down:
		if err := e.st.OpenIncident(ctx, en.id, en.downCause); err != nil {
			log.Printf("[%s] open incident: %v", en.name, err)
		}
		log.Printf("MONITOR DOWN  [%s] %s", en.name, en.downCause)
		metrics.IncidentsOpened.Inc()
		metrics.AlertsSent.WithLabelValues("down").Inc()
		e.notify(ctx, alert.Event{Monitor: en.name, Kind: alert.Down,
			Region: en.lastRegion, Cause: en.downCause, At: time.Now()})
	case evaluate.Recovered:
		if err := e.st.ResolveIncident(ctx, en.id); err != nil {
			log.Printf("[%s] resolve incident: %v", en.name, err)
		}
		log.Printf("MONITOR RECOVERED  [%s] (%dms)", en.name, en.lastMs)
		metrics.AlertsSent.WithLabelValues("recovered").Inc()
		e.notify(ctx, alert.Event{Monitor: en.name, Kind: alert.Recovered,
			Region: en.lastRegion, At: time.Now()})
	}
	// Keep the per-monitor up/down gauge fresh on every evaluation.
	up := 0.0
	if en.engine.Up() {
		up = 1.0
	}
	metrics.MonitorUp.WithLabelValues(en.name).Set(up)
}

func (e *evaluator) notify(ctx context.Context, ev alert.Event) {
	if err := e.notifier.Send(ctx, ev); err != nil {
		log.Printf("[%s] alert: %v", ev.Monitor, err)
	}
}

func causeOf(r message.CheckResult) string {
	if r.Error != "" {
		return r.Error
	}
	return "unexpected status " + strconv.Itoa(r.StatusCode)
}
