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
	"fmt"
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
	id          int64
	name        string
	engine      *evaluate.Monitor
	lastRegion  string // region of the most recent result (for alert context)
	lastMs      int    // latency of the most recent result
	downCause   string // cause from the most recent down result
	certAlerted bool   // already warned about the current cert nearing expiry
}

type evaluator struct {
	st           *store.Store
	notifiers    []alert.Notifier
	defaultTo    string // fallback email for monitors with no explicit recipients
	certWarnDays int    // warn when a cert is within this many days of expiring
	cfg          evaluate.Config
	mu           sync.Mutex
	monitors     map[int64]*entry
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, env.DBURL())
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer st.Close()

	var notifiers []alert.Notifier
	if url := env.AlertWebhookURL(); url != "" {
		notifiers = append(notifiers, alert.NewWebhook(url))
		log.Print("alerts: webhook enabled")
	}
	defaultTo := ""
	if env.SMTPHost() != "" && env.SMTPUser() != "" {
		notifiers = append(notifiers, alert.NewEmail(
			env.SMTPHost(), env.SMTPPort(), env.SMTPUser(), env.SMTPPass(), env.SMTPFrom()))
		defaultTo = env.AlertEmailTo()
		log.Printf("alerts: email enabled (default recipient: %s)", defaultTo)
	}
	if len(notifiers) == 0 {
		log.Print("alerts: none configured, logging only")
	}

	e := &evaluator{
		st:           st,
		notifiers:    notifiers,
		defaultTo:    defaultTo,
		certWarnDays: env.CertWarnDays(),
		cfg:          evaluate.Config{Freshness: env.ConsensusFreshness(), Stability: env.ConsensusStability()},
		monitors:     make(map[int64]*entry),
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
	e.checkCert(ctx, en, r.CertExpiry)
	en.engine.Observe(r.Region, evaluate.Sample{Up: r.Up, At: r.CheckedAt})
	e.commit(ctx, en, en.engine.Evaluate(time.Now()))
}

// checkCert records a monitor's TLS cert expiry and emails once when it's within
// the warning window. Re-arms if the cert is renewed (expiry moves out again).
// Caller must hold e.mu.
func (e *evaluator) checkCert(ctx context.Context, en *entry, expiry time.Time) {
	if expiry.IsZero() {
		return // non-HTTPS or no cert seen
	}
	if err := e.st.SetCertExpiry(ctx, en.id, expiry); err != nil {
		log.Printf("[%s] cert expiry: %v", en.name, err)
	}
	daysLeft := int(time.Until(expiry).Hours() / 24)
	if daysLeft >= e.certWarnDays {
		en.certAlerted = false // healthy / renewed — re-arm the warning
		return
	}
	if en.certAlerted {
		return // already warned about this cert
	}
	en.certAlerted = true
	cause := fmt.Sprintf("expires in %d days (%s)", daysLeft, expiry.Format("2006-01-02"))
	log.Printf("CERT EXPIRING  [%s] %s", en.name, cause)
	metrics.AlertsSent.WithLabelValues("cert").Inc()
	e.notify(ctx, alert.Event{Monitor: en.name, Kind: alert.CertExpiring,
		Cause: cause, At: time.Now(), To: e.recipients(ctx, en.id)})
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
			Region: en.lastRegion, Cause: en.downCause, At: time.Now(), To: e.recipients(ctx, en.id)})
	case evaluate.Recovered:
		if err := e.st.ResolveIncident(ctx, en.id); err != nil {
			log.Printf("[%s] resolve incident: %v", en.name, err)
		}
		log.Printf("MONITOR RECOVERED  [%s] (%dms)", en.name, en.lastMs)
		metrics.AlertsSent.WithLabelValues("recovered").Inc()
		e.notify(ctx, alert.Event{Monitor: en.name, Kind: alert.Recovered,
			Region: en.lastRegion, At: time.Now(), To: e.recipients(ctx, en.id)})
	}
	// Keep the per-monitor up/down gauge fresh on every evaluation.
	up := 0.0
	if en.engine.Up() {
		up = 1.0
	}
	metrics.MonitorUp.WithLabelValues(en.name).Set(up)
}

// recipients fetches a monitor's alert emails at send time (always fresh),
// falling back to the default recipient when the monitor lists none.
func (e *evaluator) recipients(ctx context.Context, monitorID int64) []string {
	to, err := e.st.NotifyEmails(ctx, monitorID)
	if err != nil {
		log.Printf("recipients for monitor %d: %v", monitorID, err)
	}
	return alert.Recipients(to, e.defaultTo)
}

func (e *evaluator) notify(ctx context.Context, ev alert.Event) {
	for _, n := range e.notifiers {
		if err := n.Send(ctx, ev); err != nil {
			log.Printf("[%s] alert: %v", ev.Monitor, err)
		}
	}
}

func causeOf(r message.CheckResult) string {
	if r.Error != "" {
		return r.Error
	}
	return "unexpected status " + strconv.Itoa(r.StatusCode)
}
