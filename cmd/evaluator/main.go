// Command evaluator is the single owner of check storage and incident state.
// It subscribes to check results, writes each to the time-series table, and —
// using the same up/down state machine as v1 — opens and resolves incidents on
// transitions. Only one evaluator should run (it holds the authoritative state).
package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/nats-io/nats.go"

	"github.com/saimuu1/uptime-monitor/internal/check"
	"github.com/saimuu1/uptime-monitor/internal/env"
	"github.com/saimuu1/uptime-monitor/internal/evaluate"
	"github.com/saimuu1/uptime-monitor/internal/message"
	"github.com/saimuu1/uptime-monitor/internal/store"
)

// evaluator holds the last-known up/down state per monitor. The map is guarded
// because, although NATS dispatches one subscription's messages serially, the
// lock keeps the invariant explicit and safe.
type evaluator struct {
	st     *store.Store
	mu     sync.Mutex
	lastUp map[int64]bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, env.DBURL())
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer st.Close()

	e := &evaluator{st: st, lastUp: make(map[int64]bool)}

	// Seed in-memory state from the DB so a restart mid-outage doesn't reopen a
	// duplicate incident: a monitor with an open incident starts "down".
	monitors, err := st.EnabledMonitors(ctx)
	if err != nil {
		log.Fatalf("load monitors: %v", err)
	}
	for _, m := range monitors {
		open, err := st.HasOpenIncident(ctx, m.ID)
		if err != nil {
			log.Printf("[%s] seed state: %v", m.Name, err)
			continue
		}
		e.lastUp[m.ID] = !open // open incident => currently down
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

	log.Printf("evaluator up, seeded %d monitor(s)", len(monitors))
	<-ctx.Done()
	log.Print("evaluator shutdown complete")
}

func (e *evaluator) handleResult(ctx context.Context, data []byte) {
	var r message.CheckResult
	if err := json.Unmarshal(data, &r); err != nil {
		log.Printf("bad result: %v", err)
		return
	}

	// Persist the raw check at the time it actually ran.
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

	// Unknown monitor (e.g. added after startup): assume up as the baseline.
	prevUp, known := e.lastUp[r.MonitorID]
	if !known {
		prevUp = true
	}

	switch evaluate.Transition(prevUp, r.Up) {
	case evaluate.Down:
		cause := r.Error
		if cause == "" {
			cause = "unexpected status " + strconv.Itoa(r.StatusCode)
		}
		if err := e.st.OpenIncident(ctx, r.MonitorID, cause); err != nil {
			log.Printf("[%s] open incident: %v", r.Name, err)
		}
		log.Printf("MONITOR DOWN  [%s] (region=%s) %s", r.Name, r.Region, cause)
	case evaluate.Recovered:
		if err := e.st.ResolveIncident(ctx, r.MonitorID); err != nil {
			log.Printf("[%s] resolve incident: %v", r.Name, err)
		}
		log.Printf("MONITOR RECOVERED  [%s] (region=%s, %dms)", r.Name, r.Region, r.LatencyMs)
	}
	e.lastUp[r.MonitorID] = r.Up
}
