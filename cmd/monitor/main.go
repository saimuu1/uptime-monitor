// Command monitor is the v1 single-process uptime monitor: it loads a list of
// monitors, checks each on its own interval, stores every result, and logs +
// records incidents when a monitor changes up/down state.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/saimuu1/uptime-monitor/internal/check"
	"github.com/saimuu1/uptime-monitor/internal/config"
	"github.com/saimuu1/uptime-monitor/internal/env"
	"github.com/saimuu1/uptime-monitor/internal/evaluate"
	"github.com/saimuu1/uptime-monitor/internal/store"
)

const region = "local" // v1 checks from one place; v3 gives each checker a real region.

func main() {
	// A single context, cancelled on Ctrl-C / SIGTERM, is threaded through every
	// goroutine so shutdown is clean.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://uptime:uptime@localhost:5433/uptime?sslmode=disable"
	}

	st, err := store.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer st.Close()

	// Seed the monitors table from config, then read back what's enabled.
	cfg, err := config.Load(env.ConfigPath())
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	for _, m := range cfg.StoreMonitors() {
		if _, err := st.UpsertMonitor(ctx, m); err != nil {
			log.Fatalf("seed: %v", err)
		}
	}
	monitors, err := st.EnabledMonitors(ctx)
	if err != nil {
		log.Fatalf("load monitors: %v", err)
	}
	if len(monitors) == 0 {
		log.Fatal("no enabled monitors")
	}

	log.Printf("starting %d monitor(s), region=%s", len(monitors), region)

	// One goroutine per monitor. Each owns its own last-known state, so there's
	// no shared map and no locking between monitors.
	var wg sync.WaitGroup
	for _, m := range monitors {
		wg.Add(1)
		go func(m store.Monitor) {
			defer wg.Done()
			runMonitor(ctx, st, m)
		}(m)
	}
	wg.Wait()
	log.Print("shutdown complete")
}

// runMonitor checks one monitor on its interval until ctx is cancelled.
func runMonitor(ctx context.Context, st *store.Store, m store.Monitor) {
	cm := check.Monitor{
		ID:             m.ID,
		Name:           m.Name,
		URL:            m.URL,
		Method:         m.Method,
		Timeout:        time.Duration(m.TimeoutMs) * time.Millisecond,
		ExpectedStatus: m.ExpectedStatus,
	}

	// Seed state from the DB: if there's an open incident we start "down" so a
	// restart mid-outage won't open a duplicate, and a real recovery still logs.
	lastUp := true
	if open, err := st.HasOpenIncident(ctx, m.ID); err != nil {
		log.Printf("[%s] warning: could not read incident state: %v", m.Name, err)
	} else if open {
		lastUp = false
	}

	ticker := time.NewTicker(time.Duration(m.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	// Check once immediately, then on every tick.
	doCheck := func() {
		res := check.Do(ctx, cm)
		if err := st.InsertCheck(ctx, m.ID, region, time.Now(), res); err != nil {
			log.Printf("[%s] store check: %v", m.Name, err)
		}

		switch evaluate.Transition(lastUp, res.Up) {
		case evaluate.Down:
			cause := res.Err
			if cause == "" {
				cause = statusCause(res.StatusCode, cm.ExpectedStatus)
			}
			if err := st.OpenIncident(ctx, m.ID, cause); err != nil {
				log.Printf("[%s] open incident: %v", m.Name, err)
			}
			log.Printf("MONITOR DOWN  [%s] %s", m.Name, cause)
		case evaluate.Recovered:
			if err := st.ResolveIncident(ctx, m.ID); err != nil {
				log.Printf("[%s] resolve incident: %v", m.Name, err)
			}
			log.Printf("MONITOR RECOVERED  [%s] (%dms)", m.Name, res.LatencyMs)
		}
		lastUp = res.Up
	}

	doCheck()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			doCheck()
		}
	}
}

func statusCause(got, want int) string {
	return "unexpected status " + strconv.Itoa(got) + " (want " + strconv.Itoa(want) + ")"
}
