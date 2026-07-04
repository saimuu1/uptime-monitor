// Command scheduler owns the monitor list. On each monitor's interval it
// publishes a "check this" job to NATS; it does no checking itself. This is the
// half of the v1 monitor that decided *what* to check.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

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

	monitors, err := st.EnabledMonitors(ctx)
	if err != nil {
		log.Fatalf("load monitors: %v", err)
	}
	if len(monitors) == 0 {
		log.Fatal("no enabled monitors")
	}
	log.Printf("scheduling %d monitor(s)", len(monitors))

	go metrics.Serve(ctx, env.MetricsAddr())

	// One ticker per monitor, each publishing a job on its own interval.
	var wg sync.WaitGroup
	for _, m := range monitors {
		wg.Add(1)
		go func(m store.Monitor) {
			defer wg.Done()
			publishLoop(ctx, nc, m)
		}(m)
	}
	wg.Wait()
	log.Print("scheduler shutdown complete")
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
