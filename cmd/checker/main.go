// Command checker performs checks. It subscribes to check-request jobs in a
// NATS queue group (so each job goes to exactly one checker), runs the HTTP
// check, and publishes the result. It is stateless and touches no database, so
// you can run as many instances as you like — killing one doesn't stop
// monitoring, the rest keep pulling jobs.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/saimuu1/uptime-monitor/internal/check"
	"github.com/saimuu1/uptime-monitor/internal/env"
	"github.com/saimuu1/uptime-monitor/internal/message"
	"github.com/saimuu1/uptime-monitor/internal/metrics"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	region := env.Region()

	nc, err := nats.Connect(env.NatsURL(), nats.Name("checker-"+region))
	if err != nil {
		log.Fatalf("nats: %v", err)
	}
	defer nc.Drain()

	// QueueSubscribe with a shared group name means NATS delivers each job to
	// only one member of the group — that's the load balancing.
	sub, err := nc.QueueSubscribe(message.SubjectCheckRequest, message.QueueCheckers, func(msg *nats.Msg) {
		handleJob(ctx, nc, region, msg.Data)
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	go metrics.Serve(ctx, env.MetricsAddr())

	log.Printf("checker up (region=%s), waiting for jobs", region)
	<-ctx.Done()
	log.Print("checker shutdown complete")
}

func handleJob(ctx context.Context, nc *nats.Conn, region string, data []byte) {
	var job message.CheckJob
	if err := json.Unmarshal(data, &job); err != nil {
		log.Printf("bad job: %v", err)
		return
	}

	res := check.Do(ctx, check.Monitor{
		ID:              job.MonitorID,
		Name:            job.Name,
		URL:             job.URL,
		Method:          job.Method,
		Timeout:         time.Duration(job.TimeoutMs) * time.Millisecond,
		ExpectedStatus:  job.ExpectedStatus,
		ExpectedKeyword: job.ExpectedKeyword,
	})

	result := message.CheckResult{
		MonitorID:       job.MonitorID,
		Name:            job.Name,
		Region:          region,
		CheckedAt:       time.Now(),
		Up:              res.Up,
		StatusCode:      res.StatusCode,
		LatencyMs:       res.LatencyMs,
		Error:           res.Err,
		CertExpiry:      res.CertExpiry,
		SlowThresholdMs: job.SlowThresholdMs,
	}

	payload, err := json.Marshal(result)
	if err != nil {
		log.Printf("[%s] marshal result: %v", job.Name, err)
		return
	}
	if err := nc.Publish(message.SubjectCheckResult, payload); err != nil {
		log.Printf("[%s] publish result: %v", job.Name, err)
		return
	}

	outcome := "up"
	if !res.Up {
		outcome = "down"
	}
	metrics.ChecksTotal.WithLabelValues(region, outcome).Inc()
	metrics.CheckLatency.WithLabelValues(region).Observe(float64(res.LatencyMs))
	log.Printf("checked [%s] -> %s (%dms)", job.Name, outcome, res.LatencyMs)
}
