// Package env centralizes the environment-variable lookups shared by the
// binaries, each with a sensible local-dev default.
package env

import (
	"os"
	"time"
)

// DBURL returns DATABASE_URL, defaulting to the local docker-compose Postgres
// (host port 5433 to avoid clashing with other local Postgres instances).
func DBURL() string {
	return orDefault("DATABASE_URL", "postgres://uptime:uptime@localhost:5433/uptime?sslmode=disable")
}

// NatsURL returns NATS_URL, defaulting to the local docker-compose NATS.
func NatsURL() string {
	return orDefault("NATS_URL", "nats://127.0.0.1:4222")
}

// Region returns REGION (where a checker runs), defaulting to "local".
func Region() string {
	return orDefault("REGION", "local")
}

// ConsensusFreshness: ignore a region's sample once older than this (v3).
func ConsensusFreshness() time.Duration {
	return orDuration("CONSENSUS_FRESHNESS", 30*time.Second)
}

// ConsensusStability: a new consensus must hold this long before it commits (v3).
func ConsensusStability() time.Duration {
	return orDuration("CONSENSUS_STABILITY", 5*time.Second)
}

// AlertWebhookURL is the Discord/Slack incoming webhook; empty disables it.
func AlertWebhookURL() string {
	return os.Getenv("ALERT_WEBHOOK_URL")
}

// SMTP settings for email alerts. Email is enabled when host + user are set.
func SMTPHost() string { return os.Getenv("SMTP_HOST") }
func SMTPPort() string { return orDefault("SMTP_PORT", "587") }
func SMTPUser() string { return os.Getenv("SMTP_USER") }
func SMTPPass() string { return os.Getenv("SMTP_PASS") }

// SMTPFrom is the From address; defaults to the SMTP username.
func SMTPFrom() string { return orDefault("SMTP_FROM", os.Getenv("SMTP_USER")) }

// WebAddr is the listen address for the status page.
func WebAddr() string {
	return orDefault("WEB_ADDR", ":8090")
}

// MetricsAddr is the listen address for a service's /metrics endpoint.
func MetricsAddr() string {
	return orDefault("METRICS_ADDR", ":2112")
}

func orDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func orDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
