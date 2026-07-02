// Package env centralizes the environment-variable lookups shared by the v2
// binaries, each with a sensible local-dev default.
package env

import "os"

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

func orDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
