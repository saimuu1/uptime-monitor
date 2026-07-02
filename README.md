# Uptime Monitor

A self-hosted service that watches your websites/APIs, records whether they're up
and how long they take, and detects when they go down or recover.

This repo follows the milestone plan in [`PLAN.md`](PLAN.md). **Current milestone:
v3 — checkers run in multiple regions; the evaluator only alerts when a majority
of regions *agree* a monitor is down and the state holds long enough to rule out
flapping. It sends webhook alerts and serves a status page.**

## Architecture

**v1 (all-in-one, still available as `cmd/monitor`):** one process, one goroutine
+ ticker per monitor, checking → storing → evaluating in-line.

**v2 (the split):**

```
                    ┌──────────────┐
   reads monitors ─▶│  scheduler   │  one ticker per monitor
                    └──────┬───────┘
                           │ publish CheckJob
                    NATS  checks.request  (queue group "checkers")
                           │
              ┌────────────┼────────────┐   each job → exactly one checker
              ▼            ▼             ▼
        ┌─────────┐  ┌─────────┐   ┌─────────┐   stateless, no DB,
        │checker A│  │checker B│ … │checker N│   run as many as you want
        └────┬────┘  └────┬────┘   └────┬────┘
             └───────── publish CheckResult ─────────┐
                    NATS  checks.result              │
                           │                         ▼
                    ┌──────────────┐   writes `checks`, owns incident state,
                    │  evaluator   │   logs DOWN / RECOVERED (single instance)
                    └──────────────┘
```

Shared message types live in `internal/message`. Checkers join the `checkers`
NATS **queue group**, so each job is delivered to exactly one of them — that's the
load balancing and the failover. TimescaleDB (Postgres + the timescaledb
extension) backs both the relational tables (`monitors`, `incidents`) and the
time-series `checks` hypertable.

**v3 adds, all inside the evaluator (`internal/evaluate.Monitor`):**

- **Consensus** — each region's latest result is a vote. A monitor is down only
  on a *strict majority* of fresh regions reporting down; ties stay up. A region
  whose checker died stops voting once its last sample goes stale
  (`CONSENSUS_FRESHNESS`), so it can't cause a false alarm.
- **Flap suppression** — a new consensus must hold for `CONSENSUS_STABILITY`
  before it's committed, so a brief blip never opens an incident.
- **Alerts** (`internal/alert`) — on committed DOWN/RECOVERED, POST to a
  Discord/Slack webhook (`ALERT_WEBHOOK_URL`; unset = log only).
- **Status page** (`cmd/web` + `web/`) — current state + 24h uptime per monitor.

The engine is pure (time is injected), so consensus and flapping are unit-tested
without a clock or network — see `internal/evaluate/evaluate_test.go`.

## Prerequisites

- Go 1.22+
- Docker (for the database)
- [`goose`](https://github.com/pressly/goose) — `go install github.com/pressly/goose/v3/cmd/goose@latest`

## Quickstart

```bash
# 1. Start TimescaleDB (host port 5433 to avoid clashing with other local Postgres)
docker compose -f deploy/docker-compose.yml up -d

# 2. Run migrations
export DATABASE_URL="postgres://uptime:uptime@localhost:5433/uptime?sslmode=disable"
goose -dir migrations postgres "$DATABASE_URL" up

# 3. Configure what to watch
cp config.example.yaml config.yaml   # then edit

# 4a. Run the all-in-one v1 process...
go run ./cmd/monitor

# 4b. ...or the v2/v3 split (needs NATS from compose). In separate terminals:
ALERT_WEBHOOK_URL=https://discord.com/api/webhooks/... go run ./cmd/evaluator
REGION=east    go run ./cmd/checker    # run checkers in as many regions as you like
REGION=west    go run ./cmd/checker
REGION=central go run ./cmd/checker
go run ./cmd/scheduler                 # publishes the check jobs
go run ./cmd/web                       # status page at http://localhost:8090
```

Knobs (all optional, with defaults): `REGION` (`local`) tags a checker's results;
`NATS_URL` (`nats://127.0.0.1:4222`); `ALERT_WEBHOOK_URL` (unset = log only);
`CONSENSUS_FRESHNESS` (`30s`); `CONSENSUS_STABILITY` (`5s`); `WEB_ADDR` (`:8090`).
Kill one region's checker and monitoring continues with no false alarm; take the
target down for real and every surviving region agrees → alert + status page flips.

`config.yaml` is the seed: on start its monitors are upserted (matched by `name`)
into the DB, which is the source of truth thereafter. `DATABASE_URL` overrides the
default connection string.

## Try the outage flow

```bash
# a local target the monitor watches (config.example.yaml points at :8080)
python3 -m http.server 8080
```

Run the monitor, then Ctrl-C the target: you'll see `MONITOR DOWN` and an
`incidents` row open. Restart it: `MONITOR RECOVERED` and the incident resolves.

## Tests

```bash
go test ./...
```

- `internal/check` — table-driven against `httptest` (200 / 500 / timeout / refused).
- `internal/evaluate` — the state machine: consensus majority, tie handling, stale
  regions excluded, flap suppression, and sustained-outage commit.
- `internal/alert` — webhook payload + non-2xx handling against `httptest`.

## Handy queries

```sql
SELECT * FROM incidents ORDER BY id DESC;
SELECT name, up, count(*) FROM checks c JOIN monitors m ON m.id=c.monitor_id
  GROUP BY name, up;
```

## Layout

| Path | Role |
|---|---|
| `cmd/monitor` | v1 all-in-one binary (still usable) |
| `cmd/scheduler` | v2: owns monitors, publishes check jobs |
| `cmd/checker` | v2: stateless worker, one per region |
| `cmd/evaluator` | v2/v3: stores checks, consensus, incidents, alerts |
| `cmd/web` | v3: status page server |
| `internal/config` | load YAML, upsert monitors |
| `internal/check` | perform one HTTP check |
| `internal/store` | database reads/writes (pgx) |
| `internal/evaluate` | v1 transition + v3 consensus/flap engine |
| `internal/alert` | webhook notifications |
| `internal/message` | NATS subjects + job/result payloads |
| `internal/env` | shared env-var config with defaults |
| `web` | embedded status-page template |
| `migrations` | goose SQL migrations |
| `deploy` | docker-compose (+ terraform in v4) |

## Next milestones

See `PLAN.md`: v4 dockerizes each binary and deploys via Terraform to
DigitalOcean across real regions, with CI/CD and Prometheus/Grafana.

## Stopping

```bash
docker compose -f deploy/docker-compose.yml down       # keep data
docker compose -f deploy/docker-compose.yml down -v     # wipe the volume too
```
