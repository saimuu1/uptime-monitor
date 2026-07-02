# Uptime Monitor

A self-hosted service that watches your websites/APIs, records whether they're up
and how long they take, and detects when they go down or recover.

This repo follows the milestone plan in [`PLAN.md`](PLAN.md). **Current milestone:
v2 — the scheduler, checkers, and evaluator are separate processes connected by a
NATS queue, so checking scales horizontally and survives a checker dying.**

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

# 4b. ...or the v2 split (needs NATS from compose). In separate terminals:
go run ./cmd/evaluator                 # one instance (owns incident state)
REGION=east go run ./cmd/checker       # run as many checkers as you like
REGION=west go run ./cmd/checker
go run ./cmd/scheduler                 # publishes the check jobs
```

`REGION` tags each checker's results (defaults to `local`); `NATS_URL` overrides
the queue connection (default `nats://127.0.0.1:4222`). Kill a checker and watch
the others keep pulling jobs.

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
- `internal/evaluate` — the up/down state machine, including a full outage sequence.

## Handy queries

```sql
SELECT * FROM incidents ORDER BY id DESC;
SELECT name, up, count(*) FROM checks c JOIN monitors m ON m.id=c.monitor_id
  GROUP BY name, up;
```

## Layout

| Path | Role |
|---|---|
| `cmd/monitor` | v1 single binary (splits into api/checker/evaluator in v2) |
| `internal/config` | load YAML, upsert monitors |
| `internal/check` | perform one HTTP check |
| `internal/store` | database reads/writes (pgx) |
| `internal/evaluate` | up/down state machine (grows into consensus in v3) |
| `internal/alert` | notifications (v3) |
| `migrations` | goose SQL migrations |
| `deploy` | docker-compose (+ terraform in v4) |

## Next milestones

See `PLAN.md`: v2 splits scheduler/checker/evaluator over NATS; v3 adds
multi-region consensus + a status page + real webhook alerts; v4 deploys via
Terraform to DigitalOcean.

## Stopping

```bash
docker compose -f deploy/docker-compose.yml down       # keep data
docker compose -f deploy/docker-compose.yml down -v     # wipe the volume too
```
