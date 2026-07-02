# Uptime Monitor — Build Plan
A self-hosted service that watches your websites and APIs, detects when they go
down, and alerts you. This document is the spec: work through it with Claude Code
one milestone at a time.
---
## 1. What we're building (the one-paragraph version)
A program that, on a fixed interval, sends a network request to each service you
tell it to watch, records whether the service responded correctly and how long it
took, and notifies you when a service transitions from up to down (or back). Later
milestones split the checking across multiple regions, add a public status page,
and deploy the whole thing to the cloud as infrastructure-as-code.
We build it in four versions. Each version is usable on its own and adds exactly
one hard concept. Do not skip ahead — a working v1 you actually run beats a
half-finished v4.
---
## 2. Tech stack (decisions already made — don't re-litigate mid-build)
| Concern | Choice | Why |
|---|---|---|
| Language | **Go** | Native language of the cloud/infra ecosystem; goroutines map perfectly onto "run many checks in parallel". |
| Relational DB | **PostgreSQL** | Holds config, monitors, incidents — the stuff that needs correctness and joins. |
| Time-series data | **TimescaleDB** | A Postgres extension, so v1 runs on a single database engine. Handles the flood of check results. |
| Message queue (v2+) | **NATS** | Simple, Go-native, decouples scheduling from checking. |
| HTTP router | **Go stdlib `net/http`** (Go 1.22+ routing) | No framework needed; add `chi` only if routing gets complex. |
| DB migrations | **goose** | Plain SQL migration files, easy to read. |
| Local dev | **Docker Compose** | One command brings up Postgres/Timescale + the app. |
| IaC (v4) | **Terraform** | Define cloud servers as code across regions. |
| Cloud (v4) | **DigitalOcean** (primary) | Clean Terraform provider and good region spread (NYC, London, Frankfurt, Singapore). *Alternatives: Fly.io for easy-mode multi-region, Hetzner for cheapest.* |
| Notifications | **Webhook first** (Discord/Slack), email later | A webhook is trivial to test; email needs a provider (Resend/SMTP). |
| Observability (v4) | **Prometheus + Grafana** | Monitor the monitor. |
If you're new to Go: that's fine and expected. Ask Claude Code to explain idioms
as it writes them (goroutines, channels, error handling, `context`).
---
## 3. Repository layout
```
uptime-monitor/
├── cmd/
│   └── monitor/          # v1: the single binary (main.go)
├── internal/
│   ├── config/           # load config, target list
│   ├── check/            # perform one HTTP check, return a result
│   ├── store/            # database reads/writes
│   ├── evaluate/         # up/down state, incidents (grows in v3)
│   └── alert/            # send notifications
├── migrations/           # goose SQL migration files
├── deploy/
│   ├── docker-compose.yml
│   └── terraform/        # v4
├── web/                  # v3: status page
├── config.example.yaml
├── PLAN.md               # this file
└── README.md
```
In v2 you'll split `cmd/monitor` into `cmd/api`, `cmd/checker`, and
`cmd/evaluator`. Keep the internal packages stable so that split is cheap.
---
## 4. Data model
### PostgreSQL (config + incidents)
```sql
-- monitors: what we watch
CREATE TABLE monitors (
    id               BIGSERIAL PRIMARY KEY,
    name             TEXT NOT NULL,
    url              TEXT NOT NULL,
    method           TEXT NOT NULL DEFAULT 'GET',
    interval_seconds INT  NOT NULL DEFAULT 30,
    timeout_ms       INT  NOT NULL DEFAULT 5000,
    expected_status  INT  NOT NULL DEFAULT 200,
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- incidents: a period where a monitor was down
CREATE TABLE incidents (
    id          BIGSERIAL PRIMARY KEY,
    monitor_id  BIGINT NOT NULL REFERENCES monitors(id),
    started_at  TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,          -- NULL means still ongoing
    cause       TEXT
);
```
### TimescaleDB (check results — the time-series)
```sql
-- one row per check, per monitor, per region
CREATE TABLE checks (
    time        TIMESTAMPTZ NOT NULL,
    monitor_id  BIGINT NOT NULL,
    region      TEXT NOT NULL DEFAULT 'local',
    up          BOOLEAN NOT NULL,
    status_code INT,
    latency_ms  INT,
    error       TEXT
);
SELECT create_hypertable('checks', 'time');
CREATE INDEX ON checks (monitor_id, time DESC);
```
---
## 5. Milestones
### v1 — Prove the loop (single process, one machine)
**Goal:** one binary reads a list of monitors, checks each on its interval, stores
results, and logs when a monitor changes state.
Build:
- `docker-compose.yml` with a TimescaleDB container (it's Postgres + the
  extension, so one container covers both tables for now).
- goose migrations for the three tables above.
- `internal/config`: load monitors from `config.yaml` on first run and upsert them
  into the `monitors` table. After that, the DB is the source of truth.
- `internal/check`: given a monitor, perform the HTTP request with a timeout using
  `context.WithTimeout`, return a result struct (up, status_code, latency_ms, error).
- `cmd/monitor`: load enabled monitors; for each, start a goroutine with a
  `time.Ticker` at its interval; each tick runs a check and writes a `checks` row.
- State transitions: keep the last-known up/down state per monitor in memory.
  On up→down, insert an `incidents` row and log `MONITOR DOWN`. On down→up,
  set `resolved_at` and log `MONITOR RECOVERED`.
**Done when:** you run `docker compose up`, point it at 2–3 real URLs (include one
that's reliably up and one you can kill on purpose), and watch it log a DOWN and a
RECOVERED when you take the target offline and bring it back.
---
### v2 — Split the worker (queue between scheduler and checker)
**Goal:** the thing that decides *what to check* is now separate from the thing
that *does the checking*, connected by a queue. This is the scalability pattern.
Build:
- Add a NATS container to compose.
- `cmd/api` (or `cmd/scheduler`): owns the monitor list and, on each monitor's
  interval, publishes a "check this" job to a NATS subject.
- `cmd/checker`: subscribes, performs checks, publishes results back.
- `cmd/evaluator`: subscribes to results, writes to `checks`, manages incident
  state and alerts (move the v1 transition logic here).
- Run 2+ checker instances to see the queue distribute work.
**Done when:** you can start and stop checker instances freely and checks keep
flowing; killing one checker doesn't stop monitoring.
---
### v3 — Multiple regions + consensus + status page
**Goal:** check from more than one location and only alert when locations *agree*
something is down. Add a page you can look at.
Build:
- Give each checker a `region` label; tag every result with it.
- Evaluator consensus: a monitor is "down" only when a majority of regions report
  it down within a short window. This kills false alarms from one bad network path.
- Flapping/dedup: don't open a new incident (or re-alert) for a monitor that's
  bouncing up/down within N seconds; require it to be stable.
- Real notifications in `internal/alert`: send to a Discord/Slack webhook on
  DOWN and RECOVERED. Add email (Resend or SMTP) if you want.
- `web/`: a simple status page — read the latest state per monitor and last 24h of
  `checks`, render up/down + a small uptime %. Plain HTML/JS is fine.
**Done when:** killing one region's checker does NOT trigger an alert, but a real
outage (seen by all regions) does — and the status page reflects it.
---
### v4 — Deploy as infrastructure-as-code
**Goal:** the whole system runs in the cloud, defined in code, across real regions.
Build:
- Dockerize each binary (multi-stage Go builds → tiny images).
- `deploy/terraform/`: Terraform that provisions one small server per region on
  DigitalOcean and runs a checker on each; provisions a central box (or managed
  Postgres) for the api/evaluator/DB.
- CI/CD: GitHub Actions to build images and run tests on push.
- Observability: expose Prometheus metrics from each service; a Grafana dashboard
  showing check volume, alert counts, and the monitor's own health.
**Done when:** `terraform apply` stands the whole thing up across ≥2 regions, and
you can watch your own services from three continents.
---
## 6. Local development
```bash
# bring up databases + services
docker compose -f deploy/docker-compose.yml up
# run migrations (from host, or as a compose step)
goose -dir migrations postgres "$DATABASE_URL" up
# run the monitor (v1)
go run ./cmd/monitor
```
Keep a `config.example.yaml`:
```yaml
monitors:
  - name: "My blog"
    url: "https://example.com"
    interval_seconds: 30
  - name: "My API"
    url: "https://api.example.com/health"
    interval_seconds: 15
    expected_status: 200
```
---
## 7. Testing
- Unit-test `internal/check` against a local `httptest.Server` you can make return
  200, 500, time out, or refuse connections.
- Unit-test the evaluator's state machine: feed it sequences of results and assert
  it opens/resolves incidents correctly, including the flapping case.
- Aim for tests on the *logic* (check outcome, state transitions, consensus), not
  on the HTTP plumbing.
---
## 8. Stretch goals (once v4 works)
- SSL certificate expiry checks (warn before a cert expires).
- Keyword checks (page is "up" only if it contains expected text).
- Latency percentiles (p50/p95/p99) on the status page.
- Maintenance windows (suppress alerts during planned downtime).
- On-call escalation (if unacknowledged in 10 min, escalate).
- A proper query API over historical uptime for reporting.
