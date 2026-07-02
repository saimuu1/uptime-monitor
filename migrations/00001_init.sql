-- +goose Up
-- +goose StatementBegin
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- +goose StatementEnd

-- monitors: what we watch. Config seeds this on first run; afterwards the DB is
-- the source of truth. The UNIQUE(name) lets the config loader upsert by name.
-- +goose StatementBegin
CREATE TABLE monitors (
    id               BIGSERIAL PRIMARY KEY,
    name             TEXT NOT NULL UNIQUE,
    url              TEXT NOT NULL,
    method           TEXT NOT NULL DEFAULT 'GET',
    interval_seconds INT  NOT NULL DEFAULT 30,
    timeout_ms       INT  NOT NULL DEFAULT 5000,
    expected_status  INT  NOT NULL DEFAULT 200,
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- +goose StatementEnd

-- incidents: a period where a monitor was down. resolved_at NULL == ongoing.
-- +goose StatementBegin
CREATE TABLE incidents (
    id          BIGSERIAL PRIMARY KEY,
    monitor_id  BIGINT NOT NULL REFERENCES monitors(id),
    started_at  TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    cause       TEXT
);
-- +goose StatementEnd

-- checks: one row per check, per monitor, per region — the time-series flood.
-- +goose StatementBegin
CREATE TABLE checks (
    time        TIMESTAMPTZ NOT NULL,
    monitor_id  BIGINT NOT NULL,
    region      TEXT NOT NULL DEFAULT 'local',
    up          BOOLEAN NOT NULL,
    status_code INT,
    latency_ms  INT,
    error       TEXT
);
-- +goose StatementEnd

-- +goose StatementBegin
SELECT create_hypertable('checks', 'time');
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX ON checks (monitor_id, time DESC);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS checks;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS incidents;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS monitors;
-- +goose StatementEnd
