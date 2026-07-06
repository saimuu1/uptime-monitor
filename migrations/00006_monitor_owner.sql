-- +goose Up
-- +goose StatementBegin
ALTER TABLE monitors ADD COLUMN user_id BIGINT REFERENCES users(id) ON DELETE CASCADE;
-- +goose StatementEnd

-- Names were globally unique (for config upsert-by-name). With per-user monitors
-- that's wrong — two users may watch the same site. Keep uniqueness only for
-- system monitors (user_id IS NULL, seeded from config) via a partial index.
-- +goose StatementBegin
ALTER TABLE monitors DROP CONSTRAINT monitors_name_key;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE UNIQUE INDEX monitors_system_name ON monitors (name) WHERE user_id IS NULL;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX monitors_user_id ON monitors (user_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS monitors_user_id;
-- +goose StatementEnd
-- +goose StatementBegin
DROP INDEX IF EXISTS monitors_system_name;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE monitors ADD CONSTRAINT monitors_name_key UNIQUE (name);
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE monitors DROP COLUMN user_id;
-- +goose StatementEnd
