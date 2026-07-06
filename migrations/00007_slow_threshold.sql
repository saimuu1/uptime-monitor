-- +goose Up
-- +goose StatementBegin
ALTER TABLE monitors ADD COLUMN slow_threshold_ms INT NOT NULL DEFAULT 0;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE monitors DROP COLUMN slow_threshold_ms;
-- +goose StatementEnd
