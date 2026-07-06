-- +goose Up
-- +goose StatementBegin
ALTER TABLE monitors ADD COLUMN muted_until TIMESTAMPTZ;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE monitors DROP COLUMN muted_until;
-- +goose StatementEnd
