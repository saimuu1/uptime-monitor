-- +goose Up
-- +goose StatementBegin
ALTER TABLE monitors ADD COLUMN cert_expiry TIMESTAMPTZ;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE monitors DROP COLUMN cert_expiry;
-- +goose StatementEnd
