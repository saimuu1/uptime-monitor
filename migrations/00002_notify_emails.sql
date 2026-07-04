-- +goose Up
-- +goose StatementBegin
ALTER TABLE monitors ADD COLUMN notify_emails TEXT[] NOT NULL DEFAULT '{}';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE monitors DROP COLUMN notify_emails;
-- +goose StatementEnd
