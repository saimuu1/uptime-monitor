-- +goose Up
-- +goose StatementBegin
ALTER TABLE monitors ADD COLUMN expected_keyword TEXT NOT NULL DEFAULT '';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE monitors DROP COLUMN expected_keyword;
-- +goose StatementEnd
