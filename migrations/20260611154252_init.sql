-- +goose Up
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- +goose StatementBegin
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unprocessed ON outbox_events (created_at) WHERE processed_at IS NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_outbox_unprocessed;
DROP TABLE IF EXISTS outbox_events;
-- +goose StatementEnd
