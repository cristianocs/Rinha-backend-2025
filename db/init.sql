CREATE TABLE payments (
    id SERIAL PRIMARY KEY,
    correlation_id UUID NOT NULL UNIQUE,
    amount DECIMAL(10, 2) NOT NULL,
    processor VARCHAR(10) NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_payments_requested_at ON payments (requested_at);