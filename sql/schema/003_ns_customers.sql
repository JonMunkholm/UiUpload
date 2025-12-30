-- +goose Up
CREATE TABLE ns_customers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    salesforce_id_io    TEXT,
    internal_id         TEXT,
    name                TEXT,
    duplicate           TEXT,
    company_name        TEXT,
    balance             NUMERIC,
    unbilled_orders     NUMERIC,
    overdue_balance     NUMERIC,
    days_overdue        NUMERIC
);

-- +goose Down
DROP TABLE IF EXISTS ns_customers;
