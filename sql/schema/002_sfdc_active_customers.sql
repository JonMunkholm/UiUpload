-- +goose Up
CREATE TABLE sfdc_customers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    account_id_casesafe TEXT,
    account_name        TEXT,
    last_activity       DATE,
    type                TEXT
);

-- +goose Down
DROP TABLE IF EXISTS sfdc_customers;
