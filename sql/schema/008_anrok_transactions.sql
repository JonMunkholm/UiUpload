-- +goose Up
CREATE TABLE anrok_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    transaction_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    overall_vat_id_status TEXT,
    valid_vat_ids TEXT,
    other_vat_ids TEXT,
    invoice_date DATE,
    tax_date DATE,
    transaction_currency TEXT,
    sales_amount NUMERIC,
    exempt_reason TEXT,
    tax_amount NUMERIC,
    invoice_amount NUMERIC,
    void BOOLEAN,
    customer_address_line_1 TEXT,
    customer_address_city TEXT,
    customer_address_region TEXT,
    customer_address_postal_code TEXT,
    customer_address_country TEXT,
    customer_country_code TEXT,
    jurisdictions TEXT,
    jurisdiction_ids TEXT,
    return_ids TEXT

);

-- +goose Down
DROP TABLE IF EXISTS anrok_transactions;
