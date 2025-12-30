-- +goose Up
CREATE TABLE ns_invoice_detail (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    sfdc_opp_id TEXT,
    sfdc_opp_line_id TEXT,
    sfdc_pricebook_id TEXT,
    customer_internal_id TEXT,
    product_internal_id TEXT,
    type TEXT,
    date DATE,
    date_due DATE,
    document_number TEXT,
    name TEXT,
    memo TEXT,
    item TEXT,
    qty NUMERIC,
    contract_quantity NUMERIC,
    unit_price NUMERIC,
    amount NUMERIC,
    start_date_line DATE,
    end_date_line_level DATE,
    account TEXT,
    shipping_address_city TEXT,
    shipping_address_state TEXT,
    shipping_address_country TEXT

);

-- +goose Down
DROP TABLE IF EXISTS ns_invoice_detail;
