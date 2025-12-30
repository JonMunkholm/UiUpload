-- +goose Up
CREATE TABLE ns_so_detail (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    sfdc_opp_id TEXT,
    sfdc_opp_line_id TEXT,
    customer_internal_id TEXT,
    product_internal_id TEXT,
    customer_project TEXT NOT NULL,
    so_number TEXT,
    document_date DATE,
    start_date DATE,
    end_date DATE,
    item_name TEXT,
    item_display_name TEXT,
    line_start_date DATE,
    line_end_date DATE,
    quantity NUMERIC,
    unit_price NUMERIC,
    amount_gross NUMERIC,
    terms_days_till_net_due NUMERIC
);

-- +goose Down
DROP TABLE IF EXISTS ns_so_detail;
