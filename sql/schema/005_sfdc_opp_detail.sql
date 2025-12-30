-- +goose Up
CREATE TABLE sfdc_opp_detail (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    opportunity_id TEXT,
    opportunity_product_casesafe_id TEXT,
    opportunity_name TEXT,
    account_name TEXT,
    close_date DATE,
    booked_date DATE,
    fiscal_period TEXT,
    payment_schedule TEXT,
    payment_due TEXT,
    contract_start_date DATE,
    contract_end_date DATE,
    term_in_months_deprecated NUMERIC,
    product_name TEXT,
    deployment_type TEXT,
    amount NUMERIC,
    quantity NUMERIC,
    list_price NUMERIC,
    sales_price NUMERIC,
    total_price NUMERIC,
    start_date DATE,
    end_date DATE,
    term_in_months NUMERIC,
    product_code TEXT,
    total_amount_due_customer NUMERIC,
    total_amount_due_partner NUMERIC,
    active_product BOOLEAN
);

-- +goose Down
DROP TABLE IF EXISTS sfdc_opp_detail;
