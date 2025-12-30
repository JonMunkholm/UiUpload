-- +goose Up
CREATE TABLE sfdc_price_book (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    price_book_name         TEXT,
    list_price              NUMERIC,
    product_name            TEXT,
    product_code            TEXT,
    product_id_casesafe     TEXT
);

-- +goose Down
DROP TABLE IF EXISTS sfdc_price_book;
