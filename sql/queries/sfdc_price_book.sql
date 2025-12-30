-- name: InsertSfdcPriceBook :exec
INSERT INTO sfdc_price_book (
    price_book_name,
    list_price,
    product_name,
    product_code,
    product_id_casesafe
)
VALUES ($1, $2, $3, $4, $5);

-- name: ResetSfdcPriceBook :exec
DELETE FROM sfdc_price_book;
