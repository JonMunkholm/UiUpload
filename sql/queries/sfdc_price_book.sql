-- name: InsertSfdcPriceBook :exec
INSERT INTO sfdc_price_book (
    price_book_name,
    list_price,
    product_name,
    product_code,
    product_id_casesafe,
    upload_id
)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: DeleteSfdcPriceBookByUploadId :execrows
DELETE FROM sfdc_price_book WHERE upload_id = $1;

-- name: ResetSfdcPriceBook :exec
DELETE FROM sfdc_price_book;

-- name: CountSfdcPriceBook :one
SELECT COUNT(*) FROM sfdc_price_book;
