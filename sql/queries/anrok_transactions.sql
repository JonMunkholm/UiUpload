-- name: InsertAnrokTransaction :exec
INSERT INTO anrok_transactions (
    transaction_id,
    customer_id,
    customer_name,
    overall_vat_id_status,
    valid_vat_ids,
    other_vat_ids,
    invoice_date,
    tax_date,
    transaction_currency,
    sales_amount,
    exempt_reason,
    tax_amount,
    invoice_amount,
    void,
    customer_address_line_1,
    customer_address_city,
    customer_address_region,
    customer_address_postal_code,
    customer_address_country,
    customer_country_code,
    jurisdictions,
    jurisdiction_ids,
    return_ids
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23);

-- name: ResetAnrokTransactions :exec
DELETE FROM anrok_transactions;

-- name: CountAnrokTransactions :one
SELECT COUNT(*) FROM anrok_transactions;
