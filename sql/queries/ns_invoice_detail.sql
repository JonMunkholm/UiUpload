-- name: InsertNsInvoiceDetail :exec
INSERT INTO ns_invoice_detail (
    sfdc_opp_id,
    sfdc_opp_line_id,
    sfdc_pricebook_id,
    customer_internal_id,
    product_internal_id,
    type,
    date,
    date_due,
    document_number,
    name,
    memo,
    item,
    qty,
    contract_quantity,
    unit_price,
    amount,
    start_date_line,
    end_date_line_level,
    account,
    shipping_address_city,
    shipping_address_state,
    shipping_address_country
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22);

-- name: ResetNsInvoiceDetail :exec
DELETE FROM ns_invoice_detail;
