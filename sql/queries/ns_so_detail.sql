-- name: InsertNsSoDetail :exec
INSERT INTO ns_so_detail (
    sfdc_opp_id,
    sfdc_opp_line_id,
    customer_internal_id,
    product_internal_id,
    customer_project,
    so_number,
    document_date,
    start_date,
    end_date,
    item_name,
    item_display_name,
    line_start_date,
    line_end_date,
    quantity,
    unit_price,
    amount_gross,
    terms_days_till_net_due
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17);

-- name: ResetNsSoDetail :exec
DELETE FROM ns_so_detail;

-- name: CountNsSoDetail :one
SELECT COUNT(*) FROM ns_so_detail;
