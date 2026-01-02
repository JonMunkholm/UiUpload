-- name: InsertSfdcOppDetail :exec
INSERT INTO sfdc_opp_detail (
    opportunity_id,
    opportunity_product_casesafe_id,
    opportunity_name,
    account_name,
    close_date,
    booked_date,
    fiscal_period,
    payment_schedule,
    payment_due,
    contract_start_date,
    contract_end_date,
    term_in_months_deprecated,
    product_name,
    deployment_type,
    amount,
    quantity,
    list_price,
    sales_price,
    total_price,
    start_date,
    end_date,
    term_in_months,
    product_code,
    total_amount_due_customer,
    total_amount_due_partner,
    active_product,
    upload_id
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27);

-- name: DeleteSfdcOppDetailByUploadId :execrows
DELETE FROM sfdc_opp_detail WHERE upload_id = $1;

-- name: ResetSfdcOppDetail :exec
DELETE FROM sfdc_opp_detail;

-- name: CountSfdcOppDetail :one
SELECT COUNT(*) FROM sfdc_opp_detail;
