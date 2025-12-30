-- name: InsertSfdcCustomer :exec
INSERT INTO sfdc_customers (
    account_id_casesafe,
    account_name,
    last_activity,
    type
)
VALUES ($1, $2, $3, $4);

-- name: ResetSfdcCustomers :exec
DELETE FROM sfdc_customers;
