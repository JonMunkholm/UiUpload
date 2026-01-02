-- name: InsertSfdcCustomer :exec
INSERT INTO sfdc_customers (
    account_id_casesafe,
    account_name,
    last_activity,
    type,
    upload_id
)
VALUES ($1, $2, $3, $4, $5);

-- name: DeleteSfdcCustomersByUploadId :execrows
DELETE FROM sfdc_customers WHERE upload_id = $1;

-- name: ResetSfdcCustomers :exec
DELETE FROM sfdc_customers;

-- name: CountSfdcCustomers :one
SELECT COUNT(*) FROM sfdc_customers;
