-- name: InsertNsCustomer :exec
INSERT INTO ns_customers (
    salesforce_id_io,
    internal_id,
    name,
    duplicate,
    company_name,
    balance,
    unbilled_orders,
    overdue_balance,
    days_overdue
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: ResetNsCustomers :exec
DELETE FROM ns_customers;
