-- +goose Up
-- +goose NO TRANSACTION
-- Business key indexes for common lookup patterns
-- Using CONCURRENTLY to avoid locking production tables

-- SFDC Customers: primary business key for customer lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sfdc_customers_account_id
    ON sfdc_customers(account_id_casesafe);

-- SFDC Opportunity Detail: opportunity lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sfdc_opp_detail_opp_id
    ON sfdc_opp_detail(opportunity_id);

-- SFDC Opportunity Detail: product-level lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sfdc_opp_detail_product_id
    ON sfdc_opp_detail(opportunity_product_casesafe_id);

-- NetSuite Customers: Salesforce integration ID lookup
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ns_customers_sf_id
    ON ns_customers(salesforce_id_io);

-- NetSuite Customers: internal ID for NetSuite lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ns_customers_internal_id
    ON ns_customers(internal_id);

-- NetSuite Invoice Detail: document number lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ns_invoice_detail_doc_num
    ON ns_invoice_detail(document_number);

-- NetSuite Invoice Detail: SFDC opportunity line linkage
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ns_invoice_detail_sfdc_line
    ON ns_invoice_detail(sfdc_opp_line_id);

-- Anrok Transactions: transaction ID lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_anrok_transactions_tx_id
    ON anrok_transactions(transaction_id);

-- Anrok Transactions: customer ID lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_anrok_transactions_customer_id
    ON anrok_transactions(customer_id);

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS idx_anrok_transactions_customer_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_anrok_transactions_tx_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_ns_invoice_detail_sfdc_line;
DROP INDEX CONCURRENTLY IF EXISTS idx_ns_invoice_detail_doc_num;
DROP INDEX CONCURRENTLY IF EXISTS idx_ns_customers_internal_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_ns_customers_sf_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_sfdc_opp_detail_product_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_sfdc_opp_detail_opp_id;
DROP INDEX CONCURRENTLY IF EXISTS idx_sfdc_customers_account_id;
