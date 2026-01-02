-- +goose Up
-- Add upload_id to all data tables to link rows to their source upload
-- This enables rollback functionality (delete all rows from a specific upload)

-- First, make csv_uploads.id a proper primary key so it can be referenced
ALTER TABLE csv_uploads ADD CONSTRAINT csv_uploads_pkey PRIMARY KEY (id);

-- Add upload_id column to all 7 data tables
ALTER TABLE sfdc_customers ADD COLUMN upload_id UUID REFERENCES csv_uploads(id) ON DELETE SET NULL;
ALTER TABLE sfdc_price_book ADD COLUMN upload_id UUID REFERENCES csv_uploads(id) ON DELETE SET NULL;
ALTER TABLE sfdc_opp_detail ADD COLUMN upload_id UUID REFERENCES csv_uploads(id) ON DELETE SET NULL;
ALTER TABLE ns_customers ADD COLUMN upload_id UUID REFERENCES csv_uploads(id) ON DELETE SET NULL;
ALTER TABLE ns_invoice_detail ADD COLUMN upload_id UUID REFERENCES csv_uploads(id) ON DELETE SET NULL;
ALTER TABLE ns_so_detail ADD COLUMN upload_id UUID REFERENCES csv_uploads(id) ON DELETE SET NULL;
ALTER TABLE anrok_transactions ADD COLUMN upload_id UUID REFERENCES csv_uploads(id) ON DELETE SET NULL;

-- Partial indexes for efficient rollback queries (only index non-null values)
CREATE INDEX idx_sfdc_customers_upload_id ON sfdc_customers(upload_id) WHERE upload_id IS NOT NULL;
CREATE INDEX idx_sfdc_price_book_upload_id ON sfdc_price_book(upload_id) WHERE upload_id IS NOT NULL;
CREATE INDEX idx_sfdc_opp_detail_upload_id ON sfdc_opp_detail(upload_id) WHERE upload_id IS NOT NULL;
CREATE INDEX idx_ns_customers_upload_id ON ns_customers(upload_id) WHERE upload_id IS NOT NULL;
CREATE INDEX idx_ns_invoice_detail_upload_id ON ns_invoice_detail(upload_id) WHERE upload_id IS NOT NULL;
CREATE INDEX idx_ns_so_detail_upload_id ON ns_so_detail(upload_id) WHERE upload_id IS NOT NULL;
CREATE INDEX idx_anrok_transactions_upload_id ON anrok_transactions(upload_id) WHERE upload_id IS NOT NULL;

-- Track rollback status on uploads
ALTER TABLE csv_uploads ADD COLUMN status TEXT DEFAULT 'active' CHECK (status IN ('active', 'rolled_back'));

-- +goose Down
ALTER TABLE csv_uploads DROP COLUMN IF EXISTS status;

DROP INDEX IF EXISTS idx_anrok_transactions_upload_id;
DROP INDEX IF EXISTS idx_ns_so_detail_upload_id;
DROP INDEX IF EXISTS idx_ns_invoice_detail_upload_id;
DROP INDEX IF EXISTS idx_ns_customers_upload_id;
DROP INDEX IF EXISTS idx_sfdc_opp_detail_upload_id;
DROP INDEX IF EXISTS idx_sfdc_price_book_upload_id;
DROP INDEX IF EXISTS idx_sfdc_customers_upload_id;

ALTER TABLE anrok_transactions DROP COLUMN IF EXISTS upload_id;
ALTER TABLE ns_so_detail DROP COLUMN IF EXISTS upload_id;
ALTER TABLE ns_invoice_detail DROP COLUMN IF EXISTS upload_id;
ALTER TABLE ns_customers DROP COLUMN IF EXISTS upload_id;
ALTER TABLE sfdc_opp_detail DROP COLUMN IF EXISTS upload_id;
ALTER TABLE sfdc_price_book DROP COLUMN IF EXISTS upload_id;
ALTER TABLE sfdc_customers DROP COLUMN IF EXISTS upload_id;

ALTER TABLE csv_uploads DROP CONSTRAINT IF EXISTS csv_uploads_pkey;
