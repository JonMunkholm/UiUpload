package tables

import (
	"context"

	"github.com/JonMunkholm/TUI/internal/core"
	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/jackc/pgx/v5/pgtype"
)

func init() {
	registerAnrokTransactions()
}

func registerAnrokTransactions() {
	core.Register(core.TableDefinition{
		Info: core.TableInfo{
			Key:       "anrok_transactions",
			Group:     "Anrok",
			Label:     "Transactions",
			Directory: "Transactions",
			UniqueKey: []string{"Transaction ID"},
		},
		FieldSpecs: []core.FieldSpec{
			{Name: "Transaction ID", DBColumn: "transaction_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer ID", DBColumn: "customer_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer name", DBColumn: "customer_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Overall VAT ID validation status", DBColumn: "overall_vat_id_status", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Valid VAT IDs", DBColumn: "valid_vat_ids", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Other VAT IDs", DBColumn: "other_vat_ids", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Invoice date", DBColumn: "invoice_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "Tax date", DBColumn: "tax_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "Transaction currency", DBColumn: "transaction_currency", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Sales amount", DBColumn: "sales_amount", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "Exempt reasons", DBColumn: "exempt_reason", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Tax amount", DBColumn: "tax_amount", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "Invoice amount", DBColumn: "invoice_amount", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "Void", DBColumn: "void", Type: core.FieldBool, Required: false, AllowEmpty: true},
			{Name: "Customer address line 1", DBColumn: "customer_address_line_1", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer address city", DBColumn: "customer_address_city", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer address region", DBColumn: "customer_address_region", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer address postal code", DBColumn: "customer_address_postal_code", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer address country", DBColumn: "customer_address_country", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer country code", DBColumn: "customer_country_code", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Jurisdictions", DBColumn: "jurisdictions", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Jurisdictions IDs", DBColumn: "jurisdiction_ids", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Return IDs", DBColumn: "return_ids", Type: core.FieldText, Required: false, AllowEmpty: true},
		},
		BuildParams: func(row []string, idx core.HeaderIndex, uploadID pgtype.UUID) (any, error) {
			return db.InsertAnrokTransactionParams{
				TransactionID:             core.ToPgText(getCell(row, idx, "Transaction ID")),
				CustomerID:                core.ToPgText(getCell(row, idx, "Customer ID")),
				CustomerName:              core.ToPgText(getCell(row, idx, "Customer name")),
				OverallVatIDStatus:        core.ToPgText(getCell(row, idx, "Overall VAT ID validation status")),
				ValidVatIds:               core.ToPgText(getCell(row, idx, "Valid VAT IDs")),
				OtherVatIds:               core.ToPgText(getCell(row, idx, "Other VAT IDs")),
				InvoiceDate:               core.ToPgDate(getCell(row, idx, "Invoice date")),
				TaxDate:                   core.ToPgDate(getCell(row, idx, "Tax date")),
				TransactionCurrency:       core.ToPgText(getCell(row, idx, "Transaction currency")),
				SalesAmount:               core.ToPgNumeric(getCell(row, idx, "Sales amount")),
				ExemptReason:              core.ToPgText(getCell(row, idx, "Exempt reasons")),
				TaxAmount:                 core.ToPgNumeric(getCell(row, idx, "Tax amount")),
				InvoiceAmount:             core.ToPgNumeric(getCell(row, idx, "Invoice amount")),
				Void:                      core.ToPgBool(getCell(row, idx, "Void")),
				CustomerAddressLine1:      core.ToPgText(getCell(row, idx, "Customer address line 1")),
				CustomerAddressCity:       core.ToPgText(getCell(row, idx, "Customer address city")),
				CustomerAddressRegion:     core.ToPgText(getCell(row, idx, "Customer address region")),
				CustomerAddressPostalCode: core.ToPgText(getCell(row, idx, "Customer address postal code")),
				CustomerAddressCountry:    core.ToPgText(getCell(row, idx, "Customer address country")),
				CustomerCountryCode:       core.ToPgText(getCell(row, idx, "Customer country code")),
				Jurisdictions:             core.ToPgText(getCell(row, idx, "Jurisdictions")),
				JurisdictionIds:           core.ToPgText(getCell(row, idx, "Jurisdictions IDs")),
				ReturnIds:                 core.ToPgText(getCell(row, idx, "Return IDs")),
				UploadID:                  uploadID,
			}, nil
		},
		Insert: func(ctx context.Context, dbtx core.DBTX, params any) error {
			return db.New(dbtx).InsertAnrokTransaction(ctx, params.(db.InsertAnrokTransactionParams))
		},
		Reset: func(ctx context.Context, dbtx core.DBTX) error {
			return db.New(dbtx).ResetAnrokTransactions(ctx)
		},
		DeleteByUploadID: func(ctx context.Context, dbtx core.DBTX, uploadID pgtype.UUID) (int64, error) {
			return db.New(dbtx).DeleteAnrokTransactionsByUploadId(ctx, uploadID)
		},
	})
}
