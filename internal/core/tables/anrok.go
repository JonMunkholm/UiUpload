package tables

import (
	"context"

	"github.com/JonMunkholm/TUI/internal/core"
	db "github.com/JonMunkholm/TUI/internal/database"
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
		},
		FieldSpecs: []core.FieldSpec{
			{Name: "Transaction ID", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer ID", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Overall VAT ID validation status", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Valid VAT IDs", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Other VAT IDs", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Invoice date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "Tax date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "Transaction currency", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Sales amount", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "Exempt reasons", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Tax amount", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "Invoice amount", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "Void", Type: core.FieldBool, Required: false, AllowEmpty: true},
			{Name: "Customer address line 1", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer address city", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer address region", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer address postal code", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer address country", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Customer country code", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Jurisdictions", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Jurisdictions IDs", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "Return IDs", Type: core.FieldText, Required: false, AllowEmpty: true},
		},
		BuildParams: func(row []string, idx core.HeaderIndex) (any, error) {
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
			}, nil
		},
		Insert: func(ctx context.Context, dbtx core.DBTX, params any) error {
			return db.New(dbtx).InsertAnrokTransaction(ctx, params.(db.InsertAnrokTransactionParams))
		},
		Reset: func(ctx context.Context, dbtx core.DBTX) error {
			return db.New(dbtx).ResetAnrokTransactions(ctx)
		},
	})
}
