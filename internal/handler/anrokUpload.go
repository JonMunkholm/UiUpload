package handler

import (
	"context"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/JonMunkholm/TUI/internal/schema"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AnrokUpload struct {
	BaseUploader
}

func NewAnrokUpload(pool *pgxpool.Pool) *AnrokUpload {
	return &AnrokUpload{
		BaseUploader: BaseUploader{Pool: pool},
	}
}

func (a *AnrokUpload) SetProps() error {
	return a.BaseUploader.SetProps("Anrok", a.makeDirMap)
}

/* ----------------------------------------
	Insert Actions
---------------------------------------- */

func (a *AnrokUpload) InsertAnrokTransactions() tea.Cmd {
	return a.RunUpload("Transactions")
}

/* ----------------------------------------
	Build Param functions
---------------------------------------- */

func (a *AnrokUpload) BuildAnrokTransactionParams(row []string, headerIdx HeaderIndex) (db.InsertAnrokTransactionParams, error) {
	vrow, err := validateRow(row, headerIdx, schema.AnrokFieldSpecs)
	if err != nil {
		return db.InsertAnrokTransactionParams{}, err
	}

	return db.InsertAnrokTransactionParams{
		TransactionID:             ToPgText(vrow["Transaction ID"]),
		CustomerID:                ToPgText(vrow["Customer ID"]),
		CustomerName:              ToPgText(vrow["Customer name"]),
		OverallVatIDStatus:        ToPgText(vrow["Overall VAT ID validation status"]),
		ValidVatIds:               ToPgText(vrow["Valid VAT IDs"]),
		OtherVatIds:               ToPgText(vrow["Other VAT IDs"]),
		InvoiceDate:               ToPgDate(vrow["Invoice date"]),
		TaxDate:                   ToPgDate(vrow["Tax date"]),
		TransactionCurrency:       ToPgText(vrow["Transaction currency"]),
		SalesAmount:               ToPgNumeric(vrow["Sales amount"]),
		ExemptReason:              ToPgText(vrow["Exempt reasons"]),
		TaxAmount:                 ToPgNumeric(vrow["Tax amount"]),
		InvoiceAmount:             ToPgNumeric(vrow["Invoice amount"]),
		Void:                      ToPgBool(vrow["Void"]),
		CustomerAddressLine1:      ToPgText(vrow["Customer address line 1"]),
		CustomerAddressCity:       ToPgText(vrow["Customer address city"]),
		CustomerAddressRegion:     ToPgText(vrow["Customer address region"]),
		CustomerAddressPostalCode: ToPgText(vrow["Customer address postal code"]),
		CustomerAddressCountry:    ToPgText(vrow["Customer address country"]),
		CustomerCountryCode:       ToPgText(vrow["Customer country code"]),
		Jurisdictions:             ToPgText(vrow["Jurisdictions"]),
		JurisdictionIds:           ToPgText(vrow["Jurisdictions IDs"]),
		ReturnIds:                 ToPgText(vrow["Return IDs"]),
	}, nil
}

/* ----------------------------------------
	Directory Map
---------------------------------------- */

func (a *AnrokUpload) makeDirMap() map[string]CsvProps {
	return map[string]CsvProps{
		"Transactions": CsvHandler[db.InsertAnrokTransactionParams]{
			specs:  schema.AnrokFieldSpecs,
			build:  a.BuildAnrokTransactionParams,
			insert: a.insertAnrokTransaction(),
		},
	}
}

/* ----------------------------------------
	Insert Wrapper
---------------------------------------- */

func (a *AnrokUpload) insertAnrokTransaction() InsertFn[db.InsertAnrokTransactionParams] {
	return func(ctx context.Context, queries *db.Queries, arg db.InsertAnrokTransactionParams) (bool, error) {
		err := queries.InsertAnrokTransaction(ctx, arg)
		return err == nil, err
	}
}
