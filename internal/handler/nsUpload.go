package handler

import (
	"context"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/JonMunkholm/TUI/internal/schema"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/jackc/pgx/v5/pgxpool"
)

type NsUpload struct {
	BaseUploader
}

func NewNsUpload(pool *pgxpool.Pool) *NsUpload {
	return &NsUpload{
		BaseUploader: BaseUploader{Pool: pool},
	}
}

func (n *NsUpload) SetProps() error {
	return n.BaseUploader.SetProps("NS", n.makeDirMap)
}

/* ----------------------------------------
	Insert Actions
---------------------------------------- */

func (n *NsUpload) InsertNsCustomers() tea.Cmd {
	return n.RunUpload("Customers")
}

func (n *NsUpload) InsertNsSoDetail() tea.Cmd {
	return n.RunUpload("SoDetail")
}

func (n *NsUpload) InsertNsInvoiceDetail() tea.Cmd {
	return n.RunUpload("InvoiceDetail")
}

/* ----------------------------------------
	Build Param functions
---------------------------------------- */

func (n *NsUpload) BuildNsCustomerParams(row []string, headerIdx HeaderIndex) (db.InsertNsCustomerParams, error) {
	vrow, err := validateRow(row, headerIdx, schema.NsCustomerFieldSpecs)
	if err != nil {
		return db.InsertNsCustomerParams{}, err
	}

	return db.InsertNsCustomerParams{
		SalesforceIDIo: ToPgText(vrow["salesforce_id_io"]),
		InternalID:     ToPgText(vrow["internal_id"]),
		Name:           ToPgText(vrow["name"]),
		Duplicate:      ToPgText(vrow["duplicate"]),
		CompanyName:    ToPgText(vrow["company_name"]),
		Balance:        ToPgNumeric(vrow["balance"]),
		UnbilledOrders: ToPgNumeric(vrow["unbilled_orders"]),
		OverdueBalance: ToPgNumeric(vrow["overdue_balance"]),
		DaysOverdue:    ToPgNumeric(vrow["days_overdue"]),
	}, nil
}

func (n *NsUpload) BuildNsSoDetailParams(row []string, headerIdx HeaderIndex) (db.InsertNsSoDetailParams, error) {
	vrow, err := validateRow(row, headerIdx, schema.NsSoDetailFieldSpecs)
	if err != nil {
		return db.InsertNsSoDetailParams{}, err
	}

	return db.InsertNsSoDetailParams{
		SfdcOppID:           ToPgText(vrow["sfdc_opp_id"]),
		SfdcOppLineID:       ToPgText(vrow["sfdc_opp_line_id"]),
		CustomerInternalID:  ToPgText(vrow["customer_internal_id"]),
		ProductInternalID:   ToPgText(vrow["product_internal_id"]),
		CustomerProject:     vrow["customer_project"],
		SoNumber:            ToPgText(vrow["so_number"]),
		DocumentDate:        ToPgDate(vrow["document_date"]),
		StartDate:           ToPgDate(vrow["start_date"]),
		EndDate:             ToPgDate(vrow["end_date"]),
		ItemName:            ToPgText(vrow["item_name"]),
		ItemDisplayName:     ToPgText(vrow["item_display_name"]),
		LineStartDate:       ToPgDate(vrow["line_start_date"]),
		LineEndDate:         ToPgDate(vrow["line_end_date"]),
		Quantity:            ToPgNumeric(vrow["quantity"]),
		UnitPrice:           ToPgNumeric(vrow["unit_price"]),
		AmountGross:         ToPgNumeric(vrow["amount_gross"]),
		TermsDaysTillNetDue: ToPgNumeric(vrow["terms_days_till_net_due"]),
	}, nil
}

func (n *NsUpload) BuildNsInvoiceDetailParams(row []string, headerIdx HeaderIndex) (db.InsertNsInvoiceDetailParams, error) {
	vrow, err := validateRow(row, headerIdx, schema.NsInvoiceDetailFieldSpecs)
	if err != nil {
		return db.InsertNsInvoiceDetailParams{}, err
	}

	return db.InsertNsInvoiceDetailParams{
		SfdcOppID:              ToPgText(vrow["sfdc_opp_id"]),
		SfdcOppLineID:          ToPgText(vrow["sfdc_opp_line_id"]),
		SfdcPricebookID:        ToPgText(vrow["sfdc_pricebook_id"]),
		CustomerInternalID:     ToPgText(vrow["customer_internal_id"]),
		ProductInternalID:      ToPgText(vrow["product_internal_id"]),
		Type:                   ToPgText(vrow["type"]),
		Date:                   ToPgDate(vrow["date"]),
		DateDue:                ToPgDate(vrow["date_due"]),
		DocumentNumber:         ToPgText(vrow["document_number"]),
		Name:                   ToPgText(vrow["name"]),
		Memo:                   ToPgText(vrow["memo"]),
		Item:                   ToPgText(vrow["item"]),
		Qty:                    ToPgNumeric(vrow["qty"]),
		ContractQuantity:       ToPgNumeric(vrow["contract_quantity"]),
		UnitPrice:              ToPgNumeric(vrow["unit_price"]),
		Amount:                 ToPgNumeric(vrow["amount"]),
		StartDateLine:          ToPgDate(vrow["start_date_line"]),
		EndDateLineLevel:       ToPgDate(vrow["end_date_line_level"]),
		Account:                ToPgText(vrow["account"]),
		ShippingAddressCity:    ToPgText(vrow["shipping_address_city"]),
		ShippingAddressState:   ToPgText(vrow["shipping_address_state"]),
		ShippingAddressCountry: ToPgText(vrow["shipping_address_country"]),
	}, nil
}

/* ----------------------------------------
	Directory Map
---------------------------------------- */

func (n *NsUpload) makeDirMap() map[string]CsvProps {
	return map[string]CsvProps{
		"Customers": CsvHandler[db.InsertNsCustomerParams]{
			specs:  schema.NsCustomerFieldSpecs,
			build:  n.BuildNsCustomerParams,
			insert: n.insertNsCustomer(),
		},
		"SoDetail": CsvHandler[db.InsertNsSoDetailParams]{
			specs:  schema.NsSoDetailFieldSpecs,
			build:  n.BuildNsSoDetailParams,
			insert: n.insertNsSoDetail(),
		},
		"InvoiceDetail": CsvHandler[db.InsertNsInvoiceDetailParams]{
			specs:  schema.NsInvoiceDetailFieldSpecs,
			build:  n.BuildNsInvoiceDetailParams,
			insert: n.insertNsInvoiceDetail(),
		},
	}
}

/* ----------------------------------------
	Insert Wrappers
---------------------------------------- */

func (n *NsUpload) insertNsCustomer() InsertFn[db.InsertNsCustomerParams] {
	return func(ctx context.Context, queries *db.Queries, arg db.InsertNsCustomerParams) (bool, error) {
		err := queries.InsertNsCustomer(ctx, arg)
		return err == nil, err
	}
}

func (n *NsUpload) insertNsSoDetail() InsertFn[db.InsertNsSoDetailParams] {
	return func(ctx context.Context, queries *db.Queries, arg db.InsertNsSoDetailParams) (bool, error) {
		err := queries.InsertNsSoDetail(ctx, arg)
		return err == nil, err
	}
}

func (n *NsUpload) insertNsInvoiceDetail() InsertFn[db.InsertNsInvoiceDetailParams] {
	return func(ctx context.Context, queries *db.Queries, arg db.InsertNsInvoiceDetailParams) (bool, error) {
		err := queries.InsertNsInvoiceDetail(ctx, arg)
		return err == nil, err
	}
}
