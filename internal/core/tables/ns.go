package tables

import (
	"context"

	"github.com/JonMunkholm/TUI/internal/core"
	db "github.com/JonMunkholm/TUI/internal/database"
)

func init() {
	registerNsCustomers()
	registerNsSoDetail()
	registerNsInvoiceDetail()
}

func registerNsCustomers() {
	core.Register(core.TableDefinition{
		Info: core.TableInfo{
			Key:       "ns_customers",
			Group:     "NS",
			Label:     "Customers",
			Directory: "Customers",
			UniqueKey: []string{"internal_id"},
		},
		FieldSpecs: []core.FieldSpec{
			{Name: "salesforce_id_io", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "internal_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "duplicate", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "company_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "balance", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "unbilled_orders", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "overdue_balance", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "days_overdue", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
		},
		BuildParams: func(row []string, idx core.HeaderIndex) (any, error) {
			return db.InsertNsCustomerParams{
				SalesforceIDIo: core.ToPgText(getCell(row, idx, "salesforce_id_io")),
				InternalID:     core.ToPgText(getCell(row, idx, "internal_id")),
				Name:           core.ToPgText(getCell(row, idx, "name")),
				Duplicate:      core.ToPgText(getCell(row, idx, "duplicate")),
				CompanyName:    core.ToPgText(getCell(row, idx, "company_name")),
				Balance:        core.ToPgNumeric(getCell(row, idx, "balance")),
				UnbilledOrders: core.ToPgNumeric(getCell(row, idx, "unbilled_orders")),
				OverdueBalance: core.ToPgNumeric(getCell(row, idx, "overdue_balance")),
				DaysOverdue:    core.ToPgNumeric(getCell(row, idx, "days_overdue")),
			}, nil
		},
		Insert: func(ctx context.Context, dbtx core.DBTX, params any) error {
			return db.New(dbtx).InsertNsCustomer(ctx, params.(db.InsertNsCustomerParams))
		},
		Reset: func(ctx context.Context, dbtx core.DBTX) error {
			return db.New(dbtx).ResetNsCustomers(ctx)
		},
	})
}

func registerNsSoDetail() {
	core.Register(core.TableDefinition{
		Info: core.TableInfo{
			Key:       "ns_so_detail",
			Group:     "NS",
			Label:     "SO Detail",
			Directory: "SoDetail",
			UniqueKey: []string{"sfdc_opp_id", "sfdc_opp_line_id"},
		},
		FieldSpecs: []core.FieldSpec{
			{Name: "sfdc_opp_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "sfdc_opp_line_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "customer_internal_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "product_internal_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "customer_project", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "so_number", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "document_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "start_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "end_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "item_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "item_display_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "line_start_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "line_end_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "quantity", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "unit_price", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "amount_gross", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "terms_days_till_net_due", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
		},
		BuildParams: func(row []string, idx core.HeaderIndex) (any, error) {
			return db.InsertNsSoDetailParams{
				SfdcOppID:           core.ToPgText(getCell(row, idx, "sfdc_opp_id")),
				SfdcOppLineID:       core.ToPgText(getCell(row, idx, "sfdc_opp_line_id")),
				CustomerInternalID:  core.ToPgText(getCell(row, idx, "customer_internal_id")),
				ProductInternalID:   core.ToPgText(getCell(row, idx, "product_internal_id")),
				CustomerProject:     getCell(row, idx, "customer_project"), // Plain string, not pgtype.Text
				SoNumber:            core.ToPgText(getCell(row, idx, "so_number")),
				DocumentDate:        core.ToPgDate(getCell(row, idx, "document_date")),
				StartDate:           core.ToPgDate(getCell(row, idx, "start_date")),
				EndDate:             core.ToPgDate(getCell(row, idx, "end_date")),
				ItemName:            core.ToPgText(getCell(row, idx, "item_name")),
				ItemDisplayName:     core.ToPgText(getCell(row, idx, "item_display_name")),
				LineStartDate:       core.ToPgDate(getCell(row, idx, "line_start_date")),
				LineEndDate:         core.ToPgDate(getCell(row, idx, "line_end_date")),
				Quantity:            core.ToPgNumeric(getCell(row, idx, "quantity")),
				UnitPrice:           core.ToPgNumeric(getCell(row, idx, "unit_price")),
				AmountGross:         core.ToPgNumeric(getCell(row, idx, "amount_gross")),
				TermsDaysTillNetDue: core.ToPgNumeric(getCell(row, idx, "terms_days_till_net_due")),
			}, nil
		},
		Insert: func(ctx context.Context, dbtx core.DBTX, params any) error {
			return db.New(dbtx).InsertNsSoDetail(ctx, params.(db.InsertNsSoDetailParams))
		},
		Reset: func(ctx context.Context, dbtx core.DBTX) error {
			return db.New(dbtx).ResetNsSoDetail(ctx)
		},
	})
}

func registerNsInvoiceDetail() {
	core.Register(core.TableDefinition{
		Info: core.TableInfo{
			Key:       "ns_invoice_detail",
			Group:     "NS",
			Label:     "Invoice Detail",
			Directory: "InvoiceDetail",
			UniqueKey: []string{"sfdc_opp_id", "sfdc_opp_line_id"},
		},
		FieldSpecs: []core.FieldSpec{
			{Name: "sfdc_opp_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "sfdc_opp_line_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "sfdc_pricebook_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "customer_internal_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "product_internal_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "type", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "date_due", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "document_number", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "memo", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "item", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "qty", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "contract_quantity", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "unit_price", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "amount", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "start_date_line", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "end_date_line_level", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "account", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "shipping_address_city", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "shipping_address_state", Type: core.FieldText, Required: false, AllowEmpty: true, Normalizer: NormalizeUsState},
			{Name: "shipping_address_country", Type: core.FieldText, Required: false, AllowEmpty: true},
		},
		BuildParams: func(row []string, idx core.HeaderIndex) (any, error) {
			return db.InsertNsInvoiceDetailParams{
				SfdcOppID:              core.ToPgText(getCell(row, idx, "sfdc_opp_id")),
				SfdcOppLineID:          core.ToPgText(getCell(row, idx, "sfdc_opp_line_id")),
				SfdcPricebookID:        core.ToPgText(getCell(row, idx, "sfdc_pricebook_id")),
				CustomerInternalID:     core.ToPgText(getCell(row, idx, "customer_internal_id")),
				ProductInternalID:      core.ToPgText(getCell(row, idx, "product_internal_id")),
				Type:                   core.ToPgText(getCell(row, idx, "type")),
				Date:                   core.ToPgDate(getCell(row, idx, "date")),
				DateDue:                core.ToPgDate(getCell(row, idx, "date_due")),
				DocumentNumber:         core.ToPgText(getCell(row, idx, "document_number")),
				Name:                   core.ToPgText(getCell(row, idx, "name")),
				Memo:                   core.ToPgText(getCell(row, idx, "memo")),
				Item:                   core.ToPgText(getCell(row, idx, "item")),
				Qty:                    core.ToPgNumeric(getCell(row, idx, "qty")),
				ContractQuantity:       core.ToPgNumeric(getCell(row, idx, "contract_quantity")),
				UnitPrice:              core.ToPgNumeric(getCell(row, idx, "unit_price")),
				Amount:                 core.ToPgNumeric(getCell(row, idx, "amount")),
				StartDateLine:          core.ToPgDate(getCell(row, idx, "start_date_line")),
				EndDateLineLevel:       core.ToPgDate(getCell(row, idx, "end_date_line_level")),
				Account:                core.ToPgText(getCell(row, idx, "account")),
				ShippingAddressCity:    core.ToPgText(getCell(row, idx, "shipping_address_city")),
				ShippingAddressState:   core.ToPgText(NormalizeUsState(getCell(row, idx, "shipping_address_state"))),
				ShippingAddressCountry: core.ToPgText(getCell(row, idx, "shipping_address_country")),
			}, nil
		},
		Insert: func(ctx context.Context, dbtx core.DBTX, params any) error {
			return db.New(dbtx).InsertNsInvoiceDetail(ctx, params.(db.InsertNsInvoiceDetailParams))
		},
		Reset: func(ctx context.Context, dbtx core.DBTX) error {
			return db.New(dbtx).ResetNsInvoiceDetail(ctx)
		},
	})
}

