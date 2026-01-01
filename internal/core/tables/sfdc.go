package tables

import (
	"context"
	"strings"

	"github.com/JonMunkholm/TUI/internal/core"
	db "github.com/JonMunkholm/TUI/internal/database"
)

func init() {
	registerSfdcCustomers()
	registerSfdcPriceBook()
	registerSfdcOppDetail()
}

func registerSfdcCustomers() {
	core.Register(core.TableDefinition{
		Info: core.TableInfo{
			Key:       "sfdc_customers",
			Group:     "SFDC",
			Label:     "Customers",
			Directory: "Customers",
		},
		FieldSpecs: []core.FieldSpec{
			{Name: "account_id_casesafe", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "account_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "last_activity", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "type", Type: core.FieldText, Required: false, AllowEmpty: true},
		},
		BuildParams: func(row []string, idx core.HeaderIndex) (any, error) {
			return db.InsertSfdcCustomerParams{
				AccountIDCasesafe: core.ToPgText(getCell(row, idx, "account_id_casesafe")),
				AccountName:       core.ToPgText(getCell(row, idx, "account_name")),
				LastActivity:      core.ToPgDate(getCell(row, idx, "last_activity")),
				Type:              core.ToPgText(getCell(row, idx, "type")),
			}, nil
		},
		Insert: func(ctx context.Context, dbtx core.DBTX, params any) error {
			return db.New(dbtx).InsertSfdcCustomer(ctx, params.(db.InsertSfdcCustomerParams))
		},
		Reset: func(ctx context.Context, dbtx core.DBTX) error {
			return db.New(dbtx).ResetSfdcCustomers(ctx)
		},
	})
}

func registerSfdcPriceBook() {
	core.Register(core.TableDefinition{
		Info: core.TableInfo{
			Key:       "sfdc_price_book",
			Group:     "SFDC",
			Label:     "Price Book",
			Directory: "PriceBook",
		},
		FieldSpecs: []core.FieldSpec{
			{Name: "price_book_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "list_price", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "product_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "product_code", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "product_id_casesafe", Type: core.FieldText, Required: false, AllowEmpty: true},
		},
		BuildParams: func(row []string, idx core.HeaderIndex) (any, error) {
			return db.InsertSfdcPriceBookParams{
				PriceBookName:     core.ToPgText(getCell(row, idx, "price_book_name")),
				ListPrice:         core.ToPgNumeric(getCell(row, idx, "list_price")),
				ProductName:       core.ToPgText(getCell(row, idx, "product_name")),
				ProductCode:       core.ToPgText(getCell(row, idx, "product_code")),
				ProductIDCasesafe: core.ToPgText(getCell(row, idx, "product_id_casesafe")),
			}, nil
		},
		Insert: func(ctx context.Context, dbtx core.DBTX, params any) error {
			return db.New(dbtx).InsertSfdcPriceBook(ctx, params.(db.InsertSfdcPriceBookParams))
		},
		Reset: func(ctx context.Context, dbtx core.DBTX) error {
			return db.New(dbtx).ResetSfdcPriceBook(ctx)
		},
	})
}

func registerSfdcOppDetail() {
	core.Register(core.TableDefinition{
		Info: core.TableInfo{
			Key:       "sfdc_opp_detail",
			Group:     "SFDC",
			Label:     "Opp Detail",
			Directory: "OppDetail",
		},
		FieldSpecs: []core.FieldSpec{
			{Name: "opportunity_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "opportunity_product_casesafe_id", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "opportunity_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "account_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "close_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "booked_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "fiscal_period", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "payment_schedule", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "payment_due", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "contract_start_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "contract_end_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "term_in_months_deprecated", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "product_name", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "deployment_type", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "amount", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "quantity", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "list_price", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "sales_price", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "total_price", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "start_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "end_date", Type: core.FieldDate, Required: false, AllowEmpty: true},
			{Name: "term_in_months", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "product_code", Type: core.FieldText, Required: false, AllowEmpty: true},
			{Name: "total_amount_due_customer", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "total_amount_due_partner", Type: core.FieldNumeric, Required: false, AllowEmpty: true},
			{Name: "active_product", Type: core.FieldBool, Required: false, AllowEmpty: true},
		},
		BuildParams: func(row []string, idx core.HeaderIndex) (any, error) {
			return db.InsertSfdcOppDetailParams{
				OpportunityID:                core.ToPgText(getCell(row, idx, "opportunity_id")),
				OpportunityProductCasesafeID: core.ToPgText(getCell(row, idx, "opportunity_product_casesafe_id")),
				OpportunityName:              core.ToPgText(getCell(row, idx, "opportunity_name")),
				AccountName:                  core.ToPgText(getCell(row, idx, "account_name")),
				CloseDate:                    core.ToPgDate(getCell(row, idx, "close_date")),
				BookedDate:                   core.ToPgDate(getCell(row, idx, "booked_date")),
				FiscalPeriod:                 core.ToPgText(getCell(row, idx, "fiscal_period")),
				PaymentSchedule:              core.ToPgText(getCell(row, idx, "payment_schedule")),
				PaymentDue:                   core.ToPgText(getCell(row, idx, "payment_due")),
				ContractStartDate:            core.ToPgDate(getCell(row, idx, "contract_start_date")),
				ContractEndDate:              core.ToPgDate(getCell(row, idx, "contract_end_date")),
				TermInMonthsDeprecated:       core.ToPgNumeric(getCell(row, idx, "term_in_months_deprecated")),
				ProductName:                  core.ToPgText(getCell(row, idx, "product_name")),
				DeploymentType:               core.ToPgText(getCell(row, idx, "deployment_type")),
				Amount:                       core.ToPgNumeric(getCell(row, idx, "amount")),
				Quantity:                     core.ToPgNumeric(getCell(row, idx, "quantity")),
				ListPrice:                    core.ToPgNumeric(getCell(row, idx, "list_price")),
				SalesPrice:                   core.ToPgNumeric(getCell(row, idx, "sales_price")),
				TotalPrice:                   core.ToPgNumeric(getCell(row, idx, "total_price")),
				StartDate:                    core.ToPgDate(getCell(row, idx, "start_date")),
				EndDate:                      core.ToPgDate(getCell(row, idx, "end_date")),
				TermInMonths:                 core.ToPgNumeric(getCell(row, idx, "term_in_months")),
				ProductCode:                  core.ToPgText(getCell(row, idx, "product_code")),
				TotalAmountDueCustomer:       core.ToPgNumeric(getCell(row, idx, "total_amount_due_customer")),
				TotalAmountDuePartner:        core.ToPgNumeric(getCell(row, idx, "total_amount_due_partner")),
				ActiveProduct:                core.ToPgBool(getCell(row, idx, "active_product")),
			}, nil
		},
		Insert: func(ctx context.Context, dbtx core.DBTX, params any) error {
			return db.New(dbtx).InsertSfdcOppDetail(ctx, params.(db.InsertSfdcOppDetailParams))
		},
		Reset: func(ctx context.Context, dbtx core.DBTX) error {
			return db.New(dbtx).ResetSfdcOppDetail(ctx)
		},
	})
}

// getCell safely retrieves a cell value from a row by header name.
func getCell(row []string, idx core.HeaderIndex, name string) string {
	pos, ok := idx[strings.ToLower(name)]
	if !ok || pos >= len(row) {
		return ""
	}
	return core.CleanCell(row[pos])
}
