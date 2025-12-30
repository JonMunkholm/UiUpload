package handler

import (
	"context"

	db "github.com/JonMunkholm/TUI/internal/database"
	"github.com/JonMunkholm/TUI/internal/schema"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SfdcUpload struct {
	BaseUploader
}

func NewSfdcUpload(pool *pgxpool.Pool) *SfdcUpload {
	return &SfdcUpload{
		BaseUploader: BaseUploader{Pool: pool},
	}
}

func (s *SfdcUpload) SetProps() error {
	return s.BaseUploader.SetProps("SFDC", s.makeDirMap)
}

/* ----------------------------------------
	Insert Actions
---------------------------------------- */

func (s *SfdcUpload) InsertSfdcCustomers() tea.Cmd {
	return s.RunUpload("Customers")
}

func (s *SfdcUpload) InsertSfdcPriceBook() tea.Cmd {
	return s.RunUpload("PriceBook")
}

func (s *SfdcUpload) InsertSfdcOppDetail() tea.Cmd {
	return s.RunUpload("OppDetail")
}

/* ----------------------------------------
	Build Param functions
---------------------------------------- */

func (s *SfdcUpload) BuildSfdcCustomerParams(row []string, headerIdx HeaderIndex) (db.InsertSfdcCustomerParams, error) {
	vrow, err := validateRow(row, headerIdx, schema.SfdcCustomerFieldSpecs)
	if err != nil {
		return db.InsertSfdcCustomerParams{}, err
	}

	return db.InsertSfdcCustomerParams{
		AccountIDCasesafe: ToPgText(vrow["account_id_casesafe"]),
		AccountName:       ToPgText(vrow["account_name"]),
		LastActivity:      ToPgDate(vrow["last_activity"]),
		Type:              ToPgText(vrow["type"]),
	}, nil
}

func (s *SfdcUpload) BuildSfdcPriceBookParams(row []string, headerIdx HeaderIndex) (db.InsertSfdcPriceBookParams, error) {
	vrow, err := validateRow(row, headerIdx, schema.SfdcPriceBookFieldSpecs)
	if err != nil {
		return db.InsertSfdcPriceBookParams{}, err
	}

	return db.InsertSfdcPriceBookParams{
		PriceBookName:     ToPgText(vrow["price_book_name"]),
		ListPrice:         ToPgNumeric(vrow["list_price"]),
		ProductName:       ToPgText(vrow["product_name"]),
		ProductCode:       ToPgText(vrow["product_code"]),
		ProductIDCasesafe: ToPgText(vrow["product_id_casesafe"]),
	}, nil
}

func (s *SfdcUpload) BuildSfdcOppDetailParams(row []string, headerIdx HeaderIndex) (db.InsertSfdcOppDetailParams, error) {
	vrow, err := validateRow(row, headerIdx, schema.SfdcOppDetailFieldSpecs)
	if err != nil {
		return db.InsertSfdcOppDetailParams{}, err
	}

	return db.InsertSfdcOppDetailParams{
		OpportunityID:                ToPgText(vrow["opportunity_id"]),
		OpportunityProductCasesafeID: ToPgText(vrow["opportunity_product_casesafe_id"]),
		OpportunityName:              ToPgText(vrow["opportunity_name"]),
		AccountName:                  ToPgText(vrow["account_name"]),
		CloseDate:                    ToPgDate(vrow["close_date"]),
		BookedDate:                   ToPgDate(vrow["booked_date"]),
		FiscalPeriod:                 ToPgText(vrow["fiscal_period"]),
		PaymentSchedule:              ToPgText(vrow["payment_schedule"]),
		PaymentDue:                   ToPgText(vrow["payment_due"]),
		ContractStartDate:            ToPgDate(vrow["contract_start_date"]),
		ContractEndDate:              ToPgDate(vrow["contract_end_date"]),
		TermInMonthsDeprecated:       ToPgNumeric(vrow["term_in_months_deprecated"]),
		ProductName:                  ToPgText(vrow["product_name"]),
		DeploymentType:               ToPgText(vrow["deployment_type"]),
		Amount:                       ToPgNumeric(vrow["amount"]),
		Quantity:                     ToPgNumeric(vrow["quantity"]),
		ListPrice:                    ToPgNumeric(vrow["list_price"]),
		SalesPrice:                   ToPgNumeric(vrow["sales_price"]),
		TotalPrice:                   ToPgNumeric(vrow["total_price"]),
		StartDate:                    ToPgDate(vrow["start_date"]),
		EndDate:                      ToPgDate(vrow["end_date"]),
		TermInMonths:                 ToPgNumeric(vrow["term_in_months"]),
		ProductCode:                  ToPgText(vrow["product_code"]),
		TotalAmountDueCustomer:       ToPgNumeric(vrow["total_amount_due_customer"]),
		TotalAmountDuePartner:        ToPgNumeric(vrow["total_amount_due_partner"]),
		ActiveProduct:                ToPgBool(vrow["active_product"]),
	}, nil
}

/* ----------------------------------------
	Directory Map
---------------------------------------- */

func (s *SfdcUpload) makeDirMap() map[string]CsvProps {
	return map[string]CsvProps{
		"Customers": CsvHandler[db.InsertSfdcCustomerParams]{
			specs:  schema.SfdcCustomerFieldSpecs,
			build:  s.BuildSfdcCustomerParams,
			insert: s.insertSfdcCustomer(),
		},
		"PriceBook": CsvHandler[db.InsertSfdcPriceBookParams]{
			specs:  schema.SfdcPriceBookFieldSpecs,
			build:  s.BuildSfdcPriceBookParams,
			insert: s.insertSfdcPriceBook(),
		},
		"OppDetail": CsvHandler[db.InsertSfdcOppDetailParams]{
			specs:  schema.SfdcOppDetailFieldSpecs,
			build:  s.BuildSfdcOppDetailParams,
			insert: s.insertSfdcOppDetail(),
		},
	}
}

/* ----------------------------------------
	Insert Wrappers
---------------------------------------- */

func (s *SfdcUpload) insertSfdcCustomer() InsertFn[db.InsertSfdcCustomerParams] {
	return func(ctx context.Context, queries *db.Queries, arg db.InsertSfdcCustomerParams) (bool, error) {
		err := queries.InsertSfdcCustomer(ctx, arg)
		return err == nil, err
	}
}

func (s *SfdcUpload) insertSfdcPriceBook() InsertFn[db.InsertSfdcPriceBookParams] {
	return func(ctx context.Context, queries *db.Queries, arg db.InsertSfdcPriceBookParams) (bool, error) {
		err := queries.InsertSfdcPriceBook(ctx, arg)
		return err == nil, err
	}
}

func (s *SfdcUpload) insertSfdcOppDetail() InsertFn[db.InsertSfdcOppDetailParams] {
	return func(ctx context.Context, queries *db.Queries, arg db.InsertSfdcOppDetailParams) (bool, error) {
		err := queries.InsertSfdcOppDetail(ctx, arg)
		return err == nil, err
	}
}
