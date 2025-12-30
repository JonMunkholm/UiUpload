package schema

// SfdcCustomerFieldSpecs defines the expected CSV columns for Salesforce customer data.
var SfdcCustomerFieldSpecs = []FieldSpec{
	{Name: "account_id_casesafe", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "account_name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "last_activity", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "type", Type: FieldText, Required: false, AllowEmpty: true},
}

// SfdcPriceBookFieldSpecs defines the expected CSV columns for Salesforce price book data.
var SfdcPriceBookFieldSpecs = []FieldSpec{
	{Name: "price_book_name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "list_price", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "product_name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "product_code", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "product_id_casesafe", Type: FieldText, Required: false, AllowEmpty: true},
}

// SfdcOppDetailFieldSpecs defines the expected CSV columns for Salesforce opportunity detail data.
var SfdcOppDetailFieldSpecs = []FieldSpec{
	{Name: "opportunity_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "opportunity_product_casesafe_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "opportunity_name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "account_name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "close_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "booked_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "fiscal_period", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "payment_schedule", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "payment_due", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "contract_start_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "contract_end_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "term_in_months_deprecated", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "product_name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "deployment_type", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "amount", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "quantity", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "list_price", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "sales_price", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "total_price", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "start_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "end_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "term_in_months", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "product_code", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "total_amount_due_customer", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "total_amount_due_partner", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "active_product", Type: FieldBool, Required: false, AllowEmpty: true},
}
