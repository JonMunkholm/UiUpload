package schema

// NsCustomerFieldSpecs defines the expected CSV columns for NetSuite customer data.
var NsCustomerFieldSpecs = []FieldSpec{
	{Name: "salesforce_id_io", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "internal_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "duplicate", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "company_name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "balance", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "unbilled_orders", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "overdue_balance", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "days_overdue", Type: FieldNumeric, Required: false, AllowEmpty: true},
}

// NsSoDetailFieldSpecs defines the expected CSV columns for NetSuite SO detail data.
var NsSoDetailFieldSpecs = []FieldSpec{
	{Name: "sfdc_opp_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "sfdc_opp_line_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "customer_internal_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "product_internal_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "customer_project", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "so_number", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "document_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "start_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "end_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "item_name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "item_display_name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "line_start_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "line_end_date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "quantity", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "unit_price", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "amount_gross", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "terms_days_till_net_due", Type: FieldNumeric, Required: false, AllowEmpty: true},
}

// NsInvoiceDetailFieldSpecs defines the expected CSV columns for NetSuite invoice detail data.
var NsInvoiceDetailFieldSpecs = []FieldSpec{
	{Name: "sfdc_opp_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "sfdc_opp_line_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "sfdc_pricebook_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "customer_internal_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "product_internal_id", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "type", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "date_due", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "document_number", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "memo", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "item", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "qty", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "contract_quantity", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "unit_price", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "amount", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "start_date_line", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "end_date_line_level", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "account", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "shipping_address_city", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "shipping_address_state", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "shipping_address_country", Type: FieldText, Required: false, AllowEmpty: true},
}
