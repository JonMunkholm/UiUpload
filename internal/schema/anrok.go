package schema

// AnrokFieldSpecs defines the expected CSV columns for Anrok tax transaction reports.
var AnrokFieldSpecs = []FieldSpec{
	{Name: "Transaction ID", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Customer ID", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Customer name", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Overall VAT ID validation status", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Valid VAT IDs", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Other VAT IDs", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Invoice date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "Tax date", Type: FieldDate, Required: false, AllowEmpty: true},
	{Name: "Transaction currency", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Sales amount", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "Exempt reasons", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Tax amount", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "Invoice amount", Type: FieldNumeric, Required: false, AllowEmpty: true},
	{Name: "Void", Type: FieldBool, Required: false, AllowEmpty: true},
	{Name: "Customer address line 1", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Customer address city", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Customer address region", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Customer address postal code", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Customer address country", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Customer country code", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Jurisdictions", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Jurisdictions IDs", Type: FieldText, Required: false, AllowEmpty: true},
	{Name: "Return IDs", Type: FieldText, Required: false, AllowEmpty: true},
}
