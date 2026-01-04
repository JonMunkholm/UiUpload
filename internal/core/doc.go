// Package core provides the business logic for CSV import operations.
//
// This package is the heart of the CSV importer, containing all domain logic
// independent of any UI or transport layer. It can be used by web handlers,
// CLI tools, or tests without modification.
//
// # Architecture
//
// The package is organized around several key concepts:
//
//   - Table Definitions: Registered via the registry, each table has field specs,
//     validation rules, and database operations.
//   - Service: The main entry point for all operations (upload, query, reset).
//   - Streaming: Memory-efficient processing for large CSV files.
//   - Audit: Comprehensive logging of all data modifications.
//
// # Table Registry
//
// Tables are registered at init time using [Register]. Each [TableDefinition]
// contains everything needed to process a specific CSV type:
//
//	core.Register(TableDefinition{
//	    Info: TableInfo{Key: "customers", Group: "SFDC", Label: "Customers"},
//	    FieldSpecs: []FieldSpec{
//	        {Name: "ID", Required: true, Type: FieldText},
//	        {Name: "Amount", Type: FieldNumeric},
//	    },
//	    BuildParams: buildCustomerParams,
//	    Insert: insertCustomer,
//	})
//
// # Streaming Upload
//
// Uploads process data in a streaming fashion with O(batch_size) memory usage,
// regardless of file size. The flow is:
//
//  1. Client calls [Service.StartUploadStreaming] with an io.Reader
//  2. Service wraps reader with BOM skipping and UTF-8 sanitization
//  3. Rows are validated and inserted in batches of [Config.Upload.BatchSize]
//  4. Progress is broadcast to subscribers via [Service.SubscribeProgress]
//
// # Error Handling
//
// Technical errors are mapped to user-friendly messages using [MapError].
// Each error category has a unique code for support reference:
//
//   - DB001-DB007: Database errors (duplicates, constraints, connections)
//   - VAL001-VAL006: Validation errors (formats, missing columns)
//   - FILE001-FILE005: File errors (size, encoding, format)
//   - UPL001-UPL005: Upload errors (cancelled, timeout, not found)
//
// # Audit Logging
//
// All data modifications are recorded in the audit log with severity levels:
//
//   - Low: Template changes
//   - Medium: Cell edits
//   - High: Uploads, bulk edits, row deletions
//   - Critical: Table resets
//
// Old audit entries are automatically archived to cold storage based on the
// configured retention policy.
package core
