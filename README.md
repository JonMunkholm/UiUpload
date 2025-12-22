# CSV Importer TUI

A terminal UI application for importing CSV files into PostgreSQL databases. Built with the [Charm](https://charm.sh/) stack.

## Features

- Interactive terminal interface for selecting and importing CSV files
- Support for multiple data sources:
  - **NS** (NetSuite): SO Line Items, Invoice Sales Tax Items
  - **SFDC** (Salesforce): Opportunity Line Items
  - **Anrok**: Tax Transactions
- Robust CSV parsing:
  - Handles Excel formula prefixes (`="..."`)
  - Tolerates bare quotes in fields
  - Sanitizes non-UTF-8 characters (Windows-1252, etc.)
  - Auto-detects header row location
- Transaction safety with savepoints (partial failures don't lose successful inserts)
- Failed rows exported to `*-failed.csv` with error messages

## Requirements

- Go 1.24+
- PostgreSQL database

## Setup

1. Clone the repository

2. Copy the environment file and configure your database:
   ```bash
   cp .env.example .env
   ```

   Edit `.env` with your PostgreSQL connection string:
   ```
   DB_URL="postgres://username:password@localhost:5432/database?sslmode=disable"
   ```

3. Install dependencies:
   ```bash
   go mod download
   ```

4. Run database migrations (SQL schemas in `sql/schema/`)

5. Generate sqlc code (if modifying queries):
   ```bash
   sqlc generate
   ```

## Usage

```bash
# Build and run
go build -o csv-importer .
./csv-importer

# Or run directly
go run .
```

Place CSV files in the appropriate directory under `accounting/uploads/`:
```
accounting/uploads/
├── Anrok/
│   └── Transactions/
├── NS/
│   ├── SO_line_item_detail/
│   └── Invoice_line_item_detail-Sales_Tax/
└── SFDC/
    └── Closed_Won_Ops-Products_Report/
```

Successfully imported files are moved to an `Uploaded/` subdirectory.

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `↑` / `k` | Move cursor up |
| `↓` / `j` | Move cursor down |
| `Enter` / `Space` | Select menu item / Start import |
| `Backspace` | Go back to parent menu |
| `q` / `Ctrl+C` | Quit application |

Press any key to dismiss result messages after an import completes.

## CSV Format Specifications

### Defining Schemas

CSV schemas are defined in `internal/schema/` as `FieldSpec` slices. Each field specifies:

```go
type FieldSpec struct {
    Name       string      // Column header name (matched case-insensitively)
    Type       FieldType   // FieldText, FieldDate, FieldNumeric, FieldBool, FieldEnum
    Required   bool        // Whether the field must have a value
    AllowEmpty bool        // If Required, whether empty values are allowed (become NULL)
    EnumValues []string    // Valid values for FieldEnum type
    Normalizer func(string) string  // Optional transformation function
}
```

### Supported Field Types

| Type | Description | Example Values |
|------|-------------|----------------|
| `FieldText` | Any string value | `"Hello"`, `"ABC-123"` |
| `FieldDate` | Date values (see formats below) | `"2024-01-15"`, `"1/15/24"` |
| `FieldNumeric` | Decimal numbers | `"123.45"`, `"$1,234.56"`, `"(99.00)"` |
| `FieldBool` | Boolean values | `"true"`, `"false"`, `"yes"`, `"no"`, `"1"`, `"0"` |
| `FieldEnum` | Restricted set of values | Defined per field |

### Supported Date Formats

The following date formats are automatically recognized:
- `1/2/2006`, `01/02/2006`, `2006-01-02`
- `1-2-2006`, `01-02-2006`
- `1.2.2006`, `01.02.2006`
- `Jan 2, 2006`, `2 Jan 2006`
- Two-digit years (`1/2/06`) use a 20-year pivot from current year

### Supported Numeric Formats

- Standard: `123.45`, `-123.45`
- Currency symbols removed: `$123.45`, `€123.45`, `£123.45`
- Thousands separators removed: `1,234.56`
- Accounting negatives: `(123.45)` treated as `-123.45`

### Adding a New Upload Type

1. Define the schema in `internal/schema/`
2. Create SQL table and queries in `sql/`
3. Run `sqlc generate` to generate Go code
4. Create a handler in `internal/handler/`
5. Add the menu item in `internal/application/menu.go`
6. Create the upload directory under `accounting/uploads/`

## Troubleshooting

### "header not found within first 20 rows"

The CSV headers don't match the expected columns. Check:
- Column names match exactly (case-insensitive)
- No extra/missing columns
- Header row is within the first 20 rows of the file

### "invalid byte sequence for encoding UTF8"

The CSV contains non-UTF-8 characters (common with Excel exports). This should be handled automatically, but if issues persist:
- Re-export the CSV with UTF-8 encoding
- Check for special characters in text fields

### "failed to commit transaction" / Rollback errors

Individual row failures are now isolated with savepoints. Check the `*-failed.csv` file for specific row errors:
- **db error**: Data type mismatch or constraint violation
- **missing required column**: Required field is empty
- **invalid date/numeric**: Value doesn't match expected format

### "file exceeds maximum size"

CSV files are limited to 100MB. Split large files before importing.

### Rows importing but not appearing in database

Check if the file was already imported:
- Previously imported files are tracked in the `csv_uploads` table
- The file is moved to `Uploaded/` subdirectory after processing
- To re-import, delete the tracking record from `csv_uploads`

## Development

```bash
# Run tests
go test ./...

# Run tests with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Schema & Code Generation

This project uses [sqlc](https://sqlc.dev/) to generate type-safe Go code from SQL. There are three schema layers that must stay in sync:

```
SQL Schema (sql/schema/)     →  Database tables
SQL Queries (sql/queries/)   →  Generated Go code (internal/database/)
Go FieldSpecs (internal/schema/)  →  CSV validation & parsing
```

### Workflow for Schema Changes

1. **Modify the database schema** in `sql/schema/`:
   ```sql
   -- sql/schema/006_new_table.sql
   CREATE TABLE new_table (
       id SERIAL PRIMARY KEY,
       name TEXT NOT NULL,
       amount NUMERIC(12,2)
   );
   ```

2. **Add queries** in `sql/queries/`:
   ```sql
   -- sql/queries/new_table.sql
   -- name: InsertNewTable :exec
   INSERT INTO new_table (name, amount)
   VALUES ($1, $2);

   -- name: GetNewTableByName :many
   SELECT * FROM new_table WHERE name = $1;
   ```

3. **Regenerate Go code**:
   ```bash
   sqlc generate
   ```
   This updates `internal/database/` with new structs and methods.

4. **Define the CSV FieldSpec** in `internal/schema/`:
   ```go
   var NewTableFieldSpecs = []FieldSpec{
       {Name: "Name", Type: FieldText, Required: true},
       {Name: "Amount", Type: FieldNumeric, Required: true},
   }
   ```

5. **Create the handler** in `internal/handler/` implementing:
   - `BuildParams()` - Maps CSV row to sqlc params struct
   - `Insert()` - Calls the generated sqlc insert function

### sqlc Configuration

Configuration is in `sqlc.yaml`:
```yaml
version: "2"
sql:
  - schema: "sql/schema"
    queries: "sql/queries"
    engine: "postgresql"
    gen:
      go:
        package: "db"
        out: "internal/database"
        sql_package: "pgx/v5"
```

### Key Files After Generation

| Generated File | Contains |
|----------------|----------|
| `internal/database/models.go` | Struct definitions matching DB tables |
| `internal/database/<table>.sql.go` | Query functions (Insert, Get, etc.) |
| `internal/database/db.go` | `Queries` type and `New()` constructor |

### Keeping Schemas in Sync

When modifying an existing upload type:

1. Update SQL schema if table structure changes
2. Update SQL queries if insert/select logic changes
3. Run `sqlc generate`
4. Update FieldSpec if CSV columns change
5. Update handler's `BuildParams()` to match new struct fields
6. Run tests: `go test ./...`

## Project Structure

```
├── main.go                 # Application entry point
├── internal/
│   ├── application/        # TUI model and menu system
│   ├── csv/                # CSV parsing utilities
│   ├── database/           # sqlc-generated database code
│   ├── handler/            # Upload handlers for each data source
│   ├── schema/             # Field specs and validators
│   └── admin/              # Admin utilities (reset, etc.)
└── sql/
    ├── schema/             # Database migrations
    └── queries/            # sqlc query definitions
```
