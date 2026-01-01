.PHONY: all build dev clean templ css deps run

# Default target
all: build

# Install dependencies
deps:
	go mod download
	go install github.com/a-h/templ/cmd/templ@latest
	@if [ ! -f ./tailwindcss ]; then \
		echo "Downloading Tailwind CSS standalone CLI..."; \
		curl -sLO https://github.com/tailwindlabs/tailwindcss/releases/latest/download/tailwindcss-linux-x64; \
		chmod +x tailwindcss-linux-x64; \
		mv tailwindcss-linux-x64 ./tailwindcss; \
	fi

# Generate templ files
templ:
	templ generate

# Build CSS with Tailwind
css: deps
	./tailwindcss -i internal/web/static/css/input.css -o internal/web/static/css/output.css --minify

# Build the application
build: templ css
	go build -o csv-importer ./cmd/server

# Run in development mode
dev: templ
	@echo "Starting development server..."
	go run ./cmd/server

# Run the server
run: build
	./csv-importer

# Clean build artifacts
clean:
	rm -f csv-importer
	rm -f internal/web/static/css/output.css
	rm -f ./tailwindcss
	find . -name "*_templ.go" -delete

# Watch for changes (requires air: go install github.com/air-verse/air@latest)
watch:
	air

# Generate sqlc
sqlc:
	sqlc generate
