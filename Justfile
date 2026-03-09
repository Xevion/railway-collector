default:
    @just --list

# Build the collector binary
build:
    go build -o collector ./cmd/collector

# Run vet and build
check: 
    go vet ./...
    go build -o collector ./cmd/collector

# Run the collector
run *ARGS:
    go run ./cmd/collector {{ARGS}}

# Regenerate GraphQL client code
generate:
    go generate ./internal/railway/

# Tidy dependencies
tidy:
    go mod tidy

# Remove build artifacts
clean:
    rm -f collector
