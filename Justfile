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

# Run all tests
test *ARGS:
    go test ./... {{ARGS}}

# Generate mocks
mocks:
    go generate ./internal/collector/...

# Run mutation testing
mutate *ARGS:
    gremlins unleash {{ARGS}}

# Dry-run mutation testing (find mutants without running tests)
mutate-dry *ARGS:
    gremlins unleash --dry-run {{ARGS}}

# Remove build artifacts
clean:
    rm -f collector
