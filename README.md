# railway-collector

Collects metrics and logs from [Railway](https://railway.com) via their GraphQL API and exports them to Prometheus and/or JSONL files.

## Quick Start

```bash
# Set your Railway API token
export RAILWAY_TOKEN=your-token-here

# Build
go build -o collector ./cmd/collector

# Run — loads config.yaml from the working directory automatically if present
./collector

# Or point at a different config file
./collector -config /path/to/config.yaml
```

Metrics are exposed at `http://localhost:9106/metrics` by default.

## Configuration

Copy `config.example.yaml` to `config.yaml` and edit to taste. The collector loads `config.yaml` from the working directory automatically; use `-config` to specify a different path. All config values can also be overridden with environment variables:

| Env Var | Description |
|---------|-------------|
| `RAILWAY_TOKEN` | Railway API token (required) |
| `LOG_LEVEL` | Log level: debug, info, warn, error |
| `RAILWAY_COLLECTOR_*` | Override any config key (e.g. `RAILWAY_COLLECTOR_COLLECT__METRICS__INTERVAL=60s`) |

A `.env` file in the working directory is loaded automatically.

## What It Collects

**Metrics** (default: every 30s): CPU, memory, network, disk usage per service/environment. Exposed as Prometheus gauges.

**Logs** (default: every 15s): Deployment logs, build logs, HTTP access logs. Written to file sink (JSONL).

**Resources** (default: every 5m): Auto-discovers projects, services, and environments. Supports regex filtering.

## Development

```bash
# Requires: Go 1.26+, just
just check    # vet + build
just generate # regenerate GraphQL client
just clean    # remove binary
```
