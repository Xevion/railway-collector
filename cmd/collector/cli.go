package main

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"

	"github.com/xevion/railway-collector/internal/cli"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/state"
)

// CLI is the root kong struct for all commands.
type CLI struct {
	State  string `help:"Path to bbolt state database." type:"path"`
	Config string `help:"Path to config YAML file." type:"path"`
	JSON   bool   `help:"Output as JSON instead of table." default:"false"`

	Run       RunCmd       `cmd:"" default:"1" help:"Start the collector (default command)."`
	Cursors   CursorsCmd   `cmd:"" help:"Show metric and log cursor timestamps."`
	Coverage  CoverageCmd  `cmd:"" help:"Summarize backfill coverage per project."`
	Discovery DiscoveryCmd `cmd:"" help:"Show cached discovery data."`
	Stats     StatsCmd     `cmd:"" help:"Show database summary statistics."`
	Reset     ResetCmd     `cmd:"" help:"Delete state entries (requires exclusive DB access)."`
}

// resolveStatePath determines the database path from flag, config, or default.
func resolveStatePath(flagPath, configPath string) string {
	if flagPath != "" {
		return flagPath
	}

	// Try loading config to get state.path
	_ = godotenv.Load()
	cfg, err := config.Load(configPath)
	if err == nil && cfg.State.Path != "" {
		return cfg.State.Path
	}

	return "collector.db"
}

// openReader opens the state DB read-only using resolved path.
func openReader(flagPath, configPath string) (*state.Reader, error) {
	path := resolveStatePath(flagPath, configPath)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("state database not found: %s", path)
	}
	return state.OpenReadOnly(path)
}

// openWriter opens the state DB read-write using resolved path.
func openWriter(flagPath, configPath string) (*state.Writer, error) {
	path := resolveStatePath(flagPath, configPath)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("state database not found: %s", path)
	}
	return state.OpenReadWrite(path)
}

// formatter creates a Formatter from the shared --json flag.
func formatter(jsonOutput bool) *cli.Formatter {
	return cli.NewFormatter(jsonOutput)
}
