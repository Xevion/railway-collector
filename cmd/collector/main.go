package main

import (
	"fmt"
	"os"

	"github.com/alecthomas/kong"
)

func main() {
	var c CLI
	ctx := kong.Parse(&c,
		kong.Name("collector"),
		kong.Description("Railway metrics collector and state inspector."),
		kong.UsageOnError(),
	)
	if err := ctx.Run(&c); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
