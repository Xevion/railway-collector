package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
)

// Formatter handles output in either table or JSON format.
type Formatter struct {
	JSON   bool
	Writer io.Writer
}

// NewFormatter creates a formatter that writes to stdout.
func NewFormatter(jsonOutput bool) *Formatter {
	return &Formatter{
		JSON:   jsonOutput,
		Writer: os.Stdout,
	}
}

// WriteTable renders data as an aligned table with headers.
func (f *Formatter) WriteTable(headers []string, rows [][]string) {
	t := table.NewWriter()
	t.SetOutputMirror(f.Writer)
	t.SetStyle(table.StyleLight)

	headerRow := make(table.Row, len(headers))
	for i, h := range headers {
		headerRow[i] = h
	}
	t.AppendHeader(headerRow)

	for _, row := range rows {
		tableRow := make(table.Row, len(row))
		for i, cell := range row {
			tableRow[i] = cell
		}
		t.AppendRow(tableRow)
	}

	t.Render()
}

// WriteJSON renders data as a single indented JSON array.
func (f *Formatter) WriteJSON(v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(f.Writer, string(data))
	return err
}
