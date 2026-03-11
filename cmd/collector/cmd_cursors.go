package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
)

// CursorsCmd shows metric and log cursor timestamps.
type CursorsCmd struct {
	Stale  time.Duration `help:"Highlight cursors older than this duration." default:"0"`
	Bucket string        `help:"Filter by bucket: log, or empty for all." enum:"log," default:""`
}

type cursorJSON struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	Timestamp string `json:"timestamp"`
	Age       string `json:"age"`
	Stale     bool   `json:"stale,omitempty"`
}

func (cmd *CursorsCmd) Run(c *CLI) error {
	reader, err := openReader(c.State, c.Config)
	if err != nil {
		return err
	}
	defer reader.Close()

	now := time.Now()
	f := formatter(c.JSON)

	var allJSON []cursorJSON
	var headers []string
	var rows [][]string

	if !c.JSON {
		headers = []string{"Bucket", "Key", "Timestamp", "Age", ""}
	}

	collect := func(bucketName string) error {
		var entries []struct {
			key string
			ts  time.Time
		}

		switch bucketName {
		case "log":
			cursors, err := reader.LogCursors()
			if err != nil {
				return err
			}
			for _, c := range cursors {
				entries = append(entries, struct {
					key string
					ts  time.Time
				}{c.Key, c.Timestamp})
			}
		}

		sort.Slice(entries, func(i, j int) bool {
			return entries[i].ts.After(entries[j].ts)
		})

		for _, e := range entries {
			age := now.Sub(e.ts)
			stale := cmd.Stale > 0 && age > cmd.Stale
			marker := ""
			if stale {
				marker = "<- stale"
			}

			if c.JSON {
				allJSON = append(allJSON, cursorJSON{
					Bucket:    bucketName,
					Key:       e.key,
					Timestamp: e.ts.Format(time.RFC3339),
					Age:       age.Round(time.Second).String(),
					Stale:     stale,
				})
			} else {
				rows = append(rows, []string{
					bucketName,
					e.key,
					e.ts.Format(time.RFC3339),
					humanize.RelTime(e.ts, now, "ago", "from now"),
					marker,
				})
			}
		}
		return nil
	}

	buckets := []string{"log"}
	if cmd.Bucket != "" {
		buckets = []string{cmd.Bucket}
	}

	for _, b := range buckets {
		if err := collect(b); err != nil {
			return fmt.Errorf("reading %s cursors: %w", b, err)
		}
	}

	if c.JSON {
		return f.WriteJSON(allJSON)
	}

	if len(rows) == 0 {
		fmt.Println("No cursors found.")
		return nil
	}
	f.WriteTable(headers, rows)
	return nil
}
