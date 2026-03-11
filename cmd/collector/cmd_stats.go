package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
)

// StatsCmd shows database summary statistics.
type StatsCmd struct{}

type statsJSON struct {
	Path    string       `json:"path"`
	Size    int64        `json:"size_bytes"`
	Buckets []bucketJSON `json:"buckets"`
}

type bucketJSON struct {
	Name   string `json:"name"`
	Count  int    `json:"count"`
	Oldest string `json:"oldest_cursor,omitempty"`
	Newest string `json:"newest_cursor,omitempty"`
}

func (cmd *StatsCmd) Run(c *CLI) error {
	reader, err := openReader(c.State, c.Config)
	if err != nil {
		return err
	}
	defer reader.Close()

	f := formatter(c.JSON)

	fileSize, err := reader.DBFileSize()
	if err != nil {
		return fmt.Errorf("getting file size: %w", err)
	}

	bucketStats, err := reader.BucketStats()
	if err != nil {
		return fmt.Errorf("reading bucket stats: %w", err)
	}

	// Get cursor age ranges for log buckets
	logCursors, _ := reader.LogCursors()

	now := time.Now()

	cursorRange := func(entries []struct{ ts time.Time }) (oldest, newest string) {
		if len(entries) == 0 {
			return "", ""
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].ts.Before(entries[j].ts)
		})
		oldest = humanize.RelTime(entries[0].ts, now, "ago", "from now")
		newest = humanize.RelTime(entries[len(entries)-1].ts, now, "ago", "from now")
		return
	}

	logTimes := make([]struct{ ts time.Time }, len(logCursors))
	for i, c := range logCursors {
		logTimes[i].ts = c.Timestamp
	}

	logOldest, logNewest := cursorRange(logTimes)

	if c.JSON {
		var buckets []bucketJSON
		for _, bs := range bucketStats {
			bj := bucketJSON{Name: bs.Name, Count: bs.Count}
			switch bs.Name {
			case "log_cursors":
				bj.Oldest = logOldest
				bj.Newest = logNewest
			}
			buckets = append(buckets, bj)
		}
		return f.WriteJSON(statsJSON{
			Path:    resolveStatePath(c.State, c.Config),
			Size:    fileSize,
			Buckets: buckets,
		})
	}

	fmt.Printf("State DB: %s (%s)\n\n", resolveStatePath(c.State, c.Config), humanize.Bytes(uint64(fileSize)))

	headers := []string{"Bucket", "Entries", "Oldest", "Newest"}
	var rows [][]string
	for _, bs := range bucketStats {
		oldest, newest := "", ""
		switch bs.Name {
		case "log_cursors":
			oldest, newest = logOldest, logNewest
		}
		rows = append(rows, []string{
			bs.Name,
			fmt.Sprintf("%d", bs.Count),
			oldest,
			newest,
		})
	}
	f.WriteTable(headers, rows)
	return nil
}
