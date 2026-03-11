package main

import (
	"fmt"
)

// ResetCmd deletes state entries. Requires exclusive DB access.
type ResetCmd struct {
	Bucket string `help:"Bucket to clear: log_cursors, discovery_cache, coverage." required:""`
	Key    string `help:"Delete a single key instead of the entire bucket."`
}

var validBuckets = map[string]bool{
	"log_cursors":     true,
	"discovery_cache": true,
	"coverage":        true,
}

func (cmd *ResetCmd) Run(c *CLI) error {
	if !validBuckets[cmd.Bucket] {
		return fmt.Errorf("unknown bucket %q; valid: log_cursors, discovery_cache, coverage", cmd.Bucket)
	}

	writer, err := openWriter(c.State, c.Config)
	if err != nil {
		return err
	}
	defer writer.Close()

	if cmd.Key != "" {
		if err := writer.DeleteKey(cmd.Bucket, cmd.Key); err != nil {
			return err
		}
		fmt.Printf("Deleted key %q from %s.\n", cmd.Key, cmd.Bucket)
		return nil
	}

	count, err := writer.DeleteBucket(cmd.Bucket)
	if err != nil {
		return err
	}
	fmt.Printf("Deleted %d entries from %s.\n", count, cmd.Bucket)
	return nil
}
