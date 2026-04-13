package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// Config holds all benchmark configuration constants.
// Values can be overridden via environment variables.
type Config struct {
	// Index generation settings
	NumFields        int
	NumUniqueIDs     int
	TotalDocs        int
	TargetDocSizeBytes int
	NumSplits        int

	// Directory paths
	BaseDir             string
	SourceIndexDir      string
	CopySplitDirPrefix  string
	HardLinkSplitDirPrefix string

	// Bleve settings
	IndexBatchSize int

	// Boolean query batch size
	MaxBooleanClauses int
}

// DefaultConfig returns a Config with default values, overridden by
// environment variables where set.
func DefaultConfig() *Config {
	baseDir := envOrDefault("SPLITINDEX_BASEDIR",
		filepath.Join(homeDir(), "luceneIndex"))

	c := &Config{
		NumFields:          envIntOrDefault("SPLITINDEX_NUM_FIELDS", 20),
		NumUniqueIDs:       envIntOrDefault("SPLITINDEX_NUM_UNIQUE_IDS", 10_000),
		TotalDocs:          envIntOrDefault("SPLITINDEX_TOTAL_DOCS", 100_000),
		TargetDocSizeBytes: envIntOrDefault("SPLITINDEX_TARGET_DOC_SIZE_BYTES", 1024),
		NumSplits:          envIntOrDefault("SPLITINDEX_NUM_SPLITS", 10),
		BaseDir:            baseDir,
		IndexBatchSize:     envIntOrDefault("SPLITINDEX_INDEX_BATCH_SIZE", 1000),
		MaxBooleanClauses:  1024,
	}

	c.SourceIndexDir = filepath.Join(c.BaseDir, "source_index")
	c.CopySplitDirPrefix = filepath.Join(c.BaseDir, "copy_split", "dir")
	c.HardLinkSplitDirPrefix = filepath.Join(c.BaseDir, "hardlink_split", "dir")

	return c
}

func (c *Config) Print() {
	fmt.Println("============================================================")
	fmt.Println("  Go Index Splitting Benchmark (Bleve)")
	fmt.Println("============================================================")
	fmt.Printf("  Configuration:\n")
	fmt.Printf("    Total documents:   %d\n", c.TotalDocs)
	fmt.Printf("    Unique IDs:        %d\n", c.NumUniqueIDs)
	fmt.Printf("    Fields per doc:    %d\n", c.NumFields)
	fmt.Printf("    Number of splits:  %d\n", c.NumSplits)
	fmt.Printf("    Doc size:          ~%d bytes\n", c.TargetDocSizeBytes)
	fmt.Printf("    Base directory:    %s\n", c.BaseDir)
	fmt.Println("============================================================")
	fmt.Println()
}

// CopySplitDir returns the directory path for a copy-based split partition.
func (c *Config) CopySplitDir(partition int) string {
	return c.CopySplitDirPrefix + strconv.Itoa(partition)
}

// HardLinkSplitDir returns the directory path for a hard-link-based split partition.
func (c *Config) HardLinkSplitDir(partition int) string {
	return c.HardLinkSplitDirPrefix + strconv.Itoa(partition)
}

func homeDir() string {
	if h, err := os.UserHomeDir(); err == nil {
		return h
	}
	return "/tmp"
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
