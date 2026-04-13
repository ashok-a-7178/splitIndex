package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// dirSizeMB returns the total size of all files in a directory in megabytes.
func dirSizeMB(dir string) float64 {
	return float64(dirSizeBytes(dir)) / (1024.0 * 1024.0)
}

// dirSizeBytes returns the total size of all files in a directory in bytes.
func dirSizeBytes(dir string) int64 {
	var size int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}

// deleteDirectory recursively removes a directory and all its contents.
func deleteDirectory(dir string) error {
	return os.RemoveAll(dir)
}

// copyDirectory copies all files from src to dst using io.Copy.
func copyDirectory(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dst, err)
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("readdir %s: %w", src, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			// Recursively copy subdirectories
			srcSub := filepath.Join(src, entry.Name())
			dstSub := filepath.Join(dst, entry.Name())
			if err := copyDirectory(srcSub, dstSub); err != nil {
				return err
			}
			continue
		}

		srcFile := filepath.Join(src, entry.Name())
		dstFile := filepath.Join(dst, entry.Name())
		if err := copyFile(srcFile, dstFile); err != nil {
			return err
		}
	}
	return nil
}

// copyFile copies a single file from src to dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open %s: %w", src, err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("copy %s -> %s: %w", src, dst, err)
	}
	return out.Sync()
}

// hardLinkDirectory creates hard links for immutable files (*.zap segment
// files) in src into dst, and copies mutable files (root.bolt, index_meta.json,
// etc.) so each partition gets its own independent copy of the metadata.
//
// This is necessary because Bleve's scorch backend has:
//   - *.zap files — immutable segment files (safe to hard-link)
//   - root.bolt — mutable BoltDB tracking active segments (must be copied)
//   - index_meta.json — mutable index metadata (must be copied)
func hardLinkDirectory(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dst, err)
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("readdir %s: %w", src, err)
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := hardLinkDirectory(srcPath, dstPath); err != nil {
				return err
			}
			continue
		}

		// Hard-link immutable segment files; copy everything else
		if isImmutableFile(entry.Name()) {
			if err := os.Link(srcPath, dstPath); err != nil {
				return fmt.Errorf("link %s -> %s: %w", srcPath, dstPath, err)
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return fmt.Errorf("copy mutable file %s: %w", entry.Name(), err)
			}
		}
	}
	return nil
}

// isImmutableFile returns true for files that are immutable and safe to
// hard-link. In Bleve's scorch backend, *.zap segment files are immutable.
func isImmutableFile(name string) bool {
	return filepath.Ext(name) == ".zap"
}

// countFiles returns the number of files in a directory (non-recursive).
func countFiles(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() {
			count++
		}
	}
	return count
}

// countFilesRecursive returns the total number of files in a directory tree.
func countFilesRecursive(dir string) int {
	count := 0
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	return count
}
