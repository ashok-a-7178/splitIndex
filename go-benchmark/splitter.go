package main

import (
	"fmt"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/searcher"
)

// CopySplitter implements Approach 1: File Copy + Term Deletion.
//
// This approach:
//  1. Copies the entire source index directory to each target directory
//     using file I/O (io.Copy).
//  2. Opens a Bleve index on each copied directory and deletes documents
//     whose ID values do NOT belong to that partition.
//  3. Closing the index triggers compaction.
type CopySplitter struct {
	cfg *Config
}

// NewCopySplitter creates a new CopySplitter with the given config.
func NewCopySplitter(cfg *Config) *CopySplitter {
	return &CopySplitter{cfg: cfg}
}

// Split runs the copy-based split benchmark.
// Returns [totalTimeMs, copyTimeMs, deleteTimeMs].
func (s *CopySplitter) Split() ([3]int64, error) {
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("  APPROACH 1: Sequential Copy + Delete  ")
	fmt.Println("========================================")
	fmt.Println()

	// Collect unique IDs
	uniqueIDs := collectUniqueIDs(s.cfg.NumUniqueIDs)
	fmt.Printf("[Copy] Found %d unique IDs\n", len(uniqueIDs))

	// Assign IDs to partitions (round-robin)
	partitionIDs := assignPartitions(uniqueIDs, s.cfg.NumSplits)
	for i := 0; i < s.cfg.NumSplits; i++ {
		fmt.Printf("[Copy] Partition %d keeps %d IDs\n", i, len(partitionIDs[i]))
	}

	totalStart := time.Now()

	// Phase 1: Copy source index to all target directories
	fmt.Printf("\n[Copy] Phase 1: Copying source index to %d directories...\n", s.cfg.NumSplits)
	copyStart := time.Now()
	for i := 0; i < s.cfg.NumSplits; i++ {
		targetDir := s.cfg.CopySplitDir(i)
		_ = deleteDirectory(targetDir)
		if err := copyDirectory(s.cfg.SourceIndexDir, targetDir); err != nil {
			return [3]int64{}, fmt.Errorf("copy to dir%d: %w", i, err)
		}
		fmt.Printf("[Copy]   Copied to dir%d (%.2f MB)\n", i, dirSizeMB(targetDir))
	}
	copyTime := time.Since(copyStart).Milliseconds()
	fmt.Printf("[Copy] Phase 1 completed in %d ms\n", copyTime)

	// Phase 2: Delete non-matching IDs from each partition
	fmt.Printf("\n[Copy] Phase 2: Deleting non-matching documents from each partition...\n")
	deleteStart := time.Now()
	for i := 0; i < s.cfg.NumSplits; i++ {
		targetDir := s.cfg.CopySplitDir(i)
		if err := deleteNonMatchingDocs(targetDir, partitionIDs[i], s.cfg, "Copy", i); err != nil {
			return [3]int64{}, fmt.Errorf("delete from dir%d: %w", i, err)
		}
	}
	deleteTime := time.Since(deleteStart).Milliseconds()
	fmt.Printf("[Copy] Phase 2 completed in %d ms\n", deleteTime)

	totalTime := time.Since(totalStart).Milliseconds()
	fmt.Printf("\n[Copy] TOTAL TIME: %d ms\n", totalTime)
	fmt.Printf("[Copy]   Copy phase:   %d ms\n", copyTime)
	fmt.Printf("[Copy]   Delete phase: %d ms\n", deleteTime)

	// Print final stats
	printPartitionStats(s.cfg, s.cfg.CopySplitDirPrefix, "Copy")

	return [3]int64{totalTime, copyTime, deleteTime}, nil
}

// HardLinkSplitter implements Approach 2: Hard Link + Term Deletion.
//
// This approach:
//  1. Creates hard links from the source index files to each target directory
//     using os.Link(). This is nearly instant and uses no additional disk space.
//  2. Opens a Bleve index on each linked directory and deletes documents whose
//     ID values do NOT belong to that partition.
//  3. Closing the index triggers compaction.
type HardLinkSplitter struct {
	cfg *Config
}

// NewHardLinkSplitter creates a new HardLinkSplitter with the given config.
func NewHardLinkSplitter(cfg *Config) *HardLinkSplitter {
	return &HardLinkSplitter{cfg: cfg}
}

// Split runs the hard-link-based split benchmark.
// Returns [totalTimeMs, linkTimeMs, deleteTimeMs].
func (s *HardLinkSplitter) Split() ([3]int64, error) {
	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("  APPROACH 2: Hard Link + Delete        ")
	fmt.Println("========================================")
	fmt.Println()

	// Collect unique IDs
	uniqueIDs := collectUniqueIDs(s.cfg.NumUniqueIDs)
	fmt.Printf("[HardLink] Found %d unique IDs\n", len(uniqueIDs))

	// Assign IDs to partitions (round-robin)
	partitionIDs := assignPartitions(uniqueIDs, s.cfg.NumSplits)
	for i := 0; i < s.cfg.NumSplits; i++ {
		fmt.Printf("[HardLink] Partition %d keeps %d IDs\n", i, len(partitionIDs[i]))
	}

	totalStart := time.Now()

	// Phase 1: Create hard links from source to all target directories
	fmt.Printf("\n[HardLink] Phase 1: Creating hard links to %d directories...\n", s.cfg.NumSplits)
	linkStart := time.Now()
	for i := 0; i < s.cfg.NumSplits; i++ {
		targetDir := s.cfg.HardLinkSplitDir(i)
		_ = deleteDirectory(targetDir)
		if err := hardLinkDirectory(s.cfg.SourceIndexDir, targetDir); err != nil {
			return [3]int64{}, fmt.Errorf("hardlink to dir%d: %w", i, err)
		}
		fmt.Printf("[HardLink]   Linked to dir%d\n", i)
	}
	linkTime := time.Since(linkStart).Milliseconds()
	fmt.Printf("[HardLink] Phase 1 completed in %d ms\n", linkTime)

	// Phase 2: Delete non-matching IDs from each partition
	fmt.Printf("\n[HardLink] Phase 2: Deleting non-matching documents from each partition...\n")
	deleteStart := time.Now()
	for i := 0; i < s.cfg.NumSplits; i++ {
		targetDir := s.cfg.HardLinkSplitDir(i)
		if err := deleteNonMatchingDocs(targetDir, partitionIDs[i], s.cfg, "HardLink", i); err != nil {
			return [3]int64{}, fmt.Errorf("delete from dir%d: %w", i, err)
		}
	}
	deleteTime := time.Since(deleteStart).Milliseconds()
	fmt.Printf("[HardLink] Phase 2 completed in %d ms\n", deleteTime)

	totalTime := time.Since(totalStart).Milliseconds()
	fmt.Printf("\n[HardLink] TOTAL TIME: %d ms\n", totalTime)
	fmt.Printf("[HardLink]   Link phase:   %d ms\n", linkTime)
	fmt.Printf("[HardLink]   Delete phase: %d ms\n", deleteTime)

	// Print final stats
	printPartitionStats(s.cfg, s.cfg.HardLinkSplitDirPrefix, "HardLink")

	return [3]int64{totalTime, linkTime, deleteTime}, nil
}

// =========================================================================
//  Shared helpers for both splitters
// =========================================================================

// collectUniqueIDs generates the list of unique ID values.
func collectUniqueIDs(numUniqueIDs int) []string {
	ids := make([]string, numUniqueIDs)
	for i := 0; i < numUniqueIDs; i++ {
		ids[i] = fmt.Sprintf("ID_%d", i)
	}
	return ids
}

// assignPartitions distributes IDs to partitions using round-robin.
func assignPartitions(ids []string, numSplits int) [][]string {
	partitions := make([][]string, numSplits)
	for i := range partitions {
		partitions[i] = make([]string, 0)
	}
	for i, id := range ids {
		partitions[i%numSplits] = append(partitions[i%numSplits], id)
	}
	return partitions
}

// deleteNonMatchingDocs opens the Bleve index at targetDir and deletes all
// documents whose "ID" field is NOT in keepIDs.
func deleteNonMatchingDocs(targetDir string, keepIDs []string, cfg *Config, label string, partition int) error {
	keepSet := make(map[string]bool, len(keepIDs))
	for _, id := range keepIDs {
		keepSet[id] = true
	}

	// Build list of IDs to delete
	allIDs := collectUniqueIDs(cfg.NumUniqueIDs)
	var deleteIDs []string
	for _, id := range allIDs {
		if !keepSet[id] {
			deleteIDs = append(deleteIDs, id)
		}
	}

	fmt.Printf("[%s]   Partition %d: deleting %d IDs, keeping %d IDs\n",
		label, partition, len(deleteIDs), len(keepIDs))

	// Increase disjunction max clause count for large delete batches
	origMax := searcher.DisjunctionMaxClauseCount
	searcher.DisjunctionMaxClauseCount = cfg.MaxBooleanClauses * 2
	defer func() { searcher.DisjunctionMaxClauseCount = origMax }()

	// Open the index
	index, err := bleve.Open(targetDir)
	if err != nil {
		return fmt.Errorf("open index at %s: %w", targetDir, err)
	}
	defer index.Close()

	// Delete documents by their Bleve document IDs
	// In Bleve, we need to find document IDs that match the ID field values
	// we want to delete. We'll search for each ID value and collect doc IDs.
	batch := index.NewBatch()
	batchCount := 0

	for _, idValue := range deleteIDs {
		// Search for documents with this ID value
		query := bleve.NewTermQuery(idValue)
		query.SetField("ID")
		searchReq := bleve.NewSearchRequest(query)
		searchReq.Size = cfg.TotalDocs / cfg.NumUniqueIDs * 2 // generous upper bound
		searchReq.Fields = []string{} // no fields needed

		results, err := index.Search(searchReq)
		if err != nil {
			return fmt.Errorf("search for ID %s: %w", idValue, err)
		}

		for _, hit := range results.Hits {
			batch.Delete(hit.ID)
			batchCount++
		}

		// Flush batch periodically
		if batchCount >= cfg.IndexBatchSize {
			if err := index.Batch(batch); err != nil {
				return fmt.Errorf("batch delete: %w", err)
			}
			batch = index.NewBatch()
			batchCount = 0
		}
	}

	// Flush remaining deletes
	if batchCount > 0 {
		if err := index.Batch(batch); err != nil {
			return fmt.Errorf("final batch delete: %w", err)
		}
	}

	// Get final doc count
	docCount, _ := index.DocCount()
	fmt.Printf("[%s]   Partition %d: done. Docs remaining: %d, size: %.2f MB\n",
		label, partition, docCount, dirSizeMB(targetDir))

	return nil
}

// printPartitionStats prints document counts for all partitions.
func printPartitionStats(cfg *Config, dirPrefix, label string) {
	fmt.Printf("\n[%s] Final partition statistics:\n", label)
	var totalDocs uint64
	for i := 0; i < cfg.NumSplits; i++ {
		targetDir := dirPrefix + fmt.Sprintf("%d", i)
		index, err := bleve.Open(targetDir)
		if err != nil {
			fmt.Printf("[%s]   dir%d: ERROR — %v\n", label, i, err)
			continue
		}
		docCount, _ := index.DocCount()
		totalDocs += docCount
		fmt.Printf("[%s]   dir%d: %d docs, %.2f MB\n",
			label, i, docCount, dirSizeMB(targetDir))
		index.Close()
	}
	fmt.Printf("[%s]   Total docs across all partitions: %d\n", label, totalDocs)
}
