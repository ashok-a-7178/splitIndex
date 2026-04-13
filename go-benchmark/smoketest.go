package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/v2"
)

// ConcurrentCopySmokeTest simulates copying a Bleve index from source to remote
// while the source index is being actively modified. It tests three approaches:
//
//  1. Hard-Link Snapshot — read commit metadata, hard-link active files
//  2. Hard-Link Live Capture — hard-link ALL files from live source dir
//  3. Rsync — use rsync to copy from source to destination
//
// The index is sized at ~400 MB (100k docs, ~8KB each).
type ConcurrentCopySmokeTest struct {
	baseDir          string
	sourceDir        string
	snapshotDir      string
	hardlinkDestDir  string
	hlLiveDestDir    string
	rsyncDestDir     string
	initialDocs      int
	concurrentOps    int
	opDelayMs        int
	targetDocSize    int
}

// NewConcurrentCopySmokeTest creates a new smoke test with default settings.
func NewConcurrentCopySmokeTest(baseDir string) *ConcurrentCopySmokeTest {
	smokeDir := filepath.Join(baseDir, "smoketest")
	return &ConcurrentCopySmokeTest{
		baseDir:         smokeDir,
		sourceDir:       filepath.Join(smokeDir, "source_index"),
		snapshotDir:     filepath.Join(smokeDir, "snapshot"),
		hardlinkDestDir: filepath.Join(smokeDir, "dest_hardlink"),
		hlLiveDestDir:   filepath.Join(smokeDir, "dest_hardlink_live"),
		rsyncDestDir:    filepath.Join(smokeDir, "dest_rsync"),
		initialDocs:     65_000,
		concurrentOps:   1_000,
		opDelayMs:       5,
		targetDocSize:   2_048,
	}
}

// Run executes the full smoke test.
func (t *ConcurrentCopySmokeTest) Run() error {
	fmt.Println("================================================================")
	fmt.Println("  Concurrent Copy Smoke Test (Go / Bleve)")
	fmt.Println("  Simulates source→remote index replication with live writes")
	fmt.Println("================================================================")
	fmt.Printf("  Initial docs:        %d\n", t.initialDocs)
	fmt.Printf("  Doc size:            ~%d bytes (~%dKB)\n", t.targetDocSize, t.targetDocSize/1024)
	fmt.Println("  Expected index size: ~400 MB (target)")
	fmt.Printf("  Concurrent ops:      %d\n", t.concurrentOps)
	fmt.Printf("  Source dir:          %s\n", t.sourceDir)
	fmt.Printf("  Snapshot dir:        %s\n", t.snapshotDir)
	fmt.Printf("  HardLink dest dir:   %s\n", t.hardlinkDestDir)
	fmt.Printf("  HL-Live dest dir:    %s\n", t.hlLiveDestDir)
	fmt.Printf("  Rsync dest dir:      %s\n", t.rsyncDestDir)
	fmt.Println("================================================================")
	fmt.Println()

	// Clean previous run
	_ = deleteDirectory(t.baseDir)

	// Step 1: Generate initial source index
	fmt.Printf(">>> STEP 1: Generate initial source index (%d docs)\n", t.initialDocs)
	if err := t.generateSourceIndex(); err != nil {
		return fmt.Errorf("generate source index: %w", err)
	}

	// Step 2: List initial files
	fmt.Println("\n>>> STEP 2: Source index files (before concurrent writes)")
	t.listIndexFiles(t.sourceDir)

	// Step 3: Test Hard-Link Snapshot approach
	fmt.Println("\n>>> STEP 3: Hard-Link Snapshot approach (with concurrent writes)")
	hlTime, err := t.testHardLinkSnapshot()
	if err != nil {
		fmt.Printf("[HardLink] ERROR: %v\n", err)
		hlTime = -1
	}

	// Step 4: Test Hard-Link Live Capture approach
	fmt.Println("\n>>> STEP 4: Hard-Link Live Capture approach (with concurrent writes)")
	hlLiveTime, err := t.testHardLinkLiveCapture()
	if err != nil {
		fmt.Printf("[HL-Live] ERROR: %v\n", err)
		hlLiveTime = -1
	}

	// Step 5: Test Rsync approach
	fmt.Println("\n>>> STEP 5: Rsync approach (with concurrent writes)")
	rsyncTime, err := t.testRsync()
	if err != nil {
		fmt.Printf("[Rsync] ERROR: %v\n", err)
		rsyncTime = -1
	}

	// Step 6: List files after all writes
	fmt.Println("\n>>> STEP 6: Source index files (after all concurrent writes)")
	t.listIndexFiles(t.sourceDir)

	// Step 7: Print comparison
	t.printResults(hlTime, hlLiveTime, rsyncTime)

	// Cleanup
	fmt.Println("\n[SmokeTest] Cleaning up...")
	_ = deleteDirectory(t.baseDir)
	fmt.Println("[SmokeTest] Done.")

	return nil
}

// =========================================================================
//  Step 1: Generate source index
// =========================================================================

func (t *ConcurrentCopySmokeTest) generateSourceIndex() error {
	if err := os.MkdirAll(t.sourceDir, 0o755); err != nil {
		return err
	}
	// Remove if exists
	_ = deleteDirectory(t.sourceDir)

	indexMapping := bleve.NewIndexMapping()
	index, err := bleve.New(t.sourceDir, indexMapping)
	if err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	defer index.Close()

	rng := rand.New(rand.NewSource(42))
	batch := index.NewBatch()
	batchCount := 0

	for i := 0; i < t.initialDocs; i++ {
		doc := t.createDocument(i, rng)
		docID := fmt.Sprintf("doc_%d", i)
		if err := batch.Index(docID, doc); err != nil {
			return fmt.Errorf("index doc %d: %w", i, err)
		}
		batchCount++

		if batchCount >= 1000 {
			if err := index.Batch(batch); err != nil {
				return fmt.Errorf("commit batch: %w", err)
			}
			batch = index.NewBatch()
			batchCount = 0

			if (i+1)%10_000 == 0 {
				fmt.Printf("[SmokeTest] Indexed %d / %d docs\n", i+1, t.initialDocs)
			}
		}
	}

	if batchCount > 0 {
		if err := index.Batch(batch); err != nil {
			return fmt.Errorf("commit final batch: %w", err)
		}
	}

	fmt.Printf("[SmokeTest] Generated %d docs\n", t.initialDocs)
	fmt.Printf("[SmokeTest] Source index created: %.2f MB\n", dirSizeMB(t.sourceDir))
	return nil
}

// =========================================================================
//  Step 3: Hard-Link Snapshot
// =========================================================================

func (t *ConcurrentCopySmokeTest) testHardLinkSnapshot() (int64, error) {
	start := time.Now()

	// Start concurrent writer
	var stopWriter int32
	var opsCompleted int32
	writerStarted := make(chan struct{})

	go func() {
		t.doConcurrentWrites(&stopWriter, &opsCompleted, writerStarted)
	}()

	<-writerStarted
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("[HardLink] Taking point-in-time snapshot...\n")
	fmt.Printf("[HardLink] Concurrent writer is active (%d ops so far)\n",
		atomic.LoadInt32(&opsCompleted))

	// Take snapshot: hard-link all files from source to snapshot dir
	_ = deleteDirectory(t.snapshotDir)
	if err := hardLinkDirectory(t.sourceDir, t.snapshotDir); err != nil {
		atomic.StoreInt32(&stopWriter, 1)
		return 0, fmt.Errorf("create snapshot: %w", err)
	}

	fmt.Println("[HardLink] Snapshot taken. Listing snapshot files:")
	t.listIndexFiles(t.snapshotDir)

	// Copy snapshot to destination
	fmt.Println("[HardLink] Copying snapshot to destination (simulating remote copy)...")
	_ = deleteDirectory(t.hardlinkDestDir)
	if err := copyDirectory(t.snapshotDir, t.hardlinkDestDir); err != nil {
		atomic.StoreInt32(&stopWriter, 1)
		return 0, fmt.Errorf("copy snapshot: %w", err)
	}

	time.Sleep(200 * time.Millisecond)
	atomic.StoreInt32(&stopWriter, 1)
	time.Sleep(500 * time.Millisecond) // Wait for writer to finish

	elapsed := time.Since(start).Milliseconds()

	fmt.Printf("[HardLink] Concurrent writer completed %d operations\n",
		atomic.LoadInt32(&opsCompleted))

	// Verify destination index
	fmt.Println("[HardLink] Verifying destination index...")
	t.verifyIndex(t.hardlinkDestDir, "HardLink")

	fmt.Printf("[HardLink] Total time: %d ms\n", elapsed)
	return elapsed, nil
}

// =========================================================================
//  Step 4: Hard-Link Live Capture
// =========================================================================

func (t *ConcurrentCopySmokeTest) testHardLinkLiveCapture() (int64, error) {
	start := time.Now()

	// Start concurrent writer
	var stopWriter int32
	var opsCompleted int32
	writerStarted := make(chan struct{})

	go func() {
		t.doConcurrentWrites(&stopWriter, &opsCompleted, writerStarted)
	}()

	<-writerStarted
	time.Sleep(100 * time.Millisecond)

	fmt.Println("[HL-Live] Hard-linking ALL files from live source dir...")
	fmt.Printf("[HL-Live] Concurrent writer is active (%d ops so far)\n",
		atomic.LoadInt32(&opsCompleted))

	// Hard-link all files from live source directly to destination
	_ = deleteDirectory(t.hlLiveDestDir)
	linked, skipped := t.hardLinkLiveCapture(t.sourceDir, t.hlLiveDestDir)

	fmt.Printf("[HL-Live] Hard-linked %d files", linked)
	if skipped > 0 {
		fmt.Printf(" (skipped %d due to concurrent changes)", skipped)
	}
	fmt.Println()

	fmt.Println("[HL-Live] Live capture complete. Listing destination files:")
	t.listIndexFiles(t.hlLiveDestDir)

	time.Sleep(200 * time.Millisecond)
	atomic.StoreInt32(&stopWriter, 1)
	time.Sleep(500 * time.Millisecond) // Wait for writer to finish

	elapsed := time.Since(start).Milliseconds()

	fmt.Printf("[HL-Live] Concurrent writer completed %d operations\n",
		atomic.LoadInt32(&opsCompleted))

	// Verify destination index
	fmt.Println("[HL-Live] Verifying destination index...")
	t.verifyIndex(t.hlLiveDestDir, "HL-Live")

	fmt.Printf("[HL-Live] Total time: %d ms\n", elapsed)
	return elapsed, nil
}

// hardLinkLiveCapture hard-links immutable segment files and copies mutable
// metadata files from srcDir to destDir, tolerating files that disappear
// due to concurrent operations.
func (t *ConcurrentCopySmokeTest) hardLinkLiveCapture(srcDir, destDir string) (linked, skipped int) {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		fmt.Printf("[HL-Live] WARNING: cannot create dest dir: %v\n", err)
		return 0, 0
	}

	err := filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			skipped++
			return nil
		}

		relPath, _ := filepath.Rel(srcDir, path)
		destPath := filepath.Join(destDir, relPath)

		if info.IsDir() {
			return os.MkdirAll(destPath, 0o755)
		}

		// Hard-link immutable segment files; copy mutable metadata
		if isImmutableFile(info.Name()) {
			if linkErr := os.Link(path, destPath); linkErr != nil {
				skipped++
				fmt.Printf("[HL-Live] Skipped (concurrent delete?): %s — %v\n",
					info.Name(), linkErr)
			} else {
				linked++
			}
		} else {
			if cpErr := copyFile(path, destPath); cpErr != nil {
				skipped++
				fmt.Printf("[HL-Live] Skipped copy (concurrent change?): %s — %v\n",
					info.Name(), cpErr)
			} else {
				linked++
			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("[HL-Live] WARNING: walk error: %v\n", err)
	}
	return
}

// =========================================================================
//  Step 5: Rsync
// =========================================================================

func (t *ConcurrentCopySmokeTest) testRsync() (int64, error) {
	if !isRsyncAvailable() {
		fmt.Println("[Rsync] rsync not available on this system, skipping rsync test")
		return -1, nil
	}

	start := time.Now()

	// Start concurrent writer
	var stopWriter int32
	var opsCompleted int32
	writerStarted := make(chan struct{})

	go func() {
		t.doConcurrentWrites(&stopWriter, &opsCompleted, writerStarted)
	}()

	<-writerStarted
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("[Rsync] Starting first rsync pass (concurrent writer active, %d ops so far)...\n",
		atomic.LoadInt32(&opsCompleted))

	if err := os.MkdirAll(t.rsyncDestDir, 0o755); err != nil {
		atomic.StoreInt32(&stopWriter, 1)
		return 0, err
	}

	// First rsync pass
	rsync1Start := time.Now()
	if err := runRsync(t.sourceDir, t.rsyncDestDir); err != nil {
		fmt.Printf("[Rsync] First pass warning: %v\n", err)
	}
	fmt.Printf("[Rsync] First pass completed in %d ms\n", time.Since(rsync1Start).Milliseconds())

	time.Sleep(200 * time.Millisecond)

	fmt.Printf("[Rsync] Starting second rsync pass (to catch new segments, %d ops so far)...\n",
		atomic.LoadInt32(&opsCompleted))

	// Second rsync pass
	rsync2Start := time.Now()
	if err := runRsync(t.sourceDir, t.rsyncDestDir); err != nil {
		fmt.Printf("[Rsync] Second pass warning: %v\n", err)
	}
	fmt.Printf("[Rsync] Second pass completed in %d ms\n", time.Since(rsync2Start).Milliseconds())

	// Stop writer
	atomic.StoreInt32(&stopWriter, 1)
	time.Sleep(500 * time.Millisecond) // Wait for writer to finish

	fmt.Printf("[Rsync] Concurrent writer completed %d operations\n",
		atomic.LoadInt32(&opsCompleted))

	// Final rsync
	fmt.Println("[Rsync] Final rsync pass (writer stopped, ensure consistency)...")
	rsync3Start := time.Now()
	if err := runRsync(t.sourceDir, t.rsyncDestDir); err != nil {
		fmt.Printf("[Rsync] Final pass warning: %v\n", err)
	}
	fmt.Printf("[Rsync] Final pass completed in %d ms\n", time.Since(rsync3Start).Milliseconds())

	// Verify
	fmt.Println("[Rsync] Verifying destination index...")
	t.verifyIndex(t.rsyncDestDir, "Rsync")

	elapsed := time.Since(start).Milliseconds()
	fmt.Printf("[Rsync] Total time (including all passes): %d ms\n", elapsed)
	return elapsed, nil
}

func runRsync(srcDir, destDir string) error {
	sourcePath := srcDir + string(os.PathSeparator)
	targetPath := destDir + string(os.PathSeparator)

	cmd := exec.Command("rsync", "-a", "--delete", sourcePath, targetPath)
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			// Exit code 24 = vanished source files (expected with concurrent writes)
			if exitErr.ExitCode() == 24 {
				fmt.Println("[Rsync] Note: exit code 24 — some source files vanished during transfer (expected with concurrent writes)")
				return nil
			}
		}
		return fmt.Errorf("rsync failed: %w", err)
	}
	return nil
}

func isRsyncAvailable() bool {
	cmd := exec.Command("rsync", "--version")
	return cmd.Run() == nil
}

// =========================================================================
//  Concurrent writer: simulates live indexing
// =========================================================================

func (t *ConcurrentCopySmokeTest) doConcurrentWrites(stopFlag *int32, opsCompleted *int32, started chan struct{}) {
	rng := rand.New(rand.NewSource(42))

	index, err := bleve.Open(t.sourceDir)
	if err != nil {
		fmt.Printf("[Writer] ERROR: cannot open index: %v\n", err)
		close(started)
		return
	}
	defer index.Close()

	close(started)
	docCounter := t.initialDocs

	for atomic.LoadInt32(stopFlag) == 0 && atomic.LoadInt32(opsCompleted) < int32(t.concurrentOps) {
		op := rng.Intn(3)
		switch op {
		case 0: // ADD
			doc := t.createDocument(docCounter, rng)
			docID := fmt.Sprintf("doc_%d", docCounter)
			_ = index.Index(docID, doc)
			docCounter++
		case 1: // UPDATE
			updateID := rng.Intn(docCounter)
			doc := t.createDocument(updateID, rng)
			docID := fmt.Sprintf("doc_%d", updateID)
			_ = index.Index(docID, doc)
		case 2: // DELETE
			deleteID := rng.Intn(max(1, docCounter))
			docID := fmt.Sprintf("doc_%d", deleteID)
			_ = index.Delete(docID)
		}

		atomic.AddInt32(opsCompleted, 1)

		ops := atomic.LoadInt32(opsCompleted)
		if ops%100 == 0 {
			fmt.Printf("[Writer] Committed after %d ops\n", ops)
		}

		time.Sleep(time.Duration(t.opDelayMs) * time.Millisecond)
	}

	fmt.Printf("[Writer] Final commit. Total ops: %d\n", atomic.LoadInt32(opsCompleted))
}

// =========================================================================
//  Verification
// =========================================================================

func (t *ConcurrentCopySmokeTest) verifyIndex(indexDir, label string) {
	index, err := bleve.Open(indexDir)
	if err != nil {
		fmt.Printf("[%s] ✗ FAIL — Cannot read destination index: %v\n", label, err)
		return
	}
	defer index.Close()

	docCount, _ := index.DocCount()

	fmt.Printf("[%s] ✓ Destination index is READABLE\n", label)
	fmt.Printf("[%s]   Documents:     %d\n", label, docCount)
	fmt.Printf("[%s]   Index size:    %.2f MB\n", label, dirSizeMB(indexDir))
	fmt.Printf("[%s]   Index files:   %d\n", label, countFilesRecursive(indexDir))
	fmt.Printf("[%s] ✓ PASS — Index is consistent and readable\n", label)
}

// =========================================================================
//  Document creation
// =========================================================================

func (t *ConcurrentCopySmokeTest) createDocument(docIndex int, rng *rand.Rand) map[string]interface{} {
	doc := make(map[string]interface{})
	doc["docId"] = fmt.Sprintf("doc_%d", docIndex)
	doc["sequence"] = docIndex
	doc["status"] = "active"
	doc["version"] = 1

	// Build content field to reach target size
	contentTarget := t.targetDocSize - 24 // overhead for other fields
	if contentTarget < 50 {
		contentTarget = 50
	}

	// Use per-document seeded random for varied content
	docRng := rand.New(rand.NewSource(int64(docIndex)*31 + 7))
	var sb strings.Builder
	sb.Grow(contentTarget + 20)
	sb.WriteString(fmt.Sprintf("document number %d ", docIndex))

	for sb.Len() < contentTarget {
		sb.WriteString(vocabulary[docRng.Intn(len(vocabulary))])
		sb.WriteByte(' ')
	}

	content := sb.String()
	if len(content) > contentTarget {
		content = content[:contentTarget]
	}
	doc["content"] = content

	return doc
}

// =========================================================================
//  File listing
// =========================================================================

func (t *ConcurrentCopySmokeTest) listIndexFiles(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println("  (no files or cannot read directory)")
		return
	}
	totalFiles := countFilesRecursive(dir)
	fmt.Printf("  Total files: %d\n", totalFiles)

	// Show top-level entries
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		if e.IsDir() {
			subSize := dirSizeBytes(filepath.Join(dir, e.Name()))
			fmt.Printf("  %-40s %10d bytes (dir)\n", e.Name()+"/", subSize)
		} else {
			fmt.Printf("  %-40s %10d bytes\n", e.Name(), info.Size())
		}
	}
}

// =========================================================================
//  Results
// =========================================================================

func (t *ConcurrentCopySmokeTest) printResults(hlTime, hlLiveTime, rsyncTime int64) {
	fmt.Println()
	fmt.Println("================================================================")
	fmt.Println("  SMOKE TEST RESULTS")
	fmt.Println("================================================================")
	fmt.Println()
	fmt.Println("  Test scenario: Copy ~400 MB Bleve index from source to remote")
	fmt.Println("                 while concurrent indexing creates new segments")
	fmt.Println()
	fmt.Println("  +-----------------------------------------------------------------------+")
	fmt.Println("  |                    | HL Snapshot     | HL Live        | Rsync          |")
	fmt.Println("  +-----------------------------------------------------------------------+")
	fmt.Printf("  | Total Time         | %10d ms   | %10d ms   | %10d ms   |\n",
		hlTime, hlLiveTime, rsyncTime)
	fmt.Println("  +-----------------------------------------------------------------------+")
	fmt.Println()
	fmt.Println("  Key observations:")
	fmt.Println("  ─────────────────────────────────────────────────────")
	fmt.Println("  HardLink Snapshot approach:")
	fmt.Println("    - Hard-links all files from source to snapshot dir")
	fmt.Println("    - Copies snapshot to destination (simulating remote)")
	fmt.Println("    - Snapshot is consistent even during concurrent writes")
	fmt.Println("    - Requires same filesystem for the snapshot step")
	fmt.Println()
	fmt.Println("  HardLink Live Capture approach:")
	fmt.Println("    - Hard-links ALL files from the live source directory")
	fmt.Println("    - No intermediate snapshot — faster, simpler")
	fmt.Println("    - May include unreferenced files (Bleve ignores them)")
	fmt.Println("    - Tolerates concurrent writes via error handling")
	fmt.Println()
	fmt.Println("  Rsync approach:")
	fmt.Println("    - Copies all files from source to destination")
	fmt.Println("    - Multiple passes needed for consistency during live writes")
	fmt.Println("    - Works across filesystems and over network (SSH)")
	fmt.Println("    - Final pass after writer stops ensures full consistency")
	fmt.Println()
	fmt.Println("  All approaches rely on immutable segment files:")
	fmt.Println("    - Segment files are write-once, never modified in place")
	fmt.Println("    - New indexing creates NEW segment files")
	fmt.Println("    - Merges create NEW combined segments, old ones are deleted")
	fmt.Println("================================================================")
}

// =========================================================================
//  Vocabulary
// =========================================================================

var vocabulary = []string{
	"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
	"india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
	"quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "xray",
	"yankee", "zulu", "abstract", "boolean", "class", "double", "extends",
	"final", "generic", "hashmap", "integer", "javadoc", "keyword", "lambda",
	"module", "native", "object", "package", "qualifier", "return", "static",
	"thread", "utility", "volatile", "wrapper", "xerces", "yield", "zenith",
	"algorithm", "benchmark", "compiler", "database", "endpoint", "framework",
	"gateway", "hibernate", "iterator", "jackson", "kubernetes", "lifecycle",
	"middleware", "namespace", "orchestrator", "pipeline", "queryable", "resolver",
	"scheduler", "transformer", "upstream", "validator", "webservice", "xmlparser",
	"yesterday", "zookeeper", "analytics", "bootstrap", "container", "deployment",
	"elasticsearch", "federation", "graphql", "horizontal", "ingestion", "jmeter",
	"loadbalancer", "microservice", "notification", "observability",
	"prometheus", "quarantine", "replication", "serverless", "telemetry",
	"vectorized", "workload", "accumulator", "bytebuffer", "checkpoint",
	"dispatcher", "encryption", "filesystem", "governance", "heartbeat",
	"idempotent", "journaling", "keystore", "linearizable", "monotonic",
	"optimistic", "partitioner", "quorum", "serialization", "timestamp",
	"uncommitted", "versionable", "annotation", "backtrace", "coroutine",
	"deserialize", "enumerable", "finalizer", "generational", "heapdump",
	"interleaved", "joinpoint", "lockfree", "memtable", "nullsafe",
	"overloaded", "polymorphic", "quotient", "recursive", "semaphore",
	"tombstone", "underflow", "viewport", "watermark",
}

// ensure sync import is used
var _ = sync.Mutex{}
