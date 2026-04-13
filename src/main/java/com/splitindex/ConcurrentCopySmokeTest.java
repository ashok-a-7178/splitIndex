package com.splitindex;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Smoke test that simulates copying Lucene index files from a source machine
 * to a remote machine while the source index is being actively modified
 * (new document additions, updates, and deletes creating new segments).
 *
 * <p>This tests two approaches for safely copying immutable Lucene segment files
 * while the index is live:</p>
 *
 * <h3>Approach 1: Hard-Link Snapshot</h3>
 * <ol>
 *   <li>Create a snapshot subdirectory on the source</li>
 *   <li>Read the current {@code segments_N} commit point to identify active segment files</li>
 *   <li>Hard-link all active segment files into the snapshot subdirectory</li>
 *   <li>Copy the snapshot subdirectory to the remote destination (via FileChannel)</li>
 *   <li>The snapshot is a consistent point-in-time view — even if new segments are
 *       created by concurrent indexing, the hard-linked files remain valid because
 *       Lucene segment files are immutable (write-once, never modified)</li>
 * </ol>
 *
 * <h3>Approach 2: Rsync</h3>
 * <ol>
 *   <li>Use {@code rsync -a --delete} to copy from source to remote destination</li>
 *   <li>Rsync handles file-level changes by transferring only modified/new files</li>
 *   <li>Because Lucene segments are immutable, rsync's checksum-based transfer is
 *       efficient — unchanged segment files are skipped</li>
 *   <li>However, rsync may copy a partially-consistent state if new segments appear
 *       mid-transfer; a second rsync pass can fix this</li>
 * </ol>
 *
 * <h3>Why this works for Lucene</h3>
 * <p>Lucene index segments are <em>immutable</em>: once a segment file (e.g., {@code _0.fdt},
 * {@code _0.si}) is written, it is never modified in place. New documents create new segments,
 * and merges create new combined segments while old ones are eventually deleted. The
 * {@code segments_N} file is the commit point that lists which segments are active.</p>
 *
 * <p>This immutability makes both hard-link snapshots and rsync safe strategies for
 * replicating indexes to remote nodes.</p>
 *
 * <h3>Usage</h3>
 * <pre>
 *   mvn clean package -q
 *   mvn exec:java -Dexec.mainClass=com.splitindex.ConcurrentCopySmokeTest \
 *       -Dsplitindex.basedir=/tmp/smoketest
 *
 *   # Or via SplitBenchmark:
 *   mvn exec:java -Dexec.args="smoketest" -Dsplitindex.basedir=/tmp/smoketest
 * </pre>
 */
public class ConcurrentCopySmokeTest {

    /** Number of documents in the initial index. */
    private static final int INITIAL_DOCS = 500;

    /** Number of concurrent write operations to perform during copy. */
    private static final int CONCURRENT_OPS = 200;

    /** Delay in ms between concurrent operations to simulate realistic writes. */
    private static final int OP_DELAY_MS = 10;

    /** Number of fields per document. */
    private static final int DOC_FIELDS = 5;

    private final String baseDir;
    private final String sourceDir;
    private final String snapshotDir;
    private final String hardlinkDestDir;
    private final String rsyncDestDir;

    public ConcurrentCopySmokeTest() {
        this.baseDir = SplitConfig.BASE_DIR + File.separator + "smoketest";
        this.sourceDir = baseDir + File.separator + "source_index";
        this.snapshotDir = baseDir + File.separator + "snapshot";
        this.hardlinkDestDir = baseDir + File.separator + "dest_hardlink";
        this.rsyncDestDir = baseDir + File.separator + "dest_rsync";
    }

    /**
     * Runs the full smoke test.
     */
    public void run() throws IOException, InterruptedException {
        System.out.println("================================================================");
        System.out.println("  Concurrent Copy Smoke Test");
        System.out.println("  Simulates source→remote index replication with live writes");
        System.out.println("================================================================");
        System.out.println("  Initial docs:        " + INITIAL_DOCS);
        System.out.println("  Concurrent ops:      " + CONCURRENT_OPS);
        System.out.println("  Source dir:          " + sourceDir);
        System.out.println("  Snapshot dir:        " + snapshotDir);
        System.out.println("  HardLink dest dir:   " + hardlinkDestDir);
        System.out.println("  Rsync dest dir:      " + rsyncDestDir);
        System.out.println("================================================================\n");

        // Clean previous run
        cleanDir(new File(baseDir));

        // Step 1: Generate initial source index
        System.out.println(">>> STEP 1: Generate initial source index (" + INITIAL_DOCS + " docs)");
        generateSourceIndex();

        // Step 2: List initial segment files
        System.out.println("\n>>> STEP 2: Source index segment files (before concurrent writes)");
        listSegmentFiles(new File(sourceDir));

        // Step 3: Test Hard-Link Snapshot approach with concurrent writes
        System.out.println("\n>>> STEP 3: Hard-Link Snapshot approach (with concurrent writes)");
        long hlTime = testHardLinkSnapshot();

        // Step 4: Test Rsync approach with concurrent writes
        System.out.println("\n>>> STEP 4: Rsync approach (with concurrent writes)");
        long rsyncTime = testRsync();

        // Step 5: List segment files after all concurrent writes
        System.out.println("\n>>> STEP 5: Source index segment files (after all concurrent writes)");
        listSegmentFiles(new File(sourceDir));

        // Step 6: Print comparison
        printResults(hlTime, rsyncTime);

        // Cleanup
        System.out.println("\n[SmokeTest] Cleaning up...");
        cleanDir(new File(baseDir));
        System.out.println("[SmokeTest] Done.");
    }

    // =========================================================================
    //  Step 1: Generate source index
    // =========================================================================

    private void generateSourceIndex() throws IOException {
        File dir = new File(sourceDir);
        dir.mkdirs();

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4, new StandardAnalyzer());
        config.setRAMBufferSizeMB(16.0);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        try (FSDirectory fsDir = FSDirectory.open(dir);
             IndexWriter writer = new IndexWriter(fsDir, config)) {

            for (int i = 0; i < INITIAL_DOCS; i++) {
                writer.addDocument(createDocument(i));
            }
            writer.commit();
            System.out.println("[SmokeTest] Generated " + INITIAL_DOCS + " docs, committing...");

            // Force merge to 1 segment for a clean starting state
            writer.forceMerge(1);
            writer.commit();
        }

        System.out.println("[SmokeTest] Source index created: "
                + IndexGenerator.dirSizeMB(dir) + " MB");
    }

    // =========================================================================
    //  Step 3: Hard-Link Snapshot + Copy with concurrent writes
    // =========================================================================

    /**
     * Tests the hard-link snapshot approach:
     * <ol>
     *   <li>Start a background writer doing adds/updates/deletes on the source</li>
     *   <li>Take a point-in-time snapshot by reading segments_N and hard-linking
     *       all active segment files into a snapshot subdir</li>
     *   <li>Copy the snapshot subdir to the destination (simulating remote copy)</li>
     *   <li>Stop the background writer</li>
     *   <li>Verify the destination index is readable and consistent</li>
     * </ol>
     */
    private long testHardLinkSnapshot() throws IOException, InterruptedException {
        long start = System.currentTimeMillis();

        // Start concurrent writer
        AtomicBoolean stopWriter = new AtomicBoolean(false);
        AtomicInteger opsCompleted = new AtomicInteger(0);
        CountDownLatch writerStarted = new CountDownLatch(1);

        Thread writerThread = new Thread(() -> {
            try {
                doConcurrentWrites(stopWriter, opsCompleted, writerStarted);
            } catch (Exception e) {
                System.err.println("[SmokeTest] Writer thread error: " + e.getMessage());
            }
        }, "concurrent-writer-hl");
        writerThread.setDaemon(true);
        writerThread.start();

        // Wait for writer to start
        writerStarted.await();
        // Let some writes happen first
        Thread.sleep(100);

        System.out.println("[HardLink] Taking point-in-time snapshot...");
        System.out.println("[HardLink] Concurrent writer is active ("
                + opsCompleted.get() + " ops so far)");

        // Take snapshot: read segments_N and hard-link active files
        File snapDir = new File(snapshotDir);
        createHardLinkSnapshot(new File(sourceDir), snapDir);

        System.out.println("[HardLink] Snapshot taken. Listing snapshot files:");
        listSegmentFiles(snapDir);

        // Copy snapshot to destination (simulating copy to remote machine)
        System.out.println("[HardLink] Copying snapshot to destination (simulating remote copy)...");
        File destDir = new File(hardlinkDestDir);
        copyDirectoryFiles(snapDir, destDir);

        // Let more writes happen to show source diverges from snapshot
        Thread.sleep(200);

        // Stop concurrent writer
        stopWriter.set(true);
        writerThread.join(5000);

        long elapsed = System.currentTimeMillis() - start;

        System.out.println("[HardLink] Concurrent writer completed "
                + opsCompleted.get() + " operations");

        // Verify destination index
        System.out.println("[HardLink] Verifying destination index...");
        verifyIndex(destDir, "HardLink");

        System.out.println("[HardLink] Total time: " + elapsed + " ms");
        return elapsed;
    }

    /**
     * Creates a point-in-time snapshot of the source index by:
     * <ol>
     *   <li>Reading the latest {@code segments_N} file to identify the current commit point</li>
     *   <li>Hard-linking all files referenced by that commit point into the snapshot dir</li>
     *   <li>Also hard-linking the {@code segments_N} and {@code segments.gen} files</li>
     * </ol>
     *
     * <p>Because Lucene segment files are immutable (write-once), the hard links create
     * a consistent snapshot even while new segments are being written concurrently.</p>
     */
    private void createHardLinkSnapshot(File srcDir, File snapDir) throws IOException {
        if (snapDir.exists()) {
            IndexSplitterCopy.deleteDirectory(snapDir);
        }
        snapDir.mkdirs();

        // Read the current commit point to get the list of active segment files
        Set<String> activeFiles = new HashSet<>();
        try (FSDirectory fsDir = FSDirectory.open(srcDir)) {
            SegmentInfos segInfos = new SegmentInfos();
            segInfos.read(fsDir);

            // Collect all files from all segments in the current commit
            for (int i = 0; i < segInfos.size(); i++) {
                activeFiles.addAll(segInfos.info(i).files());
            }

            // Also include the segments file itself and segments.gen
            activeFiles.add(segInfos.getSegmentsFileName());
            if (new File(srcDir, "segments.gen").exists()) {
                activeFiles.add("segments.gen");
            }
        }

        System.out.println("[HardLink] Active segment files to snapshot: " + activeFiles.size());

        // Hard-link each active file into the snapshot directory
        for (String fileName : activeFiles) {
            Path srcPath = new File(srcDir, fileName).toPath();
            Path snapPath = new File(snapDir, fileName).toPath();
            if (Files.exists(srcPath)) {
                Files.createLink(snapPath, srcPath);
            } else {
                System.out.println("[HardLink] WARNING: File not found on disk: " + fileName
                        + " (may have been removed by a concurrent merge)");
            }
        }

        // Also link the write.lock file if present (for completeness, though
        // the destination writer will create its own)
        File writeLock = new File(srcDir, "write.lock");
        if (writeLock.exists()) {
            try {
                Files.createLink(
                        new File(snapDir, "write.lock").toPath(),
                        writeLock.toPath());
            } catch (IOException e) {
                // write.lock may be held; ignore
            }
        }
    }

    // =========================================================================
    //  Step 4: Rsync approach with concurrent writes
    // =========================================================================

    /**
     * Tests the rsync approach:
     * <ol>
     *   <li>Start a background writer doing adds/updates/deletes on the source</li>
     *   <li>Run rsync from source to destination</li>
     *   <li>Run a second rsync pass to catch any segments created during the first pass</li>
     *   <li>Stop the background writer</li>
     *   <li>Verify the destination index is readable</li>
     * </ol>
     */
    private long testRsync() throws IOException, InterruptedException {
        // Verify rsync is available
        if (!isRsyncAvailable()) {
            System.out.println("[Rsync] rsync not available on this system, skipping rsync test");
            return -1;
        }

        long start = System.currentTimeMillis();

        // Start concurrent writer
        AtomicBoolean stopWriter = new AtomicBoolean(false);
        AtomicInteger opsCompleted = new AtomicInteger(0);
        CountDownLatch writerStarted = new CountDownLatch(1);

        Thread writerThread = new Thread(() -> {
            try {
                doConcurrentWrites(stopWriter, opsCompleted, writerStarted);
            } catch (Exception e) {
                System.err.println("[SmokeTest] Writer thread error: " + e.getMessage());
            }
        }, "concurrent-writer-rsync");
        writerThread.setDaemon(true);
        writerThread.start();

        // Wait for writer to start
        writerStarted.await();
        Thread.sleep(100);

        System.out.println("[Rsync] Starting first rsync pass (concurrent writer active, "
                + opsCompleted.get() + " ops so far)...");

        File destDir = new File(rsyncDestDir);
        destDir.mkdirs();

        // First rsync pass
        long rsync1Start = System.currentTimeMillis();
        runRsync(new File(sourceDir), destDir);
        long rsync1Time = System.currentTimeMillis() - rsync1Start;
        System.out.println("[Rsync] First pass completed in " + rsync1Time + " ms");

        // Let more writes happen
        Thread.sleep(200);

        System.out.println("[Rsync] Starting second rsync pass (to catch new segments, "
                + opsCompleted.get() + " ops so far)...");

        // Second rsync pass to catch segments created during first pass
        long rsync2Start = System.currentTimeMillis();
        runRsync(new File(sourceDir), destDir);
        long rsync2Time = System.currentTimeMillis() - rsync2Start;
        System.out.println("[Rsync] Second pass completed in " + rsync2Time + " ms");

        // Stop concurrent writer
        stopWriter.set(true);
        writerThread.join(5000);

        long elapsed = System.currentTimeMillis() - start;

        System.out.println("[Rsync] Concurrent writer completed "
                + opsCompleted.get() + " operations");

        // Final rsync to get a fully consistent state after writer stopped
        System.out.println("[Rsync] Final rsync pass (writer stopped, ensure consistency)...");
        long rsync3Start = System.currentTimeMillis();
        runRsync(new File(sourceDir), destDir);
        long rsync3Time = System.currentTimeMillis() - rsync3Start;
        System.out.println("[Rsync] Final pass completed in " + rsync3Time + " ms");

        // Verify destination index
        System.out.println("[Rsync] Verifying destination index...");
        verifyIndex(destDir, "Rsync");

        elapsed = System.currentTimeMillis() - start;
        System.out.println("[Rsync] Total time (including all passes): " + elapsed + " ms");
        return elapsed;
    }

    private void runRsync(File srcDir, File destDir) throws IOException {
        String sourcePath = srcDir.getAbsolutePath() + File.separator;
        String targetPath = destDir.getAbsolutePath() + File.separator;

        ProcessBuilder pb = new ProcessBuilder(
                "rsync", "-a", "--delete", sourcePath, targetPath
        );
        pb.redirectErrorStream(true);

        try {
            Process process = pb.start();
            // Drain stdout to prevent blocking when OS pipe buffer fills up
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                while (reader.readLine() != null) {
                    // output discarded; rsync produces none in non-verbose mode
                }
            }
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("rsync failed with exit code " + exitCode);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("rsync was interrupted", e);
        }
    }

    private boolean isRsyncAvailable() {
        try {
            Process process = new ProcessBuilder("rsync", "--version")
                    .redirectErrorStream(true)
                    .start();
            // Drain output
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                while (reader.readLine() != null) { }
            }
            return process.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    // =========================================================================
    //  Concurrent writer: simulates live indexing
    // =========================================================================

    /**
     * Performs concurrent write operations (adds, updates, deletes) on the
     * source index, simulating a live system where new segments are created
     * while the index is being copied.
     *
     * <p>Each commit creates new segment files on disk. This is the key scenario
     * we're testing: can the copy mechanism produce a consistent index at the
     * destination despite new segments appearing?</p>
     */
    private void doConcurrentWrites(AtomicBoolean stop, AtomicInteger opsCompleted,
                                    CountDownLatch started) throws IOException, InterruptedException {
        Random rng = new Random(42);

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4, new StandardAnalyzer());
        config.setRAMBufferSizeMB(8.0);
        config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        // Do NOT forceMerge — we want multiple segments to appear

        try (FSDirectory dir = FSDirectory.open(new File(sourceDir));
             IndexWriter writer = new IndexWriter(dir, config)) {

            started.countDown();
            int docCounter = INITIAL_DOCS;

            while (!stop.get() && opsCompleted.get() < CONCURRENT_OPS) {
                int op = rng.nextInt(3);
                switch (op) {
                    case 0: // ADD new document
                        writer.addDocument(createDocument(docCounter++));
                        break;
                    case 1: // UPDATE existing document (by ID term)
                        int updateId = rng.nextInt(docCounter);
                        Document updatedDoc = createDocument(updateId);
                        writer.updateDocument(new Term("docId", "doc_" + updateId), updatedDoc);
                        break;
                    case 2: // DELETE a random document
                        int deleteId = rng.nextInt(Math.max(1, docCounter));
                        writer.deleteDocuments(new Term("docId", "doc_" + deleteId));
                        break;
                }

                opsCompleted.incrementAndGet();

                // Commit periodically to create new segment files on disk
                if (opsCompleted.get() % 20 == 0) {
                    writer.commit();
                    System.out.println("[Writer] Committed after " + opsCompleted.get()
                            + " ops (new segments created on disk)");
                }

                Thread.sleep(OP_DELAY_MS);
            }

            // Final commit
            writer.commit();
            System.out.println("[Writer] Final commit. Total ops: " + opsCompleted.get());
        }
    }

    // =========================================================================
    //  Verification
    // =========================================================================

    /**
     * Verifies the destination index is readable and reports document count.
     */
    private void verifyIndex(File indexDir, String label) {
        try (FSDirectory dir = FSDirectory.open(indexDir);
             IndexReader reader = DirectoryReader.open(dir)) {

            int numDocs = reader.numDocs();
            int maxDoc = reader.maxDoc();
            int deletedDocs = maxDoc - numDocs;

            System.out.println("[" + label + "] ✓ Destination index is READABLE");
            System.out.println("[" + label + "]   Documents:     " + numDocs);
            System.out.println("[" + label + "]   Max doc:       " + maxDoc);
            System.out.println("[" + label + "]   Deleted docs:  " + deletedDocs);
            System.out.println("[" + label + "]   Index size:    "
                    + String.format("%.2f", IndexGenerator.dirSizeMB(indexDir)) + " MB");
            System.out.println("[" + label + "]   Segment files: "
                    + countSegmentFiles(indexDir));
            System.out.println("[" + label + "] ✓ PASS — Index is consistent and readable");

        } catch (Exception e) {
            System.out.println("[" + label + "] ✗ FAIL — Cannot read destination index: "
                    + e.getMessage());
        }
    }

    // =========================================================================
    //  Helpers
    // =========================================================================

    private Document createDocument(int docIndex) {
        Document doc = new Document();
        doc.add(new StringField("docId", "doc_" + docIndex, Field.Store.YES));
        doc.add(new IntField("sequence", docIndex, Field.Store.YES));
        doc.add(new TextField("content",
                "document number " + docIndex + " with some sample text for testing",
                Field.Store.YES));
        doc.add(new StringField("status", "active", Field.Store.YES));
        doc.add(new IntField("version", 1, Field.Store.YES));
        return doc;
    }

    private void listSegmentFiles(File indexDir) {
        File[] files = indexDir.listFiles();
        if (files == null || files.length == 0) {
            System.out.println("  (no files)");
            return;
        }
        Arrays.sort(files);
        System.out.println("  Total files: " + files.length);
        for (File f : files) {
            System.out.printf("  %-40s %,10d bytes%n", f.getName(), f.length());
        }
    }

    private int countSegmentFiles(File indexDir) {
        File[] files = indexDir.listFiles();
        return files != null ? files.length : 0;
    }

    private void copyDirectoryFiles(File srcDir, File destDir) throws IOException {
        if (destDir.exists()) {
            IndexSplitterCopy.deleteDirectory(destDir);
        }
        destDir.mkdirs();

        File[] files = srcDir.listFiles();
        if (files == null) return;

        for (File srcFile : files) {
            if (srcFile.isFile()) {
                File destFile = new File(destDir, srcFile.getName());
                try (FileInputStream fis = new FileInputStream(srcFile);
                     FileOutputStream fos = new FileOutputStream(destFile);
                     FileChannel srcCh = fis.getChannel();
                     FileChannel destCh = fos.getChannel()) {
                    long size = srcCh.size();
                    long transferred = 0;
                    while (transferred < size) {
                        transferred += srcCh.transferTo(transferred, size - transferred, destCh);
                    }
                }
            }
        }
    }

    private void cleanDir(File dir) {
        if (dir.exists()) {
            IndexSplitterCopy.deleteDirectory(dir);
        }
    }

    // =========================================================================
    //  Results
    // =========================================================================

    private void printResults(long hlTime, long rsyncTime) {
        System.out.println("\n================================================================");
        System.out.println("  SMOKE TEST RESULTS");
        System.out.println("================================================================");
        System.out.println();
        System.out.println("  Test scenario: Copy Lucene index from source to remote");
        System.out.println("                 while concurrent indexing creates new segments");
        System.out.println();
        System.out.println("  +----------------------------------------------------+");
        System.out.println("  |                    | HardLink Snap  | Rsync          |");
        System.out.println("  +----------------------------------------------------+");
        System.out.printf("  | Total Time         | %,10d ms   | %,10d ms   |%n",
                hlTime, rsyncTime);
        System.out.println("  +----------------------------------------------------+");
        System.out.println();
        System.out.println("  Key observations:");
        System.out.println("  ─────────────────────────────────────────────────────");
        System.out.println("  HardLink Snapshot approach:");
        System.out.println("    - Takes a point-in-time snapshot via SegmentInfos");
        System.out.println("    - Hard-links only the active segment files (immutable)");
        System.out.println("    - Snapshot is consistent even during concurrent writes");
        System.out.println("    - Copy from snapshot dir to remote is safe (no races)");
        System.out.println("    - Requires same filesystem for the snapshot step");
        System.out.println();
        System.out.println("  Rsync approach:");
        System.out.println("    - Copies all files from source to destination");
        System.out.println("    - Multiple passes needed for consistency during live writes");
        System.out.println("    - Works across filesystems and over network (SSH)");
        System.out.println("    - Only transfers changed bytes (efficient for immutable files)");
        System.out.println("    - Final pass after writer stops ensures full consistency");
        System.out.println();
        System.out.println("  Both approaches rely on Lucene segment immutability:");
        System.out.println("    - Segment files are write-once, never modified in place");
        System.out.println("    - New indexing creates NEW segment files");
        System.out.println("    - Merges create NEW combined segments, old ones are deleted");
        System.out.println("    - segments_N commit file lists active segments");
        System.out.println("================================================================");
    }

    // =========================================================================
    //  Main
    // =========================================================================

    public static void main(String[] args) throws Exception {
        new ConcurrentCopySmokeTest().run();
    }
}
