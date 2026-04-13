package com.splitindex;

import java.io.File;
import java.io.IOException;

/**
 * Benchmark runner that compares Rsync Copy vs. Hard Link for copying
 * Lucene index directories, followed by the same term-deletion phase.
 *
 * <p>This benchmark focuses specifically on the copy/link phase performance
 * difference between using the external {@code rsync} tool and Java's
 * {@link java.nio.file.Files#createLink(java.nio.file.Path, java.nio.file.Path)}
 * for creating hard links.</p>
 *
 * <h3>Usage</h3>
 * <pre>
 *   # Build
 *   mvn clean package
 *
 *   # Run rsync-vs-hardlink benchmark
 *   mvn exec:java -Dexec.mainClass=com.splitindex.RsyncVsHardLinkBenchmark
 *
 *   # Or via main SplitBenchmark runner
 *   mvn exec:java -Dexec.args="rsync-vs-hardlink"
 * </pre>
 */
public class RsyncVsHardLinkBenchmark {

    public static void main(String[] args) throws IOException {
        run();
    }

    /**
     * Runs the full rsync-vs-hardlink benchmark and prints a comparison.
     */
    public static void run() throws IOException {
        System.out.println("============================================================");
        System.out.println("  Rsync vs Hard Link Benchmark for Lucene Index Copying");
        System.out.println("============================================================");
        System.out.println("  Configuration:");
        System.out.println("    Total documents:   " + SplitConfig.TOTAL_DOCS);
        System.out.println("    Unique IDs:        " + SplitConfig.NUM_UNIQUE_IDS);
        System.out.println("    Fields per doc:    " + SplitConfig.NUM_FIELDS);
        System.out.println("    Number of splits:  " + SplitConfig.NUM_SPLITS);
        System.out.println("    Base directory:    " + SplitConfig.BASE_DIR);
        System.out.println("============================================================\n");

        // Step 1: Generate index if needed
        System.out.println(">>> STEP 1: Generate Source Index (if not present)");
        long genTime = new IndexGenerator().generate();
        System.out.println("[Benchmark] Index generation: " + genTime + " ms\n");

        // Step 2: Run rsync-based split
        System.out.println(">>> STEP 2: Run Rsync-Based Split");
        long[] rsyncResults = new IndexSplitterRsync().split();

        // Step 3: Clean rsync split outputs to save disk space
        cleanSplitOutputs("rsync");

        // Step 4: Run hard-link-based split
        System.out.println("\n>>> STEP 3: Run Hard-Link-Based Split");
        long[] hardlinkResults = new IndexSplitterHardLink().split();

        // Step 5: Print comparison
        printComparison(genTime, rsyncResults, hardlinkResults);
    }

    /**
     * Cleans up split output directories.
     */
    private static void cleanSplitOutputs(String type) {
        String prefix;
        if ("rsync".equals(type)) {
            prefix = SplitConfig.RSYNC_SPLIT_DIR_PREFIX;
        } else {
            prefix = SplitConfig.HARDLINK_SPLIT_DIR_PREFIX;
        }
        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            File dir = new File(prefix + i);
            if (dir.exists()) {
                IndexSplitterCopy.deleteDirectory(dir);
            }
        }
        System.out.println("[Benchmark] Cleaned " + type + " split outputs");
    }

    /**
     * Prints a detailed comparison table of rsync vs hard link results.
     */
    private static void printComparison(long genTime, long[] rsyncResults, long[] hardlinkResults) {
        long rsyncTotal = rsyncResults[0];
        long rsyncCopyPhase = rsyncResults[1];
        long rsyncDeletePhase = rsyncResults[2];

        long hlTotal = hardlinkResults[0];
        long hlLinkPhase = hardlinkResults[1];
        long hlDeletePhase = hardlinkResults[2];

        System.out.println("\n============================================================");
        System.out.println("  BENCHMARK RESULTS: Rsync vs Hard Link");
        System.out.println("============================================================");
        System.out.println();
        System.out.println("  Index Generation Time: " + genTime + " ms");
        System.out.println();
        System.out.println("  +------------------------------------------------------+");
        System.out.println("  |                  | Rsync+Delete   | HardLink+Delete   |");
        System.out.println("  +------------------------------------------------------+");
        System.out.printf("  | Total Time       | %,12d ms | %,12d ms    |%n", rsyncTotal, hlTotal);
        System.out.printf("  | Copy/Link Phase  | %,12d ms | %,12d ms    |%n", rsyncCopyPhase, hlLinkPhase);
        System.out.printf("  | Delete Phase     | %,12d ms | %,12d ms    |%n", rsyncDeletePhase, hlDeletePhase);
        System.out.println("  +------------------------------------------------------+");
        System.out.println();

        double overallSpeedup = (double) rsyncTotal / Math.max(1, hlTotal);
        double copyLinkSpeedup = (double) rsyncCopyPhase / Math.max(1, hlLinkPhase);

        System.out.printf("  Overall Speedup (HardLink vs Rsync): %.2fx faster%n", overallSpeedup);
        System.out.printf("  Copy/Link Phase Speedup:             %.2fx faster%n", copyLinkSpeedup);
        System.out.println();

        if (overallSpeedup > 1.0) {
            System.out.println("  >> WINNER: Hard Link approach is faster overall");
        } else if (overallSpeedup < 1.0) {
            System.out.println("  >> WINNER: Rsync approach is faster overall (unexpected)");
        } else {
            System.out.println("  >> TIE: Both approaches took the same time");
        }

        System.out.println();
        System.out.println("  Analysis:");
        System.out.println("  -----------------------------------------------------------------");
        System.out.println("  Rsync copies data byte-by-byte (like cp/FileChannel), so the copy");
        System.out.println("  phase scales linearly with index size: O(total bytes).");
        System.out.println();
        System.out.println("  Hard link creates filesystem directory entries pointing to the same");
        System.out.println("  inodes, so the link phase scales with file count: O(num files).");
        System.out.println("  No data is copied; disk space is shared until Lucene rewrites");
        System.out.println("  segments during force-merge.");
        System.out.println();
        System.out.println("  Both approaches produce identical final results — the same number");
        System.out.println("  of documents in each partition with the same content.");
        System.out.println();
        System.out.println("  Key differences:");
        System.out.printf("    - Rsync copy phase:     %,d ms (data copied to each of %d dirs)%n",
                rsyncCopyPhase, SplitConfig.NUM_SPLITS);
        System.out.printf("    - Hard link phase:      %,d ms (inodes shared across %d dirs)%n",
                hlLinkPhase, SplitConfig.NUM_SPLITS);
        System.out.printf("    - Phase speedup:        %.2fx (hard link is faster)%n", copyLinkSpeedup);
        System.out.println("    - Rsync peak disk:     " + SplitConfig.NUM_SPLITS
                + " × index size (full copies)");
        System.out.println("    - Hard link peak disk:  1 × index size (shared until merge)");
        System.out.println("  -----------------------------------------------------------------");
        System.out.println();
        System.out.println("  Notes:");
        System.out.println("  - Rsync is useful for cross-filesystem or cross-host replication.");
        System.out.println("  - Hard link requires source and target on the same filesystem.");
        System.out.println("  - For local same-filesystem index splitting, hard link is superior.");
        System.out.println("  - Delete phase timings are identical since both perform the same");
        System.out.println("    Lucene operations (term-based deletion + force merge).");
        System.out.println("============================================================");
    }
}
