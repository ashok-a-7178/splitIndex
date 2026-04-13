package com.splitindex;

import java.io.File;
import java.io.IOException;

/**
 * Main benchmark runner that orchestrates the full benchmark:
 * <ol>
 *   <li>Generates a test Lucene 4 index (if not already present)</li>
 *   <li>Runs Approach 1: Sequential Copy + Term Deletion</li>
 *   <li>Runs Approach 2: Hard Link + Term Deletion</li>
 *   <li>Prints a comparative summary</li>
 * </ol>
 *
 * <h3>Usage</h3>
 * <pre>
 *   # Build
 *   mvn clean package
 *
 *   # Run with defaults (100K docs for quick test)
 *   mvn exec:java
 *
 *   # Or run the JAR directly
 *   java -cp target/splitIndex-1.0-SNAPSHOT.jar com.splitindex.SplitBenchmark
 *
 *   # Run with custom base directory
 *   java -Dsplitindex.basedir=/data/benchmark -cp target/splitIndex-1.0-SNAPSHOT.jar com.splitindex.SplitBenchmark
 *
 *   # Run individual steps
 *   java -cp target/splitIndex-1.0-SNAPSHOT.jar com.splitindex.SplitBenchmark generate
 *   java -cp target/splitIndex-1.0-SNAPSHOT.jar com.splitindex.SplitBenchmark copy
 *   java -cp target/splitIndex-1.0-SNAPSHOT.jar com.splitindex.SplitBenchmark hardlink
 *   java -cp target/splitIndex-1.0-SNAPSHOT.jar com.splitindex.SplitBenchmark clean
 * </pre>
 */
public class SplitBenchmark {

    public static void main(String[] args) throws IOException {
        System.out.println("============================================================");
        System.out.println("  Lucene 4 Index Splitting Benchmark");
        System.out.println("============================================================");
        System.out.println("  Configuration:");
        System.out.println("    Total documents:   " + SplitConfig.TOTAL_DOCS);
        System.out.println("    Unique IDs:        " + SplitConfig.NUM_UNIQUE_IDS);
        System.out.println("    Fields per doc:    " + SplitConfig.NUM_FIELDS);
        System.out.println("    Number of splits:  " + SplitConfig.NUM_SPLITS);
        System.out.println("    Base directory:    " + SplitConfig.BASE_DIR);
        System.out.println("============================================================\n");

        String mode = (args.length > 0) ? args[0].toLowerCase() : "all";

        switch (mode) {
            case "generate":
                runGenerate();
                break;
            case "copy":
                runCopySplit();
                break;
            case "hardlink":
                runHardLinkSplit();
                break;
            case "rsync":
                runRsyncSplit();
                break;
            case "rsync-vs-hardlink":
                RsyncVsHardLinkBenchmark.run();
                break;
            case "clean":
                runClean();
                break;
            case "all":
            default:
                runFullBenchmark();
                break;
        }
    }

    private static void runFullBenchmark() throws IOException {
        // Step 1: Generate index
        long genTime = runGenerate();

        // Step 2: Run copy-based split
        long[] copyResults = runCopySplit();

        // Step 3: Clean copy split output (to avoid disk space issues)
        cleanSplitOutputs("copy");

        // Step 4: Run hard-link-based split
        long[] hardlinkResults = runHardLinkSplit();

        // Step 5: Print comparison
        printComparison(genTime, copyResults, hardlinkResults);

        // Step 6: Clean up
        System.out.println("\n[Benchmark] To clean up all data, run: SplitBenchmark clean");
    }

    private static long runGenerate() throws IOException {
        System.out.println("\n>>> STEP: Generate Source Index");
        return new IndexGenerator().generate();
    }

    private static long[] runCopySplit() throws IOException {
        System.out.println("\n>>> STEP: Run Copy-Based Split");
        return new IndexSplitterCopy().split();
    }

    private static long[] runHardLinkSplit() throws IOException {
        System.out.println("\n>>> STEP: Run Hard-Link-Based Split");
        return new IndexSplitterHardLink().split();
    }

    private static long[] runRsyncSplit() throws IOException {
        System.out.println("\n>>> STEP: Run Rsync-Based Split");
        return new IndexSplitterRsync().split();
    }

    private static void runClean() {
        System.out.println("\n>>> STEP: Clean All Benchmark Data");
        File baseDir = new File(SplitConfig.BASE_DIR);
        if (baseDir.exists()) {
            IndexSplitterCopy.deleteDirectory(baseDir);
            System.out.println("[Clean] Deleted: " + baseDir.getAbsolutePath());
        } else {
            System.out.println("[Clean] Nothing to clean.");
        }
    }

    private static void cleanSplitOutputs(String type) {
        String prefix;
        switch (type) {
            case "copy":
                prefix = SplitConfig.COPY_SPLIT_DIR_PREFIX;
                break;
            case "rsync":
                prefix = SplitConfig.RSYNC_SPLIT_DIR_PREFIX;
                break;
            default:
                prefix = SplitConfig.HARDLINK_SPLIT_DIR_PREFIX;
                break;
        }
        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            File dir = new File(prefix + i);
            if (dir.exists()) {
                IndexSplitterCopy.deleteDirectory(dir);
            }
        }
        System.out.println("[Benchmark] Cleaned " + type + " split outputs");
    }

    private static void printComparison(long genTime, long[] copyResults, long[] hardlinkResults) {
        System.out.println("\n============================================================");
        System.out.println("  BENCHMARK RESULTS COMPARISON");
        System.out.println("============================================================");
        System.out.println();
        System.out.println("  Index Generation Time:  " + genTime + " ms");
        System.out.println();
        System.out.println("  +-------------------------------------------------+");
        System.out.println("  |                 | Copy+Delete | HardLink+Delete  |");
        System.out.println("  +-------------------------------------------------+");
        System.out.printf("  | Total Time      | %,10d ms | %,10d ms    |%n", copyResults[0], hardlinkResults[0]);
        System.out.printf("  | Copy/Link Phase | %,10d ms | %,10d ms    |%n", copyResults[1], hardlinkResults[1]);
        System.out.printf("  | Delete Phase    | %,10d ms | %,10d ms    |%n", copyResults[2], hardlinkResults[2]);
        System.out.println("  +-------------------------------------------------+");
        System.out.println();

        double speedup = (double) copyResults[0] / hardlinkResults[0];
        double copyLinkSpeedup = (double) copyResults[1] / Math.max(1, hardlinkResults[1]);
        System.out.printf("  Overall Speedup (HardLink vs Copy): %.2fx faster%n", speedup);
        System.out.printf("  Copy/Link Phase Speedup:            %.2fx faster%n", copyLinkSpeedup);
        System.out.println();

        if (speedup > 1.0) {
            System.out.println("  >> WINNER: Hard Link approach is faster overall");
        } else if (speedup < 1.0) {
            System.out.println("  >> WINNER: Copy approach is faster overall (unexpected)");
        } else {
            System.out.println("  >> TIE: Both approaches took the same time");
        }

        System.out.println();
        System.out.println("  Notes:");
        System.out.println("  - The copy/link phase difference is most significant for large indices.");
        System.out.println("  - With 40GB index, hard link phase should be near-instant vs minutes for copy.");
        System.out.println("  - Delete phase timings should be similar since both do the same Lucene operations.");
        System.out.println("  - Hard link approach also saves disk space during the split process.");
        System.out.println("============================================================");
    }
}
