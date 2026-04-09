package com.splitindex;

import java.io.File;
import java.io.IOException;

/**
 * Benchmark runner that creates and benchmarks sample Lucene indices of
 * different sizes (5 GB and 10 GB) using both the sequential-copy and
 * hard-link splitting approaches.
 *
 * <h3>Usage</h3>
 * <pre>
 *   # Build
 *   mvn clean package
 *
 *   # Run all sample benchmarks (5 GB + 10 GB)
 *   mvn exec:java -Dexec.mainClass=com.splitindex.SampleIndexBenchmark
 *
 *   # Run only the 5 GB benchmark
 *   mvn exec:java -Dexec.mainClass=com.splitindex.SampleIndexBenchmark -Dexec.args="5gb"
 *
 *   # Run only the 10 GB benchmark
 *   mvn exec:java -Dexec.mainClass=com.splitindex.SampleIndexBenchmark -Dexec.args="10gb"
 *
 *   # Clean all sample benchmark data
 *   mvn exec:java -Dexec.mainClass=com.splitindex.SampleIndexBenchmark -Dexec.args="clean"
 *
 *   # Custom base directory
 *   mvn exec:java -Dexec.mainClass=com.splitindex.SampleIndexBenchmark \
 *       -Dsplitindex.basedir=/data/benchmark
 * </pre>
 *
 * <h3>Approximate Index Sizes</h3>
 * <ul>
 *   <li><strong>5 GB profile:</strong> ~2,600,000 docs, 100 fields, 1024 bytes/doc</li>
 *   <li><strong>10 GB profile:</strong> ~5,200,000 docs, 100 fields, 1024 bytes/doc</li>
 * </ul>
 */
public class SampleIndexBenchmark {

    public static void main(String[] args) throws IOException {
        String mode = (args.length > 0) ? args[0].toLowerCase() : "all";

        switch (mode) {
            case "5gb":
                runProfileBenchmark(BenchmarkProfile.profile5GB());
                break;
            case "10gb":
                runProfileBenchmark(BenchmarkProfile.profile10GB());
                break;
            case "clean":
                cleanAll();
                break;
            case "all":
            default:
                runAllBenchmarks();
                break;
        }
    }

    /**
     * Runs benchmarks for both the 5 GB and 10 GB profiles, then prints a
     * cross-profile comparison.
     */
    private static void runAllBenchmarks() throws IOException {
        BenchmarkProfile profile5 = BenchmarkProfile.profile5GB();
        BenchmarkProfile profile10 = BenchmarkProfile.profile10GB();

        printBanner();
        System.out.println("  Will benchmark two sample index sizes:");
        System.out.println("    1) " + profile5);
        System.out.println("    2) " + profile10);
        System.out.println("============================================================\n");

        // --- 5 GB benchmark ---
        long[] results5 = runProfileBenchmark(profile5);

        // --- 10 GB benchmark ---
        long[] results10 = runProfileBenchmark(profile10);

        // --- Cross-profile comparison ---
        if (results5 != null && results10 != null) {
            printCrossProfileComparison(profile5, results5, profile10, results10);
        }
    }

    /**
     * Runs the full benchmark (generate + copy-split + hardlink-split) for a
     * single profile and returns timing results.
     *
     * @return an array of 7 longs:
     *         [genTime, copyTotal, copyPhase, copyDelete, hlTotal, hlPhase, hlDelete],
     *         or {@code null} if the benchmark could not complete
     */
    private static long[] runProfileBenchmark(BenchmarkProfile profile) throws IOException {
        System.out.println("\n############################################################");
        System.out.println("  SAMPLE INDEX BENCHMARK: " + profile.getName().toUpperCase());
        System.out.println("############################################################");
        System.out.println("  " + profile);
        System.out.println();

        // Step 1: Generate index
        long genTime = generateIndex(profile);

        // Step 2: Copy-based split
        long[] copyResults = runCopySplit(profile);

        // Clean copy output to free disk space
        cleanSplitOutputs(profile, "copy");

        // Step 3: Hard-link-based split
        long[] hlResults = runHardLinkSplit(profile);

        // Step 4: Print comparison for this profile
        printProfileComparison(profile, genTime, copyResults, hlResults);

        // Clean hard-link output
        cleanSplitOutputs(profile, "hardlink");

        return new long[]{
                genTime,
                copyResults[0], copyResults[1], copyResults[2],
                hlResults[0], hlResults[1], hlResults[2]
        };
    }

    private static long generateIndex(BenchmarkProfile profile) throws IOException {
        System.out.println("\n>>> STEP: Generate Source Index (" + profile.getName() + ")");
        return new IndexGenerator(profile).generate();
    }

    private static long[] runCopySplit(BenchmarkProfile profile) throws IOException {
        System.out.println("\n>>> STEP: Run Copy-Based Split (" + profile.getName() + ")");
        return new IndexSplitterCopy(profile).split();
    }

    private static long[] runHardLinkSplit(BenchmarkProfile profile) throws IOException {
        System.out.println("\n>>> STEP: Run Hard-Link-Based Split (" + profile.getName() + ")");
        return new IndexSplitterHardLink(profile).split();
    }

    /**
     * Prints the comparison table for a single profile.
     */
    private static void printProfileComparison(BenchmarkProfile profile, long genTime,
                                               long[] copyResults, long[] hlResults) {
        System.out.println("\n============================================================");
        System.out.println("  BENCHMARK RESULTS: " + profile.getName().toUpperCase());
        System.out.println("============================================================");
        System.out.println();
        System.out.printf("  Profile:              %s%n", profile.getName());
        System.out.printf("  Total documents:      %,d%n", profile.getTotalDocs());
        System.out.printf("  Fields per doc:       %d%n", profile.getNumFields());
        System.out.printf("  Unique IDs:           %,d%n", profile.getNumUniqueIds());
        System.out.printf("  Number of splits:     %d%n", profile.getNumSplits());
        System.out.printf("  Index generation:     %,d ms%n", genTime);
        System.out.println();
        System.out.println("  +-------------------------------------------------+");
        System.out.println("  |                 | Copy+Delete | HardLink+Delete  |");
        System.out.println("  +-------------------------------------------------+");
        System.out.printf("  | Total Time      | %,10d ms | %,10d ms    |%n", copyResults[0], hlResults[0]);
        System.out.printf("  | Copy/Link Phase | %,10d ms | %,10d ms    |%n", copyResults[1], hlResults[1]);
        System.out.printf("  | Delete Phase    | %,10d ms | %,10d ms    |%n", copyResults[2], hlResults[2]);
        System.out.println("  +-------------------------------------------------+");
        System.out.println();

        double speedup = (double) copyResults[0] / Math.max(1, hlResults[0]);
        double copyLinkSpeedup = (double) copyResults[1] / Math.max(1, hlResults[1]);
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
        System.out.println("============================================================\n");
    }

    /**
     * Prints a side-by-side comparison between two different profile benchmark runs.
     */
    private static void printCrossProfileComparison(BenchmarkProfile p1, long[] r1,
                                                    BenchmarkProfile p2, long[] r2) {
        // r[]: genTime, copyTotal, copyPhase, copyDelete, hlTotal, hlPhase, hlDelete
        System.out.println("\n############################################################");
        System.out.println("  CROSS-PROFILE COMPARISON");
        System.out.println("############################################################");
        System.out.println();
        System.out.printf("  %-25s | %15s | %15s%n", "Metric", p1.getName(), p2.getName());
        System.out.println("  " + repeat("-", 25) + "-+-" + repeat("-", 15) + "-+-" + repeat("-", 15));
        System.out.printf("  %-25s | %,13d ms | %,13d ms%n", "Index Generation", r1[0], r2[0]);
        System.out.printf("  %-25s | %,13d ms | %,13d ms%n", "Copy+Delete Total", r1[1], r2[1]);
        System.out.printf("  %-25s | %,13d ms | %,13d ms%n", "  Copy Phase", r1[2], r2[2]);
        System.out.printf("  %-25s | %,13d ms | %,13d ms%n", "  Delete Phase", r1[3], r2[3]);
        System.out.printf("  %-25s | %,13d ms | %,13d ms%n", "HardLink+Delete Total", r1[4], r2[4]);
        System.out.printf("  %-25s | %,13d ms | %,13d ms%n", "  Link Phase", r1[5], r2[5]);
        System.out.printf("  %-25s | %,13d ms | %,13d ms%n", "  Delete Phase", r1[6], r2[6]);
        System.out.println();

        double speedup5 = (double) r1[1] / Math.max(1, r1[4]);
        double speedup10 = (double) r2[1] / Math.max(1, r2[4]);
        System.out.printf("  Overall HardLink Speedup (%s): %.2fx%n", p1.getName(), speedup5);
        System.out.printf("  Overall HardLink Speedup (%s): %.2fx%n", p2.getName(), speedup10);
        System.out.println();
        System.out.println("  Key Insight: As index size grows, the hard link advantage");
        System.out.println("  becomes more pronounced because the copy phase scales with");
        System.out.println("  O(file size) while hard linking scales with O(file count).");
        System.out.println("############################################################\n");
    }

    /**
     * Cleans split output directories for a profile.
     */
    private static void cleanSplitOutputs(BenchmarkProfile profile, String type) {
        String prefix = "copy".equals(type)
                ? profile.getCopySplitDirPrefix()
                : profile.getHardlinkSplitDirPrefix();
        for (int i = 0; i < profile.getNumSplits(); i++) {
            File dir = new File(prefix + i);
            if (dir.exists()) {
                IndexSplitterCopy.deleteDirectory(dir);
            }
        }
        System.out.println("[SampleBenchmark] Cleaned " + type + " split outputs for " + profile.getName());
    }

    /**
     * Cleans all sample benchmark data (both profiles).
     */
    private static void cleanAll() {
        System.out.println("\n>>> Cleaning all sample benchmark data...");
        BenchmarkProfile[] profiles = {BenchmarkProfile.profile5GB(), BenchmarkProfile.profile10GB()};
        for (BenchmarkProfile profile : profiles) {
            File sourceDir = new File(profile.getSourceIndexDir());
            if (sourceDir.exists()) {
                IndexSplitterCopy.deleteDirectory(sourceDir);
                System.out.println("[Clean] Deleted: " + sourceDir.getAbsolutePath());
            }
            cleanSplitOutputs(profile, "copy");
            cleanSplitOutputs(profile, "hardlink");
        }
        System.out.println("[Clean] Done.");
    }

    private static void printBanner() {
        System.out.println("============================================================");
        System.out.println("  Lucene 4 Index Splitting — Sample Index Benchmark");
        System.out.println("============================================================");
    }

    private static String repeat(String s, int count) {
        StringBuilder sb = new StringBuilder(s.length() * count);
        for (int i = 0; i < count; i++) {
            sb.append(s);
        }
        return sb.toString();
    }
}
