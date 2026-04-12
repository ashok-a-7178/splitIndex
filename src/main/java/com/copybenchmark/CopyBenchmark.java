package com.copybenchmark;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * Main entry point for the <strong>rsync vs hard-link copy benchmark</strong>.
 *
 * <h3>Workflow</h3>
 * <ol>
 *   <li>Seed a source Lucene index with {@link CopyBenchmarkConfig#SEED_DOC_COUNT} documents.</li>
 *   <li>Start a live-indexing thread that continuously writes to the source.</li>
 *   <li>While live indexing is running, copy the source to a destination using <em>rsync</em>.</li>
 *   <li>Collect time, CPU, memory and IO metrics during the rsync copy.</li>
 *   <li>Validate data availability at the rsync destination.</li>
 *   <li>Clean up the rsync destination.</li>
 *   <li>Repeat steps 2-6 using <em>hard-link</em> copy.</li>
 *   <li>Print a side-by-side comparison of both approaches.</li>
 * </ol>
 *
 * <p>Run with: {@code mvn exec:java -Dexec.mainClass=com.copybenchmark.CopyBenchmark}</p>
 */
public class CopyBenchmark {

    /* ------------------------------------------------------------------ */
    /*  Benchmark result holder                                            */
    /* ------------------------------------------------------------------ */

    private static final class RunResult {
        final String method;
        final long copyTimeMs;
        final int liveDocsAdded;
        final boolean dataValid;
        final SystemMetricsCollector srcMetrics;
        final SystemMetricsCollector dstMetrics;

        RunResult(String method, long copyTimeMs, int liveDocsAdded,
                  boolean dataValid,
                  SystemMetricsCollector srcMetrics,
                  SystemMetricsCollector dstMetrics) {
            this.method = method;
            this.copyTimeMs = copyTimeMs;
            this.liveDocsAdded = liveDocsAdded;
            this.dataValid = dataValid;
            this.srcMetrics = srcMetrics;
            this.dstMetrics = dstMetrics;
        }
    }

    /* ------------------------------------------------------------------ */
    /*  Main                                                               */
    /* ------------------------------------------------------------------ */

    public static void main(String[] args) throws Exception {
        System.out.println("============================================");
        System.out.println(" Lucene Index Copy Benchmark");
        System.out.println(" rsync  vs  hard-link  (with live indexing)");
        System.out.println("============================================");
        System.out.println();

        File sourceDir = new File(CopyBenchmarkConfig.SOURCE_INDEX_DIR);
        File rsyncDest = new File(CopyBenchmarkConfig.RSYNC_DEST_DIR);
        File hlinkDest = new File(CopyBenchmarkConfig.HARDLINK_DEST_DIR);

        // ----------------------------------------------------------
        // Step 1: Seed the source index
        // ----------------------------------------------------------
        seedSourceIndex(sourceDir);

        // ----------------------------------------------------------
        // Step 2: Benchmark rsync copy (with live indexing)
        // ----------------------------------------------------------
        System.out.println(">>> Starting rsync benchmark …");
        RunResult rsyncResult = benchmarkCopy("rsync", sourceDir, rsyncDest);

        // ----------------------------------------------------------
        // Step 3: Re-seed source index (fresh state for second run)
        // ----------------------------------------------------------
        cleanDirectory(sourceDir);
        seedSourceIndex(sourceDir);

        // ----------------------------------------------------------
        // Step 4: Benchmark hard-link copy (with live indexing)
        // ----------------------------------------------------------
        System.out.println(">>> Starting hard-link benchmark …");
        RunResult hlinkResult = benchmarkCopy("hardlink", sourceDir, hlinkDest);

        // ----------------------------------------------------------
        // Step 5: Print comparison
        // ----------------------------------------------------------
        printComparison(rsyncResult, hlinkResult);

        // ----------------------------------------------------------
        // Step 6: Cleanup
        // ----------------------------------------------------------
        cleanDirectory(rsyncDest);
        cleanDirectory(hlinkDest);
        cleanDirectory(sourceDir);

        System.out.println("Benchmark finished. All temporary data cleaned up.");
    }

    /* ------------------------------------------------------------------ */
    /*  Seed index                                                         */
    /* ------------------------------------------------------------------ */

    private static void seedSourceIndex(File sourceDir) throws IOException {
        System.out.printf("Seeding source index with %,d documents at %s …%n",
                CopyBenchmarkConfig.SEED_DOC_COUNT, sourceDir);
        long t0 = System.currentTimeMillis();

        if (!sourceDir.exists() && !sourceDir.mkdirs()) {
            throw new IOException("Cannot create source dir: " + sourceDir);
        }

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4,
                new StandardAnalyzer());
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        config.setRAMBufferSizeMB(CopyBenchmarkConfig.RAM_BUFFER_SIZE_MB);

        IndexWriter writer = new IndexWriter(FSDirectory.open(sourceDir), config);
        Random rng = new Random(0);

        for (int i = 0; i < CopyBenchmarkConfig.SEED_DOC_COUNT; i++) {
            writer.addDocument(buildSeedDoc(i, rng));
            if ((i + 1) % 10_000 == 0) {
                writer.commit();
                System.out.printf("  … seeded %,d docs%n", i + 1);
            }
        }
        writer.commit();
        writer.close();

        long elapsed = System.currentTimeMillis() - t0;
        System.out.printf("Source index seeded in %,d ms (size: %.2f MB).%n%n",
                elapsed, dirSizeMB(sourceDir));
    }

    private static Document buildSeedDoc(int id, Random rng) {
        Document doc = new Document();
        doc.add(new StringField("id", "SEED_" + id, Field.Store.YES));
        doc.add(new IntField("seq", id, Field.Store.YES));
        for (int f = 0; f < CopyBenchmarkConfig.NUM_FIELDS; f++) {
            StringBuilder sb = new StringBuilder();
            for (int w = 0; w < 8; w++) {
                if (w > 0) sb.append(' ');
                sb.append("word" + rng.nextInt(5000));
            }
            doc.add(new TextField("field_" + f, sb.toString(), Field.Store.YES));
        }
        return doc;
    }

    /* ------------------------------------------------------------------ */
    /*  Core benchmark routine                                             */
    /* ------------------------------------------------------------------ */

    private static RunResult benchmarkCopy(String method, File sourceDir, File destDir)
            throws Exception {

        // 1. Start live indexing on the source
        LiveIndexingTask liveTask = new LiveIndexingTask(sourceDir);
        Thread liveThread = new Thread(liveTask, "live-indexer-" + method);
        liveThread.setDaemon(true);

        // 2. Prepare metrics collectors
        SystemMetricsCollector srcMetrics = new SystemMetricsCollector("source-" + method);
        SystemMetricsCollector dstMetrics = new SystemMetricsCollector("dest-" + method);

        // 3. Start live indexing + source metrics
        liveThread.start();
        srcMetrics.start();

        // small delay to let live indexing produce a few batches
        Thread.sleep(500);

        // 4. Perform the copy while measuring destination metrics
        dstMetrics.start();
        long copyTimeMs;
        if ("rsync".equals(method)) {
            RsyncCopier copier = new RsyncCopier(sourceDir, destDir);
            copyTimeMs = copier.copy();
        } else {
            HardLinkCopier copier = new HardLinkCopier(sourceDir, destDir);
            copyTimeMs = copier.copy();
        }
        dstMetrics.stop();

        // 5. Stop live indexing + source metrics
        liveTask.stop();
        liveThread.join(10_000);
        srcMetrics.stop();

        int liveDocsAdded = liveTask.getDocsAdded();
        System.out.printf("[%s] Copy completed in %,d ms  |  live docs added: %,d%n",
                method, copyTimeMs, liveDocsAdded);
        System.out.printf("[%s] Source size: %.2f MB  |  Dest size: %.2f MB%n",
                method, dirSizeMB(sourceDir), dirSizeMB(destDir));

        // 6. Print metrics
        srcMetrics.printSummary();
        dstMetrics.printSummary();

        // 7. Validate data availability at destination
        DataAvailabilityValidator validator = new DataAvailabilityValidator(destDir);
        boolean valid = validator.validate();

        return new RunResult(method, copyTimeMs, liveDocsAdded, valid, srcMetrics, dstMetrics);
    }

    /* ------------------------------------------------------------------ */
    /*  Comparison table                                                   */
    /* ------------------------------------------------------------------ */

    private static void printComparison(RunResult rsync, RunResult hlink) {
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════╗");
        System.out.println("║          BENCHMARK COMPARISON: rsync vs hard-link             ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.printf( "║  %-28s │  %12s  │  %12s  ║%n", "Metric", "rsync", "hard-link");
        System.out.println("╠═══════════════════════════════════════════════════════════════╣");
        System.out.printf( "║  %-28s │  %,10d ms  │  %,10d ms  ║%n",
                "Copy time", rsync.copyTimeMs, hlink.copyTimeMs);
        System.out.printf( "║  %-28s │  %,12d  │  %,12d  ║%n",
                "Live docs added during copy", rsync.liveDocsAdded, hlink.liveDocsAdded);
        System.out.printf( "║  %-28s │  %12s  │  %12s  ║%n",
                "Data valid at dest", rsync.dataValid ? "YES" : "NO",
                hlink.dataValid ? "YES" : "NO");

        // Source metrics summary
        printMetricLine("Src avg CPU (%)",
                avgCpu(rsync.srcMetrics), avgCpu(hlink.srcMetrics));
        printMetricLine("Src max CPU (%)",
                maxCpu(rsync.srcMetrics), maxCpu(hlink.srcMetrics));
        printMetricLine("Src max heap (MB)",
                maxHeapMB(rsync.srcMetrics), maxHeapMB(hlink.srcMetrics));
        printMetricLine("Src IO read (MB)",
                ioReadMB(rsync.srcMetrics), ioReadMB(hlink.srcMetrics));
        printMetricLine("Src IO write (MB)",
                ioWriteMB(rsync.srcMetrics), ioWriteMB(hlink.srcMetrics));

        // Destination metrics summary
        printMetricLine("Dst avg CPU (%)",
                avgCpu(rsync.dstMetrics), avgCpu(hlink.dstMetrics));
        printMetricLine("Dst max CPU (%)",
                maxCpu(rsync.dstMetrics), maxCpu(hlink.dstMetrics));
        printMetricLine("Dst max heap (MB)",
                maxHeapMB(rsync.dstMetrics), maxHeapMB(hlink.dstMetrics));
        printMetricLine("Dst IO read (MB)",
                ioReadMB(rsync.dstMetrics), ioReadMB(hlink.dstMetrics));
        printMetricLine("Dst IO write (MB)",
                ioWriteMB(rsync.dstMetrics), ioWriteMB(hlink.dstMetrics));

        System.out.println("╠═══════════════════════════════════════════════════════════════╣");

        // Speedup
        if (hlink.copyTimeMs == 0) {
            System.out.printf("║  %-28s │       %8s faster          ║%n",
                    "Hard-link speedup", "instant");
        } else {
            double speedup = rsync.copyTimeMs > 0
                    ? (double) rsync.copyTimeMs / hlink.copyTimeMs
                    : 0;
            System.out.printf("║  %-28s │       %8.2fx faster         ║%n",
                    "Hard-link speedup", speedup);
        }
        System.out.println("╚═══════════════════════════════════════════════════════════════╝");
        System.out.println();
    }

    private static void printMetricLine(String label, double rsyncVal, double hlinkVal) {
        System.out.printf("║  %-28s │  %12.2f  │  %12.2f  ║%n",
                label, rsyncVal, hlinkVal);
    }

    /* ------------------------------------------------------------------ */
    /*  Metric helpers                                                     */
    /* ------------------------------------------------------------------ */

    private static double avgCpu(SystemMetricsCollector c) {
        java.util.List<MetricsSnapshot> snaps = c.getSnapshots();
        if (snaps.isEmpty()) return 0;
        double sum = 0;
        for (MetricsSnapshot s : snaps) sum += s.processCpuLoad;
        return (sum / snaps.size()) * 100;
    }

    private static double maxCpu(SystemMetricsCollector c) {
        double max = 0;
        for (MetricsSnapshot s : c.getSnapshots()) {
            if (s.processCpuLoad > max) max = s.processCpuLoad;
        }
        return max * 100;
    }

    private static double maxHeapMB(SystemMetricsCollector c) {
        long max = 0;
        for (MetricsSnapshot s : c.getSnapshots()) {
            if (s.heapUsedBytes > max) max = s.heapUsedBytes;
        }
        return max / (1024.0 * 1024.0);
    }

    private static double ioReadMB(SystemMetricsCollector c) {
        java.util.List<MetricsSnapshot> snaps = c.getSnapshots();
        if (snaps.size() < 2) return 0;
        long delta = snaps.get(snaps.size() - 1).ioReadBytes - snaps.get(0).ioReadBytes;
        return delta / (1024.0 * 1024.0);
    }

    private static double ioWriteMB(SystemMetricsCollector c) {
        java.util.List<MetricsSnapshot> snaps = c.getSnapshots();
        if (snaps.size() < 2) return 0;
        long delta = snaps.get(snaps.size() - 1).ioWriteBytes - snaps.get(0).ioWriteBytes;
        return delta / (1024.0 * 1024.0);
    }

    /* ------------------------------------------------------------------ */
    /*  Utilities                                                          */
    /* ------------------------------------------------------------------ */

    private static double dirSizeMB(File dir) {
        long bytes = dirSizeBytes(dir);
        return bytes / (1024.0 * 1024.0);
    }

    private static long dirSizeBytes(File dir) {
        if (dir == null || !dir.exists()) return 0;
        long size = 0;
        File[] children = dir.listFiles();
        if (children != null) {
            for (File f : children) {
                size += f.isDirectory() ? dirSizeBytes(f) : f.length();
            }
        }
        return size;
    }

    private static void cleanDirectory(File dir) {
        if (!dir.exists()) return;
        File[] children = dir.listFiles();
        if (children != null) {
            for (File f : children) {
                if (f.isDirectory()) {
                    cleanDirectory(f);
                }
                f.delete();
            }
        }
        dir.delete();
    }
}
