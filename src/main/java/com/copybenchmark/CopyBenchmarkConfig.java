package com.copybenchmark;

import java.io.File;

/**
 * Configuration constants for the rsync vs hard-link copy benchmark.
 *
 * <p>Paths, index sizing knobs, live-indexing parameters and
 * metrics-sampling intervals are all centralised here.</p>
 */
public final class CopyBenchmarkConfig {

    private CopyBenchmarkConfig() { }

    /* ------------------------------------------------------------------ */
    /*  Directory layout                                                   */
    /* ------------------------------------------------------------------ */

    /** Root directory that contains every artefact produced by the benchmark. */
    public static final String BENCHMARK_ROOT =
            System.getProperty("benchmark.root",
                    System.getProperty("java.io.tmpdir") + File.separator + "copybenchmark");

    /** Source Lucene index that will be continuously written to. */
    public static final String SOURCE_INDEX_DIR =
            BENCHMARK_ROOT + File.separator + "source_index";

    /** Destination directory created by rsync. */
    public static final String RSYNC_DEST_DIR =
            BENCHMARK_ROOT + File.separator + "rsync_dest";

    /** Destination directory created by hard-link copy. */
    public static final String HARDLINK_DEST_DIR =
            BENCHMARK_ROOT + File.separator + "hardlink_dest";

    /* ------------------------------------------------------------------ */
    /*  Source index generation                                            */
    /* ------------------------------------------------------------------ */

    /** Number of documents written to the source index before the benchmark starts. */
    public static final int SEED_DOC_COUNT = 100_000;

    /** Number of searchable fields per document. */
    public static final int NUM_FIELDS = 50;

    /** Approximate target size (bytes) for each document. */
    public static final int TARGET_DOC_SIZE_BYTES = 1024;

    /** IndexWriter RAM buffer size in MB. */
    public static final double RAM_BUFFER_SIZE_MB = 128.0;

    /* ------------------------------------------------------------------ */
    /*  Live indexing (background thread during copy)                      */
    /* ------------------------------------------------------------------ */

    /** Documents added per batch by the live-indexing thread. */
    public static final int LIVE_BATCH_SIZE = 500;

    /** Pause between live-indexing batches (ms) to avoid overwhelming the disk. */
    public static final long LIVE_BATCH_PAUSE_MS = 50;

    /* ------------------------------------------------------------------ */
    /*  Metrics collection                                                 */
    /* ------------------------------------------------------------------ */

    /** How often the metrics collector samples CPU / memory / IO (ms). */
    public static final long METRICS_SAMPLE_INTERVAL_MS = 200;

    /* ------------------------------------------------------------------ */
    /*  Validation                                                         */
    /* ------------------------------------------------------------------ */

    /** Number of random term queries used to verify data availability. */
    public static final int VALIDATION_QUERY_COUNT = 20;
}
