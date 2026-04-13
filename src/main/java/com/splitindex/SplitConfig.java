package com.splitindex;

import java.io.File;

/**
 * Configuration constants for the Lucene index splitting benchmark.
 *
 * <p>All settings can be overridden via system properties for quick smoke tests.
 * For example:
 * <pre>
 *   java -Dsplitindex.basedir=/tmp/bench \
 *        -Dsplitindex.totalDocs=1000 \
 *        -Dsplitindex.numUniqueIds=100 \
 *        -Dsplitindex.numFields=10 \
 *        -Dsplitindex.numSplits=3 \
 *        -cp target/splitIndex-1.0-SNAPSHOT.jar com.splitindex.SplitBenchmark rsync-vs-hardlink
 * </pre>
 *
 * <p>For full-scale benchmarking with 40M records and 40GB index, use the
 * defaults or set explicit large values.</p>
 */
public final class SplitConfig {

    private SplitConfig() {
        // utility class
    }

    // ---- Index generation settings ----

    /** Number of fields per document (including the ID field). */
    public static final int NUM_FIELDS =
            Integer.getInteger("splitindex.numFields", 100);

    /** Number of unique ID values distributed across documents. */
    public static final int NUM_UNIQUE_IDS =
            Integer.getInteger("splitindex.numUniqueIds", 10_000);

    /** Total number of documents to generate. */
    public static final int TOTAL_DOCS =
            Integer.getInteger("splitindex.totalDocs", 10_000_000);

    /** Target document size in bytes (approximately). */
    public static final int TARGET_DOC_SIZE_BYTES =
            Integer.getInteger("splitindex.targetDocSizeBytes", 1024);

    /** Number of sub-indices to split into. */
    public static final int NUM_SPLITS =
            Integer.getInteger("splitindex.numSplits", 10);

    // ---- Directory paths ----

    /** Base directory for all benchmark data. */
    public static final String BASE_DIR =
            System.getProperty("splitindex.basedir",
                    System.getProperty("user.home") + File.separator + "luceneIndex");

    /** Source index directory. */
    public static final String SOURCE_INDEX_DIR = BASE_DIR + File.separator + "source_index";

    /** Directory prefix for copy-based split approach. */
    public static final String COPY_SPLIT_DIR_PREFIX = BASE_DIR + File.separator + "copy_split" + File.separator + "dir";

    /** Directory prefix for hard-link-based split approach. */
    public static final String HARDLINK_SPLIT_DIR_PREFIX = BASE_DIR + File.separator + "hardlink_split" + File.separator + "dir";

    /** Directory prefix for rsync-based split approach. */
    public static final String RSYNC_SPLIT_DIR_PREFIX = BASE_DIR + File.separator + "rsync_split" + File.separator + "dir";

    // ---- Lucene settings ----

    /** RAM buffer size for IndexWriter (in MB). */
    public static final double RAM_BUFFER_SIZE_MB = 256.0;

    /** Batch size for document indexing. */
    public static final int INDEX_BATCH_SIZE =
            Integer.getInteger("splitindex.indexBatchSize", 10_000);

    /** Number of producer threads for parallel document creation. */
    public static final int NUM_PRODUCER_THREADS = Runtime.getRuntime().availableProcessors();

    /** Capacity of the document batch queue (number of batches buffered between producers and writer). */
    public static final int QUEUE_CAPACITY = NUM_PRODUCER_THREADS * 2;

    /** Maximum number of boolean clauses per deletion query. */
    public static final int MAX_BOOLEAN_CLAUSES = 1024;
}
