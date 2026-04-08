package com.splitindex;

import java.io.File;

/**
 * Configuration constants for the Lucene index splitting benchmark.
 *
 * <p>Adjust these values based on your environment and testing needs.
 * For full-scale benchmarking with 40M records and 40GB index, use the
 * FULL_SCALE constants. For quick smoke tests, use the default (smaller) values.</p>
 */
public final class SplitConfig {

    private SplitConfig() {
        // utility class
    }

    // ---- Index generation settings ----

    /** Number of fields per document (including the ID field). */
    public static final int NUM_FIELDS = 100;

    /** Number of unique ID values distributed across documents. */
    public static final int NUM_UNIQUE_IDS = 10_000;

    /** Total number of documents to generate. */
    public static final int TOTAL_DOCS = 40_000_000;

    /** Target document size in bytes (approximately). */
    public static final int TARGET_DOC_SIZE_BYTES = 1024;

    /** Number of sub-indices to split into. */
    public static final int NUM_SPLITS = 10;

    // ---- Directory paths ----

    /** Base directory for all benchmark data. */
    public static final String BASE_DIR = System.getProperty("splitindex.basedir",
            System.getProperty("java.io.tmpdir") + File.separator + "splitindex_benchmark");

    /** Source index directory. */
    public static final String SOURCE_INDEX_DIR = BASE_DIR + File.separator + "source_index";

    /** Directory prefix for copy-based split approach. */
    public static final String COPY_SPLIT_DIR_PREFIX = BASE_DIR + File.separator + "copy_split" + File.separator + "dir";

    /** Directory prefix for hard-link-based split approach. */
    public static final String HARDLINK_SPLIT_DIR_PREFIX = BASE_DIR + File.separator + "hardlink_split" + File.separator + "dir";

    // ---- Lucene settings ----

    /** RAM buffer size for IndexWriter (in MB). */
    public static final double RAM_BUFFER_SIZE_MB = 256.0;

    /** Batch size for document indexing. */
    public static final int INDEX_BATCH_SIZE = 10_000;

    /** Maximum number of boolean clauses per deletion query. */
    public static final int MAX_BOOLEAN_CLAUSES = 1024;
}
