package com.splitindex;

import java.io.File;

/**
 * Encapsulates all configuration for a single benchmark run.
 *
 * <p>Allows running the split benchmark with different index sizes
 * (e.g., 5 GB, 10 GB, 19 GB) without changing static constants.</p>
 *
 * <p>Use the factory methods {@link #profile5GB()}, {@link #profile10GB()},
 * and {@link #fromSplitConfig()} for common presets.</p>
 */
public final class BenchmarkProfile {

    private final String name;
    private final int numFields;
    private final int numUniqueIds;
    private final int totalDocs;
    private final int targetDocSizeBytes;
    private final int numSplits;
    private final String baseDir;
    private final double ramBufferSizeMB;
    private final int indexBatchSize;
    private final int maxBooleanClauses;

    private BenchmarkProfile(Builder builder) {
        this.name = builder.name;
        this.numFields = builder.numFields;
        this.numUniqueIds = builder.numUniqueIds;
        this.totalDocs = builder.totalDocs;
        this.targetDocSizeBytes = builder.targetDocSizeBytes;
        this.numSplits = builder.numSplits;
        this.baseDir = builder.baseDir;
        this.ramBufferSizeMB = builder.ramBufferSizeMB;
        this.indexBatchSize = builder.indexBatchSize;
        this.maxBooleanClauses = builder.maxBooleanClauses;
    }

    // ---- Factory methods for common profiles ----

    /**
     * Creates a profile that generates an approximately 5 GB index.
     *
     * <p>Configuration: ~2,600,000 docs, 100 fields, 1024 bytes/doc, 10 splits.</p>
     */
    public static BenchmarkProfile profile5GB() {
        return new Builder("5GB")
                .totalDocs(2_600_000)
                .numFields(100)
                .numUniqueIds(10_000)
                .targetDocSizeBytes(1024)
                .numSplits(10)
                .build();
    }

    /**
     * Creates a profile that generates an approximately 10 GB index.
     *
     * <p>Configuration: ~5,200,000 docs, 100 fields, 1024 bytes/doc, 10 splits.</p>
     */
    public static BenchmarkProfile profile10GB() {
        return new Builder("10GB")
                .totalDocs(5_200_000)
                .numFields(100)
                .numUniqueIds(10_000)
                .targetDocSizeBytes(1024)
                .numSplits(10)
                .build();
    }

    /**
     * Creates a profile from the existing {@link SplitConfig} constants
     * (backward-compatible with the original 19 GB benchmark).
     */
    public static BenchmarkProfile fromSplitConfig() {
        return new Builder("default")
                .totalDocs(SplitConfig.TOTAL_DOCS)
                .numFields(SplitConfig.NUM_FIELDS)
                .numUniqueIds(SplitConfig.NUM_UNIQUE_IDS)
                .targetDocSizeBytes(SplitConfig.TARGET_DOC_SIZE_BYTES)
                .numSplits(SplitConfig.NUM_SPLITS)
                .baseDir(SplitConfig.BASE_DIR)
                .ramBufferSizeMB(SplitConfig.RAM_BUFFER_SIZE_MB)
                .indexBatchSize(SplitConfig.INDEX_BATCH_SIZE)
                .maxBooleanClauses(SplitConfig.MAX_BOOLEAN_CLAUSES)
                .build();
    }

    // ---- Getters ----

    public String getName()              { return name; }
    public int getNumFields()            { return numFields; }
    public int getNumUniqueIds()         { return numUniqueIds; }
    public int getTotalDocs()            { return totalDocs; }
    public int getTargetDocSizeBytes()   { return targetDocSizeBytes; }
    public int getNumSplits()            { return numSplits; }
    public String getBaseDir()           { return baseDir; }
    public double getRamBufferSizeMB()   { return ramBufferSizeMB; }
    public int getIndexBatchSize()       { return indexBatchSize; }
    public int getMaxBooleanClauses()    { return maxBooleanClauses; }

    // ---- Derived paths ----

    public String getSourceIndexDir() {
        return baseDir + File.separator + name + "_source_index";
    }

    public String getCopySplitDirPrefix() {
        return baseDir + File.separator + name + "_copy_split" + File.separator + "dir";
    }

    public String getHardlinkSplitDirPrefix() {
        return baseDir + File.separator + name + "_hardlink_split" + File.separator + "dir";
    }

    @Override
    public String toString() {
        return String.format("BenchmarkProfile[%s: %,d docs, %d fields, %d unique IDs, "
                        + "%d bytes/doc, %d splits, baseDir=%s]",
                name, totalDocs, numFields, numUniqueIds,
                targetDocSizeBytes, numSplits, baseDir);
    }

    // ---- Builder ----

    public static class Builder {
        private final String name;
        private int numFields = 100;
        private int numUniqueIds = 10_000;
        private int totalDocs = 10_000_000;
        private int targetDocSizeBytes = 1024;
        private int numSplits = 10;
        private String baseDir = System.getProperty("splitindex.basedir",
                System.getProperty("java.io.tmpdir") + File.separator + "splitindex_bench");
        private double ramBufferSizeMB = 256.0;
        private int indexBatchSize = 10_000;
        private int maxBooleanClauses = 1024;

        public Builder(String name) {
            this.name = name;
        }

        public Builder numFields(int val)            { this.numFields = val; return this; }
        public Builder numUniqueIds(int val)          { this.numUniqueIds = val; return this; }
        public Builder totalDocs(int val)             { this.totalDocs = val; return this; }
        public Builder targetDocSizeBytes(int val)    { this.targetDocSizeBytes = val; return this; }
        public Builder numSplits(int val)             { this.numSplits = val; return this; }
        public Builder baseDir(String val)            { this.baseDir = val; return this; }
        public Builder ramBufferSizeMB(double val)    { this.ramBufferSizeMB = val; return this; }
        public Builder indexBatchSize(int val)        { this.indexBatchSize = val; return this; }
        public Builder maxBooleanClauses(int val)     { this.maxBooleanClauses = val; return this; }

        public BenchmarkProfile build() {
            return new BenchmarkProfile(this);
        }
    }
}
