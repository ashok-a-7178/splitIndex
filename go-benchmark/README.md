# Go Index Splitting Benchmark

A Go port of the Java Lucene 4 Index Splitting Benchmark. Uses [Bleve](https://github.com/blevesearch/bleve) (a Go full-text search library with segment-based storage similar to Lucene) to benchmark two approaches for splitting a large index into sub-indices.

## Approaches Benchmarked

### Approach 1: Sequential File Copy + Term Deletion
1. Copy the entire source index directory to each target directory using `io.Copy`.
2. Open a Bleve index on each copied directory.
3. Search-and-delete documents whose ID values do NOT belong to that partition.
4. Close the index (triggers compaction).

### Approach 2: Hard Link + Term Deletion
1. Create hard links from source index files to each target directory using `os.Link()`.
2. Open a Bleve index on each linked directory.
3. Search-and-delete documents using the same approach.
4. Close the index (triggers compaction).

## Concurrent Copy Smoke Test

Tests three approaches for copying an index while it's being actively written to:
1. **Hard-Link Snapshot** — hard-link all files, then copy to destination
2. **Hard-Link Live Capture** — hard-link ALL files directly from live source
3. **Rsync** — multi-pass rsync with concurrent writes

## Prerequisites

- **Go 1.21+**

## Build

```bash
cd go-benchmark
go build -o go-benchmark .
```

## Run

### Full Benchmark (both approaches)
```bash
./go-benchmark
# or
./go-benchmark all
```

### Individual Steps
```bash
# Generate source index only
./go-benchmark generate

# Run copy-based split only
./go-benchmark copy

# Run hard-link-based split only
./go-benchmark hardlink

# Run concurrent copy smoke test (~400MB index)
./go-benchmark smoketest

# Clean all benchmark data
./go-benchmark clean
```

### Configuration via Environment Variables

```bash
# Custom base directory
SPLITINDEX_BASEDIR=/data/benchmark ./go-benchmark

# Custom document count and size
SPLITINDEX_TOTAL_DOCS=1000000 SPLITINDEX_TARGET_DOC_SIZE_BYTES=2048 ./go-benchmark

# All configurable variables:
SPLITINDEX_BASEDIR          # Base directory for all data (default: ~/luceneIndex)
SPLITINDEX_TOTAL_DOCS       # Total documents to generate (default: 100,000)
SPLITINDEX_NUM_UNIQUE_IDS   # Number of unique ID values (default: 10,000)
SPLITINDEX_NUM_FIELDS       # Fields per document (default: 20)
SPLITINDEX_NUM_SPLITS       # Number of partitions (default: 10)
SPLITINDEX_TARGET_DOC_SIZE_BYTES  # Approximate document size (default: 1024)
SPLITINDEX_INDEX_BATCH_SIZE # Batch size for indexing (default: 1000)
```

## Key Differences from Java Version

| Feature | Java (Lucene 4) | Go (Bleve) |
|---------|-----------------|------------|
| Search engine | Apache Lucene 4.10.4 | Bleve v2 (scorch backend) |
| Storage | Segment files (immutable) | Segment files (immutable) |
| Deletion | BooleanQuery with TermQuery | Search + batch delete |
| Force merge | `writer.forceMerge(1)` | Automatic compaction on close |
| Config | System properties (`-D`) | Environment variables |
