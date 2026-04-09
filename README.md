# Lucene 4 Index Splitter Benchmark

A Java project that benchmarks two approaches for splitting a large Lucene 4 index into multiple sub-indices based on an ID field.

## Problem

Given a Lucene 4 index with:
- **10 million** documents (~19 GB index size)
- **100 fields** per document
- An **ID** field with **10,000 unique values** (non-unique across documents)

Split the index into **10 sub-indices** by partitioning the ID values using round-robin assignment (ID_0 → dir0, ID_1 → dir1, ..., ID_9 → dir9, ID_10 → dir0, etc.).

## Two Approaches Benchmarked

### Approach 1: Sequential File Copy + Term Deletion
1. Copy the entire source index directory to each of the 10 target directories using `FileChannel.transferTo()`.
2. Open an `IndexWriter` on each copied directory.
3. Delete documents whose ID values do NOT belong to that partition using batched `BooleanQuery` with `TermQuery` clauses.
4. Force-merge each index to reclaim space.

### Approach 2: Java Hard Link + Term Deletion
1. Create hard links from source index files to each of the 10 target directories using `Files.createLink()`. This is nearly instant and uses no additional disk space.
2. Open an `IndexWriter` on each linked directory.
3. Delete non-matching documents using the same batched `BooleanQuery` approach.
4. Force-merge each index. Lucene writes new segment files; original hard-linked files are freed when unlinked.

## Project Structure

```
src/main/java/com/splitindex/
├── SplitConfig.java             # Configuration constants (doc count, field count, paths, etc.)
├── IndexGenerator.java          # Generates the test Lucene 4 index with 100 fields
├── IndexSplitterCopy.java       # Approach 1: Sequential copy + BooleanQuery deletion
├── IndexSplitterHardLink.java   # Approach 2: Hard link + BooleanQuery deletion
└── SplitBenchmark.java          # Main benchmark runner with comparison output
```

## Prerequisites

- **Java 8+**
- **Maven 3.x**
- Source and target directories must be on the **same filesystem** (for hard links)

## Build

```bash
mvn clean package
```

## Run

### Full Benchmark (both approaches)
```bash
mvn exec:java
```

### Individual Steps
```bash
# Generate source index only
mvn exec:java -Dexec.args="generate"

# Run copy-based split only
mvn exec:java -Dexec.args="copy"

# Run hard-link-based split only
mvn exec:java -Dexec.args="hardlink"

# Clean all benchmark data
mvn exec:java -Dexec.args="clean"
```

### Custom Base Directory
```bash
mvn exec:java -Dsplitindex.basedir=/data/benchmark
```

## Benchmark Results

Detailed benchmark results for both small and large index configurations are available in [benchmark.md](benchmark.md).

### Large Index Results (10M docs, ~19 GB, 100 fields, 10 splits)

```
  +-------------------------------------------------+
  |                 | Copy+Delete | HardLink+Delete  |
  +-------------------------------------------------+
  | Total Time      |  1,999,016 ms |  1,361,625 ms    |
  | Copy/Link Phase |    621,546 ms |        346 ms    |
  | Delete Phase    |  1,377,470 ms |  1,361,279 ms    |
  +-------------------------------------------------+

  Overall Speedup (HardLink vs Copy): 1.47x faster
  Copy/Link Phase Speedup:            1796.38x faster
```

### Key Findings
- **Hard link phase is ~1796x faster** than file copy (346 ms vs 621,546 ms for ~19 GB)
- **Delete phase timing is nearly identical** between both approaches (same Lucene operations)
- **Hard link approach saves significant disk space** during the split process (no duplicate data until Lucene rewrites segments)
- At scale, the copy phase dominates the total time—hard linking eliminates this bottleneck entirely

## How It Works

### ID Partitioning
The 10,000 unique IDs are distributed across 10 partitions using round-robin:
- Partition 0: ID_0, ID_10, ID_20, ... (1,000 IDs)
- Partition 1: ID_1, ID_11, ID_21, ... (1,000 IDs)
- ...
- Partition 9: ID_9, ID_19, ID_29, ... (1,000 IDs)

### Deletion Strategy
For each partition, we build a list of IDs to **delete** (all 9,000 IDs that don't belong). These are deleted using batched `BooleanQuery` with `TermQuery` clauses (max 1,024 clauses per batch) to stay within Lucene's clause limit.

### Hard Link Advantage
`Files.createLink()` creates a new directory entry pointing to the same inode (on-disk data blocks). This means:
- **O(number of files)** instead of **O(total file size)**
- No additional disk space until files are modified
- When Lucene's `IndexWriter` commits, it writes new segment files; the original linked files remain intact for other partitions

## Dependencies

- Apache Lucene 4.10.4 (core, analyzers-common, queryparser)