# Lucene 4 Index Splitter Benchmark

A Java project that benchmarks two approaches for splitting a large Lucene 4 index into multiple sub-indices based on an ID field.

## Problem

Given a Lucene 4 index with:
- **40 million** documents (40GB index size)
- **50 fields** per document
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
├── IndexGenerator.java          # Generates the test Lucene 4 index with 50 fields
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

### Full-Scale Benchmark (40M docs)

Edit `SplitConfig.java` and change:
```java
public static final int TOTAL_DOCS = 40_000_000;
```

Then rebuild and run:
```bash
mvn clean package && mvn exec:java
```

## Sample Output (100K docs, 421 MB index)

```
  +-------------------------------------------------+
  |                 | Copy+Delete | HardLink+Delete  |
  +-------------------------------------------------+
  | Total Time      |     48,758 ms |     40,412 ms    |
  | Copy/Link Phase |      5,703 ms |          3 ms    |
  | Delete Phase    |     43,055 ms |     40,409 ms    |
  +-------------------------------------------------+

  Overall Speedup (HardLink vs Copy): 1.21x faster
  Copy/Link Phase Speedup:            1901.00x faster
```

### Key Findings
- **Hard link phase is ~1900x faster** than file copy (3 ms vs 5,703 ms for 421 MB)
- **Delete phase timing is identical** between both approaches (same Lucene operations)
- **With a 40GB index**, the copy phase would take minutes while hard linking remains near-instant
- **Hard link approach saves disk space** during the split process (no duplicate data until Lucene rewrites segments)

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