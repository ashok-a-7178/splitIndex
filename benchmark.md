# Lucene 4 Index Splitting Benchmark Results

## Table of Contents
- [Small Index Benchmark](#small-index-benchmark)
- [Large Index Benchmark](#large-index-benchmark)
- [Comparison: Small vs Large Index](#comparison-small-vs-large-index)
- [Conclusions](#conclusions)

---

## Small Index Benchmark

### Configuration

| Parameter        | Value       |
|------------------|-------------|
| Total documents  | 100,000     |
| Unique IDs       | 10,000      |
| Fields per doc   | 50          |
| Number of splits | 10          |
| Index size       | ~421 MB     |

### Results

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

### Partition Details (Small Index)

Each partition: **10,000 docs**, ~42 MB

---

## Large Index Benchmark

### Configuration

| Parameter        | Value        |
|------------------|--------------|
| Total documents  | 10,000,000   |
| Unique IDs       | 10,000       |
| Fields per doc   | 100          |
| Number of splits | 10           |
| Index size       | ~19 GB       |

### Results

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

> **WINNER: Hard Link approach is faster overall**

### Phase 1: Copy / Hard Link

| Approach  | Phase 1 Time  | Per-Partition Size (before delete) |
|-----------|---------------|-------------------------------------|
| Copy      | 621,546 ms    | ~19,069 MB each (10 full copies)    |
| Hard Link | 346 ms        | 0 MB additional (shared inodes)     |

### Phase 2: Delete + Force Merge

| Partition | Docs Remaining | Final Size (MB) |
|-----------|----------------|------------------|
| dir0      | 1,000,000      | 1,950.99         |
| dir1      | 1,000,000      | 1,950.98         |
| dir2      | 1,000,000      | 1,950.98         |
| dir3      | 1,000,000      | 1,950.98         |
| dir4      | 1,000,000      | 1,951.04         |
| dir5      | 1,000,000      | 1,951.00         |
| dir6      | 1,000,000      | 1,951.01         |
| dir7      | 1,000,000      | 1,950.99         |
| dir8      | 1,000,000      | 1,951.00         |
| dir9      | 1,000,000      | 1,950.98         |
| **Total** | **10,000,000** | **~19,510 MB**   |

### Detailed Timing

| Metric               | Copy+Delete     | HardLink+Delete |
|----------------------|-----------------|-----------------|
| Copy/Link Phase      | 621,546 ms      | 346 ms          |
| Delete Phase         | 1,377,470 ms    | 1,361,279 ms    |
| **Total Time**       | **1,999,016 ms** | **1,361,625 ms** |
| Total Time (minutes) | ~33.3 min       | ~22.7 min       |
| Build Time           | 56:02 min       | 56:02 min       |

---

## Comparison: Small vs Large Index

| Metric                  | Small Index (421 MB) | Large Index (~19 GB) |
|-------------------------|----------------------|----------------------|
| **Copy Phase**          | 5,703 ms             | 621,546 ms           |
| **Link Phase**          | 3 ms                 | 346 ms               |
| **Copy/Link Speedup**   | 1,901x               | 1,796x               |
| **Delete Phase (Copy)**  | 43,055 ms            | 1,377,470 ms         |
| **Delete Phase (Link)**  | 40,409 ms            | 1,361,279 ms         |
| **Overall Speedup**     | 1.21x                | 1.47x                |

### Key Observations

| Observation | Detail |
|-------------|--------|
| Copy phase scales linearly | 421 MB → 5.7s, 19 GB → 621s (roughly proportional to index size) |
| Hard link phase stays near-instant | 3 ms → 346 ms (scales with file count, not file size) |
| Delete phase dominates total time | ~88% of total time for small index, ~69–100% for large index |
| Overall speedup increases with index size | 1.21x (small) → 1.47x (large) because copy phase grows faster |

---

## Conclusions

1. **The hard link approach is the clear winner**, especially for large indices where the copy phase becomes a significant bottleneck.

2. **Copy/Link phase speedup is consistently ~1800x**, regardless of index size. The hard link operation is O(number of files) vs O(total file size) for copy.

3. **Delete phase timings are nearly identical** between both approaches since they perform the same Lucene operations (term-based deletion + force merge).

4. **The overall speedup improves with index size**: as the index grows, the copy phase takes proportionally longer, making the hard link advantage more pronounced.

5. **Hard link saves disk space during the split**: the copy approach temporarily requires N × index_size of disk space (10 × 19 GB = 190 GB), while hard link requires only the original index size until Lucene rewrites segments during force merge.

6. **For production-scale indices (40 GB+)**, the hard link approach can save 10+ minutes of wall-clock time compared to copying, with identical final results.

### Notes

- The copy/link phase difference is most significant for large indices.
- With a 40 GB index, the hard link phase should be near-instant vs minutes for copy.
- Delete phase timings should be similar since both do the same Lucene operations.
- Hard link approach also saves disk space during the split process.
- Source and target directories must be on the **same filesystem** for hard links to work.
