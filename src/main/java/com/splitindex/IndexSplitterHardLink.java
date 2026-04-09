package com.splitindex;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Approach 2: Java Hard Link + BooleanQuery Term Deletion.
 *
 * <p>This approach:
 * <ol>
 *   <li>Creates hard links from the source index files to each of the 10 target directories
 *       using {@link Files#createLink(Path, Path)}. This is nearly instant and uses no
 *       additional disk space (files share the same on-disk blocks via hard links).</li>
 *   <li>Opens an IndexWriter on each linked directory and deletes documents whose ID values
 *       do NOT belong to that partition. When Lucene commits changes, it writes new segment
 *       files while the original hard-linked files become eligible for removal once unlinked.</li>
 *   <li>Force-merges each resulting index to reclaim space from deleted documents.</li>
 * </ol>
 *
 * <p><strong>Key advantage:</strong> The link phase is O(number of files), not O(file size),
 * making it dramatically faster than sequential file copy for large indices (e.g., 40GB).</p>
 *
 * <p><strong>Requirement:</strong> Source and target must be on the same filesystem for hard
 * links to work. Hard links are not supported across different mount points or filesystems.</p>
 */
public class IndexSplitterHardLink {

    private final BenchmarkProfile profile;

    /**
     * Creates a splitter using the default {@link SplitConfig} constants.
     */
    public IndexSplitterHardLink() {
        this(BenchmarkProfile.fromSplitConfig());
    }

    /**
     * Creates a splitter using the supplied {@link BenchmarkProfile}.
     *
     * @param profile benchmark configuration
     */
    public IndexSplitterHardLink(BenchmarkProfile profile) {
        this.profile = profile;
    }

    /**
     * Runs the hard-link-based split benchmark.
     *
     * @return an array of 3 longs: [totalTimeMs, linkTimeMs, deleteTimeMs]
     */
    public long[] split() throws IOException {
        System.out.println("\n========================================");
        System.out.println("  APPROACH 2: Hard Link + Delete        ");
        System.out.println("========================================\n");

        File sourceDir = new File(profile.getSourceIndexDir());
        if (!sourceDir.exists()) {
            throw new IOException("Source index not found at: " + sourceDir.getAbsolutePath());
        }

        // Collect unique IDs
        List<String> uniqueIds = collectUniqueIds();
        System.out.println("[HardLink] Found " + uniqueIds.size() + " unique IDs in source index");

        // Assign IDs to partitions (round-robin)
        @SuppressWarnings("unchecked")
        List<String>[] partitionIds = new List[profile.getNumSplits()];
        for (int i = 0; i < profile.getNumSplits(); i++) {
            partitionIds[i] = new ArrayList<>();
        }
        for (int i = 0; i < uniqueIds.size(); i++) {
            partitionIds[i % profile.getNumSplits()].add(uniqueIds.get(i));
        }

        for (int i = 0; i < profile.getNumSplits(); i++) {
            System.out.println("[HardLink] Partition " + i + " keeps " + partitionIds[i].size() + " IDs");
        }

        long totalStart = System.currentTimeMillis();

        // Phase 1: Create hard links from source to all target directories
        System.out.println("\n[HardLink] Phase 1: Creating hard links to " + profile.getNumSplits() + " directories...");
        long linkStart = System.currentTimeMillis();
        for (int i = 0; i < profile.getNumSplits(); i++) {
            File targetDir = new File(profile.getHardlinkSplitDirPrefix() + i);
            hardLinkDirectory(sourceDir, targetDir);
            System.out.println("[HardLink]   Linked to dir" + i);
        }
        long linkTime = System.currentTimeMillis() - linkStart;
        System.out.println("[HardLink] Phase 1 completed in " + linkTime + " ms");

        // Phase 2: Delete non-matching IDs from each partition
        System.out.println("\n[HardLink] Phase 2: Deleting non-matching documents from each partition...");
        long deleteStart = System.currentTimeMillis();
        for (int i = 0; i < profile.getNumSplits(); i++) {
            File targetDir = new File(profile.getHardlinkSplitDirPrefix() + i);
            deleteNonMatchingDocs(targetDir, partitionIds[i], i);
        }
        long deleteTime = System.currentTimeMillis() - deleteStart;
        System.out.println("[HardLink] Phase 2 completed in " + deleteTime + " ms");

        long totalTime = System.currentTimeMillis() - totalStart;
        System.out.println("\n[HardLink] TOTAL TIME: " + totalTime + " ms");
        System.out.println("[HardLink]   Link phase:   " + linkTime + " ms");
        System.out.println("[HardLink]   Delete phase:  " + deleteTime + " ms");

        // Print final stats
        printPartitionStats();

        return new long[]{totalTime, linkTime, deleteTime};
    }

    /**
     * Collects all unique ID values (generated deterministically).
     */
    private List<String> collectUniqueIds() {
        List<String> ids = new ArrayList<>(profile.getNumUniqueIds());
        for (int i = 0; i < profile.getNumUniqueIds(); i++) {
            ids.add("ID_" + i);
        }
        return ids;
    }

    /**
     * Creates hard links for all files in sourceDir into targetDir.
     * If the target directory exists, it is cleaned first.
     *
     * <p>Uses {@link Files#createLink(Path, Path)} which creates a new directory
     * entry pointing to the same inode as the source file. Both paths must be on
     * the same filesystem.</p>
     */
    private void hardLinkDirectory(File sourceDir, File targetDir) throws IOException {
        if (targetDir.exists()) {
            IndexSplitterCopy.deleteDirectory(targetDir);
        }
        targetDir.mkdirs();

        File[] files = sourceDir.listFiles();
        if (files == null) return;

        for (File srcFile : files) {
            if (srcFile.isFile()) {
                Path linkPath = new File(targetDir, srcFile.getName()).toPath();
                Path existingPath = srcFile.toPath();
                Files.createLink(linkPath, existingPath);
            }
        }
    }

    /**
     * Deletes documents from the index at targetDir whose ID is NOT in keepIds.
     * Uses BooleanQuery batches to delete by term.
     */
    private void deleteNonMatchingDocs(File targetDir, List<String> keepIds, int partitionIndex)
            throws IOException {

        // Build list of IDs to DELETE
        List<String> allIds = collectUniqueIds();
        List<String> deleteIds = new ArrayList<>();
        java.util.Set<String> keepSet = new java.util.HashSet<>(keepIds);
        for (String id : allIds) {
            if (!keepSet.contains(id)) {
                deleteIds.add(id);
            }
        }

        System.out.println("[HardLink]   Partition " + partitionIndex
                + ": deleting " + deleteIds.size() + " IDs, keeping " + keepIds.size() + " IDs");

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4, new StandardAnalyzer());
        config.setRAMBufferSizeMB(profile.getRamBufferSizeMB());
        config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        try (FSDirectory dir = FSDirectory.open(targetDir);
             IndexWriter writer = new IndexWriter(dir, config)) {

            // Delete in batches to respect BooleanQuery max clause count
            int batchSize = profile.getMaxBooleanClauses();
            for (int start = 0; start < deleteIds.size(); start += batchSize) {
                int end = Math.min(start + batchSize, deleteIds.size());
                BooleanQuery bq = new BooleanQuery();
                for (int j = start; j < end; j++) {
                    bq.add(new TermQuery(new Term("ID", deleteIds.get(j))),
                            BooleanClause.Occur.SHOULD);
                }
                writer.deleteDocuments(bq);
            }

            writer.commit();
            System.out.println("[HardLink]   Partition " + partitionIndex + ": force merging...");
            writer.forceMerge(1);
            writer.commit();

            System.out.println("[HardLink]   Partition " + partitionIndex + ": done. Docs remaining: "
                    + writer.numDocs() + ", size: " + IndexGenerator.dirSizeMB(targetDir) + " MB");
        }
    }

    /** Prints document counts for all partitions. */
    private void printPartitionStats() throws IOException {
        System.out.println("\n[HardLink] Final partition statistics:");
        long totalDocs = 0;
        for (int i = 0; i < profile.getNumSplits(); i++) {
            File targetDir = new File(profile.getHardlinkSplitDirPrefix() + i);
            try (FSDirectory dir = FSDirectory.open(targetDir);
                 IndexReader reader = DirectoryReader.open(dir)) {
                int numDocs = reader.numDocs();
                totalDocs += numDocs;
                System.out.println("[HardLink]   dir" + i + ": " + numDocs + " docs, "
                        + IndexGenerator.dirSizeMB(targetDir) + " MB");
            }
        }
        System.out.println("[HardLink]   Total docs across all partitions: " + totalDocs);
    }

    public static void main(String[] args) throws IOException {
        new IndexSplitterHardLink().split();
    }
}
