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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Approach 1: Sequential File Copy + BooleanQuery Term Deletion.
 *
 * <p>This approach:
 * <ol>
 *   <li>Copies the entire source index directory to each of the 10 target directories
 *       using sequential file I/O (FileChannel transfer).</li>
 *   <li>Opens an IndexWriter on each copied directory and deletes documents whose ID
 *       values do NOT belong to that partition, using BooleanQuery with TermQuery clauses.</li>
 *   <li>Force-merges each resulting index to reclaim space from deleted documents.</li>
 * </ol>
 */
public class IndexSplitterCopy {

    /**
     * Runs the copy-based split benchmark.
     *
     * @return an array of 3 longs: [totalTimeMs, copyTimeMs, deleteTimeMs]
     */
    public long[] split() throws IOException {
        System.out.println("\n========================================");
        System.out.println("  APPROACH 1: Sequential Copy + Delete  ");
        System.out.println("========================================\n");

        File sourceDir = new File(SplitConfig.SOURCE_INDEX_DIR);
        if (!sourceDir.exists()) {
            throw new IOException("Source index not found at: " + sourceDir.getAbsolutePath());
        }

        // Collect unique IDs from the source index
        List<String> uniqueIds = collectUniqueIds();
        System.out.println("[Copy] Found " + uniqueIds.size() + " unique IDs in source index");

        // Assign IDs to partitions (round-robin)
        @SuppressWarnings("unchecked")
        List<String>[] partitionIds = new List[SplitConfig.NUM_SPLITS];
        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            partitionIds[i] = new ArrayList<>();
        }
        for (int i = 0; i < uniqueIds.size(); i++) {
            partitionIds[i % SplitConfig.NUM_SPLITS].add(uniqueIds.get(i));
        }

        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            System.out.println("[Copy] Partition " + i + " keeps " + partitionIds[i].size() + " IDs");
        }

        long totalStart = System.currentTimeMillis();

        // Phase 1: Copy source index to all target directories
        System.out.println("\n[Copy] Phase 1: Copying source index to " + SplitConfig.NUM_SPLITS + " directories...");
        long copyStart = System.currentTimeMillis();
        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            File targetDir = new File(SplitConfig.COPY_SPLIT_DIR_PREFIX + i);
            copyDirectory(sourceDir, targetDir);
            System.out.println("[Copy]   Copied to dir" + i + " (" + IndexGenerator.dirSizeMB(targetDir) + " MB)");
        }
        long copyTime = System.currentTimeMillis() - copyStart;
        System.out.println("[Copy] Phase 1 completed in " + copyTime + " ms");

        // Phase 2: Delete non-matching IDs from each partition
        System.out.println("\n[Copy] Phase 2: Deleting non-matching documents from each partition...");
        long deleteStart = System.currentTimeMillis();
        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            File targetDir = new File(SplitConfig.COPY_SPLIT_DIR_PREFIX + i);
            deleteNonMatchingDocs(targetDir, partitionIds[i], i);
        }
        long deleteTime = System.currentTimeMillis() - deleteStart;
        System.out.println("[Copy] Phase 2 completed in " + deleteTime + " ms");

        long totalTime = System.currentTimeMillis() - totalStart;
        System.out.println("\n[Copy] TOTAL TIME: " + totalTime + " ms");
        System.out.println("[Copy]   Copy phase:   " + copyTime + " ms");
        System.out.println("[Copy]   Delete phase:  " + deleteTime + " ms");

        // Print final stats
        printPartitionStats();

        return new long[]{totalTime, copyTime, deleteTime};
    }

    /**
     * Collects all unique ID values from the source index.
     */
    private List<String> collectUniqueIds() throws IOException {
        // Since we know the ID pattern, we generate them deterministically
        List<String> ids = new ArrayList<>(SplitConfig.NUM_UNIQUE_IDS);
        for (int i = 0; i < SplitConfig.NUM_UNIQUE_IDS; i++) {
            ids.add("ID_" + i);
        }
        return ids;
    }

    /**
     * Copies all files from sourceDir to targetDir using FileChannel transfer.
     */
    private void copyDirectory(File sourceDir, File targetDir) throws IOException {
        if (targetDir.exists()) {
            deleteDirectory(targetDir);
        }
        targetDir.mkdirs();

        File[] files = sourceDir.listFiles();
        if (files == null) return;

        for (File srcFile : files) {
            if (srcFile.isFile()) {
                File destFile = new File(targetDir, srcFile.getName());
                try (FileInputStream fis = new FileInputStream(srcFile);
                     FileOutputStream fos = new FileOutputStream(destFile);
                     FileChannel srcChannel = fis.getChannel();
                     FileChannel destChannel = fos.getChannel()) {
                    long size = srcChannel.size();
                    long transferred = 0;
                    while (transferred < size) {
                        transferred += srcChannel.transferTo(transferred, size - transferred, destChannel);
                    }
                }
            }
        }
    }

    /**
     * Deletes documents from the index at targetDir whose ID is NOT in keepIds.
     * Uses BooleanQuery batches to delete by term.
     */
    private void deleteNonMatchingDocs(File targetDir, List<String> keepIds, int partitionIndex)
            throws IOException {

        // Build a set of IDs to DELETE (all IDs except the ones we keep)
        List<String> allIds = collectUniqueIds();
        java.util.Set<String> keepSet = new java.util.HashSet<>(keepIds);
        List<String> deleteIds = new ArrayList<>();
        for (String id : allIds) {
            if (!keepSet.contains(id)) {
                deleteIds.add(id);
            }
        }

        System.out.println("[Copy]   Partition " + partitionIndex
                + ": deleting " + deleteIds.size() + " IDs, keeping " + keepIds.size() + " IDs");

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4, new StandardAnalyzer());
        config.setRAMBufferSizeMB(SplitConfig.RAM_BUFFER_SIZE_MB);
        config.setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        try (FSDirectory dir = FSDirectory.open(targetDir);
             IndexWriter writer = new IndexWriter(dir, config)) {

            // Delete in batches to respect BooleanQuery max clause count
            int batchSize = SplitConfig.MAX_BOOLEAN_CLAUSES;
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
            System.out.println("[Copy]   Partition " + partitionIndex + ": force merging...");
            writer.forceMerge(1);
            writer.commit();

            System.out.println("[Copy]   Partition " + partitionIndex + ": done. Docs remaining: "
                    + writer.numDocs() + ", size: " + IndexGenerator.dirSizeMB(targetDir) + " MB");
        }
    }

    /** Prints document counts for all partitions. */
    private void printPartitionStats() throws IOException {
        System.out.println("\n[Copy] Final partition statistics:");
        long totalDocs = 0;
        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            File targetDir = new File(SplitConfig.COPY_SPLIT_DIR_PREFIX + i);
            try (FSDirectory dir = FSDirectory.open(targetDir);
                 IndexReader reader = DirectoryReader.open(dir)) {
                int numDocs = reader.numDocs();
                totalDocs += numDocs;
                System.out.println("[Copy]   dir" + i + ": " + numDocs + " docs, "
                        + IndexGenerator.dirSizeMB(targetDir) + " MB");
            }
        }
        System.out.println("[Copy]   Total docs across all partitions: " + totalDocs);
    }

    /** Recursively deletes a directory. */
    static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    f.delete();
                }
            }
        }
        dir.delete();
    }

    public static void main(String[] args) throws IOException {
        new IndexSplitterCopy().split();
    }
}
