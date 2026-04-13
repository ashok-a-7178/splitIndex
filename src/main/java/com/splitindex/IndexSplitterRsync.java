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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Approach 3: Rsync Copy + BooleanQuery Term Deletion.
 *
 * <p>This approach:
 * <ol>
 *   <li>Copies the entire source index directory to each of the 10 target directories
 *       using {@code rsync -a} (archive mode, preserving permissions and timestamps).</li>
 *   <li>Opens an IndexWriter on each copied directory and deletes documents whose ID
 *       values do NOT belong to that partition, using BooleanQuery with TermQuery clauses.</li>
 *   <li>Force-merges each resulting index to reclaim space from deleted documents.</li>
 * </ol>
 *
 * <p><strong>Key characteristic:</strong> Rsync performs a full data copy similar to
 * {@link IndexSplitterCopy}, but uses the external {@code rsync} tool which may leverage
 * different I/O strategies (e.g., sendfile, buffered reads) and is commonly used in
 * production deployments for index replication.</p>
 *
 * <p><strong>Requirement:</strong> The {@code rsync} command must be available on the
 * system PATH.</p>
 */
public class IndexSplitterRsync {

    /**
     * Runs the rsync-based split benchmark.
     *
     * @return an array of 3 longs: [totalTimeMs, rsyncTimeMs, deleteTimeMs]
     */
    public long[] split() throws IOException {
        System.out.println("\n========================================");
        System.out.println("  APPROACH 3: Rsync Copy + Delete       ");
        System.out.println("========================================\n");

        File sourceDir = new File(SplitConfig.SOURCE_INDEX_DIR);
        if (!sourceDir.exists()) {
            throw new IOException("Source index not found at: " + sourceDir.getAbsolutePath());
        }

        // Verify rsync is available
        verifyRsyncAvailable();

        // Collect unique IDs
        List<String> uniqueIds = collectUniqueIds();
        System.out.println("[Rsync] Found " + uniqueIds.size() + " unique IDs in source index");

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
            System.out.println("[Rsync] Partition " + i + " keeps " + partitionIds[i].size() + " IDs");
        }

        long totalStart = System.currentTimeMillis();

        // Phase 1: Rsync source index to all target directories
        System.out.println("\n[Rsync] Phase 1: Rsync-copying source index to "
                + SplitConfig.NUM_SPLITS + " directories...");
        long rsyncStart = System.currentTimeMillis();
        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            File targetDir = new File(SplitConfig.RSYNC_SPLIT_DIR_PREFIX + i);
            rsyncDirectory(sourceDir, targetDir);
            System.out.println("[Rsync]   Copied to dir" + i + " ("
                    + IndexGenerator.dirSizeMB(targetDir) + " MB)");
        }
        long rsyncTime = System.currentTimeMillis() - rsyncStart;
        System.out.println("[Rsync] Phase 1 completed in " + rsyncTime + " ms");

        // Phase 2: Delete non-matching IDs from each partition
        System.out.println("\n[Rsync] Phase 2: Deleting non-matching documents from each partition...");
        long deleteStart = System.currentTimeMillis();
        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            File targetDir = new File(SplitConfig.RSYNC_SPLIT_DIR_PREFIX + i);
            deleteNonMatchingDocs(targetDir, partitionIds[i], i);
        }
        long deleteTime = System.currentTimeMillis() - deleteStart;
        System.out.println("[Rsync] Phase 2 completed in " + deleteTime + " ms");

        long totalTime = System.currentTimeMillis() - totalStart;
        System.out.println("\n[Rsync] TOTAL TIME: " + totalTime + " ms");
        System.out.println("[Rsync]   Rsync phase:  " + rsyncTime + " ms");
        System.out.println("[Rsync]   Delete phase:  " + deleteTime + " ms");

        // Print final stats
        printPartitionStats();

        return new long[]{totalTime, rsyncTime, deleteTime};
    }

    /**
     * Verifies that {@code rsync} is available on the system PATH.
     *
     * @throws IOException if rsync is not found or cannot be executed
     */
    private void verifyRsyncAvailable() throws IOException {
        try {
            Process process = new ProcessBuilder("rsync", "--version")
                    .redirectErrorStream(true)
                    .start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("rsync exited with code " + exitCode
                        + ". Ensure rsync is installed and available on PATH.");
            }
            System.out.println("[Rsync] rsync is available on this system");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while checking rsync availability", e);
        }
    }

    /**
     * Collects all unique ID values (generated deterministically).
     */
    private List<String> collectUniqueIds() {
        List<String> ids = new ArrayList<>(SplitConfig.NUM_UNIQUE_IDS);
        for (int i = 0; i < SplitConfig.NUM_UNIQUE_IDS; i++) {
            ids.add("ID_" + i);
        }
        return ids;
    }

    /**
     * Copies the source directory to the target using {@code rsync -a --delete}.
     *
     * <p>The {@code -a} flag enables archive mode (recursive, preserves symlinks,
     * permissions, timestamps, group, owner, and device files). The {@code --delete}
     * flag removes extraneous files from the target directory.</p>
     *
     * <p>A trailing slash on the source path means "copy the contents of this
     * directory" rather than "copy the directory itself".</p>
     *
     * @param sourceDir the source index directory
     * @param targetDir the target directory to rsync into
     * @throws IOException if rsync fails or is interrupted
     */
    private void rsyncDirectory(File sourceDir, File targetDir) throws IOException {
        if (targetDir.exists()) {
            IndexSplitterCopy.deleteDirectory(targetDir);
        }
        targetDir.mkdirs();

        // Trailing slash on source means "copy contents of directory"
        String sourcePath = sourceDir.getAbsolutePath() + File.separator;
        String targetPath = targetDir.getAbsolutePath() + File.separator;

        ProcessBuilder pb = new ProcessBuilder(
                "rsync", "-a", "--delete", sourcePath, targetPath
        );
        pb.redirectErrorStream(true);

        try {
            Process process = pb.start();

            // Drain stdout to prevent the rsync process from blocking
            // when the OS pipe buffer fills up
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                while (reader.readLine() != null) {
                    // output discarded; rsync produces none in non-verbose mode
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException("rsync failed with exit code " + exitCode
                        + " for source=" + sourcePath + " target=" + targetPath);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("rsync was interrupted", e);
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
        Set<String> keepSet = new HashSet<>(keepIds);
        List<String> deleteIds = new ArrayList<>();
        for (String id : allIds) {
            if (!keepSet.contains(id)) {
                deleteIds.add(id);
            }
        }

        System.out.println("[Rsync]   Partition " + partitionIndex
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
            System.out.println("[Rsync]   Partition " + partitionIndex + ": force merging...");
            writer.forceMerge(1);
            writer.commit();

            System.out.println("[Rsync]   Partition " + partitionIndex + ": done. Docs remaining: "
                    + writer.numDocs() + ", size: " + IndexGenerator.dirSizeMB(targetDir) + " MB");
        }
    }

    /** Prints document counts for all partitions. */
    private void printPartitionStats() throws IOException {
        System.out.println("\n[Rsync] Final partition statistics:");
        long totalDocs = 0;
        for (int i = 0; i < SplitConfig.NUM_SPLITS; i++) {
            File targetDir = new File(SplitConfig.RSYNC_SPLIT_DIR_PREFIX + i);
            try (FSDirectory dir = FSDirectory.open(targetDir);
                 IndexReader reader = DirectoryReader.open(dir)) {
                int numDocs = reader.numDocs();
                totalDocs += numDocs;
                System.out.println("[Rsync]   dir" + i + ": " + numDocs + " docs, "
                        + IndexGenerator.dirSizeMB(targetDir) + " MB");
            }
        }
        System.out.println("[Rsync]   Total docs across all partitions: " + totalDocs);
    }

    public static void main(String[] args) throws IOException {
        new IndexSplitterRsync().split();
    }
}
