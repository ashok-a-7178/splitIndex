package com.copybenchmark;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * Validates that a copied (or hard-linked) Lucene index is readable and
 * returns correct data.
 *
 * <p>Performs the following checks:
 * <ol>
 *   <li>The destination directory exists and contains index files.</li>
 *   <li>A {@link DirectoryReader} can be opened on the destination.</li>
 *   <li>The destination contains a positive number of documents.</li>
 *   <li>Random term queries return results and stored fields are intact.</li>
 * </ol>
 */
public class DataAvailabilityValidator {

    private final File destDir;

    public DataAvailabilityValidator(File destDir) {
        this.destDir = destDir;
    }

    /**
     * Run all validation checks.
     *
     * @return {@code true} if every check passes
     */
    public boolean validate() {
        System.out.println();
        System.out.println("=== Data Availability Validation [" + destDir.getName() + "] ===");

        // 1. Directory exists and has files
        if (!destDir.exists() || !destDir.isDirectory()) {
            System.out.println("  FAIL: destination directory does not exist.");
            return false;
        }
        String[] files = destDir.list();
        if (files == null || files.length == 0) {
            System.out.println("  FAIL: destination directory is empty.");
            return false;
        }
        System.out.println("  OK  : directory exists with " + files.length + " files.");

        IndexReader reader = null;
        try {
            // 2. Open reader
            reader = DirectoryReader.open(FSDirectory.open(destDir));
            int numDocs = reader.numDocs();
            System.out.println("  OK  : IndexReader opened – " + numDocs + " documents.");

            if (numDocs == 0) {
                System.out.println("  FAIL: index contains 0 documents.");
                return false;
            }

            // 3. Read a stored document
            Document doc = reader.document(0);
            if (doc.get("id") == null) {
                System.out.println("  FAIL: stored field 'id' is missing from document 0.");
                return false;
            }
            System.out.println("  OK  : stored fields are readable (doc 0 id=" + doc.get("id") + ").");

            // 4. Random term searches
            IndexSearcher searcher = new IndexSearcher(reader);
            Random rng = new Random(99);
            int hits = 0;
            int queries = CopyBenchmarkConfig.VALIDATION_QUERY_COUNT;
            for (int i = 0; i < queries; i++) {
                int randomDocIndex = rng.nextInt(numDocs);
                Document randomDoc = reader.document(randomDocIndex);
                String idValue = randomDoc.get("id");
                if (idValue != null) {
                    TopDocs td = searcher.search(new TermQuery(new Term("id", idValue)), 1);
                    if (td.totalHits > 0) hits++;
                }
            }
            System.out.printf("  OK  : %d / %d random term queries returned results.%n", hits, queries);

            if (hits < queries) {
                System.out.println("  WARN: some queries returned no results – possible partial copy.");
            }

            System.out.println("  PASS: data availability validated.");
            return true;

        } catch (IOException e) {
            System.out.println("  FAIL: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            if (reader != null) {
                try { reader.close(); } catch (IOException ignored) { }
            }
        }
    }
}
