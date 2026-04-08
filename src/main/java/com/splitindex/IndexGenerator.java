package com.splitindex;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a test Lucene 4 index with {@link SplitConfig#NUM_FIELDS} fields
 * and {@link SplitConfig#TOTAL_DOCS} documents. Each document has an "ID" field
 * whose value is one of {@link SplitConfig#NUM_UNIQUE_IDS} unique string IDs,
 * assigned in round-robin fashion.
 */
public class IndexGenerator {

    private static final Random RANDOM = new Random(42);
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

    /**
     * Generates the source index at {@link SplitConfig#SOURCE_INDEX_DIR}.
     *
     * @return elapsed time in milliseconds
     */
    public long generate() throws IOException {
        File sourceDir = new File(SplitConfig.SOURCE_INDEX_DIR);
        if (sourceDir.exists() && sourceDir.list() != null && sourceDir.list().length > 0) {
            System.out.println("[IndexGenerator] Source index already exists at: " + sourceDir.getAbsolutePath());
            System.out.println("[IndexGenerator] Skipping generation. Delete the directory to regenerate.");
            return 0;
        }
        sourceDir.mkdirs();

        System.out.println("[IndexGenerator] Generating index with " + SplitConfig.TOTAL_DOCS
                + " docs, " + SplitConfig.NUM_FIELDS + " fields, "
                + SplitConfig.NUM_UNIQUE_IDS + " unique IDs");
        System.out.println("[IndexGenerator] Target directory: " + sourceDir.getAbsolutePath());

        long start = System.currentTimeMillis();

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4, new StandardAnalyzer());
        config.setRAMBufferSizeMB(SplitConfig.RAM_BUFFER_SIZE_MB);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        try (FSDirectory dir = FSDirectory.open(sourceDir);
             IndexWriter writer = new IndexWriter(dir, config)) {

            List<Document> batch = new ArrayList<>(SplitConfig.INDEX_BATCH_SIZE);

            for (int i = 0; i < SplitConfig.TOTAL_DOCS; i++) {
                Document doc = createDocument(i);
                batch.add(doc);

                if (batch.size() >= SplitConfig.INDEX_BATCH_SIZE) {
                    writer.addDocuments(batch);
                    batch.clear();
                    if ((i + 1) % 100_000 == 0) {
                        System.out.println("[IndexGenerator] Indexed " + (i + 1) + " / " + SplitConfig.TOTAL_DOCS + " docs");
                    }
                }
            }
            if (!batch.isEmpty()) {
                writer.addDocuments(batch);
            }

            System.out.println("[IndexGenerator] Committing...");
            writer.commit();
            System.out.println("[IndexGenerator] Optimizing (forceMerge to 1 segment)...");
            writer.forceMerge(1);
            writer.commit();
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("[IndexGenerator] Index generation completed in " + elapsed + " ms");
        System.out.println("[IndexGenerator] Index size: " + dirSizeMB(sourceDir) + " MB");
        return elapsed;
    }

    /**
     * Creates a single Lucene document with NUM_FIELDS fields.
     * Field "ID" is assigned round-robin from 0..NUM_UNIQUE_IDS-1.
     */
    private Document createDocument(int docIndex) {
        Document doc = new Document();

        // Field 1: ID (StringField, indexed but not tokenized)
        String idValue = "ID_" + (docIndex % SplitConfig.NUM_UNIQUE_IDS);
        doc.add(new StringField("ID", idValue, Field.Store.YES));

        // Field 2: docSequence (stored integer for traceability)
        doc.add(new IntField("docSequence", docIndex, Field.Store.YES));

        // Remaining fields: mix of types to simulate real-world index
        int fieldIndex = 2;
        while (fieldIndex < SplitConfig.NUM_FIELDS) {
            int type = fieldIndex % 5;
            String fieldName = "field_" + fieldIndex;
            switch (type) {
                case 0:
                    // StringField (keyword-like, not tokenized)
                    doc.add(new StringField(fieldName, "val_" + RANDOM.nextInt(1000), Field.Store.YES));
                    break;
                case 1:
                    // TextField (tokenized text content)
                    doc.add(new TextField(fieldName, randomText(10 + RANDOM.nextInt(20)), Field.Store.YES));
                    break;
                case 2:
                    // IntField
                    doc.add(new IntField(fieldName, RANDOM.nextInt(100_000), Field.Store.YES));
                    break;
                case 3:
                    // LongField
                    doc.add(new LongField(fieldName, RANDOM.nextLong(), Field.Store.YES));
                    break;
                case 4:
                    // FloatField
                    doc.add(new FloatField(fieldName, RANDOM.nextFloat() * 1000, Field.Store.YES));
                    break;
                default:
                    doc.add(new StringField(fieldName, "default_" + fieldIndex, Field.Store.YES));
                    break;
            }
            fieldIndex++;
        }

        return doc;
    }

    /** Generates a random text string of approximately the given word count. */
    private String randomText(int wordCount) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < wordCount; i++) {
            if (i > 0) sb.append(' ');
            int wordLen = 3 + RANDOM.nextInt(8);
            for (int j = 0; j < wordLen; j++) {
                sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
            }
        }
        return sb.toString();
    }

    /** Returns directory size in megabytes. */
    static double dirSizeMB(File dir) {
        return dirSizeBytes(dir) / (1024.0 * 1024.0);
    }

    /** Returns directory size in bytes (recursive). */
    private static long dirSizeBytes(File dir) {
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isFile()) {
                    size += f.length();
                } else if (f.isDirectory()) {
                    size += dirSizeBytes(f);
                }
            }
        }
        return size;
    }

    public static void main(String[] args) throws IOException {
        new IndexGenerator().generate();
    }
}
