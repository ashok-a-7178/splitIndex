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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a test Lucene 4 index with {@link SplitConfig#NUM_FIELDS} fields
 * and {@link SplitConfig#TOTAL_DOCS} documents.  Each document is approximately
 * {@link SplitConfig#TARGET_DOC_SIZE_BYTES} bytes in stored content.
 *
 * <p>Term values are drawn from an open-source English dictionary
 * ({@code dictionary.txt} bundled as a classpath resource) rather than
 * randomly generated character sequences, producing a more realistic index.</p>
 *
 * <p>Each document has an "ID" field whose value is one of
 * {@link SplitConfig#NUM_UNIQUE_IDS} unique string IDs, assigned in
 * round-robin fashion.</p>
 */
public class IndexGenerator {

    private static final Random RANDOM = new Random(42);

    /** Dictionary words loaded from the classpath resource. */
    private final String[] dictionary;

    public IndexGenerator() throws IOException {
        this.dictionary = loadDictionary();
    }

    /**
     * Loads the dictionary word list from the classpath resource
     * {@code dictionary.txt}.  The file is expected to contain one
     * lower-case word per line and is derived from the NLTK words
     * corpus (an open-source English word list).
     */
    private static String[] loadDictionary() throws IOException {
        List<String> words = new ArrayList<>();
        try (InputStream is = IndexGenerator.class.getClassLoader()
                .getResourceAsStream("dictionary.txt")) {
            if (is == null) {
                throw new IOException("dictionary.txt not found on classpath. "
                        + "Place the file in src/main/resources/.");
            }
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) {
                        words.add(line);
                    }
                }
            }
        }
        if (words.isEmpty()) {
            throw new IOException("dictionary.txt is empty");
        }
        System.out.println("[IndexGenerator] Loaded " + words.size()
                + " dictionary words from classpath resource");
        return words.toArray(new String[0]);
    }

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
        System.out.println("[IndexGenerator] Target doc size: ~"
                + SplitConfig.TARGET_DOC_SIZE_BYTES + " bytes");
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
     * Creates a single Lucene document with {@link SplitConfig#NUM_FIELDS}
     * fields whose combined stored content is approximately
     * {@link SplitConfig#TARGET_DOC_SIZE_BYTES} bytes.
     *
     * <p>The field mix is:
     * <ul>
     *   <li>Field 0 – "ID" (StringField, round-robin unique IDs)</li>
     *   <li>Field 1 – "docSequence" (IntField, document ordinal)</li>
     *   <li>Fields 2..N – cycling through StringField (keyword from
     *       dictionary), TextField (multi-word dictionary phrase),
     *       IntField, LongField, and FloatField</li>
     * </ul></p>
     *
     * <p>Text/String field lengths are calibrated so that the total stored
     * value bytes across all fields approximate the target document size.</p>
     */
    private Document createDocument(int docIndex) {
        Document doc = new Document();

        // ---------- fixed-overhead fields ----------
        // Field 0: ID (StringField, indexed but not tokenized)
        String idValue = "ID_" + (docIndex % SplitConfig.NUM_UNIQUE_IDS);
        doc.add(new StringField("ID", idValue, Field.Store.YES));

        // Field 1: docSequence (stored integer for traceability)
        doc.add(new IntField("docSequence", docIndex, Field.Store.YES));

        // ---------- budget calculation ----------
        // Count the fixed-size bytes contributed by the non-text remaining
        // fields and distribute the rest evenly across text-bearing fields.
        int remaining = SplitConfig.NUM_FIELDS - 2; // fields 2..NUM_FIELDS-1

        // Of every 5 fields: 1 StringField, 1 TextField, 1 Int, 1 Long, 1 Float
        int numStringFields = 0;
        int numTextFields = 0;
        int fixedBytes = idValue.length() + 4; // ID value + docSequence int
        for (int f = 2; f < SplitConfig.NUM_FIELDS; f++) {
            switch (f % 5) {
                case 0: numStringFields++; break;
                case 1: numTextFields++;   break;
                case 2: fixedBytes += 4;   break; // IntField  (4 bytes)
                case 3: fixedBytes += 8;   break; // LongField (8 bytes)
                case 4: fixedBytes += 4;   break; // FloatField(4 bytes)
            }
        }

        int textBudget = SplitConfig.TARGET_DOC_SIZE_BYTES - fixedBytes;
        if (textBudget < 0) textBudget = 0;

        int totalTextFields = numStringFields + numTextFields;
        int bytesPerTextField = (totalTextFields > 0)
                ? textBudget / totalTextFields : 0;

        // StringFields get ~40 % of per-field budget (short keywords);
        // TextFields  get the remaining ~60 % (multi-word phrases).
        int stringFieldBytes = Math.max(4, (int) (bytesPerTextField * 0.4));
        int textFieldBytes   = Math.max(8, bytesPerTextField - stringFieldBytes);

        // ---------- populate remaining fields ----------
        int fieldIndex = 2;
        while (fieldIndex < SplitConfig.NUM_FIELDS) {
            int type = fieldIndex % 5;
            String fieldName = "field_" + fieldIndex;
            switch (type) {
                case 0:
                    // StringField – single dictionary keyword
                    doc.add(new StringField(fieldName,
                            dictionaryWord(stringFieldBytes), Field.Store.YES));
                    break;
                case 1:
                    // TextField – multi-word dictionary phrase
                    doc.add(new TextField(fieldName,
                            dictionaryPhrase(textFieldBytes), Field.Store.YES));
                    break;
                case 2:
                    doc.add(new IntField(fieldName,
                            RANDOM.nextInt(100_000), Field.Store.YES));
                    break;
                case 3:
                    doc.add(new LongField(fieldName,
                            RANDOM.nextLong(), Field.Store.YES));
                    break;
                case 4:
                    doc.add(new FloatField(fieldName,
                            RANDOM.nextFloat() * 1000, Field.Store.YES));
                    break;
                default:
                    doc.add(new StringField(fieldName,
                            dictionaryWord(stringFieldBytes), Field.Store.YES));
                    break;
            }
            fieldIndex++;
        }

        return doc;
    }

    /**
     * Returns a single dictionary word whose length is as close as possible
     * to {@code targetBytes}.  If no single word is long enough the word is
     * returned as-is (slightly under budget), keeping the keyword nature of
     * a StringField.
     */
    private String dictionaryWord(int targetBytes) {
        // Pick a random word and, if it is shorter than the target, try a
        // few more candidates and keep the one closest in length.
        String best = dictionary[RANDOM.nextInt(dictionary.length)];
        for (int attempt = 0; attempt < 3; attempt++) {
            String candidate = dictionary[RANDOM.nextInt(dictionary.length)];
            if (Math.abs(candidate.length() - targetBytes)
                    < Math.abs(best.length() - targetBytes)) {
                best = candidate;
            }
        }
        // Truncate if too long
        if (best.length() > targetBytes) {
            best = best.substring(0, targetBytes);
        }
        return best;
    }

    /**
     * Builds a multi-word phrase from dictionary words whose total length
     * (including spaces) approximates {@code targetBytes}.
     */
    private String dictionaryPhrase(int targetBytes) {
        StringBuilder sb = new StringBuilder(targetBytes);
        while (sb.length() < targetBytes) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(dictionary[RANDOM.nextInt(dictionary.length)]);
        }
        // Trim to target if slightly over
        if (sb.length() > targetBytes) {
            sb.setLength(targetBytes);
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
