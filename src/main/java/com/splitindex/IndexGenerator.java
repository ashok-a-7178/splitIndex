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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

    /** Dictionary words loaded from the classpath resource. */
    private final String[] dictionary;

    /** Pre-computed bytes-per-StringField (same for every document). */
    private final int stringFieldBytes;

    /** Pre-computed bytes-per-TextField (same for every document). */
    private final int textFieldBytes;

    /** Configuration profile for this generator. */
    private final BenchmarkProfile profile;

    /**
     * Creates a generator using the default {@link SplitConfig} constants.
     */
    public IndexGenerator() throws IOException {
        this(BenchmarkProfile.fromSplitConfig());
    }

    /**
     * Creates a generator using the supplied {@link BenchmarkProfile}.
     *
     * @param profile benchmark configuration (doc count, field count, paths, etc.)
     */
    public IndexGenerator(BenchmarkProfile profile) throws IOException {
        this.profile = profile;
        this.dictionary = loadDictionary();

        // Pre-compute field layout once (invariant across all documents)
        int maxIdLength = ("ID_" + (profile.getNumUniqueIds() - 1)).length();
        int fixedBytes = maxIdLength + 4; // ID string + docSequence int
        int numStringFields = 0;
        int numTextFields = 0;
        for (int f = 2; f < profile.getNumFields(); f++) {
            switch (f % 5) {
                case 0: numStringFields++; break;
                case 1: numTextFields++;   break;
                case 2: fixedBytes += 4;   break;
                case 3: fixedBytes += 8;   break;
                case 4: fixedBytes += 4;   break;
            }
        }
        int textBudget = Math.max(0, profile.getTargetDocSizeBytes() - fixedBytes);
        int totalTextFields = numStringFields + numTextFields;
        int bytesPerTextField = (totalTextFields > 0) ? textBudget / totalTextFields : 0;
        this.stringFieldBytes = Math.max(4, (int) (bytesPerTextField * 0.4));
        this.textFieldBytes   = Math.max(8, bytesPerTextField - this.stringFieldBytes);
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
     * Generates the source index at {@link SplitConfig#SOURCE_INDEX_DIR}
     * using a producer-consumer pattern:
     * <ul>
     *   <li>{@link SplitConfig#NUM_PRODUCER_THREADS} producer threads
     *       create {@link Document} batches in parallel</li>
     *   <li>The main thread acts as the consumer, writing batches to
     *       the {@link IndexWriter}</li>
     * </ul>
     *
     * @return elapsed time in milliseconds
     */
    /** Returns the profile used by this generator. */
    public BenchmarkProfile getProfile() {
        return profile;
    }

    public long generate() throws IOException {
        File sourceDir = new File(profile.getSourceIndexDir());
        if (sourceDir.exists() && sourceDir.list() != null && sourceDir.list().length > 0) {
            System.out.println("[IndexGenerator] Source index already exists at: " + sourceDir.getAbsolutePath());
            System.out.println("[IndexGenerator] Skipping generation. Delete the directory to regenerate.");
            return 0;
        }
        sourceDir.mkdirs();

        int numProducers = SplitConfig.NUM_PRODUCER_THREADS;
        System.out.println("[IndexGenerator] Generating index with " + profile.getTotalDocs()
                + " docs, " + profile.getNumFields() + " fields, "
                + profile.getNumUniqueIds() + " unique IDs");
        System.out.println("[IndexGenerator] Target doc size: ~"
                + profile.getTargetDocSizeBytes() + " bytes");
        System.out.println("[IndexGenerator] Using " + numProducers + " producer threads");
        System.out.println("[IndexGenerator] Target directory: " + sourceDir.getAbsolutePath());

        long start = System.currentTimeMillis();

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4, new StandardAnalyzer());
        config.setRAMBufferSizeMB(profile.getRamBufferSizeMB());
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

        try (FSDirectory dir = FSDirectory.open(sourceDir);
             IndexWriter writer = new IndexWriter(dir, config)) {

            int queueCapacity = SplitConfig.NUM_PRODUCER_THREADS * 2;
            BlockingQueue<List<Document>> queue =
                    new ArrayBlockingQueue<>(queueCapacity);
            CountDownLatch producersDone = new CountDownLatch(numProducers);
            AtomicReference<Throwable> producerError = new AtomicReference<>();

            // --- Producer threads: create documents in parallel ---
            ExecutorService producerPool = Executors.newFixedThreadPool(numProducers, r -> {
                Thread t = new Thread(r, "doc-producer");
                t.setDaemon(true);
                return t;
            });

            int docsPerProducer = profile.getTotalDocs() / numProducers;
            for (int p = 0; p < numProducers; p++) {
                int startDoc = p * docsPerProducer;
                int endDoc = (p == numProducers - 1)
                        ? profile.getTotalDocs()
                        : startDoc + docsPerProducer;

                producerPool.submit(() -> {
                    try {
                        Random rng = ThreadLocalRandom.current();
                        List<Document> batch = new ArrayList<>(profile.getIndexBatchSize());
                        for (int i = startDoc; i < endDoc; i++) {
                            if (producerError.get() != null) return;
                            batch.add(createDocument(i, rng));
                            if (batch.size() >= profile.getIndexBatchSize()) {
                                queue.put(batch);
                                batch = new ArrayList<>(profile.getIndexBatchSize());
                            }
                        }
                        if (!batch.isEmpty()) {
                            queue.put(batch);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Throwable e) {
                        producerError.compareAndSet(null, e);
                    } finally {
                        producersDone.countDown();
                    }
                });
            }
            producerPool.shutdown();

            // --- Consumer: main thread writes batches to IndexWriter ---
            int totalWritten = 0;
            while (true) {
                List<Document> batch = queue.poll(100, TimeUnit.MILLISECONDS);
                if (batch != null) {
                    writer.addDocuments(batch);
                    totalWritten += batch.size();
                    if (totalWritten % 100_000 < profile.getIndexBatchSize()) {
                        System.out.println("[IndexGenerator] Indexed "
                                + totalWritten + " / " + profile.getTotalDocs() + " docs");
                    }
                }

                Throwable err = producerError.get();
                if (err != null) {
                    producerPool.shutdownNow();
                    throw new IOException("Producer thread failed", err);
                }

                if (producersDone.await(0, TimeUnit.MILLISECONDS) && queue.isEmpty()) {
                    break;
                }
            }

            System.out.println("[IndexGenerator] All " + totalWritten + " docs indexed");
            System.out.println("[IndexGenerator] Committing...");
            writer.commit();
            System.out.println("[IndexGenerator] Optimizing (forceMerge to 1 segment)...");
            writer.forceMerge(1);
            writer.commit();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Index generation interrupted", e);
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("[IndexGenerator] Index generation completed in " + elapsed + " ms");
        System.out.println("[IndexGenerator] Index size: " + dirSizeMB(sourceDir) + " MB");
        return elapsed;
    }

    /**
     * Creates a single Lucene document using pre-computed field layout
     * constants and the supplied thread-local {@link Random}.
     */
    private Document createDocument(int docIndex, Random rng) {
        Document doc = new Document();

        String idValue = "ID_" + (docIndex % profile.getNumUniqueIds());
        doc.add(new StringField("ID", idValue, Field.Store.YES));
        doc.add(new IntField("docSequence", docIndex, Field.Store.YES));

        for (int fieldIndex = 2; fieldIndex < profile.getNumFields(); fieldIndex++) {
            String fieldName = "field_" + fieldIndex;
            switch (fieldIndex % 5) {
                case 0:
                    doc.add(new StringField(fieldName,
                            dictionaryWord(stringFieldBytes, rng), Field.Store.YES));
                    break;
                case 1:
                    doc.add(new TextField(fieldName,
                            dictionaryPhrase(textFieldBytes, rng), Field.Store.YES));
                    break;
                case 2:
                    doc.add(new IntField(fieldName,
                            rng.nextInt(100_000), Field.Store.YES));
                    break;
                case 3:
                    doc.add(new LongField(fieldName,
                            rng.nextLong(), Field.Store.YES));
                    break;
                case 4:
                    doc.add(new FloatField(fieldName,
                            rng.nextFloat() * 1000, Field.Store.YES));
                    break;
                default:
                    doc.add(new StringField(fieldName,
                            dictionaryWord(stringFieldBytes, rng), Field.Store.YES));
                    break;
            }
        }

        return doc;
    }

    private String dictionaryWord(int targetBytes, Random rng) {
        String best = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            String candidate = dictionary[rng.nextInt(dictionary.length)];
            if (candidate.length() <= targetBytes) {
                if (best == null || candidate.length() > best.length()) {
                    best = candidate;
                }
            }
        }
        if (best == null) {
            best = dictionary[rng.nextInt(dictionary.length)];
            while (best.length() > targetBytes) {
                best = dictionary[rng.nextInt(dictionary.length)];
            }
        }
        return best;
    }

    private String dictionaryPhrase(int targetBytes, Random rng) {
        StringBuilder sb = new StringBuilder(targetBytes);
        while (sb.length() < targetBytes) {
            String word = dictionary[rng.nextInt(dictionary.length)];
            int needed = sb.length() == 0 ? word.length() : 1 + word.length();
            if (sb.length() + needed > targetBytes) {
                int remaining = targetBytes - sb.length()
                        - (sb.length() == 0 ? 0 : 1);
                if (remaining < 3) break;
                boolean fitted = false;
                for (int retry = 0; retry < 5; retry++) {
                    word = dictionary[rng.nextInt(dictionary.length)];
                    if (word.length() <= remaining) {
                        if (sb.length() > 0) sb.append(' ');
                        sb.append(word);
                        fitted = true;
                        break;
                    }
                }
                if (!fitted) break;
            } else {
                if (sb.length() > 0) sb.append(' ');
                sb.append(word);
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
