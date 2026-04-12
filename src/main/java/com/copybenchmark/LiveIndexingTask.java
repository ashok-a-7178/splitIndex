package com.copybenchmark;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Runnable} that continuously adds documents to a Lucene index.
 *
 * <p>It is started in a background thread and runs until {@link #stop()} is
 * called.  Each document has a deterministic structure (string id, several
 * text / numeric fields) to ensure the index is realistic.</p>
 *
 * <p>After stopping, callers can retrieve the total number of documents
 * written via {@link #getDocsAdded()}.</p>
 */
public class LiveIndexingTask implements Runnable {

    private static final String[] WORDS = {
            "benchmark", "lucene", "index", "segment", "merge", "commit",
            "search", "query", "filter", "score", "document", "field",
            "analyzer", "token", "term", "reader", "writer", "directory",
            "codec", "posting", "shard", "replica", "cluster", "node",
            "memory", "buffer", "flush", "optimize", "delete", "update",
            "cache", "refresh", "translog", "snapshot", "restore", "backup",
            "recovery", "replication", "routing", "allocation", "balance",
            "throttle", "compress", "decompress", "encrypt", "decrypt",
            "serialize", "deserialize", "parse", "format", "validate"
    };

    private final File indexDir;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger docsAdded = new AtomicInteger(0);
    private final int batchSize;
    private final long pauseMs;

    public LiveIndexingTask(File indexDir) {
        this(indexDir,
                CopyBenchmarkConfig.LIVE_BATCH_SIZE,
                CopyBenchmarkConfig.LIVE_BATCH_PAUSE_MS);
    }

    public LiveIndexingTask(File indexDir, int batchSize, long pauseMs) {
        this.indexDir = indexDir;
        this.batchSize = batchSize;
        this.pauseMs = pauseMs;
    }

    /** Signal the task to finish after the current batch. */
    public void stop() {
        running.set(false);
    }

    /** Total documents successfully committed so far. */
    public int getDocsAdded() {
        return docsAdded.get();
    }

    @Override
    public void run() {
        IndexWriter writer = null;
        try {
            FSDirectory dir = FSDirectory.open(indexDir);
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_4,
                    new StandardAnalyzer());
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            config.setRAMBufferSizeMB(CopyBenchmarkConfig.RAM_BUFFER_SIZE_MB);
            writer = new IndexWriter(dir, config);

            Random rng = new Random(42);
            int docId = 0;

            while (running.get()) {
                for (int i = 0; i < batchSize && running.get(); i++) {
                    writer.addDocument(createDocument(docId++, rng));
                }
                writer.commit();
                docsAdded.addAndGet(batchSize);

                if (pauseMs > 0 && running.get()) {
                    Thread.sleep(pauseMs);
                }
            }

            // final commit to ensure everything is flushed
            writer.commit();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            System.err.println("[LiveIndexingTask] IO error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try { writer.close(); } catch (IOException ignored) { }
            }
        }
    }

    /* ------------------------------------------------------------------ */

    private Document createDocument(int docId, Random rng) {
        Document doc = new Document();

        doc.add(new StringField("id", "LIVE_" + docId, Field.Store.YES));
        doc.add(new LongField("timestamp", System.currentTimeMillis(), Field.Store.YES));
        doc.add(new IntField("seq", docId, Field.Store.YES));

        // Generate text fields to bulk up document size
        int numFields = CopyBenchmarkConfig.NUM_FIELDS;
        for (int f = 0; f < numFields; f++) {
            String fieldName = "field_" + f;
            String value = randomPhrase(rng, 5 + rng.nextInt(10));
            doc.add(new TextField(fieldName, value, Field.Store.YES));
        }

        return doc;
    }

    private String randomPhrase(Random rng, int wordCount) {
        StringBuilder sb = new StringBuilder();
        for (int w = 0; w < wordCount; w++) {
            if (w > 0) sb.append(' ');
            sb.append(WORDS[rng.nextInt(WORDS.length)]);
        }
        return sb.toString();
    }
}
