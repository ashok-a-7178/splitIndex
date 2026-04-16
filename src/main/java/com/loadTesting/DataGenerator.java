package com.loadTesting;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 * Data generator that creates bulk change documents using a predefined dictionary,
 * forms bulkChgArr JSONArray batches (max 500 per batch), base64 encodes them,
 * and sends them via HTTP POST to the search index worker server.
 *
 * Usage: Run main(), then enter total documents, fields per document, and
 * target document size in bytes when prompted.
 */
public class DataGenerator {

    private static final int MAX_BATCH_SIZE = 500;
    private static final String DEFAULT_URL = "http://localhost:8080/searchtestchangecollector";
    private static final String SERVICE = "newimplone";
    private static final String VERSION = "v1";
    private static final String ZSOID = "83848386";
    private static final String MODULE_ID = "1";
    private static final String CHANGE_TYPE = "1";
    private static final String SIGNATURE = "ZohoSearch-1776348447204-12c6e970b2d28946a3db14e52b19908b476b15a34ad3c3584b50e8f1bde602843235ae7aa205a74461d8df793c8901a49f5c4370c8f091f1c17e52effd4fac48";

    private final List<String> dictionary;
    private final Random random;

    public DataGenerator() {
        this.dictionary = loadDictionary();
        this.random = new Random();
    }

    public static void main(String[] args) throws Exception {
        DataGenerator generator = new DataGenerator();

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter total number of documents to create: ");
        long totalDocuments = scanner.nextLong();

        System.out.print("Enter maximum number of fields per document: ");
        int fieldsPerDocument = scanner.nextInt();

        System.out.print("Enter target data size per document in bytes (e.g., 3072 for 3KB): ");
        int documentSizeBytes = scanner.nextInt();
        scanner.close();

        System.out.println("\n--- Configuration ---");
        System.out.println("Total documents  : " + totalDocuments);
        System.out.println("Fields/document  : " + fieldsPerDocument);
        System.out.println("Size/document    : " + documentSizeBytes + " bytes");
        System.out.println("Batch size       : " + MAX_BATCH_SIZE);
        long totalBatches = (totalDocuments + MAX_BATCH_SIZE - 1) / MAX_BATCH_SIZE;
        System.out.println("Total batches    : " + totalBatches);
        System.out.println("---------------------\n");

        generator.generateAndSend(totalDocuments, fieldsPerDocument, documentSizeBytes);
    }

    /**
     * Generates all documents in batches and sends each batch to the server.
     */
    public void generateAndSend(long totalDocuments, int fieldsPerDocument, int documentSizeBytes) throws Exception {
        long entityIdCounter = 1;
        long documentsSent = 0;
        int batchNumber = 0;
        long overallStart = System.currentTimeMillis();

        while (documentsSent < totalDocuments) {
            int batchCount = (int) Math.min(MAX_BATCH_SIZE, totalDocuments - documentsSent);
            JSONArray bulkChgArr = new JSONArray();

            for (int i = 0; i < batchCount; i++) {
                JSONObject document = createDocument(entityIdCounter, fieldsPerDocument, documentSizeBytes);
                bulkChgArr.put(document);
                entityIdCounter++;
            }

            batchNumber++;
            System.out.println("Sending batch " + batchNumber + " with " + batchCount + " documents...");

            String encodedBulkChgArr = Base64.getEncoder().encodeToString(
                    bulkChgArr.toString().getBytes(StandardCharsets.UTF_8));

            sendBatch(encodedBulkChgArr);

            documentsSent += batchCount;
            System.out.println("Progress: " + documentsSent + " / " + totalDocuments + " documents sent.");
        }

        long elapsed = System.currentTimeMillis() - overallStart;
        System.out.println("\nAll " + totalDocuments + " documents sent in " + batchNumber
                + " batches. Total time: " + elapsed + " ms");
    }

    /**
     * Creates a single document JSONObject matching the bulkChgArr format.
     * The CHANGE_DATA field values are filled with dictionary words until the
     * approximate target document size is reached.
     */
    JSONObject createDocument(long entityId, int maxFields, int targetSizeBytes) {
        JSONObject document = new JSONObject();
        document.put("CHANGE_TYPE", CHANGE_TYPE);
        document.put("ENTITY_ID", String.valueOf(entityId));
        document.put("NOTIFY_ID", -1);
        document.put("ZSOID", ZSOID);
        document.put("MODULE_ID", MODULE_ID);

        JSONObject changeData = new JSONObject();

        // Reserve bytes for the outer envelope (CHANGE_TYPE, ENTITY_ID, etc.)
        int envelopeSize = document.toString().getBytes(StandardCharsets.UTF_8).length;
        int remainingBytes = Math.max(targetSizeBytes - envelopeSize, 0);

        int currentSize = 0;
        int fieldIndex = 0;

        while (fieldIndex < maxFields && currentSize < remainingBytes) {
            String fieldName = "FIELD_" + (fieldIndex + 1);
            StringBuilder valueBuilder = new StringBuilder();

            // Fill the field value with dictionary words until we approach the per-field budget
            int perFieldBudget = remainingBytes / maxFields;
            while (valueBuilder.length() < perFieldBudget) {
                String word = dictionary.get(random.nextInt(dictionary.size()));
                if (valueBuilder.length() > 0) {
                    valueBuilder.append(' ');
                }
                valueBuilder.append(word);
            }

            String fieldValue = valueBuilder.toString();
            changeData.put(fieldName, fieldValue);
            currentSize += fieldName.getBytes(StandardCharsets.UTF_8).length
                    + fieldValue.getBytes(StandardCharsets.UTF_8).length;
            fieldIndex++;
        }

        document.put("CHANGE_DATA", changeData);
        return document;
    }

    /**
     * Sends a single batch as an HTTP POST request using the same pattern as
     * SendRequestToSearchIndex.
     */
    private void sendBatch(String encodedBulkChgArr) throws Exception {
        HttpURLConnection httpConnection = null;
        try {
            long start = System.currentTimeMillis();

            String parameterString = "service=" + SERVICE
                    + "&version=" + VERSION
                    + "&bulkChgArr=" + encodedBulkChgArr
                    + "&iscsignature=" + SIGNATURE;

            URL url = new URL(DEFAULT_URL);
            httpConnection = (HttpURLConnection) url.openConnection();
            byte[] bytes = parameterString.getBytes(StandardCharsets.UTF_8);

            httpConnection.setConnectTimeout(3000);
            httpConnection.setReadTimeout(60000);
            httpConnection.setRequestMethod("POST");
            httpConnection.setRequestProperty("Content-Length", Integer.toString(bytes.length));
            httpConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);

            DataOutputStream printout = null;
            try {
                printout = new DataOutputStream(new BufferedOutputStream(httpConnection.getOutputStream()));
                printout.write(bytes);
                printout.flush();
            } finally {
                if (printout != null) {
                    printout.close();
                }
            }

            int responseCode = httpConnection.getResponseCode();
            long elapsed = System.currentTimeMillis() - start;

            if (responseCode == 200) {
                System.out.println("  Batch sent successfully. Response: " + responseCode
                        + ", Time: " + elapsed + " ms");
            } else {
                System.out.println("  Batch failed. Response: " + responseCode
                        + ", Time: " + elapsed + " ms, URL: " + url);
            }
        } finally {
            if (httpConnection != null) {
                httpConnection.disconnect();
            }
        }
    }

    /**
     * Loads the dictionary words from the resources/dictionary.txt file.
     */
    private List<String> loadDictionary() {
        List<String> words = new ArrayList<>();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("dictionary.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    words.add(line);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load dictionary.txt from resources", e);
        }
        System.out.println("Dictionary loaded with " + words.size() + " words.");
        return words;
    }
}
