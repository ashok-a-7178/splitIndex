package com.loadTesting;

import org.json.JSONArray;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Main entry-point for the load-testing tool.
 *
 * <p>Prompts the user for generation parameters (including optional
 * cardinality control on up to 5 string not-analyzed fields), then uses
 * {@link DataSetGenerator} to build document batches and
 * {@link HttpRequestSender} to push them to the search index.</p>
 *
 * <h3>Usage</h3>
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.loadTesting.LoadTestRunner
 * </pre>
 */
public class LoadTestRunner {

    private static final int MAX_CARDINALITY_FIELDS = 5;

    public static void main(String[] args) throws Exception {
        DataSetGenerator generator = new DataSetGenerator();
        HttpRequestSender sender = new HttpRequestSender();

        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter total number of documents to create: ");
        long totalDocuments = scanner.nextLong();

        System.out.print("Enter MODULE_ID (e.g., 1, 2, 3, 4, 5): ");
        String moduleId = scanner.next();

        System.out.print("Enter maximum number of fields per document (0 = use all fields from XML): ");
        int fieldsPerDocument = scanner.nextInt();

        System.out.print("Enter target data size per document in bytes (0 = no size padding, e.g., 3072 for 3KB): ");
        int documentSizeBytes = scanner.nextInt();

        List<String> allFields = generator.getFieldsForModule(moduleId);
        int effectiveFields = (fieldsPerDocument <= 0 || fieldsPerDocument > allFields.size())
                ? allFields.size() : fieldsPerDocument;

        List<String> fields;
        if (fieldsPerDocument > 0 && fieldsPerDocument < allFields.size()) {
            fields = allFields.subList(0, fieldsPerDocument);
        } else {
            fields = allFields;
        }

        // ---- Cardinality setup ----
        Map<String, Integer> cardinalityMap = collectCardinalityInput(
                scanner, generator, moduleId, fields);

        // ---- Summary ----
        System.out.println("\n--- Configuration ---");
        System.out.println("Total documents  : " + totalDocuments);
        System.out.println("MODULE_ID        : " + moduleId);
        System.out.println("Available fields : " + allFields.size());
        System.out.println("Fields/document  : " + effectiveFields);
        System.out.println("Size/document    : "
                + (documentSizeBytes > 0 ? documentSizeBytes + " bytes" : "no padding"));
        System.out.println("Batch size       : " + LoadTestConfig.MAX_BATCH_SIZE);
        long totalBatches = (totalDocuments + LoadTestConfig.MAX_BATCH_SIZE - 1)
                / LoadTestConfig.MAX_BATCH_SIZE;
        System.out.println("Total batches    : " + totalBatches);
        System.out.println("Server URL       : " + LoadTestConfig.SERVER_URL);
        if (!cardinalityMap.isEmpty()) {
            System.out.println("Cardinality      :");
            for (Map.Entry<String, Integer> entry : cardinalityMap.entrySet()) {
                System.out.println("  " + entry.getKey() + " -> " + entry.getValue() + " unique values");
            }
        }
        System.out.println("---------------------\n");

        // ---- Generate & Send ----
        long entityIdCounter = 1;
        long documentsSent = 0;
        int batchNumber = 0;
        long overallStart = System.currentTimeMillis();

        while (documentsSent < totalDocuments) {
            int batchCount = (int) Math.min(LoadTestConfig.MAX_BATCH_SIZE,
                    totalDocuments - documentsSent);

            JSONArray batchArray = generator.generateBatch(
                    entityIdCounter, batchCount, moduleId, fields,
                    documentSizeBytes, cardinalityMap);

            batchNumber++;
            System.out.println("Sending batch " + batchNumber + " with " + batchCount + " documents...");

            String encodedBatch = Base64.getEncoder().encodeToString(
                    batchArray.toString().getBytes(StandardCharsets.UTF_8));

            sender.sendBatch(encodedBatch);

            entityIdCounter += batchCount;
            documentsSent += batchCount;
            System.out.println("Progress: " + documentsSent + " / " + totalDocuments + " documents sent.");
        }

        long elapsed = System.currentTimeMillis() - overallStart;
        System.out.println("\nAll " + totalDocuments + " documents sent in "
                + batchNumber + " batches. Total time: " + elapsed + " ms");
    }

    /**
     * Prompts the user to optionally configure cardinality for up to
     * {@value #MAX_CARDINALITY_FIELDS} string not-analyzed fields.
     *
     * @return a map of fieldName → cardinality (empty if user skips)
     */
    private static Map<String, Integer> collectCardinalityInput(
            Scanner scanner, DataSetGenerator generator,
            String moduleId, List<String> selectedFields) {

        Map<String, Integer> cardinalityMap = new LinkedHashMap<>();

        // Only string-not-analyzed fields that are also in the selected field list
        List<String> eligibleFields = generator.getStringNotAnalyzedFieldsForModule(moduleId);
        eligibleFields.retainAll(selectedFields);

        if (eligibleFields.isEmpty()) {
            System.out.println("\nNo string not-analyzed fields available for cardinality control.");
            return cardinalityMap;
        }

        System.out.println("\n--- Cardinality Configuration ---");
        System.out.println("Available string not-analyzed fields for cardinality control:");
        for (int i = 0; i < eligibleFields.size(); i++) {
            System.out.println("  " + (i + 1) + ". " + eligibleFields.get(i));
        }

        System.out.print("\nHow many fields to set cardinality on? (0-"
                + Math.min(MAX_CARDINALITY_FIELDS, eligibleFields.size()) + "): ");
        int count = scanner.nextInt();
        count = Math.max(0, Math.min(count, Math.min(MAX_CARDINALITY_FIELDS, eligibleFields.size())));

        for (int i = 0; i < count; i++) {
            System.out.print("Enter field number (1-" + eligibleFields.size() + ") for cardinality field "
                    + (i + 1) + ": ");
            int fieldIdx = scanner.nextInt();
            if (fieldIdx < 1 || fieldIdx > eligibleFields.size()) {
                System.out.println("  Invalid index, skipping.");
                continue;
            }
            String fieldName = eligibleFields.get(fieldIdx - 1);
            if (cardinalityMap.containsKey(fieldName)) {
                System.out.println("  Field '" + fieldName + "' already configured, skipping.");
                continue;
            }
            System.out.print("Enter cardinality (number of unique values) for '" + fieldName + "': ");
            int cardinality = scanner.nextInt();
            if (cardinality <= 0) {
                System.out.println("  Cardinality must be > 0, skipping.");
                continue;
            }
            cardinalityMap.put(fieldName, cardinality);
            System.out.println("  -> " + fieldName + " = " + cardinality + " unique values");
        }

        return cardinalityMap;
    }
}
