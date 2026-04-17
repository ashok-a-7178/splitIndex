package com.loadTesting;

import org.json.JSONArray;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Scanner;

/**
 * Main entry-point for the load-testing tool.
 *
 * <p>Prompts the user for generation parameters, then uses
 * {@link DataSetGenerator} to build document batches and
 * {@link HttpRequestSender} to push them to the search index.</p>
 *
 * <h3>Usage</h3>
 * <pre>
 *   mvn exec:java -Dexec.mainClass=com.loadTesting.LoadTestRunner
 * </pre>
 */
public class LoadTestRunner {

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
        System.out.println("---------------------\n");

        long entityIdCounter = 1;
        long documentsSent = 0;
        int batchNumber = 0;
        long overallStart = System.currentTimeMillis();

        while (documentsSent < totalDocuments) {
            int batchCount = (int) Math.min(LoadTestConfig.MAX_BATCH_SIZE,
                    totalDocuments - documentsSent);

            JSONArray batchArray = generator.generateBatch(
                    entityIdCounter, batchCount, moduleId, fields, documentSizeBytes);

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
}
