package com.loadTesting;

import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

/**
 * Data generator that creates bulk change documents using field names parsed from
 * the service XML configuration files and values from a predefined dictionary.
 * It forms bulkChgArr JSONArray batches (max 500 per batch), base64 encodes them,
 * and sends them via HTTP POST to the search index worker server.
 *
 * The CHANGE_DATA for each document is generated based on the MODULE_ID:
 * field names are read from newimplone-field-type.xml (module-specific fields)
 * and newimplone-commonfields-type.xml (common fields + FieldGroup fields),
 * and values are filled from dictionary.txt.
 *
 * Usage: Run main(), then enter total documents and MODULE_ID when prompted.
 */
public class DataGenerator {

    private static final int MAX_BATCH_SIZE = 500;
    private static final String DEFAULT_URL = "http://localhost:8080/searchtestchangecollector";
    private static final String SERVICE = "newimplone";
    private static final String VERSION = "v1";
    private static final String ZSOID = "83848386";
    private static final String CHANGE_TYPE = "1";
    private static final String DEFAULT_SIGNATURE = "ZohoSearch-1776348447204-12c6e970b2d28946a3db14e52b19908b476b15a34ad3c3584b50e8f1bde602843235ae7aa205a74461d8df793c8901a49f5c4370c8f091f1c17e52effd4fac48";

    private static final String FIELD_TYPE_XML = "newimplone-field-type.xml";
    private static final String COMMON_FIELDS_XML = "newimplone-commonfields-type.xml";

    private final List<String> dictionary;
    private final Random random;
    private final String signature;

    /** Module-specific fields: moduleId -> ordered list of field names */
    private final Map<String, List<String>> moduleFieldsMap;

    /** Common fields that apply to all modules (from commonfields-type.xml, outside FieldGroup) */
    private final List<String> commonFields;

    /** FieldGroup fields: groupId -> list of field names */
    private final Map<String, List<String>> fieldGroupFieldsMap;

    /** Module-to-FieldGroup associations: moduleId -> set of FieldGroup IDs */
    private final Map<String, Set<String>> moduleFieldGroupMap;

    public DataGenerator() {
        this.dictionary = loadDictionary();
        this.random = new Random();
        this.signature = System.getProperty("iscsignature", DEFAULT_SIGNATURE);
        this.moduleFieldsMap = new LinkedHashMap<>();
        this.commonFields = new ArrayList<>();
        this.fieldGroupFieldsMap = new LinkedHashMap<>();
        this.moduleFieldGroupMap = new HashMap<>();
        parseFieldTypeXml();
        parseCommonFieldsXml();
    }

    public static void main(String[] args) throws Exception {
        DataGenerator generator = new DataGenerator();

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter total number of documents to create: ");
        long totalDocuments = scanner.nextLong();

        System.out.print("Enter MODULE_ID (e.g., 1, 2, 3, 4, 5): ");
        String moduleId = scanner.next();
        // Not closing scanner to avoid closing System.in

        List<String> fields = generator.getFieldsForModule(moduleId);
        System.out.println("\n--- Configuration ---");
        System.out.println("Total documents  : " + totalDocuments);
        System.out.println("MODULE_ID        : " + moduleId);
        System.out.println("Fields for module: " + fields.size());
        System.out.println("Batch size       : " + MAX_BATCH_SIZE);
        long totalBatches = (totalDocuments + MAX_BATCH_SIZE - 1) / MAX_BATCH_SIZE;
        System.out.println("Total batches    : " + totalBatches);
        System.out.println("---------------------\n");

        generator.generateAndSend(totalDocuments, moduleId);
    }

    /**
     * Returns the combined list of field names for a given module ID.
     * This includes module-specific fields from field-type.xml,
     * common fields from commonfields-type.xml, and any FieldGroup fields
     * associated with the module.
     */
    public List<String> getFieldsForModule(String moduleId) {
        Set<String> fieldSet = new LinkedHashSet<>();

        // Add module-specific fields
        List<String> moduleFields = moduleFieldsMap.get(moduleId);
        if (moduleFields != null) {
            fieldSet.addAll(moduleFields);
        }

        // Add common fields
        fieldSet.addAll(commonFields);

        // Add FieldGroup fields based on module association
        Set<String> groupIds = moduleFieldGroupMap.get(moduleId);
        if (groupIds != null) {
            for (String groupId : groupIds) {
                List<String> groupFields = fieldGroupFieldsMap.get(groupId);
                if (groupFields != null) {
                    fieldSet.addAll(groupFields);
                }
            }
        }

        return new ArrayList<>(fieldSet);
    }

    /**
     * Generates all documents in batches and sends each batch to the server.
     */
    public void generateAndSend(long totalDocuments, String moduleId) throws Exception {
        List<String> fields = getFieldsForModule(moduleId);
        if (fields.isEmpty()) {
            System.out.println("No fields found for MODULE_ID " + moduleId + ". Aborting.");
            return;
        }

        long entityIdCounter = 1;
        long documentsSent = 0;
        int batchNumber = 0;
        long overallStart = System.currentTimeMillis();

        while (documentsSent < totalDocuments) {
            int batchCount = (int) Math.min(MAX_BATCH_SIZE, totalDocuments - documentsSent);
            JSONArray bulkChgArr = new JSONArray();

            for (int i = 0; i < batchCount; i++) {
                JSONObject document = createDocument(entityIdCounter, moduleId, fields);
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
     * The CHANGE_DATA keys come from the XML field definitions for the given module,
     * and values are filled from the dictionary.
     */
    JSONObject createDocument(long entityId, String moduleId, List<String> fields) {
        JSONObject document = new JSONObject();
        document.put("CHANGE_TYPE", CHANGE_TYPE);
        document.put("ENTITY_ID", String.valueOf(entityId));
        document.put("NOTIFY_ID", -1);
        document.put("ZSOID", ZSOID);
        document.put("MODULE_ID", moduleId);

        JSONObject changeData = new JSONObject();

        for (String fieldName : fields) {
            String value = dictionary.get(random.nextInt(dictionary.size()));
            changeData.put(fieldName, value);
        }

        document.put("CHANGE_DATA", changeData);
        return document;
    }

    /**
     * Parses newimplone-field-type.xml to extract:
     * - Module-specific field names (from &lt;Module ID="X"&gt; sections)
     * - Module-to-FieldGroup associations (from &lt;DefaultModuleEntry&gt;)
     */
    private void parseFieldTypeXml() {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream(FIELD_TYPE_XML);
            if (is == null) {
                throw new RuntimeException(FIELD_TYPE_XML + " not found in classpath resources");
            }

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(is);
            doc.getDocumentElement().normalize();

            // Parse DefaultModuleEntry to get module-to-FieldGroup associations
            NodeList moduleRanges = doc.getElementsByTagName("ModuleRange");
            for (int i = 0; i < moduleRanges.getLength(); i++) {
                Element rangeElem = (Element) moduleRanges.item(i);
                int rangeStart = Integer.parseInt(rangeElem.getAttribute("RANGE_START"));
                int rangeEnd = Integer.parseInt(rangeElem.getAttribute("RANGE_END"));

                // Check if this range has FieldGroup children
                NodeList fieldGroups = rangeElem.getElementsByTagName("FieldGroup");
                Set<String> groupIds = new LinkedHashSet<>();
                for (int fg = 0; fg < fieldGroups.getLength(); fg++) {
                    groupIds.add(((Element) fieldGroups.item(fg)).getAttribute("ID"));
                }

                if (!groupIds.isEmpty()) {
                    for (int m = rangeStart; m <= rangeEnd; m++) {
                        moduleFieldGroupMap.put(String.valueOf(m), groupIds);
                    }
                }
            }

            // Parse Module sections under DefaultFieldTypeEntry
            NodeList modules = doc.getElementsByTagName("Module");
            for (int i = 0; i < modules.getLength(); i++) {
                Element moduleElem = (Element) modules.item(i);
                String moduleId = moduleElem.getAttribute("ID");
                List<String> fieldNames = extractFieldNames(moduleElem);
                moduleFieldsMap.put(moduleId, fieldNames);
            }

            System.out.println("Parsed " + FIELD_TYPE_XML + ": " + moduleFieldsMap.size()
                    + " modules, " + moduleFieldGroupMap.size() + " module-FieldGroup associations.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + FIELD_TYPE_XML, e);
        }
    }

    /**
     * Parses newimplone-commonfields-type.xml to extract:
     * - Common fields (from &lt;DefaultFieldTypeEntry&gt; top-level, outside FieldGroup)
     * - FieldGroup fields (from &lt;FieldGroup ID="X"&gt; sections)
     */
    private void parseCommonFieldsXml() {
        try {
            InputStream is = getClass().getClassLoader().getResourceAsStream(COMMON_FIELDS_XML);
            if (is == null) {
                throw new RuntimeException(COMMON_FIELDS_XML + " not found in classpath resources");
            }

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(is);
            doc.getDocumentElement().normalize();

            // Find DefaultFieldTypeEntry element
            NodeList fieldTypeEntries = doc.getElementsByTagName("DefaultFieldTypeEntry");
            if (fieldTypeEntries.getLength() == 0) {
                return;
            }

            Element fieldTypeEntry = (Element) fieldTypeEntries.item(0);

            // Collect FieldGroup IDs to exclude their children from common fields
            NodeList fieldGroups = fieldTypeEntry.getElementsByTagName("FieldGroup");
            Set<Element> fieldGroupElements = new LinkedHashSet<>();
            for (int i = 0; i < fieldGroups.getLength(); i++) {
                Element fgElem = (Element) fieldGroups.item(i);
                fieldGroupElements.add(fgElem);

                String groupId = fgElem.getAttribute("ID");
                List<String> groupFields = extractFieldNames(fgElem);
                fieldGroupFieldsMap.put(groupId, groupFields);
            }

            // Extract common fields (direct children of DefaultFieldTypeEntry, not inside FieldGroup)
            NodeList allFieldTypes = fieldTypeEntry.getChildNodes();
            for (int i = 0; i < allFieldTypes.getLength(); i++) {
                if (!(allFieldTypes.item(i) instanceof Element)) {
                    continue;
                }
                Element elem = (Element) allFieldTypes.item(i);
                if (!"DefaultFieldType".equals(elem.getTagName())) {
                    continue;
                }

                // This is a direct child DefaultFieldType (not inside a FieldGroup)
                List<String> names = resolveFieldNames(elem);
                commonFields.addAll(names);
            }

            System.out.println("Parsed " + COMMON_FIELDS_XML + ": " + commonFields.size()
                    + " common fields, " + fieldGroupFieldsMap.size() + " FieldGroups.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + COMMON_FIELDS_XML, e);
        }
    }

    /**
     * Extracts all field names from DefaultFieldType elements within a parent element.
     * Handles both FIELD_NAME and FIELD_NAME_PREFIX with FieldNameSuffix ranges.
     */
    private List<String> extractFieldNames(Element parent) {
        List<String> fieldNames = new ArrayList<>();
        NodeList fieldTypes = parent.getElementsByTagName("DefaultFieldType");
        for (int i = 0; i < fieldTypes.getLength(); i++) {
            Element fieldElem = (Element) fieldTypes.item(i);
            // Only process direct children of the parent
            if (fieldElem.getParentNode() != parent) {
                continue;
            }
            fieldNames.addAll(resolveFieldNames(fieldElem));
        }
        return fieldNames;
    }

    /**
     * Resolves field names from a single DefaultFieldType element.
     * If FIELD_NAME is present, returns it directly.
     * If FIELD_NAME_PREFIX is present with FieldNameSuffix ranges, expands them.
     */
    private List<String> resolveFieldNames(Element fieldElem) {
        List<String> names = new ArrayList<>();

        String fieldName = fieldElem.getAttribute("FIELD_NAME");
        String fieldNamePrefix = fieldElem.getAttribute("FIELD_NAME_PREFIX");

        if (fieldName != null && !fieldName.isEmpty()) {
            names.add(fieldName);
        } else if (fieldNamePrefix != null && !fieldNamePrefix.isEmpty()) {
            NodeList suffixes = fieldElem.getElementsByTagName("FieldNameSuffix");
            for (int j = 0; j < suffixes.getLength(); j++) {
                Element suffixElem = (Element) suffixes.item(j);
                int start = Integer.parseInt(suffixElem.getAttribute("RANGE_START"));
                int end = Integer.parseInt(suffixElem.getAttribute("RANGE_END"));
                for (int k = start; k <= end; k++) {
                    names.add(fieldNamePrefix + k);
                }
            }
        }

        return names;
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
                    + "&iscsignature=" + signature;

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
        InputStream is = getClass().getClassLoader().getResourceAsStream("dictionary.txt");
        if (is == null) {
            throw new RuntimeException("dictionary.txt not found in classpath resources");
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
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
