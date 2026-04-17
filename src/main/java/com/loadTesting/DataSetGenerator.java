package com.loadTesting;

import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Generates bulk-change document data sets for load testing.
 *
 * <p>Field definitions are parsed once from the service XML configuration
 * files ({@link LoadTestConfig#FIELD_TYPE_XML} and
 * {@link LoadTestConfig#COMMON_FIELDS_XML}), and field values are drawn
 * from a dictionary resource to produce realistic content.</p>
 *
 * <p>This class is <b>not</b> thread-safe; create one instance per thread
 * if used concurrently.</p>
 */
public class DataSetGenerator {

    private final List<String> dictionary;
    private final Random random;

    /** Module-specific fields: moduleId → ordered list of field names. */
    private final Map<String, List<String>> moduleFieldsMap;

    /** Common fields that apply to all modules. */
    private final List<String> commonFields;

    /** FieldGroup fields: groupId → list of field names. */
    private final Map<String, List<String>> fieldGroupFieldsMap;

    /** Module-to-FieldGroup associations: moduleId → set of group IDs. */
    private final Map<String, Set<String>> moduleFieldGroupMap;

    public DataSetGenerator() {
        this.dictionary = loadDictionary();
        this.random = new Random();
        this.moduleFieldsMap = new LinkedHashMap<>();
        this.commonFields = new ArrayList<>();
        this.fieldGroupFieldsMap = new LinkedHashMap<>();
        this.moduleFieldGroupMap = new HashMap<>();
        parseFieldTypeXml();
        parseCommonFieldsXml();
    }

    // ----------------------------------------------------------------
    //  Public API
    // ----------------------------------------------------------------

    /**
     * Returns the combined list of field names applicable to a module:
     * module-specific fields, common fields, and associated FieldGroup fields.
     */
    public List<String> getFieldsForModule(String moduleId) {
        Set<String> fieldSet = new LinkedHashSet<>();

        List<String> moduleFields = moduleFieldsMap.get(moduleId);
        if (moduleFields != null) {
            fieldSet.addAll(moduleFields);
        }

        fieldSet.addAll(commonFields);

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
     * Creates a single document {@link JSONObject} in the bulkChgArr format.
     *
     * @param entityId        incremental entity ID
     * @param moduleId        module ID value
     * @param fields          list of field names (from {@link #getFieldsForModule})
     * @param targetSizeBytes target document size in bytes (≤ 0 = single word per field)
     */
    public JSONObject createDocument(long entityId, String moduleId,
                                     List<String> fields, int targetSizeBytes) {
        JSONObject document = new JSONObject();
        document.put("CHANGE_TYPE", LoadTestConfig.CHANGE_TYPE);
        document.put("ENTITY_ID", String.valueOf(entityId));
        document.put("NOTIFY_ID", -1);
        document.put("ZSOID", LoadTestConfig.ZSOID);
        document.put("MODULE_ID", moduleId);

        JSONObject changeData = new JSONObject();

        if (targetSizeBytes <= 0 || fields.isEmpty()) {
            for (String fieldName : fields) {
                changeData.put(fieldName, randomWord());
            }
        } else {
            int envelopeSize = document.toString().getBytes(StandardCharsets.UTF_8).length;
            int remainingBytes = Math.max(targetSizeBytes - envelopeSize, 0);
            int perFieldBudget = remainingBytes / fields.size();

            for (String fieldName : fields) {
                StringBuilder valueBuilder = new StringBuilder();
                while (valueBuilder.length() < perFieldBudget) {
                    if (valueBuilder.length() > 0) {
                        valueBuilder.append(' ');
                    }
                    valueBuilder.append(randomWord());
                }
                changeData.put(fieldName, valueBuilder.toString());
            }
        }

        document.put("CHANGE_DATA", changeData);
        return document;
    }

    /**
     * Generates a batch of documents as a {@link JSONArray}.
     *
     * @param startEntityId   first entity ID in this batch
     * @param batchSize       number of documents to generate
     * @param moduleId        module ID for field selection
     * @param fields          pre-resolved field list
     * @param targetSizeBytes target document size (≤ 0 = no padding)
     * @return the batch as a JSONArray
     */
    public JSONArray generateBatch(long startEntityId, int batchSize,
                                   String moduleId, List<String> fields,
                                   int targetSizeBytes) {
        JSONArray batch = new JSONArray();
        for (int i = 0; i < batchSize; i++) {
            batch.put(createDocument(startEntityId + i, moduleId, fields, targetSizeBytes));
        }
        return batch;
    }

    // ----------------------------------------------------------------
    //  XML parsing
    // ----------------------------------------------------------------

    private void parseFieldTypeXml() {
        try {
            InputStream is = getClass().getClassLoader()
                    .getResourceAsStream(LoadTestConfig.FIELD_TYPE_XML);
            if (is == null) {
                throw new RuntimeException(
                        LoadTestConfig.FIELD_TYPE_XML + " not found in classpath resources");
            }

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(is);
            doc.getDocumentElement().normalize();

            // Module-to-FieldGroup associations
            NodeList moduleRanges = doc.getElementsByTagName("ModuleRange");
            for (int i = 0; i < moduleRanges.getLength(); i++) {
                Element rangeElem = (Element) moduleRanges.item(i);
                int rangeStart = Integer.parseInt(rangeElem.getAttribute("RANGE_START"));
                int rangeEnd = Integer.parseInt(rangeElem.getAttribute("RANGE_END"));

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

            // Module-specific fields
            NodeList modules = doc.getElementsByTagName("Module");
            for (int i = 0; i < modules.getLength(); i++) {
                Element moduleElem = (Element) modules.item(i);
                String moduleId = moduleElem.getAttribute("ID");
                moduleFieldsMap.put(moduleId, extractFieldNames(moduleElem));
            }

            System.out.println("[DataSetGenerator] Parsed " + LoadTestConfig.FIELD_TYPE_XML
                    + ": " + moduleFieldsMap.size() + " modules, "
                    + moduleFieldGroupMap.size() + " module-FieldGroup associations.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + LoadTestConfig.FIELD_TYPE_XML, e);
        }
    }

    private void parseCommonFieldsXml() {
        try {
            InputStream is = getClass().getClassLoader()
                    .getResourceAsStream(LoadTestConfig.COMMON_FIELDS_XML);
            if (is == null) {
                throw new RuntimeException(
                        LoadTestConfig.COMMON_FIELDS_XML + " not found in classpath resources");
            }

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(is);
            doc.getDocumentElement().normalize();

            NodeList fieldTypeEntries = doc.getElementsByTagName("DefaultFieldTypeEntry");
            if (fieldTypeEntries.getLength() == 0) {
                return;
            }

            Element fieldTypeEntry = (Element) fieldTypeEntries.item(0);

            // FieldGroup sections
            NodeList fieldGroups = fieldTypeEntry.getElementsByTagName("FieldGroup");
            for (int i = 0; i < fieldGroups.getLength(); i++) {
                Element fgElem = (Element) fieldGroups.item(i);
                String groupId = fgElem.getAttribute("ID");
                fieldGroupFieldsMap.put(groupId, extractFieldNames(fgElem));
            }

            // Common fields (direct children, not inside FieldGroup)
            NodeList allChildren = fieldTypeEntry.getChildNodes();
            for (int i = 0; i < allChildren.getLength(); i++) {
                if (!(allChildren.item(i) instanceof Element)) {
                    continue;
                }
                Element elem = (Element) allChildren.item(i);
                if ("DefaultFieldType".equals(elem.getTagName())) {
                    commonFields.addAll(resolveFieldNames(elem));
                }
            }

            System.out.println("[DataSetGenerator] Parsed " + LoadTestConfig.COMMON_FIELDS_XML
                    + ": " + commonFields.size() + " common fields, "
                    + fieldGroupFieldsMap.size() + " FieldGroups.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse " + LoadTestConfig.COMMON_FIELDS_XML, e);
        }
    }

    private List<String> extractFieldNames(Element parent) {
        List<String> fieldNames = new ArrayList<>();
        NodeList fieldTypes = parent.getElementsByTagName("DefaultFieldType");
        for (int i = 0; i < fieldTypes.getLength(); i++) {
            Element fieldElem = (Element) fieldTypes.item(i);
            if (!fieldElem.getParentNode().isSameNode(parent)) {
                continue;
            }
            fieldNames.addAll(resolveFieldNames(fieldElem));
        }
        return fieldNames;
    }

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

    // ----------------------------------------------------------------
    //  Dictionary
    // ----------------------------------------------------------------

    private String randomWord() {
        return dictionary.get(random.nextInt(dictionary.size()));
    }

    private static List<String> loadDictionary() {
        List<String> words = new ArrayList<>();
        InputStream is = DataSetGenerator.class.getClassLoader()
                .getResourceAsStream(LoadTestConfig.DICTIONARY_RESOURCE);
        if (is == null) {
            throw new RuntimeException(
                    LoadTestConfig.DICTIONARY_RESOURCE + " not found in classpath resources");
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
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + LoadTestConfig.DICTIONARY_RESOURCE, e);
        }
        System.out.println("[DataSetGenerator] Dictionary loaded with " + words.size() + " words.");
        return words;
    }
}
