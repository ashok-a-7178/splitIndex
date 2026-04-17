package com.loadTesting;

/**
 * Configuration constants for the load-testing module.
 *
 * <p>Centralises HTTP connection settings, service-level parameters,
 * batch sizing, and XML resource paths so that every other class in
 * the package can reference a single source of truth.</p>
 */
public final class LoadTestConfig {

    private LoadTestConfig() {
        // utility class
    }

    // ---- HTTP settings ----

    /** Target URL for the search index change collector endpoint. */
    public static final String SERVER_URL = "http://localhost:8080/searchtestchangecollector";

    /** TCP connect timeout in milliseconds. */
    public static final int CONNECT_TIMEOUT_MS = 3000;

    /** Socket read timeout in milliseconds. */
    public static final int READ_TIMEOUT_MS = 60_000;

    /** Content-Type header value for form-encoded POST bodies. */
    public static final String CONTENT_TYPE = "application/x-www-form-urlencoded";

    // ---- Service settings ----

    /** Service identifier sent in every request. */
    public static final String SERVICE = "newimplone";

    /** API version string. */
    public static final String VERSION = "v1";

    /** Organisation ID (ZSOID) stamped on every document. */
    public static final String ZSOID = "83848388";

    /** Change type code (1 = create). */
    public static final String CHANGE_TYPE = "1";

    /** Default ICS signature; override at runtime with {@code -Discsignature=...}. */
    public static final String DEFAULT_SIGNATURE =
            "ZohoSearch-1776348447204-12c6e970b2d28946a3db14e52b19908b476b15a34ad3c3584b50e8f1bde602843235ae7aa205a74461d8df793c8901a49f5c4370c8f091f1c17e52effd4fac48";

    // ---- Batch settings ----

    /** Maximum number of documents per HTTP batch. */
    public static final int MAX_BATCH_SIZE = 500;

    // ---- XML resource paths (classpath) ----

    /** Module-specific field definitions. */
    public static final String FIELD_TYPE_XML = "newimplone-field-type.xml";

    /** Common fields and FieldGroup definitions. */
    public static final String COMMON_FIELDS_XML = "newimplone-commonfields-type.xml";

    // ---- Dictionary ----

    /** Classpath resource used for generating realistic field values. */
    public static final String DICTIONARY_RESOURCE = "dictionary.txt";

    /**
     * Returns the ICS signature, preferring the {@code -Discsignature}
     * system property if set.
     */
    public static String getSignature() {
        return System.getProperty("iscsignature", DEFAULT_SIGNATURE);
    }
}
