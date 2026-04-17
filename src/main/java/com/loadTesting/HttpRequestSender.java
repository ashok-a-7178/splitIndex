package com.loadTesting;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * HTTP client responsible for sending bulk-change batches to the
 * search index change-collector endpoint.
 *
 * <p>Each call to {@link #sendBatch(String)} opens a new HTTP POST
 * connection, writes the form-encoded body, and logs the outcome.</p>
 */
public class HttpRequestSender {

    private final String serverUrl;
    private final String signature;

    /** Creates a sender using defaults from {@link LoadTestConfig}. */
    public HttpRequestSender() {
        this(LoadTestConfig.SERVER_URL, LoadTestConfig.getSignature());
    }

    /**
     * Creates a sender with explicit URL and signature (useful for testing
     * against different environments).
     */
    public HttpRequestSender(String serverUrl, String signature) {
        this.serverUrl = serverUrl;
        this.signature = signature;
    }

    /**
     * Sends a single Base64-encoded bulkChgArr batch via HTTP POST.
     *
     * @param encodedBulkChgArr Base64-encoded JSON array of change documents
     * @return the HTTP response code
     * @throws IOException if the connection or write fails
     */
    public int sendBatch(String encodedBulkChgArr) throws IOException {
        HttpURLConnection httpConnection = null;
        try {
            long start = System.currentTimeMillis();

            String parameterString = "service=" + LoadTestConfig.SERVICE
                    + "&version=" + LoadTestConfig.VERSION
                    + "&bulkChgArr=" + encodedBulkChgArr
                    + "&iscsignature=" + signature;

            URL url = new URL(serverUrl);
            httpConnection = (HttpURLConnection) url.openConnection();
            byte[] bytes = parameterString.getBytes(StandardCharsets.UTF_8);

            httpConnection.setConnectTimeout(LoadTestConfig.CONNECT_TIMEOUT_MS);
            httpConnection.setReadTimeout(LoadTestConfig.READ_TIMEOUT_MS);
            httpConnection.setRequestMethod("POST");
            httpConnection.setRequestProperty("Content-Length", Integer.toString(bytes.length));
            httpConnection.setRequestProperty("Content-Type", LoadTestConfig.CONTENT_TYPE);
            httpConnection.setDoInput(true);
            httpConnection.setDoOutput(true);

            DataOutputStream out = null;
            try {
                out = new DataOutputStream(
                        new BufferedOutputStream(httpConnection.getOutputStream()));
                out.write(bytes);
                out.flush();
            } finally {
                if (out != null) {
                    out.close();
                }
            }

            int responseCode = httpConnection.getResponseCode();
            long elapsed = System.currentTimeMillis() - start;

            if (responseCode == 200) {
                System.out.println("  Batch sent successfully. Response: " + responseCode
                        + ", Time: " + elapsed + " ms");
            } else {
                System.out.println("  Batch failed. Response: " + responseCode
                        + ", Time: " + elapsed + " ms, URL: " + serverUrl);
            }

            return responseCode;
        } finally {
            if (httpConnection != null) {
                httpConnection.disconnect();
            }
        }
    }
}
