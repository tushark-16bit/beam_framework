package com.yourco.beam.io.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.model.ApiSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Adapter for fetching data from a REST API with optional pagination.
 *
 * <p>This class contains all HTTP logic — building requests, handling auth, iterating pages.
 * It is intentionally not a Beam class so it can be unit-tested without a pipeline.
 * {@link ApiSourceTransform} wraps it in a DoFn with {@code @Setup}/{@code @Teardown}.
 *
 * <h2>Pagination strategies</h2>
 * <ul>
 *   <li>{@code PAGE_NUMBER} — appends {@code &page=N&pageSize=S}; stops when the data
 *       array is empty or absent.</li>
 *   <li>{@code CURSOR}      — reads {@link ApiSourceConfig#nextPageField} from each
 *       response; stops when that field is null or missing.</li>
 *   <li>{@code OFFSET}      — appends {@code &offset=N&limit=S}; stops when response
 *       contains fewer records than {@code pageSize}.</li>
 * </ul>
 *
 * <h2>Auth modes</h2>
 * <ul>
 *   <li>{@code NONE}    — no Authorization header</li>
 *   <li>{@code BEARER}  — {@code Authorization: Bearer {token}}</li>
 *   <li>{@code BASIC}   — {@code Authorization: Basic {base64(token)}};
 *       {@code token} should be {@code user:password}</li>
 *   <li>{@code API_KEY} — {@code X-Api-Key: {token}}</li>
 * </ul>
 */
public final class ApiSourceAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(ApiSourceAdapter.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpClient httpClient;

    public ApiSourceAdapter(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /**
     * Fetches all records from the API, handling pagination transparently.
     *
     * @param config    source configuration (endpoint, auth, pagination settings)
     * @param authToken raw credential fetched from Secret Manager; interpretation
     *                  depends on {@link ApiSourceConfig#authType}
     * @return list of records, each as a JSON string (one per data item in the response)
     */
    public List<String> fetchAll(ApiSourceConfig config, String authToken) {
        if (!config.paginationEnabled) {
            return fetchSinglePage(config, authToken, buildUrl(config.endpoint, config.queryParams));
        }
        return switch (config.paginationStrategy) {
            case "PAGE_NUMBER" -> fetchByPageNumber(config, authToken);
            case "CURSOR"      -> fetchByCursor(config, authToken);
            case "OFFSET"      -> fetchByOffset(config, authToken);
            default -> {
                LOG.warn("Unknown pagination strategy '{}', falling back to single page",
                         config.paginationStrategy);
                yield fetchSinglePage(config, authToken, buildUrl(config.endpoint, config.queryParams));
            }
        };
    }

    // ── Pagination strategies ────────────────────────────────────────────────

    private List<String> fetchByPageNumber(ApiSourceConfig config, String authToken) {
        List<String> all = new ArrayList<>();
        int page = 1;
        while (true) {
            Map<String, String> params = new HashMap<>(config.queryParams);
            params.put("page", String.valueOf(page));
            params.put("pageSize", String.valueOf(config.pageSize));
            String url = buildUrl(config.endpoint, params);

            List<String> records = fetchSinglePage(config, authToken, url);
            if (records.isEmpty()) break;
            all.addAll(records);
            page++;
            LOG.debug("Fetched page {} ({} records so far)", page, all.size());
        }
        return all;
    }

    private List<String> fetchByCursor(ApiSourceConfig config, String authToken) {
        List<String> all = new ArrayList<>();
        String cursor = null;
        do {
            Map<String, String> params = new HashMap<>(config.queryParams);
            if (cursor != null) params.put("cursor", cursor);
            params.put("pageSize", String.valueOf(config.pageSize));
            String url = buildUrl(config.endpoint, params);

            String responseBody = executeRequest(config, authToken, url);
            JsonNode root = parseJson(responseBody);

            all.addAll(extractRecords(root, config.dataArrayField));
            cursor = extractStringField(root, config.nextPageField);
        } while (cursor != null && !cursor.isBlank());
        return all;
    }

    private List<String> fetchByOffset(ApiSourceConfig config, String authToken) {
        List<String> all = new ArrayList<>();
        int offset = 0;
        while (true) {
            Map<String, String> params = new HashMap<>(config.queryParams);
            params.put("offset", String.valueOf(offset));
            params.put("limit", String.valueOf(config.pageSize));
            String url = buildUrl(config.endpoint, params);

            List<String> records = fetchSinglePage(config, authToken, url);
            all.addAll(records);
            if (records.size() < config.pageSize) break;
            offset += config.pageSize;
        }
        return all;
    }

    private List<String> fetchSinglePage(ApiSourceConfig config, String authToken, String url) {
        String responseBody = executeRequest(config, authToken, url);
        JsonNode root = parseJson(responseBody);
        return extractRecords(root, config.dataArrayField);
    }

    // ── HTTP execution ───────────────────────────────────────────────────────

    private String executeRequest(ApiSourceConfig config, String authToken, String url) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .GET();

        // Apply fixed headers from config
        config.headers.forEach(builder::header);

        // Apply auth header
        switch (config.authType.toUpperCase()) {
            case "BEARER"  -> builder.header("Authorization", "Bearer " + authToken);
            case "BASIC"   -> builder.header("Authorization", "Basic "
                + Base64.getEncoder().encodeToString(authToken.getBytes(StandardCharsets.UTF_8)));
            case "API_KEY" -> builder.header("X-Api-Key", authToken);
            default        -> { /* NONE — no auth header */ }
        }

        try {
            HttpResponse<String> response = httpClient.send(
                builder.build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new ApiSourceException(
                    "API returned HTTP " + response.statusCode() + " for: " + url
                    + " — body: " + response.body().substring(0, Math.min(200, response.body().length())));
            }
            return response.body();
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            throw new ApiSourceException("HTTP request failed for: " + url, e);
        }
    }

    // ── Response parsing ─────────────────────────────────────────────────────

    private static JsonNode parseJson(String body) {
        try {
            return JSON.readTree(body);
        } catch (IOException e) {
            throw new ApiSourceException("Could not parse API response as JSON: "
                + body.substring(0, Math.min(200, body.length())), e);
        }
    }

    /**
     * Extracts the data array from the response. If {@code dataArrayField} is null/blank,
     * treats the root as the array. Returns each element as a JSON string.
     */
    private static List<String> extractRecords(JsonNode root, String dataArrayField) {
        JsonNode array = (dataArrayField == null || dataArrayField.isBlank())
            ? root
            : navigatePath(root, dataArrayField);

        if (array == null || array.isNull() || !array.isArray()) {
            return Collections.emptyList();
        }

        List<String> records = new ArrayList<>(array.size());
        Iterator<JsonNode> elements = array.elements();
        while (elements.hasNext()) {
            records.add(elements.next().toString());
        }
        return records;
    }

    /** Reads a nested string field using dot-separated path, e.g. "meta.nextCursor". */
    private static String extractStringField(JsonNode root, String dotPath) {
        if (dotPath == null || dotPath.isBlank()) return null;
        JsonNode node = navigatePath(root, dotPath);
        return (node == null || node.isNull()) ? null : node.asText();
    }

    private static JsonNode navigatePath(JsonNode root, String dotPath) {
        JsonNode current = root;
        for (String segment : dotPath.split("\\.")) {
            if (current == null || !current.isObject()) return null;
            current = current.get(segment);
        }
        return current;
    }

    // ── URL building ─────────────────────────────────────────────────────────

    private static String buildUrl(String endpoint, Map<String, String> queryParams) {
        if (queryParams == null || queryParams.isEmpty()) return endpoint;
        StringBuilder sb = new StringBuilder(endpoint);
        sb.append(endpoint.contains("?") ? "&" : "?");
        queryParams.forEach((k, v) ->
            sb.append(encode(k)).append("=").append(encode(v)).append("&"));
        sb.setLength(sb.length() - 1); // remove trailing &
        return sb.toString();
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    // ── Exception type ───────────────────────────────────────────────────────

    public static final class ApiSourceException extends RuntimeException {
        public ApiSourceException(String msg) { super(msg); }
        public ApiSourceException(String msg, Throwable cause) { super(msg, cause); }
    }
}
