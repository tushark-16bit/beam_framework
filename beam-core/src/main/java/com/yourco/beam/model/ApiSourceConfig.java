package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Configuration for a REST API data source, fetched from the parameter DB.
 *
 * <p>All fields are serializable so this object can be stored as a DoFn field
 * and shipped to Dataflow workers. Non-serializable resources (HttpClient, auth tokens)
 * are created in {@code @Setup} inside the DoFn using the IDs stored here.
 *
 * <p>Corresponding columns in the {@code source_config} table (see ParameterRepository):
 * <pre>
 *   api_endpoint, api_auth_type, api_auth_secret_id, api_headers_json,
 *   api_query_params_json, api_pagination_enabled, api_pagination_strategy,
 *   api_page_size, api_next_page_field, api_data_array_field
 * </pre>
 *
 * <h2>Supported auth types</h2>
 * <ul>
 *   <li>{@code NONE}    — no authentication</li>
 *   <li>{@code BEARER}  — Authorization: Bearer {token from secret}</li>
 *   <li>{@code BASIC}   — Authorization: Basic {base64(user:pass from secret)}</li>
 *   <li>{@code API_KEY} — X-Api-Key: {key from secret} header</li>
 * </ul>
 *
 * <h2>Pagination strategies</h2>
 * <ul>
 *   <li>{@code PAGE_NUMBER} — append {@code ?page=N} until response is empty</li>
 *   <li>{@code CURSOR}      — follow {@code nextPageField} JSON path until null</li>
 *   <li>{@code OFFSET}      — increment {@code ?offset=N&limit=pageSize} until response < pageSize</li>
 * </ul>
 */
public final class ApiSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String endpoint;
    public final String authType;
    /** Secret Manager resource name for the auth credential. Null when authType=NONE. */
    public final String authSecretId;
    /** Fixed HTTP headers sent with every request. */
    public final Map<String, String> headers;
    /** Fixed query parameters appended to every request URL. */
    public final Map<String, String> queryParams;
    public final boolean paginationEnabled;
    /** Pagination strategy: PAGE_NUMBER | CURSOR | OFFSET */
    public final String paginationStrategy;
    public final int pageSize;
    /** JSON path (dot-separated) to the next-page cursor value in the response. */
    public final String nextPageField;
    /** JSON path to the array of records in the response body. Null means the root is the array. */
    public final String dataArrayField;

    public ApiSourceConfig(String endpoint, String authType, String authSecretId,
                           Map<String, String> headers, Map<String, String> queryParams,
                           boolean paginationEnabled, String paginationStrategy,
                           int pageSize, String nextPageField, String dataArrayField) {
        this.endpoint            = endpoint;
        this.authType            = authType != null ? authType : "NONE";
        this.authSecretId        = authSecretId;
        this.headers             = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
        this.queryParams         = queryParams != null ? Collections.unmodifiableMap(queryParams) : Collections.emptyMap();
        this.paginationEnabled   = paginationEnabled;
        this.paginationStrategy  = paginationStrategy != null ? paginationStrategy : "PAGE_NUMBER";
        this.pageSize            = pageSize > 0 ? pageSize : 100;
        this.nextPageField       = nextPageField;
        this.dataArrayField      = dataArrayField;
    }
}
