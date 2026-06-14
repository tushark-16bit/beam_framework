package com.yourco.beam.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * One optional preprocessing step that runs before the main transformation chain.
 *
 * <p>Preprocessing steps are report-specific and configured in
 * {@code report_preprocessing_config}. They run in {@code step_order} order,
 * sequentially in the driver JVM, before datasource availability is checked.
 * Examples: enriching a reference table with a BQ query, or calling an API to
 * pre-populate a helper table.
 *
 * <h2>Step types</h2>
 * <dl>
 *   <dt>{@link #BQ_QUERY}</dt>
 *   <dd>Runs a BigQuery query and materialises the result to {@code bqOutputTable}.
 *       The query may contain {@code {periodStart}}, {@code {periodEnd}}, etc.</dd>
 *   <dt>{@link #API_ENRICHMENT}</dt>
 *   <dd>Calls a REST API and writes the response to a BQ table or GCS path.
 *       Implementation is report-specific — use the {@code apiParams} map for config.</dd>
 * </dl>
 */
public final class ReportPreprocessingStep implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String BQ_QUERY       = "BQ_QUERY";
    public static final String API_ENRICHMENT = "API_ENRICHMENT";

    public final int    stepOrder;
    public final String stepType;
    public final String stepName;
    /** BQ SQL (may contain period and custom tokens). Used when {@code stepType=BQ_QUERY}. */
    public final String bqQuery;
    /** Destination BQ table for the query result ({@code project.dataset.table}). */
    public final String bqOutputTable;
    /**
     * Custom token → value mappings resolved into {@code bqQuery} before execution.
     * Stored in {@code report_preprocessing_config.query_params_json}.
     * Values may reference standard tokens (e.g. {@code "{periodStart}"}).
     * Any number of entries are supported.
     */
    public final Map<String, String> queryParams;
    /** REST endpoint. Used when {@code stepType=API_ENRICHMENT}. */
    public final String apiEndpoint;
    /** Arbitrary key-value params for the API call itself (not for SQL token resolution). */
    public final Map<String, String> apiParams;

    public ReportPreprocessingStep(int stepOrder, String stepType, String stepName,
                                   String bqQuery, String bqOutputTable,
                                   Map<String, String> queryParams,
                                   String apiEndpoint, Map<String, String> apiParams) {
        this.stepOrder     = stepOrder;
        this.stepType      = stepType;
        this.stepName      = stepName;
        this.bqQuery       = bqQuery;
        this.bqOutputTable = bqOutputTable;
        this.queryParams   = queryParams != null ? Collections.unmodifiableMap(queryParams)
                                                 : Collections.emptyMap();
        this.apiEndpoint   = apiEndpoint;
        this.apiParams     = apiParams != null ? Collections.unmodifiableMap(apiParams)
                                               : Collections.emptyMap();
    }
}
