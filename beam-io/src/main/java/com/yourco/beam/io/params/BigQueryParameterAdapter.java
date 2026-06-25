package com.yourco.beam.io.params;

import java.util.List;
import java.util.Map;

/**
 * Reads pipeline configuration parameters from the BigQuery parameter store.
 *
 * <h2>Table schema</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_config.parameter_store (
 *   ParameterName       STRING NOT NULL,   -- parameter / report name, e.g. "daily_trades_summary"
 *   ParameterGroupName  STRING NOT NULL,   -- top-level business group (--parentId), e.g. "TRADING"
 *   ParameterDataSource STRING NOT NULL,   -- subprocess variant (--reportSubprocess), e.g. "eod"
 *   SchemaOfJson        STRING,            -- JSON object: {"field": {"required": true, "type": "string"}, ...}
 *   ParametersValJson   STRING,            -- JSON object: {"field": "value", ...}
 *   EditGrpNm           STRING,
 *   LastUpdtTs          TIMESTAMP,
 *   LstUpdateUserId     STRING
 * );
 * }</pre>
 *
 * <h2>How SchemaOfJson drives validation</h2>
 * {@code SchemaOfJson} declares which fields are required:
 * <pre>{@code
 * {
 *   "source_bq_table":   {"required": true,  "type": "string"},
 *   "transform_query":   {"required": true,  "type": "string"},
 *   "output_gcs_path":   {"required": true,  "type": "string"},
 *   "row_limit":         {"required": false, "type": "integer"}
 * }
 * }</pre>
 * {@link #fetchRequiredParameters} uses this to validate that every {@code required=true}
 * field is present in {@code ParametersValJson} before returning.
 *
 * <h2>Typical call sequence</h2>
 * <pre>{@code
 * BigQueryParameterAdapter adapter = new BigQueryParameterAdapterImpl(options);
 *
 * // Fetch, validate (via SchemaOfJson), and return all parameters in one call.
 * // Three-identifier key: ParameterGroupName=--parentId, ParameterDataSource=--reportSubprocess,
 * //                       ParameterName=--reportName
 * Map<String, String> params = adapter.fetchRequiredParameters(
 *     "TRADING", "eod", "daily_trades_summary");
 *
 * String sourceTable  = params.get("source_bq_table");
 * String outputPath   = params.get("output_gcs_path");
 * }</pre>
 */
public interface BigQueryParameterAdapter {

    /**
     * Returns the names of all fields marked {@code "required": true} in
     * {@code SchemaOfJson} for the given parameter group.
     *
     * @param parameterGroupName  value of the {@code ParameterGroupName} column
     * @param parameterDataSource value of the {@code ParameterDataSource} column
     * @param parameterName       value of the {@code ParameterName} column
     * @return list of field names; empty if no schema is defined or no required fields
     */
    List<String> fetchRequiredKeys(String parameterGroupName, String parameterDataSource,
                                   String parameterName);

    /**
     * Fetches all parameters from {@code ParametersValJson} for the given group.
     * Returns the full key-value map without any validation.
     *
     * @param parameterGroupName  value of the {@code ParameterGroupName} column
     * @param parameterDataSource value of the {@code ParameterDataSource} column
     * @param parameterName       value of the {@code ParameterName} column
     * @return all key-value pairs from {@code ParametersValJson}; empty map if row not found
     */
    Map<String, String> fetchParameters(String parameterGroupName, String parameterDataSource,
                                        String parameterName);

    /**
     * Convenience: fetches parameters and validates that every field declared as
     * {@code "required": true} in {@code SchemaOfJson} is present and non-null.
     *
     * @param parameterGroupName  value of the {@code ParameterGroupName} column
     * @param parameterDataSource value of the {@code ParameterDataSource} column
     * @param parameterName       value of the {@code ParameterName} column
     * @return validated key-value map from {@code ParametersValJson}
     * @throws IllegalStateException if any required field is absent
     * @throws IllegalStateException if no parameter row is found for the given identifiers
     */
    Map<String, String> fetchRequiredParameters(String parameterGroupName,
                                                String parameterDataSource,
                                                String parameterName);
}
