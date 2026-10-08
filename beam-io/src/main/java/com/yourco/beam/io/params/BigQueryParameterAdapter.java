package com.yourco.beam.io.params;

import java.util.List;
import java.util.Map;

/**
 * Reads pipeline configuration parameters from the BigQuery parameter store.
 *
 * <h2>Table schema</h2>
 * <pre>{@code
 * CREATE TABLE dw.parameter_store (
 *   parameter_name       STRING NOT NULL,   -- parameter / report name, e.g. "daily_trades_summary"
 *   parameter_group_name  STRING NOT NULL,   -- top-level business group (--parentId), e.g. "TRADING"
 *   parameter_data_source STRING NOT NULL,   -- subprocess variant (--reportSubprocess), e.g. "eod"
 *   schema_of_json        STRING,            -- JSON object: {"field": {"required": true, "type": "string"}, ...}
 *   parameters_val_json   STRING,            -- JSON object: {"field": "value", ...}
 *   EditGrpNm           STRING,
 *   LastUpdtTs          TIMESTAMP,
 *   LstUpdateUserId     STRING
 * );
 * }</pre>
 *
 * <h2>How schema_of_json drives validation</h2>
 * {@code schema_of_json} declares which fields are required:
 * <pre>{@code
 * {
 *   "source_bq_table":   {"required": true,  "type": "string"},
 *   "transform_query":   {"required": true,  "type": "string"},
 *   "output_gcs_path":   {"required": true,  "type": "string"},
 *   "row_limit":         {"required": false, "type": "integer"}
 * }
 * }</pre>
 * {@link #fetchRequiredParameters} uses this to validate that every {@code required=true}
 * field is present in {@code parameters_val_json} before returning.
 *
 * <h2>Typical call sequence</h2>
 * <pre>{@code
 * BigQueryParameterAdapter adapter = new BigQueryParameterAdapterImpl(options);
 *
 * // Fetch, validate (via schema_of_json), and return all parameters in one call.
 * // Three-identifier key: parameter_group_name=--parentId, parameter_data_source=--reportSubprocess,
 * //                       parameter_name=--reportName
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
     * {@code schema_of_json} for the given parameter group.
     *
     * @param parameterGroupName  value of the {@code parameter_group_name} column
     * @param parameterDataSource value of the {@code parameter_data_source} column
     * @param parameterName       value of the {@code parameter_name} column
     * @return list of field names; empty if no schema is defined or no required fields
     */
    List<String> fetchRequiredKeys(String parameterGroupName, String parameterDataSource,
                                   String parameterName);

    /**
     * Fetches all parameters from {@code parameters_val_json} for the given group.
     * Returns the full key-value map without any validation.
     *
     * @param parameterGroupName  value of the {@code parameter_group_name} column
     * @param parameterDataSource value of the {@code parameter_data_source} column
     * @param parameterName       value of the {@code parameter_name} column
     * @return all key-value pairs from {@code parameters_val_json}; empty map if row not found
     */
    Map<String, String> fetchParameters(String parameterGroupName, String parameterDataSource,
                                        String parameterName);

    /**
     * Convenience: fetches parameters and validates that every field declared as
     * {@code "required": true} in {@code schema_of_json} is present and non-null.
     *
     * @param parameterGroupName  value of the {@code parameter_group_name} column
     * @param parameterDataSource value of the {@code parameter_data_source} column
     * @param parameterName       value of the {@code parameter_name} column
     * @return validated key-value map from {@code parameters_val_json}
     * @throws IllegalStateException if any required field is absent
     * @throws IllegalStateException if no parameter row is found for the given identifiers
     */
    Map<String, String> fetchRequiredParameters(String parameterGroupName,
                                                String parameterDataSource,
                                                String parameterName);
}
