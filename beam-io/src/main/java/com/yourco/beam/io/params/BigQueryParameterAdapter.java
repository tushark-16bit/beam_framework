package com.yourco.beam.io.params;

import java.util.List;
import java.util.Map;

/**
 * Reads pipeline configuration parameters from a BigQuery parameter store.
 *
 * <p>There are two BQ tables involved:
 * <dl>
 *   <dt><b>parameter_store</b></dt>
 *   <dd>Key-value rows keyed by {@code (process_name, subprocess_name, period_id, param_key)}.
 *       Holds the actual values for every configurable parameter of a pipeline run.</dd>
 *   <dt><b>required_parameters_index</b></dt>
 *   <dd>Keyed by {@code (process_name, subprocess_name, param_key)}.
 *       Declares which param keys are mandatory for each process variant.</dd>
 * </dl>
 *
 * <h2>Typical call sequence</h2>
 * <pre>{@code
 * BigQueryParameterAdapter adapter = new BigQueryParameterAdapterImpl(options);
 *
 * // 1. Look up which keys this process needs
 * List<String> required = adapter.fetchRequiredKeys("daily_trades_report", "eod");
 *
 * // 2. Fetch their values for this period
 * Map<String, String> params = adapter.fetchParameters("daily_trades_report", "eod", "2024-01", required);
 *
 * // — or use the one-call convenience method —
 * Map<String, String> params = adapter.fetchRequiredParameters("daily_trades_report", "eod", "2024-01");
 * }</pre>
 *
 * <h2>BQ table schema (DDL)</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_config.parameter_store (
 *   process_name    STRING NOT NULL,
 *   subprocess_name STRING NOT NULL,
 *   period_id       STRING NOT NULL,
 *   param_key       STRING NOT NULL,
 *   param_value     STRING,
 *   created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
 * );
 *
 * CREATE TABLE pipeline_config.required_parameters_index (
 *   process_name    STRING NOT NULL,
 *   subprocess_name STRING NOT NULL,
 *   param_key       STRING NOT NULL,
 *   is_required     BOOL NOT NULL DEFAULT TRUE,
 *   description     STRING
 * );
 * }</pre>
 */
public interface BigQueryParameterAdapter {

    /**
     * Returns all param keys that are registered as required for {@code processName/subprocess}.
     * Used to drive the subsequent {@link #fetchParameters} call without hard-coding key names.
     *
     * @return list of {@code param_key} values; empty if no required params are registered
     */
    List<String> fetchRequiredKeys(String processName, String subprocess);

    /**
     * Fetches all parameter rows for {@code (processName, subprocess, periodId)}.
     * Returns every param_key stored for that period — no key filtering.
     */
    Map<String, String> fetchParameters(String processName, String subprocess, String periodId);

    /**
     * Fetches only the specified {@code keys} from the parameter store.
     * Keys that are not present return no entry in the result map (not an error).
     *
     * @param keys param_key values to retrieve; if empty, returns an empty map
     */
    Map<String, String> fetchParameters(String processName, String subprocess,
                                        String periodId, List<String> keys);

    /**
     * Convenience: looks up required keys then fetches all of them in one call.
     * Throws {@link IllegalStateException} if any required key is absent for the period.
     */
    Map<String, String> fetchRequiredParameters(String processName, String subprocess,
                                                String periodId);
}
