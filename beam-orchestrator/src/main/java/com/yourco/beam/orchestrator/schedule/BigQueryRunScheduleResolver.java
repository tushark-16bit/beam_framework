package com.yourco.beam.orchestrator.schedule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.yourco.beam.orchestrator.model.ResolvedPeriod;
import com.yourco.beam.orchestrator.model.RunSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads {@code parameter_store} to resolve which pipeline runs to create tasks for.
 *
 * <h2>How rows opt in to orchestration</h2>
 * A {@code parameter_store} row is picked up by this resolver when its
 * {@code parameters_val_json} contains:
 * <pre>{@code
 * {
 *   "run_type":   "DATA_SOURCE_DOWNLOAD",   // or "REPORT_PROCESSING" — required
 *   "enabled":    "true",                   // optional; default true
 *   "frequency":  "MONTHLY",               // optional; matches all if absent
 *   "run_order":  "10"                     // optional; lower = runs first; default 999
 * }
 * }</pre>
 *
 * <p>Rows without {@code run_type} are ignored — existing data source and report config
 * rows are unaffected unless you add these keys.
 *
 * <h2>Frequency matching</h2>
 * A row is included when:
 * <ul>
 *   <li>{@code frequency} equals the requested frequency (case-insensitive), or</li>
 *   <li>{@code frequency} is {@code "ALL"}, or</li>
 *   <li>{@code frequency} is absent / null (matches every invocation)</li>
 * </ul>
 */
public final class BigQueryRunScheduleResolver implements RunScheduleResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryRunScheduleResolver.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final BigQuery bigquery;
    private final String   storeTable;  // `project.dataset.parameter_store`

    public BigQueryRunScheduleResolver(BigQuery bigquery, String storeTable) {
        this.bigquery   = bigquery;
        this.storeTable = storeTable;
    }

    @Override
    public List<RunSpec> resolve(String parentId, String frequency, ResolvedPeriod period) {
        String sql =
            "SELECT parameter_name, parameter_data_source, parameters_val_json,"
            + "  JSON_VALUE(parameters_val_json, '$.run_type')  AS run_type,"
            + "  JSON_VALUE(parameters_val_json, '$.enabled')   AS enabled,"
            + "  JSON_VALUE(parameters_val_json, '$.frequency') AS freq,"
            + "  CAST(COALESCE(JSON_VALUE(parameters_val_json, '$.run_order'), '999') AS INT64) AS run_order"
            + " FROM " + storeTable
            + " WHERE parameter_group_name = @parentId"
            + "   AND JSON_VALUE(parameters_val_json, '$.run_type') IS NOT NULL"
            + "   AND COALESCE(JSON_VALUE(parameters_val_json, '$.enabled'), 'true') = 'true'"
            + "   AND ("
            + "     UPPER(JSON_VALUE(parameters_val_json, '$.frequency')) = UPPER(@frequency)"
            + "     OR JSON_VALUE(parameters_val_json, '$.frequency') = 'ALL'"
            + "     OR JSON_VALUE(parameters_val_json, '$.frequency') IS NULL"
            + "   )"
            + " ORDER BY run_order, parameter_name";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("parentId",   QueryParameterValue.string(parentId))
            .addNamedParameter("frequency",  QueryParameterValue.string(frequency.toUpperCase()))
            .setUseLegacySql(false)
            .build();

        List<RunSpec> specs = new ArrayList<>();
        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                String name       = row.get("parameter_name").getStringValue();
                String subprocess = row.get("parameter_data_source").getStringValue();
                String runType    = row.get("run_type").getStringValue();
                int    runOrder   = (int) row.get("run_order").getLongValue();
                String valJson    = row.get("parameters_val_json").isNull()
                                    ? "{}" : row.get("parameters_val_json").getStringValue();

                Map<String, String> extraParams = parseExtraParams(valJson, name);

                specs.add(new RunSpec(runType, parentId, name, subprocess,
                                      period, runOrder, extraParams));
                LOG.info("Scheduled: {} | {}/{}/{} | order={}",
                         runType, parentId, name, subprocess, runOrder);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ schedule query interrupted", e);
        }

        LOG.info("Resolved {} task spec(s) for parentId={}, frequency={}", specs.size(), parentId, frequency);
        return specs;
    }

    /**
     * Extracts any keys from {@code parameters_val_json} not used by the framework
     * itself and forwards them as {@link RunSpec#extraParams} for the pipeline invocation.
     * Framework-internal orchestration keys are excluded from the map.
     */
    private static Map<String, String> parseExtraParams(String json, String name) {
        try {
            JsonNode root = JSON.readTree(json);
            Map<String, String> params = new HashMap<>();
            root.fields().forEachRemaining(e -> {
                String key = e.getKey();
                if (!isOrchestratorKey(key) && !e.getValue().isNull()) {
                    params.put(key, e.getValue().asText());
                }
            });
            return params;
        } catch (Exception e) {
            LOG.warn("Could not parse extra params from parameters_val_json for '{}': {}", name, e.getMessage());
            return Collections.emptyMap();
        }
    }

    private static boolean isOrchestratorKey(String key) {
        return switch (key) {
            case "run_type", "enabled", "frequency", "run_order" -> true;
            default -> false;
        };
    }
}
