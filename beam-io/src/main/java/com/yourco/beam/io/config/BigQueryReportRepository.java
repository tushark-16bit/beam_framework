package com.yourco.beam.io.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportDatasourceRef;
import com.yourco.beam.model.ReportEmailConfig;
import com.yourco.beam.model.ReportOutputConfig;
import com.yourco.beam.model.ReportPreprocessingStep;
import com.yourco.beam.model.ReportTransformStep;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.ReportOutputSinkType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Fetches report configuration from the {@code parameter_store} BigQuery table.
 *
 * <p>Report configs are stored as a single nested JSON blob in {@code parameters_val_json},
 * keyed by ({@code parameter_group_name=parentId}, {@code parameter_data_source=reportSubprocess},
 * {@code parameter_name=reportName}). This is the same {@code parameter_store} table used
 * by DATA_SOURCE_DOWNLOAD for source configs — reports and data sources share the table,
 * distinguished only by their lookup keys.
 *
 * <p>{@code periodId} is not part of the lookup key — report configs are period-agnostic,
 * exactly like data source configs. Period-specific SQL tokens ({@code {periodStart}},
 * {@code {periodEnd}}) are resolved at runtime by {@code QueryParameterResolver}.
 *
 * <h2>parameters_val_json structure</h2>
 * <pre>
 * {
 *   "override_key": false,
 *   "datasources": [
 *     {"datasource_name": "trades", "datasource_subprocess": "eod",
 *      "transform_alias": "raw_trades", "is_required": true}
 *   ],
 *   "preprocessing": [
 *     {"step_order": 1, "step_type": "BQ_QUERY", "step_name": "staging",
 *      "bq_query": "...", "bq_output_table": "project.dataset.table",
 *      "query_params_json": {}, "api_endpoint": null, "api_params_json": {}}
 *   ],
 *   "transforms": [
 *     {"step_order": 1, "step_name": "aggregate", "input_alias": "raw_trades",
 *      "output_alias": "summary", "query_template": "SELECT ... FROM {raw_trades}",
 *      "output_bq_table": "project.dataset.table", "query_params_json": {}}
 *   ],
 *   "outputs": [
 *     {"output_order": 1, "input_alias": "summary", "sink_type": "GCS",
 *      "output_format": "CSV", "gcs_path": "gs://bucket/reports/",
 *      "file_prefix": "trades_", "file_suffix": ".csv", "include_header": true,
 *      "bq_sink_table": null, "api_endpoint": null, "api_method": null,
 *      "api_auth_secret_id": null, "api_headers_json": null}
 *   ],
 *   "email": {"to_list": ["analyst@example.com"], "cc_list": [],
 *             "subject_template": "Report {periodId}",
 *             "body_template": "Please find the attached report."}
 * }
 * </pre>
 *
 * <p>All queries use named BQ parameters ({@code @name}) to prevent injection.
 */
public final class BigQueryReportRepository {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryReportRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final BigQuery bigquery;
    private final String storeTable; // fully-qualified: `project.dataset.parameter_store`
    private final String parentId;

    public BigQueryReportRepository(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQueryReportRepository(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery   = bigquery;
        String project  = options.getParamBqProject() != null && !options.getParamBqProject().isBlank()
                          ? options.getParamBqProject() : options.getProject();
        this.storeTable = "`" + project + "." + options.getParamBqDataset()
                        + "." + options.getParamStoreTable() + "`";
        this.parentId   = options.getParentId();
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Fetches and parses a complete {@link ReportConfig} from {@code parameter_store}.
     *
     * <p>Looks up the row keyed by (parentId, reportSubprocess, reportName) and
     * parses the nested JSON blob in {@code parameters_val_json}.
     *
     * @throws IllegalArgumentException if no matching {@code parameter_store} row exists
     */
    public ReportConfig fetchReportConfig(String reportName, String reportSubprocess,
                                          String periodId) {
        LOG.info("Fetching report config from parameter_store: parent={} report={} subprocess={}",
                 parentId, reportName, reportSubprocess);

        String sql = "SELECT parameters_val_json FROM " + storeTable
            + " WHERE parameter_group_name  = @parentId"
            + "   AND parameter_data_source = @subprocess"
            + "   AND parameter_name        = @reportName"
            + " LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("parentId",   QueryParameterValue.string(parentId))
            .addNamedParameter("subprocess", QueryParameterValue.string(reportSubprocess))
            .addNamedParameter("reportName", QueryParameterValue.string(reportName))
            .setUseLegacySql(false)
            .build();

        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                String json = row.get("parameters_val_json").isNull()
                              ? null : row.get("parameters_val_json").getStringValue();
                if (json == null || json.isBlank()) {
                    throw new IllegalArgumentException(
                        "parameters_val_json is empty for report parent=" + parentId
                        + " subprocess=" + reportSubprocess + " name=" + reportName);
                }
                return parseReportConfig(json, reportName, reportSubprocess, periodId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ report config query interrupted", e);
        }

        throw new IllegalArgumentException(
            "No parameter_store row found for report parent=" + parentId
            + " subprocess=" + reportSubprocess + " name=" + reportName);
    }

    // ── JSON parsing ──────────────────────────────────────────────────────────

    private ReportConfig parseReportConfig(String json, String reportName,
                                            String reportSubprocess, String periodId) {
        try {
            JsonNode root = JSON.readTree(json);

            boolean overrideKey = root.path("override_key").asBoolean(false);
            List<ReportDatasourceRef>     datasources   = parseDatasources(root.path("datasources"));
            List<ReportPreprocessingStep> preprocessing = parsePreprocessing(root.path("preprocessing"));
            List<ReportTransformStep>     transforms    = parseTransforms(root.path("transforms"));
            List<ReportOutputConfig>      outputs       = parseOutputs(root.path("outputs"));
            ReportEmailConfig             email         = parseEmail(root.path("email"));

            LOG.info("Report config parsed: {} datasource(s), {} preprocessing, {} transform(s), {} output(s)",
                     datasources.size(), preprocessing.size(), transforms.size(), outputs.size());

            return new ReportConfig(reportName, reportSubprocess, periodId, overrideKey,
                                    datasources, preprocessing, transforms, outputs, email);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to parse report config JSON for report=" + reportName + ": " + e.getMessage(), e);
        }
    }

    private List<ReportDatasourceRef> parseDatasources(JsonNode node) {
        if (node.isMissingNode() || !node.isArray()) return Collections.emptyList();
        List<ReportDatasourceRef> result = new ArrayList<>();
        for (JsonNode ds : node) {
            result.add(new ReportDatasourceRef(
                ds.path("datasource_name").asText(null),
                ds.path("datasource_subprocess").asText(null),
                ds.path("transform_alias").asText(null),
                ds.path("is_required").asBoolean(true)));
        }
        return result;
    }

    private List<ReportPreprocessingStep> parsePreprocessing(JsonNode node) {
        if (node.isMissingNode() || !node.isArray()) return Collections.emptyList();
        List<ReportPreprocessingStep> result = new ArrayList<>();
        for (JsonNode step : node) {
            result.add(new ReportPreprocessingStep(
                step.path("step_order").asInt(0),
                step.path("step_type").asText(null),
                step.path("step_name").asText(null),
                step.path("bq_query").asText(null),
                step.path("bq_output_table").asText(null),
                nodeToStringMap(step.path("query_params_json")),
                step.path("api_endpoint").asText(null),
                nodeToStringMap(step.path("api_params_json"))));
        }
        return result;
    }

    private List<ReportTransformStep> parseTransforms(JsonNode node) {
        if (node.isMissingNode() || !node.isArray()) return Collections.emptyList();
        List<ReportTransformStep> result = new ArrayList<>();
        for (JsonNode step : node) {
            result.add(new ReportTransformStep(
                step.path("step_order").asInt(0),
                step.path("step_name").asText(null),
                step.path("input_alias").asText(null),
                step.path("output_alias").asText(null),
                step.path("query_template").asText(null),
                step.path("output_bq_table").asText(null),
                nodeToStringMap(step.path("query_params_json"))));
        }
        return result;
    }

    private List<ReportOutputConfig> parseOutputs(JsonNode node) {
        if (node.isMissingNode() || !node.isArray()) return Collections.emptyList();
        List<ReportOutputConfig> result = new ArrayList<>();
        for (JsonNode out : node) {
            result.add(new ReportOutputConfig(
                out.path("output_order").asInt(0),
                out.path("input_alias").asText(null),
                parseSinkType(out.path("sink_type").asText(null)),
                out.path("output_format").asText(null),
                out.path("gcs_path").asText(null),
                out.path("file_prefix").asText(null),
                out.path("file_suffix").asText(null),
                out.path("include_header").asBoolean(true),
                nullIfMissing(out, "bq_sink_table"),
                nullIfMissing(out, "api_endpoint"),
                nullIfMissing(out, "api_method"),
                nullIfMissing(out, "api_auth_secret_id"),
                nullIfMissing(out, "api_headers_json")));
        }
        return result;
    }

    private ReportEmailConfig parseEmail(JsonNode node) {
        if (node.isMissingNode() || node.isNull()) return null;
        List<String> toList = parseEmailList(node.path("to_list"));
        if (toList.isEmpty()) return null;
        return new ReportEmailConfig(
            toList,
            parseEmailList(node.path("cc_list")),
            nullIfMissing(node, "subject_template"),
            nullIfMissing(node, "body_template"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static ReportOutputSinkType parseSinkType(String value) {
        if (value == null || value.isBlank()) return ReportOutputSinkType.GCS;
        try {
            return ReportOutputSinkType.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "Unknown sink_type '" + value + "' in report config — expected GCS, BQ, or API");
        }
    }

    /** Converts a JsonNode to a String→String map. Handles both inline objects and JSON strings. */
    private Map<String, String> nodeToStringMap(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) return Collections.emptyMap();
        if (node.isObject()) {
            Map<String, String> map = new HashMap<>();
            node.fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue().asText()));
            return map;
        }
        if (node.isTextual()) {
            String text = node.asText();
            if (text == null || text.isBlank()) return Collections.emptyMap();
            try {
                JsonNode parsed = JSON.readTree(text);
                if (!parsed.isObject()) return Collections.emptyMap();
                Map<String, String> map = new HashMap<>();
                parsed.fields().forEachRemaining(e -> map.put(e.getKey(), e.getValue().asText()));
                return map;
            } catch (Exception e) {
                LOG.warn("Could not parse JSON map '{}': {}", text, e.getMessage());
                return Collections.emptyMap();
            }
        }
        return Collections.emptyMap();
    }

    private List<String> parseEmailList(JsonNode node) {
        if (node.isMissingNode() || node.isNull()) return Collections.emptyList();
        if (node.isArray()) {
            List<String> result = new ArrayList<>();
            for (JsonNode item : node) {
                if (!item.isNull() && !item.asText().isBlank()) result.add(item.asText());
            }
            return result;
        }
        String str = node.asText();
        if (str == null || str.isBlank()) return Collections.emptyList();
        return Arrays.stream(str.split(","))
                     .map(String::trim)
                     .filter(s -> !s.isBlank())
                     .collect(Collectors.toList());
    }

    private static String nullIfMissing(JsonNode node, String field) {
        JsonNode v = node.path(field);
        if (v.isMissingNode() || v.isNull()) return null;
        return v.asText();
    }
}
