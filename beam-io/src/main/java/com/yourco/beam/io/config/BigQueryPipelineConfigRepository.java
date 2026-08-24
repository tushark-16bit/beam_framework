package com.yourco.beam.io.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.yourco.beam.model.DataSourceStep;
import com.yourco.beam.model.PipelineConfig;
import com.yourco.beam.model.PipelineStepConfig;
import com.yourco.beam.model.ReportStep;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Fetches a {@code PIPELINE} run's step sequence from the {@code parameter_store} BigQuery table.
 *
 * <p>Stored as a single JSON blob in {@code parameters_val_json}, keyed by
 * ({@code parameter_group_name=parentId}, {@code parameter_data_source=pipelineSubprocess},
 * {@code parameter_name=pipelineName}) — the same {@code parameter_store} table used by both
 * DATA_SOURCE_DOWNLOAD and REPORT_PROCESSING configs, distinguished only by lookup key.
 *
 * <p>This row is a thin ordered pointer list, not a duplicate of the actual per-datasource or
 * per-report config: each {@code DATA_SOURCE} step's real configuration is still fetched by name
 * via the existing {@link BigQuerySourceConfigRepository}, and the terminal {@code REPORT}
 * step's via {@link BigQueryReportRepository} — unchanged by this class.
 *
 * <h2>parameters_val_json structure</h2>
 * <pre>
 * {
 *   "steps": [
 *     {"type": "DATA_SOURCE", "datasource_name": "trades",   "subprocess_name": "eod"},
 *     {"type": "DATA_SOURCE", "datasource_name": "fx_rates", "subprocess_name": "eod"},
 *     {"type": "REPORT",      "report_name": "daily_trades_report", "report_subprocess": "eod"}
 *   ]
 * }
 * </pre>
 *
 * <p>{@link PipelineConfig}'s compact constructor rejects an empty {@code steps} array and any
 * sequence not ending in a {@code REPORT} step — "always terminating in a report" is a checked
 * invariant, not just a convention. A step with an unrecognised {@code type} value fails the
 * same way, at parse time, rather than being silently dropped.
 */
public final class BigQueryPipelineConfigRepository {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryPipelineConfigRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final BigQuery bigquery;
    private final String storeTable; // fully-qualified: `project.dataset.parameter_store`
    private final String parentId;

    public BigQueryPipelineConfigRepository(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQueryPipelineConfigRepository(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery   = bigquery;
        String project  = options.getParamBqProject() != null && !options.getParamBqProject().isBlank()
                          ? options.getParamBqProject() : options.getProject();
        this.storeTable = "`" + project + "." + options.getParamBqDataset()
                        + "." + options.getParamStoreTable() + "`";
        this.parentId   = options.getParentId();
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Fetches and parses a complete {@link PipelineConfig} from {@code parameter_store}.
     *
     * @throws IllegalArgumentException if no matching row exists, {@code steps} is empty/missing,
     *                                   a step has an unknown {@code type}, or the sequence
     *                                   doesn't end in a {@code REPORT} step
     */
    public PipelineConfig fetchPipelineConfig(String pipelineName, String pipelineSubprocess) {
        LOG.info("Fetching pipeline config from parameter_store: parent={} pipeline={} subprocess={}",
                 parentId, pipelineName, pipelineSubprocess);

        String sql = "SELECT parameters_val_json FROM " + storeTable
            + " WHERE parameter_group_name  = @parentId"
            + "   AND parameter_data_source = @subprocess"
            + "   AND parameter_name        = @pipelineName"
            + " LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("parentId",     QueryParameterValue.string(parentId))
            .addNamedParameter("subprocess",   QueryParameterValue.string(pipelineSubprocess))
            .addNamedParameter("pipelineName", QueryParameterValue.string(pipelineName))
            .setUseLegacySql(false)
            .build();

        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                String json = row.get("parameters_val_json").isNull()
                              ? null : row.get("parameters_val_json").getStringValue();
                if (json == null || json.isBlank()) {
                    throw new IllegalArgumentException(
                        "parameters_val_json is empty for pipeline parent=" + parentId
                        + " subprocess=" + pipelineSubprocess + " name=" + pipelineName);
                }
                return parsePipelineConfig(json, pipelineName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ pipeline config query interrupted", e);
        }

        throw new IllegalArgumentException(
            "No parameter_store row found for pipeline parent=" + parentId
            + " subprocess=" + pipelineSubprocess + " name=" + pipelineName);
    }

    // ── JSON parsing ──────────────────────────────────────────────────────────

    private PipelineConfig parsePipelineConfig(String json, String pipelineName) {
        try {
            JsonNode root = JSON.readTree(json);
            JsonNode stepsNode = root.path("steps");
            if (!stepsNode.isArray() || stepsNode.isEmpty()) {
                throw new IllegalArgumentException(
                    "pipeline '" + pipelineName + "' has no steps array in parameters_val_json");
            }

            List<PipelineStepConfig> steps = new ArrayList<>();
            for (JsonNode stepNode : stepsNode) {
                steps.add(parseStep(stepNode, pipelineName));
            }

            PipelineConfig pipelineConfig = new PipelineConfig(steps);
            LOG.info("Pipeline config parsed: {} step(s) for pipeline={}", steps.size(), pipelineName);
            return pipelineConfig;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to parse pipeline config JSON for pipeline=" + pipelineName + ": " + e.getMessage(), e);
        }
    }

    private PipelineStepConfig parseStep(JsonNode stepNode, String pipelineName) {
        String type = stepNode.path("type").asText(null);
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException(
                "pipeline '" + pipelineName + "' has a step with no 'type'");
        }
        return switch (type.toUpperCase()) {
            case "DATA_SOURCE" -> new DataSourceStep(
                stepNode.path("datasource_name").asText(null),
                stepNode.path("subprocess_name").asText(null));
            case "REPORT" -> new ReportStep(
                stepNode.path("report_name").asText(null),
                stepNode.path("report_subprocess").asText(null));
            default -> throw new IllegalArgumentException(
                "pipeline '" + pipelineName + "' has an unknown step type '" + type
                + "' — expected DATA_SOURCE or REPORT");
        };
    }
}
