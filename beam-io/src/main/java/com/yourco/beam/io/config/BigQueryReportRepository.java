package com.yourco.beam.io.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportDatasourceRef;
import com.yourco.beam.model.ReportEmailConfig;
import com.yourco.beam.model.ReportOutputConfig;
import com.yourco.beam.model.ReportPreprocessingStep;
import com.yourco.beam.model.ReportTransformStep;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Fetches structured report configuration from BigQuery tables.
 *
 * <p>This class is the BigQuery equivalent of the JDBC {@code ReportRepository}.
 * All six report config tables live in the same BQ dataset specified by
 * {@code --paramBqProject} and {@code --paramBqDataset}.
 *
 * <h2>Required BQ tables (same schema as the JDBC tables, now in BQ)</h2>
 * <ul>
 *   <li>{@code report_config}</li>
 *   <li>{@code report_datasource_ref}</li>
 *   <li>{@code report_preprocessing_config}</li>
 *   <li>{@code report_transformation_config}</li>
 *   <li>{@code report_output_config}</li>
 *   <li>{@code report_email_config}</li>
 * </ul>
 *
 * All queries use named BQ parameters (@name) to prevent injection.
 */
public final class BigQueryReportRepository {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryReportRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, String>> STR_MAP  = new TypeReference<>() {};
    private static final TypeReference<List<String>>        STR_LIST = new TypeReference<>() {};

    private final BigQuery bigquery;
    private final String project;
    private final String dataset;

    public BigQueryReportRepository(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQueryReportRepository(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery = bigquery;
        this.project  = options.getParamBqProject() != null
                        ? options.getParamBqProject()
                        : options.getProject();
        this.dataset  = options.getParamBqDataset();
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Assembles a complete {@link ReportConfig} from all six BQ tables.
     *
     * @throws IllegalArgumentException if no {@code report_config} row exists
     */
    public ReportConfig fetchReportConfig(String reportName, String reportSubprocess,
                                          String periodId) {
        LOG.info("Fetching report config from BQ: report={} subprocess={} period={}",
                 reportName, reportSubprocess, periodId);

        boolean overrideKey    = fetchOverrideKey(reportName, reportSubprocess, periodId);
        List<ReportDatasourceRef>     datasources    = fetchDatasourceRefs(reportName, reportSubprocess, periodId);
        List<ReportPreprocessingStep> preprocessing  = fetchPreprocessingSteps(reportName, reportSubprocess, periodId);
        List<ReportTransformStep>     transforms     = fetchTransformSteps(reportName, reportSubprocess, periodId);
        List<ReportOutputConfig>      outputs        = fetchOutputConfigs(reportName, reportSubprocess, periodId);
        ReportEmailConfig             email          = fetchEmailConfig(reportName, reportSubprocess, periodId);

        LOG.info("Report config loaded from BQ: {} datasource(s), {} preprocessing, {} transform(s), {} output(s)",
                 datasources.size(), preprocessing.size(), transforms.size(), outputs.size());

        return new ReportConfig(reportName, reportSubprocess, periodId, overrideKey,
                                datasources, preprocessing, transforms, outputs, email);
    }

    /**
     * Returns the fully-qualified BQ output table ({@code project.dataset.table})
     * for a previously-completed data source download.
     *
     * <p>Queries the {@code source_config} BQ table keyed by
     * {@code (datasource_name, subprocess_name, period_id)}.
     *
     * @throws IllegalArgumentException if no source_config row exists or BQ output fields are null
     */
    public String fetchDatasourceOutputTable(String datasourceName, String datasourceSubprocess,
                                             String periodId, FrameworkOptions options) {
        String tableName = options.getParamSourceConfigTable();
        String sql = "SELECT output_bq_project, output_bq_dataset, output_bq_table "
                   + "FROM `" + ref(tableName) + "` "
                   + "WHERE datasource_name   = @datasourceName "
                   + "  AND subprocess_name   = @subprocess "
                   + "  AND period_id         = @periodId "
                   + "LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("datasourceName", QueryParameterValue.string(datasourceName))
            .addNamedParameter("subprocess",     QueryParameterValue.string(datasourceSubprocess))
            .addNamedParameter("periodId",       QueryParameterValue.string(periodId))
            .setUseLegacySql(false)
            .build();

        for (FieldValueList row : runQuery(config).iterateAll()) {
            String proj    = str(row, "output_bq_project");
            String dataset = str(row, "output_bq_dataset");
            String table   = str(row, "output_bq_table");
            if (proj == null || dataset == null || table == null) {
                throw new IllegalArgumentException(
                    "Datasource " + datasourceName + " has null BQ output fields in source_config");
            }
            return proj + "." + dataset + "." + table;
        }
        throw new IllegalArgumentException(
            "No source_config row found for datasource=" + datasourceName
            + " subprocess=" + datasourceSubprocess + " period=" + periodId);
    }

    // ── Private loaders ───────────────────────────────────────────────────────

    private boolean fetchOverrideKey(String reportName, String reportSubprocess, String periodId) {
        String sql = "SELECT override_key FROM `" + ref("report_config") + "` "
                   + "WHERE report_name       = @reportName "
                   + "  AND report_subprocess = @subprocess "
                   + "  AND period_id         = @periodId "
                   + "LIMIT 1";
        QueryJobConfiguration config = qConfig(sql, reportName, reportSubprocess, periodId);
        for (FieldValueList row : runQuery(config).iterateAll()) {
            return !row.get("override_key").isNull()
                   && row.get("override_key").getBooleanValue();
        }
        throw new IllegalArgumentException(
            "No report_config found for report=" + reportName
            + " subprocess=" + reportSubprocess + " period=" + periodId);
    }

    private List<ReportDatasourceRef> fetchDatasourceRefs(String reportName,
                                                           String reportSubprocess,
                                                           String periodId) {
        String sql = "SELECT datasource_name, datasource_subprocess, transform_alias, is_required "
                   + "FROM `" + ref("report_datasource_ref") + "` "
                   + "WHERE report_name       = @reportName "
                   + "  AND report_subprocess = @subprocess "
                   + "  AND period_id         = @periodId "
                   + "ORDER BY datasource_name";
        List<ReportDatasourceRef> result = new ArrayList<>();
        for (FieldValueList row : runQuery(qConfig(sql, reportName, reportSubprocess, periodId)).iterateAll()) {
            boolean required = !row.get("is_required").isNull()
                               && row.get("is_required").getBooleanValue();
            result.add(new ReportDatasourceRef(
                str(row, "datasource_name"),
                str(row, "datasource_subprocess"),
                str(row, "transform_alias"),
                required));
        }
        return result;
    }

    private List<ReportPreprocessingStep> fetchPreprocessingSteps(String reportName,
                                                                   String reportSubprocess,
                                                                   String periodId) {
        String sql = "SELECT step_order, step_type, step_name, bq_query, bq_output_table, "
                   + "       query_params_json, api_endpoint, api_params_json "
                   + "FROM `" + ref("report_preprocessing_config") + "` "
                   + "WHERE report_name       = @reportName "
                   + "  AND report_subprocess = @subprocess "
                   + "  AND period_id         = @periodId "
                   + "ORDER BY step_order";
        List<ReportPreprocessingStep> result = new ArrayList<>();
        for (FieldValueList row : runQuery(qConfig(sql, reportName, reportSubprocess, periodId)).iterateAll()) {
            result.add(new ReportPreprocessingStep(
                intVal(row, "step_order"),
                str(row, "step_type"),
                str(row, "step_name"),
                str(row, "bq_query"),
                str(row, "bq_output_table"),
                parseMap(str(row, "query_params_json")),
                str(row, "api_endpoint"),
                parseMap(str(row, "api_params_json"))));
        }
        return result;
    }

    private List<ReportTransformStep> fetchTransformSteps(String reportName,
                                                           String reportSubprocess,
                                                           String periodId) {
        String sql = "SELECT step_order, step_name, input_alias, output_alias, "
                   + "       query_template, output_bq_table, query_params_json "
                   + "FROM `" + ref("report_transformation_config") + "` "
                   + "WHERE report_name       = @reportName "
                   + "  AND report_subprocess = @subprocess "
                   + "  AND period_id         = @periodId "
                   + "ORDER BY step_order";
        List<ReportTransformStep> result = new ArrayList<>();
        for (FieldValueList row : runQuery(qConfig(sql, reportName, reportSubprocess, periodId)).iterateAll()) {
            result.add(new ReportTransformStep(
                intVal(row, "step_order"),
                str(row, "step_name"),
                str(row, "input_alias"),
                str(row, "output_alias"),
                str(row, "query_template"),
                str(row, "output_bq_table"),
                parseMap(str(row, "query_params_json"))));
        }
        return result;
    }

    private List<ReportOutputConfig> fetchOutputConfigs(String reportName,
                                                         String reportSubprocess,
                                                         String periodId) {
        String sql = "SELECT output_order, input_alias, output_format, gcs_path, "
                   + "       file_prefix, file_suffix, include_header "
                   + "FROM `" + ref("report_output_config") + "` "
                   + "WHERE report_name       = @reportName "
                   + "  AND report_subprocess = @subprocess "
                   + "  AND period_id         = @periodId "
                   + "ORDER BY output_order";
        List<ReportOutputConfig> result = new ArrayList<>();
        for (FieldValueList row : runQuery(qConfig(sql, reportName, reportSubprocess, periodId)).iterateAll()) {
            boolean includeHeader = !row.get("include_header").isNull()
                                    && row.get("include_header").getBooleanValue();
            result.add(new ReportOutputConfig(
                intVal(row, "output_order"),
                str(row, "input_alias"),
                str(row, "output_format"),
                str(row, "gcs_path"),
                str(row, "file_prefix"),
                str(row, "file_suffix"),
                includeHeader));
        }
        return result;
    }

    private ReportEmailConfig fetchEmailConfig(String reportName, String reportSubprocess,
                                               String periodId) {
        String sql = "SELECT to_list, cc_list, subject_template, body_template "
                   + "FROM `" + ref("report_email_config") + "` "
                   + "WHERE report_name       = @reportName "
                   + "  AND report_subprocess = @subprocess "
                   + "  AND period_id         = @periodId "
                   + "LIMIT 1";
        for (FieldValueList row : runQuery(qConfig(sql, reportName, reportSubprocess, periodId)).iterateAll()) {
            return new ReportEmailConfig(
                parseEmailList(str(row, "to_list")),
                parseEmailList(str(row, "cc_list")),
                str(row, "subject_template"),
                str(row, "body_template"));
        }
        return null;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String ref(String table) {
        return project + "." + dataset + "." + table;
    }

    /** Builds a standard query config with the three common report name parameters. */
    private static QueryJobConfiguration qConfig(String sql,
                                                  String reportName,
                                                  String reportSubprocess,
                                                  String periodId) {
        return QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("reportName", QueryParameterValue.string(reportName))
            .addNamedParameter("subprocess", QueryParameterValue.string(reportSubprocess))
            .addNamedParameter("periodId",   QueryParameterValue.string(periodId))
            .setUseLegacySql(false)
            .build();
    }

    private TableResult runQuery(QueryJobConfiguration config) {
        try {
            return bigquery.query(config);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BQ report config query interrupted", e);
        }
    }

    private static String str(FieldValueList row, String field) {
        var v = row.get(field);
        return (v == null || v.isNull()) ? null : v.getStringValue();
    }

    private static int intVal(FieldValueList row, String field) {
        var v = row.get(field);
        return (v == null || v.isNull()) ? 0 : (int) v.getLongValue();
    }

    private Map<String, String> parseMap(String json) {
        if (json == null || json.isBlank()) return Collections.emptyMap();
        try {
            return JSON.readValue(json, STR_MAP);
        } catch (Exception e) {
            LOG.warn("Could not parse JSON map '{}': {}", json, e.getMessage());
            return Collections.emptyMap();
        }
    }

    private List<String> parseEmailList(String value) {
        if (value == null || value.isBlank()) return Collections.emptyList();
        value = value.trim();
        if (value.startsWith("[")) {
            try {
                return JSON.readValue(value, STR_LIST);
            } catch (Exception e) {
                LOG.warn("Could not parse email list as JSON '{}': {}", value, e.getMessage());
            }
        }
        return Arrays.stream(value.split(","))
                     .map(String::trim)
                     .filter(s -> !s.isBlank())
                     .collect(Collectors.toList());
    }
}
