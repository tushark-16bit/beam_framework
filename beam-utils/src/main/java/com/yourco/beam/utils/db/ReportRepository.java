package com.yourco.beam.utils.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Business-level queries against the parameter DB for report configuration.
 *
 * <p>All methods translate raw DB rows into typed domain objects. Connection
 * lifecycle is managed by the caller — see {@link ParameterRepository} for the
 * recommended try-with-resources pattern.
 *
 * <h2>Required DB tables (see README for full DDL)</h2>
 * <ul>
 *   <li>{@code report_config}</li>
 *   <li>{@code report_datasource_ref}</li>
 *   <li>{@code report_preprocessing_config}</li>
 *   <li>{@code report_transformation_config}</li>
 *   <li>{@code report_output_config}</li>
 *   <li>{@code report_email_config}</li>
 * </ul>
 */
public final class ReportRepository {

    private static final Logger LOG = LoggerFactory.getLogger(ReportRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, String>> STR_MAP  = new TypeReference<>() {};
    private static final TypeReference<List<String>>        STR_LIST = new TypeReference<>() {};

    private final DatabaseAdapter db;
    private final String schema;

    public ReportRepository(DatabaseAdapter db, FrameworkOptions options) {
        this.db     = db;
        this.schema = options.getParamDbSchema();
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Fetches and assembles the complete {@link ReportConfig} for the given report.
     *
     * @throws IllegalArgumentException if no {@code report_config} row exists
     */
    public ReportConfig fetchReportConfig(String reportName, String reportSubprocess,
                                          String periodId) {
        LOG.info("Fetching report config: report={} subprocess={} period={}",
                 reportName, reportSubprocess, periodId);

        boolean overrideKey = fetchOverrideKey(reportName, reportSubprocess, periodId);

        List<ReportDatasourceRef>    datasources    = fetchDatasourceRefs(reportName, reportSubprocess, periodId);
        List<ReportPreprocessingStep> preprocessing = fetchPreprocessingSteps(reportName, reportSubprocess, periodId);
        List<ReportTransformStep>    transforms     = fetchTransformSteps(reportName, reportSubprocess, periodId);
        List<ReportOutputConfig>     outputs        = fetchOutputConfigs(reportName, reportSubprocess, periodId);
        ReportEmailConfig            email          = fetchEmailConfig(reportName, reportSubprocess, periodId);

        LOG.info("Report config loaded: {} datasources, {} preprocessing, {} transforms, {} outputs",
                 datasources.size(), preprocessing.size(), transforms.size(), outputs.size());

        return new ReportConfig(reportName, reportSubprocess, periodId, overrideKey,
                                datasources, preprocessing, transforms, outputs, email);
    }

    /**
     * Returns the BQ output table ref ({@code project.dataset.table}) for a datasource
     * that was previously downloaded. Used to populate the alias registry.
     *
     * @throws IllegalArgumentException if no {@code source_config} row is found
     */
    public String fetchDatasourceOutputTable(String datasourceName, String datasourceSubprocess,
                                             String periodId, FrameworkOptions options) {
        String table = options.getParamDbSourceConfigTable();
        String sql = "SELECT output_bq_project, output_bq_dataset, output_bq_table "
                   + "FROM " + qualified(table)
                   + " WHERE datasource_name = ? AND subprocess_name = ? AND period_id = ?";

        List<Map<String, Object>> rows = db.query(sql, datasourceName, datasourceSubprocess, periodId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(
                "No source_config found for datasource=" + datasourceName
                + " subprocess=" + datasourceSubprocess + " period=" + periodId);
        }
        Map<String, Object> row = rows.get(0);
        String project = str(row, "output_bq_project");
        String dataset  = str(row, "output_bq_dataset");
        String bqTable  = str(row, "output_bq_table");
        if (project == null || dataset == null || bqTable == null) {
            throw new IllegalArgumentException(
                "Datasource " + datasourceName + " has no BQ output table configured");
        }
        return project + "." + dataset + "." + bqTable;
    }

    // ── Private loaders ───────────────────────────────────────────────────────

    private boolean fetchOverrideKey(String reportName, String reportSubprocess, String periodId) {
        String sql = "SELECT override_key FROM " + qualified("report_config")
                   + " WHERE report_name = ? AND report_subprocess = ? AND period_id = ?";
        List<Map<String, Object>> rows = db.query(sql, reportName, reportSubprocess, periodId);
        if (rows.isEmpty()) {
            throw new IllegalArgumentException(
                "No report_config found for report=" + reportName
                + " subprocess=" + reportSubprocess + " period=" + periodId);
        }
        Object v = rows.get(0).get("override_key");
        return v != null && (Boolean.TRUE.equals(v)
                             || "true".equalsIgnoreCase(v.toString())
                             || "1".equals(v.toString()));
    }

    private List<ReportDatasourceRef> fetchDatasourceRefs(String reportName,
                                                           String reportSubprocess,
                                                           String periodId) {
        String sql = "SELECT datasource_name, datasource_subprocess, transform_alias, is_required "
                   + "FROM " + qualified("report_datasource_ref")
                   + " WHERE report_name = ? AND report_subprocess = ? AND period_id = ?"
                   + " ORDER BY datasource_name";
        List<Map<String, Object>> rows = db.query(sql, reportName, reportSubprocess, periodId);
        List<ReportDatasourceRef> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object req = row.get("is_required");
            boolean required = req == null || Boolean.TRUE.equals(req)
                               || "true".equalsIgnoreCase(String.valueOf(req))
                               || "1".equals(String.valueOf(req));
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
                   + "FROM " + qualified("report_preprocessing_config")
                   + " WHERE report_name = ? AND report_subprocess = ? AND period_id = ?"
                   + " ORDER BY step_order";
        List<Map<String, Object>> rows = db.query(sql, reportName, reportSubprocess, periodId);
        List<ReportPreprocessingStep> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            result.add(new ReportPreprocessingStep(
                intVal(row, "step_order"),
                str(row, "step_type"),
                str(row, "step_name"),
                str(row, "bq_query"),
                str(row, "bq_output_table"),
                parseStringMap(str(row, "query_params_json")),
                str(row, "api_endpoint"),
                parseStringMap(str(row, "api_params_json"))));
        }
        return result;
    }

    private List<ReportTransformStep> fetchTransformSteps(String reportName,
                                                           String reportSubprocess,
                                                           String periodId) {
        String sql = "SELECT step_order, step_name, input_alias, output_alias, "
                   + "       query_template, output_bq_table, query_params_json "
                   + "FROM " + qualified("report_transformation_config")
                   + " WHERE report_name = ? AND report_subprocess = ? AND period_id = ?"
                   + " ORDER BY step_order";
        List<Map<String, Object>> rows = db.query(sql, reportName, reportSubprocess, periodId);
        List<ReportTransformStep> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            result.add(new ReportTransformStep(
                intVal(row, "step_order"),
                str(row, "step_name"),
                str(row, "input_alias"),
                str(row, "output_alias"),
                str(row, "query_template"),
                str(row, "output_bq_table"),
                parseStringMap(str(row, "query_params_json"))));
        }
        return result;
    }

    private List<ReportOutputConfig> fetchOutputConfigs(String reportName,
                                                         String reportSubprocess,
                                                         String periodId) {
        String sql = "SELECT output_order, input_alias, output_format, gcs_path, "
                   + "       file_prefix, file_suffix, include_header "
                   + "FROM " + qualified("report_output_config")
                   + " WHERE report_name = ? AND report_subprocess = ? AND period_id = ?"
                   + " ORDER BY output_order";
        List<Map<String, Object>> rows = db.query(sql, reportName, reportSubprocess, periodId);
        List<ReportOutputConfig> result = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object ih = row.get("include_header");
            boolean includeHeader = ih == null || Boolean.TRUE.equals(ih)
                                    || "true".equalsIgnoreCase(String.valueOf(ih))
                                    || "1".equals(String.valueOf(ih));
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

    private ReportEmailConfig fetchEmailConfig(String reportName,
                                               String reportSubprocess,
                                               String periodId) {
        String sql = "SELECT to_list, cc_list, subject_template, body_template "
                   + "FROM " + qualified("report_email_config")
                   + " WHERE report_name = ? AND report_subprocess = ? AND period_id = ?";
        List<Map<String, Object>> rows = db.query(sql, reportName, reportSubprocess, periodId);
        if (rows.isEmpty()) return null;
        Map<String, Object> row = rows.get(0);
        return new ReportEmailConfig(
            parseEmailList(str(row, "to_list")),
            parseEmailList(str(row, "cc_list")),
            str(row, "subject_template"),
            str(row, "body_template"));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private String qualified(String tableName) {
        return (schema != null && !schema.isBlank()) ? schema + "." + tableName : tableName;
    }

    private static String str(Map<String, Object> row, String key) {
        Object v = row.get(key);
        return v != null ? v.toString() : null;
    }

    private static int intVal(Map<String, Object> row, String key) {
        Object v = row.get(key);
        if (v == null) return 0;
        return v instanceof Number ? ((Number) v).intValue() : Integer.parseInt(v.toString());
    }

    private Map<String, String> parseStringMap(String json) {
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
        // Accept JSON array ["a@b.com","c@d.com"] or comma-separated a@b.com,c@d.com
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
