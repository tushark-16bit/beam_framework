package com.yourco.beam.utils.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.model.ApiSourceConfig;
import com.yourco.beam.model.BqFetchConfig;
import com.yourco.beam.model.FileSourceConfig;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Business-level queries against the parameter database.
 *
 * <p>All methods translate raw DB rows into typed domain objects. The SQL uses
 * the schema and table names from {@link FrameworkOptions} so the same framework
 * binary works against different DB environments without recompilation.
 *
 * <h2>Expected DB schema</h2>
 * <pre>{@code
 * -- One row per (datasource, period, subprocess) combination:
 * CREATE TABLE source_config (
 *   datasource_name          VARCHAR(100)  NOT NULL,
 *   period_id                VARCHAR(50)   NOT NULL,
 *   subprocess_name          VARCHAR(100)  NOT NULL,
 *   source_type              VARCHAR(20)   NOT NULL,  -- API | FILE | BQ
 *   -- API columns:
 *   api_endpoint             TEXT,
 *   api_auth_type            VARCHAR(20),             -- NONE | BEARER | BASIC | API_KEY
 *   api_auth_secret_id       TEXT,
 *   api_headers_json         TEXT,                    -- JSON object: {"X-Custom": "value"}
 *   api_query_params_json    TEXT,                    -- JSON object: {"format": "json"}
 *   api_pagination_enabled   BOOLEAN,
 *   api_pagination_strategy  VARCHAR(20),             -- PAGE_NUMBER | CURSOR | OFFSET
 *   api_page_size            INT,
 *   api_next_page_field      VARCHAR(100),
 *   api_data_array_field     VARCHAR(100),
 *   -- FILE columns:
 *   file_type                VARCHAR(20),             -- CSV | EXCEL
 *   file_location            TEXT,                    -- gs://bucket/raw/
 *   file_prefix              TEXT,
 *   file_suffix              TEXT,
 *   file_delimiter           VARCHAR(5),
 *   file_has_header          BOOLEAN,
 *   file_sheet_index         INT,
 *   -- BQ columns:
 *   bq_project_id            VARCHAR(100),
 *   bq_dataset               VARCHAR(100),
 *   bq_table                 VARCHAR(100),
 *   bq_query                 TEXT,
 *   PRIMARY KEY (datasource_name, period_id, subprocess_name)
 * );
 *
 * -- Optional required-parameters guard:
 * CREATE TABLE required_parameters (
 *   datasource_name  VARCHAR(100) NOT NULL,
 *   period_id        VARCHAR(50)  NOT NULL,
 *   subprocess_name  VARCHAR(100) NOT NULL,
 *   parameter_key    VARCHAR(200) NOT NULL,  -- e.g. "api_endpoint"
 *   PRIMARY KEY (datasource_name, period_id, subprocess_name, parameter_key)
 * );
 * }</pre>
 */
public final class ParameterRepository {

    private static final Logger LOG = LoggerFactory.getLogger(ParameterRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private final DatabaseAdapter db;
    private final String schema;
    private final String sourceConfigTable;
    private final String requiredParamsTable;

    public ParameterRepository(DatabaseAdapter db, FrameworkOptions options) {
        this.db                  = db;
        this.schema              = nullToEmpty(options.getParamDbSchema());
        this.sourceConfigTable   = qualifiedTable(this.schema, options.getParamDbSourceConfigTable());
        this.requiredParamsTable = qualifiedTable(this.schema, options.getParamDbRequiredParamsTable());
    }

    // ── Validation ───────────────────────────────────────────────────────────

    /**
     * Returns true if the {@code source_config} table has at least one row for the
     * given key AND the required_parameters table has no unmet entries.
     *
     * <p>Call this before fetching configs — fail fast with a clear message rather
     * than letting the pipeline start with missing configuration.
     */
    public boolean allRequiredParametersExist(String datasourceName, String periodId, String subprocess) {
        return getMissingParameters(datasourceName, periodId, subprocess).isEmpty();
    }

    /**
     * Returns the list of parameter keys that are declared as required but absent in
     * {@code source_config}. An empty list means everything is present.
     *
     * <p>Strategy: query the required_parameters table; for each key, check that the
     * matching column in source_config is non-null for this (datasource, period, subprocess).
     * If the required_parameters table is empty or absent, falls back to checking that
     * at least one source_config row exists.
     */
    public List<String> getMissingParameters(String datasourceName, String periodId, String subprocess) {
        // Step 1: check a source_config row exists
        String existsSql = "SELECT COUNT(*) AS cnt FROM " + sourceConfigTable
            + " WHERE datasource_name = ? AND period_id = ? AND subprocess_name = ?";
        Map<String, Object> countRow = db.queryOne(existsSql, datasourceName, periodId, subprocess)
            .orElse(Collections.emptyMap());
        long rowCount = toLong(countRow.get("cnt"));

        if (rowCount == 0) {
            LOG.warn("No source_config row found for datasource={}, period={}, subprocess={}",
                     datasourceName, periodId, subprocess);
            return List.of("source_config row missing for (" + datasourceName + ", " + periodId + ", " + subprocess + ")");
        }

        // Step 2: check required_parameters table if it exists
        try {
            String requiredSql = "SELECT parameter_key FROM " + requiredParamsTable
                + " WHERE datasource_name = ? AND period_id = ? AND subprocess_name = ?";
            List<Map<String, Object>> required = db.query(requiredSql, datasourceName, periodId, subprocess);

            if (required.isEmpty()) return Collections.emptyList();

            // For each required key, check the column is non-null in source_config
            List<String> missing = new ArrayList<>();
            for (Map<String, Object> reqRow : required) {
                String paramKey = (String) reqRow.get("parameter_key");
                String checkSql = "SELECT " + paramKey + " FROM " + sourceConfigTable
                    + " WHERE datasource_name = ? AND period_id = ? AND subprocess_name = ?";
                try {
                    Map<String, Object> valueRow = db.queryOne(checkSql, datasourceName, periodId, subprocess)
                        .orElse(Collections.emptyMap());
                    if (valueRow.get(paramKey) == null) {
                        missing.add(paramKey);
                    }
                } catch (DatabaseException e) {
                    LOG.warn("Could not check parameter '{}': {}", paramKey, e.getMessage());
                    missing.add(paramKey + " (column not found)");
                }
            }
            return missing;
        } catch (DatabaseException e) {
            // required_parameters table may not exist — treat as no constraints
            LOG.debug("required_parameters table not accessible, skipping validation: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    // ── Config fetch ─────────────────────────────────────────────────────────

    /**
     * Fetches all source configurations for the given key triple.
     *
     * <p>In most setups there is one row per key. Multiple rows are supported
     * for datasources with multiple sub-feeds registered under the same name/period/subprocess.
     */
    public List<SourceConfig> fetchSourceConfigs(String datasourceName, String periodId, String subprocess) {
        String sql = "SELECT * FROM " + sourceConfigTable
            + " WHERE datasource_name = ? AND period_id = ? AND subprocess_name = ?";

        List<Map<String, Object>> rows = db.query(sql, datasourceName, periodId, subprocess);
        if (rows.isEmpty()) {
            throw new DatabaseException(
                "No source_config found for datasource=" + datasourceName
                + ", period=" + periodId + ", subprocess=" + subprocess);
        }

        List<SourceConfig> configs = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            configs.add(rowToSourceConfig(row));
        }
        LOG.info("Fetched {} source config(s) for datasource={}, period={}, subprocess={}",
                 configs.size(), datasourceName, periodId, subprocess);
        return configs;
    }

    // ── Row mapping ──────────────────────────────────────────────────────────

    private SourceConfig rowToSourceConfig(Map<String, Object> row) {
        String datasourceName  = str(row, "datasource_name");
        String periodId        = str(row, "period_id");
        String subprocessName  = str(row, "subprocess_name");
        String sourceTypeStr   = str(row, "source_type");

        SourceType sourceType;
        try {
            sourceType = SourceType.valueOf(sourceTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new DatabaseException("Unknown source_type '" + sourceTypeStr + "' in source_config");
        }

        return switch (sourceType) {
            case API  -> SourceConfig.forApi(datasourceName, periodId, subprocessName, toApiConfig(row));
            case FILE -> SourceConfig.forFile(datasourceName, periodId, subprocessName, toFileConfig(row));
            case BQ   -> SourceConfig.forBq(datasourceName, periodId, subprocessName, toBqConfig(row));
            default   -> throw new DatabaseException(
                "source_type " + sourceType + " is not supported in DATA_SOURCE_DOWNLOAD mode");
        };
    }

    private ApiSourceConfig toApiConfig(Map<String, Object> row) {
        return new ApiSourceConfig(
            str(row, "api_endpoint"),
            str(row, "api_auth_type"),
            str(row, "api_auth_secret_id"),
            parseJsonMap(str(row, "api_headers_json")),
            parseJsonMap(str(row, "api_query_params_json")),
            bool(row, "api_pagination_enabled"),
            str(row, "api_pagination_strategy"),
            toInt(row.get("api_page_size"), 100),
            str(row, "api_next_page_field"),
            str(row, "api_data_array_field")
        );
    }

    private FileSourceConfig toFileConfig(Map<String, Object> row) {
        return new FileSourceConfig(
            str(row, "file_type"),
            str(row, "file_location"),
            str(row, "file_prefix"),
            str(row, "file_suffix"),
            str(row, "file_delimiter"),
            bool(row, "file_has_header"),
            toInt(row.get("file_sheet_index"), 0)
        );
    }

    private BqFetchConfig toBqConfig(Map<String, Object> row) {
        return new BqFetchConfig(
            str(row, "bq_project_id"),
            str(row, "bq_dataset"),
            str(row, "bq_table"),
            str(row, "bq_query")
        );
    }

    // ── Small type helpers ───────────────────────────────────────────────────

    private static String str(Map<String, Object> row, String col) {
        Object v = row.get(col);
        return v != null ? v.toString() : null;
    }

    private static boolean bool(Map<String, Object> row, String col) {
        Object v = row.get(col);
        if (v instanceof Boolean b) return b;
        if (v instanceof Number n) return n.intValue() != 0;
        return false;
    }

    private static int toInt(Object v, int defaultValue) {
        if (v instanceof Number n) return n.intValue();
        return defaultValue;
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        return 0L;
    }

    private static Map<String, String> parseJsonMap(String json) {
        if (json == null || json.isBlank()) return Collections.emptyMap();
        try {
            return JSON.readValue(json, MAP_TYPE);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    private static String qualifiedTable(String schema, String table) {
        return (schema == null || schema.isBlank()) ? table : schema + "." + table;
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
