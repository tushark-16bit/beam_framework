package com.yourco.beam.utils.db;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.model.AggregationConfig;
import com.yourco.beam.model.BncRule;
import com.yourco.beam.model.ApiSourceConfig;
import com.yourco.beam.model.BqFetchConfig;
import com.yourco.beam.model.FileSourceConfig;
import com.yourco.beam.model.LookupConfig;
import com.yourco.beam.model.OutputConfig;
import com.yourco.beam.model.QueryConfig;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.model.SourceTransformConfig;
import com.yourco.beam.model.ValidationConfig;
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
 * <h2>DB connection lifecycle</h2>
 * The {@link DatabaseAdapter} passed to the constructor is <em>not</em> owned by this
 * class — callers are responsible for closing it. The recommended pattern is:
 * <pre>{@code
 * try (DatabaseAdapter db = DatabaseAdapterFactory.create(options)) {
 *     ParameterRepository repo = new ParameterRepository(db, options);
 *     List<SourceConfig> configs = repo.fetchSourceConfigs(...);
 * } // db.close() called automatically
 * }</pre>
 * Each call to this repository creates a connection from the pool, runs one query,
 * and returns the connection immediately — it does not hold a long-lived connection.
 *
 * <h2>Required DB tables</h2>
 * See {@code beam-utils/README.md} for the full DDL including all new columns.
 */
public final class ParameterRepository {

    private static final Logger LOG = LoggerFactory.getLogger(ParameterRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, String>> STR_MAP = new TypeReference<>() {};
    private static final TypeReference<List<String>>        STR_LIST = new TypeReference<>() {};

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

    public boolean allRequiredParametersExist(String datasourceName, String periodId, String subprocess) {
        return getMissingParameters(datasourceName, periodId, subprocess).isEmpty();
    }

    public List<String> getMissingParameters(String datasourceName, String periodId, String subprocess) {
        String existsSql = "SELECT COUNT(*) AS cnt FROM " + sourceConfigTable
            + " WHERE datasource_name = ? AND period_id = ? AND subprocess_name = ?";
        Map<String, Object> countRow = db.queryOne(existsSql, datasourceName, periodId, subprocess)
            .orElse(Collections.emptyMap());
        long rowCount = toLong(countRow.get("cnt"));

        if (rowCount == 0) {
            LOG.warn("No source_config row found for datasource={}, period={}, subprocess={}",
                     datasourceName, periodId, subprocess);
            return List.of("source_config row missing for ("
                + datasourceName + ", " + periodId + ", " + subprocess + ")");
        }

        try {
            String requiredSql = "SELECT parameter_key FROM " + requiredParamsTable
                + " WHERE datasource_name = ? AND period_id = ? AND subprocess_name = ?";
            List<Map<String, Object>> required = db.query(requiredSql, datasourceName, periodId, subprocess);

            if (required.isEmpty()) return Collections.emptyList();

            List<String> missing = new ArrayList<>();
            for (Map<String, Object> reqRow : required) {
                String paramKey = (String) reqRow.get("parameter_key");
                String checkSql = "SELECT " + paramKey + " FROM " + sourceConfigTable
                    + " WHERE datasource_name = ? AND period_id = ? AND subprocess_name = ?";
                try {
                    Map<String, Object> valueRow = db.queryOne(checkSql, datasourceName, periodId, subprocess)
                        .orElse(Collections.emptyMap());
                    if (valueRow.get(paramKey) == null) missing.add(paramKey);
                } catch (DatabaseException e) {
                    LOG.warn("Could not check parameter '{}': {}", paramKey, e.getMessage());
                    missing.add(paramKey + " (column not found)");
                }
            }
            return missing;
        } catch (DatabaseException e) {
            LOG.debug("required_parameters table not accessible, skipping validation: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    // ── Config fetch ─────────────────────────────────────────────────────────

    public List<SourceConfig> fetchSourceConfigs(String datasourceName, String periodId,
                                                  String subprocess) {
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

        SourceConfig.Builder builder = SourceConfig.builder()
            .datasourceName(datasourceName)
            .periodId(periodId)
            .subprocessName(subprocessName)
            .sourceType(sourceType)
            .queryConfig(toQueryConfig(row))
            .outputConfig(toOutputConfig(row))
            .sourceTransforms(toSourceTransforms(str(row, "source_transforms_json")))
            .validationConfig(toValidationConfig(row));

        switch (sourceType) {
            case API  -> builder.apiConfig(toApiConfig(row));
            case FILE -> builder.fileConfig(toFileConfig(row));
            case BQ   -> builder.bqFetchConfig(toBqConfig(row));
            default   -> throw new DatabaseException(
                "source_type " + sourceType + " is not supported in DATA_SOURCE_DOWNLOAD mode");
        }

        return builder.build();
    }

    // ── Source-type-specific mappings ─────────────────────────────────────────

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
            str(row, "bq_query"),
            parseJsonMap(str(row, "query_params_json"))
        );
    }

    // ── New column mappings ───────────────────────────────────────────────────

    private QueryConfig toQueryConfig(Map<String, Object> row) {
        return new QueryConfig(
            str(row, "bq_query"),
            parseJsonMap(str(row, "query_params_json"))
        );
    }

    private OutputConfig toOutputConfig(Map<String, Object> row) {
        String outputType = str(row, "output_type");
        if (outputType == null || outputType.isBlank()) return null;
        return new OutputConfig(
            outputType,
            str(row, "output_bq_project"),
            str(row, "output_bq_dataset"),
            str(row, "output_bq_table"),
            str(row, "output_gcs_path"),
            str(row, "output_write_mode")
        );
    }

    private List<SourceTransformConfig> toSourceTransforms(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            JsonNode array = JSON.readTree(json);
            if (!array.isArray()) return Collections.emptyList();

            List<SourceTransformConfig> transforms = new ArrayList<>();
            for (JsonNode node : array) {
                String type = node.path("type").asText();
                switch (type.toUpperCase()) {
                    case SourceTransformConfig.GROUP_BY -> {
                        List<String> fields = parseStringList(node.path("groupByFields").toString());
                        List<AggregationConfig> aggs = parseAggregations(node.path("aggregations").toString());
                        transforms.add(SourceTransformConfig.groupBy(fields, aggs));
                    }
                    case SourceTransformConfig.SORT_BY -> {
                        List<String> fields = parseStringList(node.path("sortByFields").toString());
                        boolean desc = node.path("sortDescending").asBoolean(false);
                        transforms.add(SourceTransformConfig.sortBy(fields, desc));
                    }
                    case SourceTransformConfig.LOOKUP -> {
                        LookupConfig lookupCfg = parseLookupConfig(node);
                        transforms.add(SourceTransformConfig.lookup(lookupCfg));
                    }
                    default -> LOG.warn("Unknown source transform type '{}' — skipping", type);
                }
            }
            return transforms;
        } catch (Exception e) {
            LOG.error("Failed to parse source_transforms_json: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private LookupConfig parseLookupConfig(JsonNode node) {
        List<String> outputFields = parseStringList(node.path("lookupOutputFields").toString());
        return new LookupConfig(
            node.path("lookupSourceType").asText(LookupConfig.SOURCE_BQ),
            node.path("lookupBqTableRef").asText(null),
            node.path("lookupJdbcQuery").asText(null),
            node.path("lookupKeyField").asText(),
            node.path("dataKeyField").asText(),
            outputFields
        );
    }

    private List<AggregationConfig> parseAggregations(String json) {
        if (json == null || json.isBlank() || json.equals("null")) return Collections.emptyList();
        try {
            JsonNode array = JSON.readTree(json);
            if (!array.isArray()) return Collections.emptyList();
            List<AggregationConfig> aggs = new ArrayList<>();
            for (JsonNode node : array) {
                aggs.add(new AggregationConfig(
                    node.path("field").asText(),
                    node.path("function").asText(),
                    node.path("outputField").asText()
                ));
            }
            return aggs;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private ValidationConfig toValidationConfig(Map<String, Object> row) {
        long minRows = toLong(row.get("min_row_count"));
        long maxRows = row.get("max_row_count") != null ? toLong(row.get("max_row_count")) : ValidationConfig.NO_MAX;
        List<String> requiredHeaders = parseStringList(str(row, "required_headers_json"));
        List<BncRule> bncRules = parseBncRules(str(row, "bnc_rules_json"));
        return new ValidationConfig(minRows, maxRows, requiredHeaders, bncRules);
    }

    private List<BncRule> parseBncRules(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            JsonNode array = JSON.readTree(json);
            if (!array.isArray()) return Collections.emptyList();
            List<BncRule> rules = new ArrayList<>();
            for (JsonNode node : array) {
                rules.add(new BncRule(
                    node.path("field").asText(),
                    node.path("expectedTotal").asDouble(),
                    node.path("tolerancePct").asDouble(0.01)
                ));
            }
            return rules;
        } catch (Exception e) {
            LOG.error("Failed to parse bnc_rules_json: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    // ── Type helpers ─────────────────────────────────────────────────────────

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
            return JSON.readValue(json, STR_MAP);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    private static List<String> parseStringList(String json) {
        if (json == null || json.isBlank() || json.equals("null")) return Collections.emptyList();
        try {
            return JSON.readValue(json, STR_LIST);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private static String qualifiedTable(String schema, String table) {
        return (schema == null || schema.isBlank()) ? table : schema + "." + table;
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
