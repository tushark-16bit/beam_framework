package com.yourco.beam.io.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.yourco.beam.model.AggregationConfig;
import com.yourco.beam.model.ApiSourceConfig;
import com.yourco.beam.model.BncRule;
import com.yourco.beam.model.BqFetchConfig;
import com.yourco.beam.model.FileSourceConfig;
import com.yourco.beam.model.LookupConfig;
import com.yourco.beam.model.QueryConfig;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.model.SourceFailureEmailConfig;
import com.yourco.beam.model.SourceSchemaField;
import com.yourco.beam.model.SourceTransformConfig;
import com.yourco.beam.model.ValidationConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fetches source configuration from the {@code parameter_store} BigQuery table.
 *
 * <p>Source configs are stored as JSON in {@code parameters_val_json}, keyed by
 * ({@code parameter_group_name=parentId}, {@code parameter_data_source=subprocessName},
 * {@code parameter_name=datasourceName}). Required fields are declared in {@code schema_of_json}.
 *
 * <p>{@code periodId} is not part of the lookup key — source configs are period-agnostic.
 * Period-specific filtering is applied via {@code {periodStart}}/{@code {periodEnd}} token
 * substitution inside the query at runtime.
 *
 * <h2>parameters_val_json structure</h2>
 * <pre>
 * {
 *   "source_type":              "BQ" | "API" | "FILE",
 *   // BQ source
 *   "bq_project_id":            "my-project",
 *   "bq_dataset":               "raw_data",
 *   "bq_table":                 "trades",
 *   "bq_query":                 "SELECT * FROM ... WHERE trade_date BETWEEN '{periodStart}' ...",
 *   "query_params_json":        "{\"key\": \"value\"}",
 *   "bq_schema_json":           "[{\"name\":\"trade_id\",\"type\":\"STRING\"},
 *                                 {\"name\":\"amount\",\"type\":\"FLOAT64\"},
 *                                 {\"name\":\"trade_date\",\"type\":\"DATE\"}]",
 *   // API source
 *   "api_endpoint":             "https://api.example.com/v1/data",
 *   "api_auth_type":            "BEARER",
 *   "api_auth_secret_id":       "projects/.../secrets/.../versions/latest",
 *   "api_headers_json":         "{\"Accept\": \"application/json\"}",
 *   "api_query_params_json":    "{}",
 *   "api_pagination_enabled":   "true",
 *   "api_pagination_strategy":  "PAGE_NUMBER",
 *   "api_page_size":            "100",
 *   "api_next_page_field":      "nextPage",
 *   "api_data_array_field":     "data",
 *   // FILE source
 *   "file_type":                "CSV",
 *   "file_location":            "gs://bucket/files/",
 *   "file_prefix":              "trades_",
 *   "file_suffix":              ".csv",
 *   "file_delimiter":           ",",
 *   "file_has_header":          "true",
 *   "file_sheet_index":         "0",
 *   // Transforms + validation
 *   "source_transforms_json":   "[{\"type\":\"GROUP_BY\", ...}]",
 *   "min_row_count":            "1",
 *   "max_row_count":            "100000",
 *   "required_headers_json":    "[\"trade_id\",\"amount\"]",
 *   "bnc_rules_json":           "[{\"field\":\"amount\",\"expectedTotal\":5000000}]"
 * }
 * </pre>
 *
 * <h2>schema_of_json structure</h2>
 * <pre>
 * {
 *   "source_type": {"required": true, "type": "string"},
 *   "bq_query":    {"required": true, "type": "string"}
 * }
 * </pre>
 *
 * <h2>bq_schema_json — explicit column schema (optional, BQ sources only)</h2>
 * <p>A JSON array of {@code {"name": "<column>", "type": "<BQ SQL type>"}} objects. When
 * present, {@code DataSourcePipelineFactory.fetchBqSchema()} uses it directly instead of
 * calling {@code BigQuerySchemaUtils.fetchBeamSchema()} — no {@code bigquery.tables.get}
 * permission is needed, and the fetched rows are converted strictly against the declared
 * types (see {@link com.yourco.beam.model.SourceSchemaField}). {@code type} must be one of
 * the real BigQuery SQL type names: {@code STRING}, {@code INT64}, {@code FLOAT64},
 * {@code BOOLEAN}, {@code BYTES}, {@code DATE}, {@code DATETIME}, {@code TIME},
 * {@code TIMESTAMP}, {@code NUMERIC}, {@code BIGNUMERIC}. When absent, schema resolution
 * falls back to the existing behaviour
 * (metadata fetch, then a name-only preview-query fallback).
 *
 * <p>All queries use named BQ parameters ({@code @name}) to prevent injection.
 */
public final class BigQuerySourceConfigRepository {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceConfigRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, String>> STR_MAP  = new TypeReference<>() {};
    private static final TypeReference<List<String>>        STR_LIST = new TypeReference<>() {};

    private final BigQuery bigquery;
    private final String storeTable; // parameter_store fully-qualified: `project.dataset.table`

    public BigQuerySourceConfigRepository(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQuerySourceConfigRepository(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery = bigquery;
        String project = options.getParamBqProject() != null && !options.getParamBqProject().isBlank()
                         ? options.getParamBqProject() : options.getProject();
        this.storeTable = "`" + project + "." + options.getParamBqDataset()
                        + "." + options.getParamStoreTable() + "`";
    }

    // ── Config fetch ──────────────────────────────────────────────────────────

    /**
     * Fetches and parses the source config from {@code parameter_store}.
     *
     * <p>{@code periodId} is passed into the resulting {@link SourceConfig} for
     * checkpoint keying but is not used to filter the {@code parameter_store} row.
     */
    public List<SourceConfig> fetchSourceConfigs(String parentId, String datasource,
                                                   String subprocess, int periodId) {
        String sql = "SELECT parameters_val_json, schema_of_json FROM " + storeTable
            + " WHERE parameter_group_name = @parentId"
            + "   AND parameter_data_source = @subprocess"
            + "   AND parameter_name = @datasource"
            + " LIMIT 1";

        try {
            for (FieldValueList row : bigquery.query(
                    qConfig(sql, parentId, datasource, subprocess)).iterateAll()) {
                Map<String, String> params = parseValJson(row, parentId, datasource, subprocess);
                validateRequiredFields(row, params, parentId, datasource, subprocess);
                SourceConfig config = mapToSourceConfig(params, parentId, datasource, subprocess, periodId);
                LOG.info("Fetched source config for parent={}, datasource={}, subprocess={}, period={}",
                         parentId, datasource, subprocess, periodId);
                return List.of(config);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ query interrupted", e);
        }

        throw new IllegalStateException(
            "No parameter_store row found for parent=" + parentId
            + ", datasource=" + datasource + ", subprocess=" + subprocess);
    }

    // ── Row mapping ───────────────────────────────────────────────────────────

    private SourceConfig mapToSourceConfig(Map<String, String> params, String parentId,
                                            String datasource, String subprocess, int periodId) {
        String sourceTypeStr = params.get("source_type");
        if (sourceTypeStr == null || sourceTypeStr.isBlank()) {
            throw new IllegalStateException(
                "source_type is missing in parameters_val_json for "
                + parentId + "/" + subprocess + "/" + datasource);
        }

        SourceType sourceType;
        try {
            sourceType = SourceType.valueOf(sourceTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                "Unknown source_type '" + sourceTypeStr + "' in parameter_store for "
                + parentId + "/" + subprocess + "/" + datasource);
        }

        SourceConfig.Builder builder = SourceConfig.builder()
            .parentId(parentId)
            .datasourceName(datasource)
            .periodId(periodId)
            .subprocessName(subprocess)
            .sourceType(sourceType)
            .queryConfig(toQueryConfig(params))
            .sourceTransforms(toSourceTransforms(params.get("source_transforms_json")))
            .validationConfig(toValidationConfig(params))
            .failureEmailConfig(toFailureEmailConfig(params));

        switch (sourceType) {
            case API  -> builder.apiConfig(toApiConfig(params));
            case FILE -> builder.fileConfig(toFileConfig(params));
            case BQ   -> builder.bqFetchConfig(toBqConfig(params));
            default   -> throw new IllegalStateException(
                "source_type " + sourceType + " is not supported in DATA_SOURCE_DOWNLOAD");
        }

        return builder.build();
    }

    private ApiSourceConfig toApiConfig(Map<String, String> p) {
        return new ApiSourceConfig(
            p.get("api_endpoint"),
            p.get("api_auth_type"),
            p.get("api_auth_secret_id"),
            parseJsonMap(p.get("api_headers_json")),
            parseJsonMap(p.get("api_query_params_json")),
            parseBool(p.get("api_pagination_enabled"), false),
            p.get("api_pagination_strategy"),
            parseIntOrDefault(p.get("api_page_size"), 100),
            p.get("api_next_page_field"),
            p.get("api_data_array_field")
        );
    }

    private FileSourceConfig toFileConfig(Map<String, String> p) {
        return new FileSourceConfig(
            p.get("file_type"),
            p.get("file_location"),
            p.get("file_prefix"),
            p.get("file_suffix"),
            p.get("file_delimiter"),
            parseBool(p.get("file_has_header"), false),
            parseIntOrDefault(p.get("file_sheet_index"), 0)
        );
    }

    private BqFetchConfig toBqConfig(Map<String, String> p) {
        return new BqFetchConfig(
            p.get("bq_project_id"),
            p.get("bq_dataset"),
            p.get("bq_table"),
            p.get("bq_query"),
            parseJsonMap(p.get("query_params_json")),
            parseSchemaFields(p.get("bq_schema_json"))
        );
    }

    /**
     * Parses {@code bq_schema_json} into an ordered list of {@link SourceSchemaField}.
     * Entries missing {@code name} or {@code type} are skipped with a warning rather than
     * failing the whole config fetch — a malformed schema entry surfaces later, and more
     * clearly, when {@code BigQuerySchemaUtils.toBeamSchema()} rejects an unrecognised type.
     */
    private List<SourceSchemaField> parseSchemaFields(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            JsonNode array = JSON.readTree(json);
            if (!array.isArray()) return Collections.emptyList();
            List<SourceSchemaField> fields = new ArrayList<>();
            for (JsonNode node : array) {
                String name = node.path("name").asText(null);
                String type = node.path("type").asText(null);
                if (name == null || name.isBlank() || type == null || type.isBlank()) {
                    LOG.warn("Skipping malformed bq_schema_json entry (missing name/type): {}", node);
                    continue;
                }
                fields.add(new SourceSchemaField(name, type));
            }
            return fields;
        } catch (Exception e) {
            LOG.error("Failed to parse bq_schema_json: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private QueryConfig toQueryConfig(Map<String, String> p) {
        return new QueryConfig(
            p.get("bq_query"),
            parseJsonMap(p.get("query_params_json"))
        );
    }

    private ValidationConfig toValidationConfig(Map<String, String> p) {
        long minRows = parseLongOrDefault(p.get("min_row_count"), 0L);
        long maxRows = (p.containsKey("max_row_count") && p.get("max_row_count") != null)
                       ? parseLongOrDefault(p.get("max_row_count"), ValidationConfig.NO_MAX)
                       : ValidationConfig.NO_MAX;
        List<String>  requiredHeaders = parseStringList(p.get("required_headers_json"));
        List<BncRule> bncRules        = parseBncRules(p.get("bnc_rules_json"));
        return new ValidationConfig(minRows, maxRows, requiredHeaders, bncRules);
    }

    private SourceFailureEmailConfig toFailureEmailConfig(Map<String, String> p) {
        List<String> toList = splitCsv(p.get("failure_email_to"));
        if (toList.isEmpty()) return null;
        return new SourceFailureEmailConfig(
            toList,
            splitCsv(p.get("failure_email_cc")),
            p.getOrDefault("failure_email_subject",
                "FAILED: {datasourceName} download for period {periodId}"),
            p.getOrDefault("failure_email_body",
                "Data source download failed.\n\n"
                + "Datasource : {datasourceName}\n"
                + "Period     : {periodId}\n"
                + "Status     : {staCd}\n\n"
                + "Error:\n{errorMessage}\n\n"
                + "BnC Summary:\n{bncSummary}"),
            p.getOrDefault("email_smtp_host", "smtp.gmail.com"),
            parseIntOrDefault(p.get("email_smtp_port"), 587),
            p.get("smtp_password_secret_id"),
            p.get("from_address")
        );
    }

    private static List<String> splitCsv(String value) {
        if (value == null || value.isBlank()) return Collections.emptyList();
        List<String> result = new ArrayList<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) result.add(trimmed);
        }
        return result;
    }

    // ── Schema validation ─────────────────────────────────────────────────────

    private void validateRequiredFields(FieldValueList row, Map<String, String> params,
                                         String parentId, String datasource, String subprocess) {
        String schemaJson = row.get("schema_of_json").isNull()
                            ? null : row.get("schema_of_json").getStringValue();
        if (schemaJson == null || schemaJson.isBlank()) return;

        try {
            JsonNode schema = JSON.readTree(schemaJson);
            if (!schema.isObject()) return;

            List<String> missing = new ArrayList<>();
            schema.fields().forEachRemaining(entry -> {
                if (entry.getValue().path("required").asBoolean(false)) {
                    String key = entry.getKey();
                    String val = params.get(key);
                    if (val == null || val.isBlank()) {
                        missing.add(key);
                    }
                }
            });

            if (!missing.isEmpty()) {
                throw new IllegalStateException(
                    "Required source config fields missing in parameters_val_json for "
                    + parentId + "/" + subprocess + "/" + datasource + ": " + missing);
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Failed to parse schema_of_json for source config {}/{}/{}: {}",
                     parentId, subprocess, datasource, e.getMessage());
        }
    }

    // ── JSON / type parsing ───────────────────────────────────────────────────

    private Map<String, String> parseValJson(FieldValueList row, String parentId,
                                              String datasource, String subprocess) {
        String valJson = row.get("parameters_val_json").isNull()
                         ? null : row.get("parameters_val_json").getStringValue();
        if (valJson == null || valJson.isBlank()) {
            LOG.warn("parameters_val_json is empty for source config {}/{}/{}", parentId, subprocess, datasource);
            return Collections.emptyMap();
        }
        try {
            JsonNode node = JSON.readTree(valJson);
            if (!node.isObject()) return Collections.emptyMap();
            Map<String, String> result = new HashMap<>();
            node.fields().forEachRemaining(e ->
                result.put(e.getKey(), e.getValue().isNull() ? null : e.getValue().asText()));
            return result;
        } catch (Exception e) {
            throw new IllegalStateException(
                "Failed to parse parameters_val_json for source config "
                + parentId + "/" + subprocess + "/" + datasource + ": " + e.getMessage(), e);
        }
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
                        List<String>           fields = parseStringList(node.path("groupByFields").toString());
                        List<AggregationConfig> aggs  = parseAggregations(node.path("aggregations").toString());
                        transforms.add(SourceTransformConfig.groupBy(fields, aggs));
                    }
                    case SourceTransformConfig.SORT_BY -> {
                        List<String> fields = parseStringList(node.path("sortByFields").toString());
                        boolean desc = node.path("sortDescending").asBoolean(false);
                        transforms.add(SourceTransformConfig.sortBy(fields, desc));
                    }
                    case SourceTransformConfig.LOOKUP ->
                        transforms.add(SourceTransformConfig.lookup(parseLookupConfig(node)));
                    default ->
                        LOG.warn("Unknown source transform type '{}' — skipping", type);
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
            node.path("lookupBqTableRef").asText(),
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

    // ── Type coercion helpers ─────────────────────────────────────────────────

    private static boolean parseBool(String value, boolean defaultValue) {
        if (value == null || value.isBlank()) return defaultValue;
        return Boolean.parseBoolean(value.trim());
    }

    private static int parseIntOrDefault(String value, int defaultValue) {
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long parseLongOrDefault(String value, long defaultValue) {
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
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

    private static QueryJobConfiguration qConfig(String sql, String parentId,
                                                   String datasource, String subprocess) {
        return QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("parentId",   QueryParameterValue.string(parentId))
            .addNamedParameter("datasource", QueryParameterValue.string(datasource))
            .addNamedParameter("subprocess", QueryParameterValue.string(subprocess))
            .setUseLegacySql(false)
            .build();
    }
}
