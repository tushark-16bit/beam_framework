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
 * Fetches source configuration from the {@code source_config} BigQuery table.
 *
 * <p>All source configuration lives in the BQ dataset specified by
 * {@code --paramBqProject} and {@code --paramBqDataset}.
 *
 * <h2>Required BQ table</h2>
 * {@code source_config} — one row per (parent_id, subprocess_name, datasource_name, period_id).
 * Three-identifier key: {@code parent_id} (--parentId), {@code subprocess_name} (--subprocessName),
 * {@code datasource_name} (--datasourceName). Also filtered by {@code period_id} (--periodId, from MSTR_Per).
 * Table name configured via {@code --paramSourceConfigTable} (default: {@code source_config}).
 *
 * <p>All queries use named BQ parameters ({@code @name}) to prevent injection.
 */
public final class BigQuerySourceConfigRepository {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceConfigRepository.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, String>> STR_MAP  = new TypeReference<>() {};
    private static final TypeReference<List<String>>        STR_LIST = new TypeReference<>() {};

    private final BigQuery bigquery;
    private final String sourceConfigTable;

    public BigQuerySourceConfigRepository(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQuerySourceConfigRepository(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery = bigquery;
        String project = options.getParamBqProject() != null && !options.getParamBqProject().isBlank()
                         ? options.getParamBqProject() : options.getProject();
        String dataset = options.getParamBqDataset();
        this.sourceConfigTable = project + "." + dataset + "." + options.getParamSourceConfigTable();
    }

    // ── Validation ────────────────────────────────────────────────────────────

    /**
     * Returns a non-empty list if no {@code source_config} row exists for this
     * (datasource, period, subprocess) combination. Empty list means the config is present.
     *
     * <p>Required-field validation for runtime parameters is handled separately by
     * {@code BigQueryParameterAdapter.fetchRequiredParameters()}, which reads
     * {@code SchemaOfJson} from the {@code parameter_store} table.
     */
    public List<String> getMissingParameters(String parentId, String datasource,
                                              String subprocess, String periodId) {
        String sql = "SELECT COUNT(*) AS cnt FROM `" + sourceConfigTable + "`"
            + " WHERE parent_id = @parentId AND datasource_name = @datasource"
            + " AND subprocess_name = @subprocess AND period_id = @periodId";

        try {
            for (FieldValueList row : bigquery.query(
                    qConfig(sql, parentId, datasource, subprocess, periodId)).iterateAll()) {
                if (row.get("cnt").getLongValue() == 0) {
                    LOG.warn("No source_config row for parent={}, datasource={}, subprocess={}, period={}",
                             parentId, datasource, subprocess, periodId);
                    return List.of("source_config row missing for ("
                        + parentId + ", " + datasource + ", " + subprocess + ", " + periodId + ")");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ query interrupted", e);
        }
        return Collections.emptyList();
    }

    // ── Config fetch ──────────────────────────────────────────────────────────

    public List<SourceConfig> fetchSourceConfigs(String parentId, String datasource,
                                                   String subprocess, String periodId) {
        String sql = "SELECT * FROM `" + sourceConfigTable + "`"
            + " WHERE parent_id = @parentId AND datasource_name = @datasource"
            + " AND subprocess_name = @subprocess AND period_id = @periodId";

        List<SourceConfig> configs = new ArrayList<>();
        try {
            for (FieldValueList row : bigquery.query(
                    qConfig(sql, parentId, datasource, subprocess, periodId)).iterateAll()) {
                configs.add(rowToSourceConfig(row));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ query interrupted", e);
        }

        if (configs.isEmpty()) {
            throw new IllegalStateException(
                "No source_config found for parent=" + parentId + ", datasource=" + datasource
                + ", subprocess=" + subprocess + ", period=" + periodId);
        }
        LOG.info("Fetched {} source config(s) for parent={}, datasource={}, subprocess={}, period={}",
                 configs.size(), parentId, datasource, subprocess, periodId);
        return configs;
    }

    // ── Row mapping ───────────────────────────────────────────────────────────

    private SourceConfig rowToSourceConfig(FieldValueList row) {
        String parentId       = str(row, "parent_id");
        String datasourceName = str(row, "datasource_name");
        String periodId       = str(row, "period_id");
        String subprocessName = str(row, "subprocess_name");
        String sourceTypeStr  = str(row, "source_type");

        SourceType sourceType;
        try {
            sourceType = SourceType.valueOf(sourceTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unknown source_type '" + sourceTypeStr + "' in source_config");
        }

        SourceConfig.Builder builder = SourceConfig.builder()
            .parentId(parentId)
            .datasourceName(datasourceName)
            .periodId(periodId)
            .subprocessName(subprocessName)
            .sourceType(sourceType)
            .queryConfig(toQueryConfig(row))
            .sourceTransforms(toSourceTransforms(str(row, "source_transforms_json")))
            .validationConfig(toValidationConfig(row));

        switch (sourceType) {
            case API  -> builder.apiConfig(toApiConfig(row));
            case FILE -> builder.fileConfig(toFileConfig(row));
            case BQ   -> builder.bqFetchConfig(toBqConfig(row));
            default   -> throw new IllegalStateException(
                "source_type " + sourceType + " is not supported in DATA_SOURCE_DOWNLOAD");
        }

        return builder.build();
    }

    private ApiSourceConfig toApiConfig(FieldValueList row) {
        return new ApiSourceConfig(
            str(row, "api_endpoint"),
            str(row, "api_auth_type"),
            str(row, "api_auth_secret_id"),
            parseJsonMap(str(row, "api_headers_json")),
            parseJsonMap(str(row, "api_query_params_json")),
            bool(row, "api_pagination_enabled"),
            str(row, "api_pagination_strategy"),
            toInt(row, "api_page_size", 100),
            str(row, "api_next_page_field"),
            str(row, "api_data_array_field")
        );
    }

    private FileSourceConfig toFileConfig(FieldValueList row) {
        return new FileSourceConfig(
            str(row, "file_type"),
            str(row, "file_location"),
            str(row, "file_prefix"),
            str(row, "file_suffix"),
            str(row, "file_delimiter"),
            bool(row, "file_has_header"),
            toInt(row, "file_sheet_index", 0)
        );
    }

    private BqFetchConfig toBqConfig(FieldValueList row) {
        return new BqFetchConfig(
            str(row, "bq_project_id"),
            str(row, "bq_dataset"),
            str(row, "bq_table"),
            str(row, "bq_query"),
            parseJsonMap(str(row, "query_params_json"))
        );
    }

    private QueryConfig toQueryConfig(FieldValueList row) {
        return new QueryConfig(
            str(row, "bq_query"),
            parseJsonMap(str(row, "query_params_json"))
        );
    }

    private ValidationConfig toValidationConfig(FieldValueList row) {
        long minRows  = toLong(row, "min_row_count");
        long maxRows  = row.get("max_row_count").isNull()
                        ? ValidationConfig.NO_MAX
                        : row.get("max_row_count").getLongValue();
        List<String> requiredHeaders = parseStringList(str(row, "required_headers_json"));
        List<BncRule> bncRules       = parseBncRules(str(row, "bnc_rules_json"));
        return new ValidationConfig(minRows, maxRows, requiredHeaders, bncRules);
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
                        transforms.add(SourceTransformConfig.lookup(parseLookupConfig(node)));
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

    // ── Type helpers ──────────────────────────────────────────────────────────

    private static String str(FieldValueList row, String col) {
        try {
            var fv = row.get(col);
            return fv.isNull() ? null : fv.getStringValue();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static boolean bool(FieldValueList row, String col) {
        try {
            var fv = row.get(col);
            return !fv.isNull() && fv.getBooleanValue();
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static int toInt(FieldValueList row, String col, int defaultValue) {
        try {
            var fv = row.get(col);
            return fv.isNull() ? defaultValue : (int) fv.getLongValue();
        } catch (IllegalArgumentException e) {
            return defaultValue;
        }
    }

    private static long toLong(FieldValueList row, String col) {
        try {
            var fv = row.get(col);
            return fv.isNull() ? 0L : fv.getLongValue();
        } catch (IllegalArgumentException e) {
            return 0L;
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

    private static QueryJobConfiguration qConfig(String sql, String parentId, String datasource,
                                                  String subprocess, String periodId) {
        return QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("parentId",   QueryParameterValue.string(parentId))
            .addNamedParameter("datasource", QueryParameterValue.string(datasource))
            .addNamedParameter("subprocess", QueryParameterValue.string(subprocess))
            .addNamedParameter("periodId",   QueryParameterValue.string(periodId))
            .setUseLegacySql(false)
            .build();
    }
}
