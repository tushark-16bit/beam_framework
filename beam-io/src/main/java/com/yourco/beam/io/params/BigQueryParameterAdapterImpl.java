package com.yourco.beam.io.params;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BQ-client implementation of {@link BigQueryParameterAdapter}.
 *
 * <p>Looks up a single row from {@code parameter_store} by
 * ({@code ParameterGroupName}, {@code ParameterDataSource}, {@code ParameterName}),
 * parses {@code ParametersValJson} into a {@code Map<String, String>}, and uses
 * {@code SchemaOfJson} to identify which fields are required.
 *
 * <p>All queries use named parameters ({@code @name}) to prevent SQL injection.
 */
public final class BigQueryParameterAdapterImpl implements BigQueryParameterAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryParameterAdapterImpl.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final BigQuery bigquery;
    private final String storeTable;   // fully-qualified: project.dataset.table

    public BigQueryParameterAdapterImpl(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQueryParameterAdapterImpl(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery = bigquery;
        String project = options.getParamBqProject() != null && !options.getParamBqProject().isBlank()
                         ? options.getParamBqProject() : options.getProject();
        this.storeTable = "`" + project + "." + options.getParamBqDataset()
                        + "." + options.getParamStoreTable() + "`";

        LOG.info("BigQueryParameterAdapter initialised: {}", storeTable);
    }

    // ── Interface ─────────────────────────────────────────────────────────────

    @Override
    public List<String> fetchRequiredKeys(String parameterGroupName, String parameterDataSource,
                                          String parameterName) {
        JsonNode schema = fetchSchemaNode(parameterGroupName, parameterDataSource, parameterName);
        if (schema == null || !schema.isObject()) return Collections.emptyList();

        List<String> required = new ArrayList<>();
        schema.fields().forEachRemaining(entry -> {
            JsonNode spec = entry.getValue();
            if (spec.path("required").asBoolean(false)) {
                required.add(entry.getKey());
            }
        });
        LOG.info("SchemaOfJson declares {} required field(s) for {}/{}/{}",
                 required.size(), parameterGroupName, parameterDataSource, parameterName);
        return required;
    }

    @Override
    public Map<String, String> fetchParameters(String parameterGroupName, String parameterDataSource,
                                               String parameterName) {
        FieldValueList row = fetchRow(parameterGroupName, parameterDataSource, parameterName);
        if (row == null) {
            LOG.warn("No parameter row found for {}/{}/{}", parameterGroupName, parameterDataSource, parameterName);
            return Collections.emptyMap();
        }
        return parseValJson(row, parameterGroupName, parameterDataSource, parameterName);
    }

    @Override
    public Map<String, String> fetchRequiredParameters(String parameterGroupName,
                                                       String parameterDataSource,
                                                       String parameterName) {
        FieldValueList row = fetchRow(parameterGroupName, parameterDataSource, parameterName);
        if (row == null) {
            throw new IllegalStateException(
                "No parameter_store row found for ParameterGroupName=" + parameterGroupName
                + ", ParameterDataSource=" + parameterDataSource
                + ", ParameterName=" + parameterName);
        }

        Map<String, String> params = parseValJson(row, parameterGroupName, parameterDataSource, parameterName);
        List<String> requiredKeys  = parseRequiredKeysFromSchema(row);

        List<String> missing = new ArrayList<>();
        for (String key : requiredKeys) {
            if (!params.containsKey(key) || params.get(key) == null) {
                missing.add(key);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                "Required parameters missing from ParametersValJson for "
                + parameterGroupName + "/" + parameterDataSource + "/" + parameterName
                + ": " + missing);
        }

        LOG.info("Fetched and validated {} parameter(s) for {}/{}/{}",
                 params.size(), parameterGroupName, parameterDataSource, parameterName);
        return params;
    }

    // ── BQ fetch ──────────────────────────────────────────────────────────────

    private FieldValueList fetchRow(String groupName, String dataSource, String paramName) {
        String sql = "SELECT ParametersValJson, SchemaOfJson FROM " + storeTable
            + " WHERE ParameterGroupName = @groupName"
            + "   AND ParameterDataSource = @dataSource"
            + "   AND ParameterName = @paramName"
            + " LIMIT 1";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("groupName",  QueryParameterValue.string(groupName))
            .addNamedParameter("dataSource", QueryParameterValue.string(dataSource))
            .addNamedParameter("paramName",  QueryParameterValue.string(paramName))
            .setUseLegacySql(false)
            .build();

        try {
            for (FieldValueList row : bigquery.query(config).iterateAll()) {
                return row;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("BQ parameter query interrupted", e);
        }
        return null;
    }

    // ── JSON parsing ──────────────────────────────────────────────────────────

    private Map<String, String> parseValJson(FieldValueList row, String groupName,
                                             String dataSource, String paramName) {
        String valJson = row.get("ParametersValJson").isNull()
                         ? null : row.get("ParametersValJson").getStringValue();
        if (valJson == null || valJson.isBlank()) {
            LOG.warn("ParametersValJson is empty for {}/{}/{}", groupName, dataSource, paramName);
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
            LOG.error("Failed to parse ParametersValJson for {}/{}/{}: {}",
                      groupName, dataSource, paramName, e.getMessage());
            return Collections.emptyMap();
        }
    }

    private List<String> parseRequiredKeysFromSchema(FieldValueList row) {
        String schemaJson = row.get("SchemaOfJson").isNull()
                            ? null : row.get("SchemaOfJson").getStringValue();
        if (schemaJson == null || schemaJson.isBlank()) return Collections.emptyList();
        try {
            JsonNode schema = JSON.readTree(schemaJson);
            if (!schema.isObject()) return Collections.emptyList();
            List<String> required = new ArrayList<>();
            schema.fields().forEachRemaining(entry -> {
                if (entry.getValue().path("required").asBoolean(false)) {
                    required.add(entry.getKey());
                }
            });
            return required;
        } catch (Exception e) {
            LOG.error("Failed to parse SchemaOfJson: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private JsonNode fetchSchemaNode(String groupName, String dataSource, String paramName) {
        FieldValueList row = fetchRow(groupName, dataSource, paramName);
        if (row == null) return null;
        String schemaJson = row.get("SchemaOfJson").isNull()
                            ? null : row.get("SchemaOfJson").getStringValue();
        if (schemaJson == null || schemaJson.isBlank()) return null;
        try {
            return JSON.readTree(schemaJson);
        } catch (Exception e) {
            LOG.error("Failed to parse SchemaOfJson: {}", e.getMessage());
            return null;
        }
    }
}
