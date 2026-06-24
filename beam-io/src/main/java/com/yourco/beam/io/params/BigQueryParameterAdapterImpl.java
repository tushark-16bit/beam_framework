package com.yourco.beam.io.params;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BQ-client implementation of {@link BigQueryParameterAdapter}.
 *
 * <p>All queries use named parameters ({@code @paramName}) to prevent SQL injection.
 * Parameter fetches run as interactive BQ queries (not Dataflow jobs) since they
 * are small and latency-sensitive.
 *
 * <p>Table references are fully-qualified: {@code `project.dataset.table`}.
 * Both the project and dataset are read from {@link FrameworkOptions} at construction time.
 *
 * <h2>Authentication</h2>
 * Uses Application Default Credentials. Locally: run
 * {@code gcloud auth application-default login}. On GCP: uses the Compute/Dataflow
 * service account automatically.
 */
public final class BigQueryParameterAdapterImpl implements BigQueryParameterAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryParameterAdapterImpl.class);

    private final BigQuery bigquery;
    private final String project;
    private final String dataset;
    private final String storeTable;     // e.g. "parameter_store"
    private final String requiredTable;  // e.g. "required_parameters_index"

    public BigQueryParameterAdapterImpl(FrameworkOptions options) {
        this(BigQueryOptions.getDefaultInstance().getService(), options);
    }

    BigQueryParameterAdapterImpl(BigQuery bigquery, FrameworkOptions options) {
        this.bigquery      = bigquery;
        this.project       = options.getParamBqProject() != null
                             ? options.getParamBqProject()
                             : options.getProject();
        this.dataset       = options.getParamBqDataset();
        this.storeTable    = options.getParamStoreTable();
        this.requiredTable = options.getParamRequiredTable();

        LOG.info("BigQueryParameterAdapter initialised: {}.{} (store={}, required={})",
                 project, dataset, storeTable, requiredTable);
    }

    // ── Interface implementation ──────────────────────────────────────────────

    @Override
    public List<String> fetchRequiredKeys(String processName, String subprocess) {
        LOG.info("Fetching required param keys for process={} subprocess={}", processName, subprocess);
        String sql = "SELECT param_key FROM `" + ref(requiredTable) + "` "
                   + "WHERE process_name = @processName "
                   + "  AND subprocess_name = @subprocess "
                   + "  AND is_required = TRUE";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("processName", QueryParameterValue.string(processName))
            .addNamedParameter("subprocess",  QueryParameterValue.string(subprocess))
            .setUseLegacySql(false)
            .build();

        List<String> keys = new ArrayList<>();
        for (FieldValueList row : runQuery(config).iterateAll()) {
            keys.add(row.get("param_key").getStringValue());
        }
        LOG.info("Found {} required key(s) for {}/{}", keys.size(), processName, subprocess);
        return keys;
    }

    @Override
    public Map<String, String> fetchParameters(String processName, String subprocess,
                                               String periodId) {
        LOG.info("Fetching all params for process={} subprocess={} period={}",
                 processName, subprocess, periodId);
        String sql = "SELECT param_key, param_value FROM `" + ref(storeTable) + "` "
                   + "WHERE process_name = @processName "
                   + "  AND subprocess_name = @subprocess "
                   + "  AND period_id = @periodId";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("processName", QueryParameterValue.string(processName))
            .addNamedParameter("subprocess",  QueryParameterValue.string(subprocess))
            .addNamedParameter("periodId",    QueryParameterValue.string(periodId))
            .setUseLegacySql(false)
            .build();

        return collectKeyValues(config, processName, subprocess, periodId);
    }

    @Override
    public Map<String, String> fetchParameters(String processName, String subprocess,
                                               String periodId, List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return new HashMap<>();
        }
        LOG.info("Fetching {} specific param(s) for process={} subprocess={} period={}",
                 keys.size(), processName, subprocess, periodId);

        // IN UNNEST(@keys) lets BQ handle a list param safely without string concatenation
        String sql = "SELECT param_key, param_value FROM `" + ref(storeTable) + "` "
                   + "WHERE process_name = @processName "
                   + "  AND subprocess_name = @subprocess "
                   + "  AND period_id = @periodId "
                   + "  AND param_key IN UNNEST(@keys)";

        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
            .addNamedParameter("processName", QueryParameterValue.string(processName))
            .addNamedParameter("subprocess",  QueryParameterValue.string(subprocess))
            .addNamedParameter("periodId",    QueryParameterValue.string(periodId))
            .addNamedParameter("keys",        QueryParameterValue.array(
                                                 keys.toArray(new String[0]), String.class))
            .setUseLegacySql(false)
            .build();

        return collectKeyValues(config, processName, subprocess, periodId);
    }

    @Override
    public Map<String, String> fetchRequiredParameters(String processName, String subprocess,
                                                       String periodId) {
        List<String> required = fetchRequiredKeys(processName, subprocess);
        if (required.isEmpty()) {
            LOG.warn("No required params registered for {}/{} — fetching all params", processName, subprocess);
            return fetchParameters(processName, subprocess, periodId);
        }

        Map<String, String> params = fetchParameters(processName, subprocess, periodId, required);

        // Validate all required keys are present
        List<String> missing = new ArrayList<>();
        for (String key : required) {
            if (!params.containsKey(key)) {
                missing.add(key);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                "Required parameters missing from parameter_store for "
                + processName + "/" + subprocess + "/" + periodId + ": " + missing);
        }
        return params;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Returns the fully-qualified BQ table reference: {@code project.dataset.table} */
    private String ref(String table) {
        return project + "." + dataset + "." + table;
    }

    private TableResult runQuery(QueryJobConfiguration config) {
        try {
            return bigquery.query(config);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BQ parameter query interrupted", e);
        }
    }

    private Map<String, String> collectKeyValues(QueryJobConfiguration config,
                                                  String processName, String subprocess,
                                                  String periodId) {
        Map<String, String> result = new HashMap<>();
        for (FieldValueList row : runQuery(config).iterateAll()) {
            String key   = row.get("param_key").getStringValue();
            String value = row.get("param_value").isNull() ? null
                           : row.get("param_value").getStringValue();
            result.put(key, value);
        }
        LOG.info("Loaded {} param(s) for {}/{}/{}", result.size(), processName, subprocess, periodId);
        return result;
    }
}
