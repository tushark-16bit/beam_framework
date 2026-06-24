package com.yourco.beam.runner.example;

import com.yourco.beam.io.config.BigQueryReportRepository;
import com.yourco.beam.io.params.BigQueryParameterAdapter;
import com.yourco.beam.io.params.BigQueryParameterAdapterImpl;
import com.yourco.beam.io.report.BigQueryJobService;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.ProcessType;
import com.yourco.beam.options.SinkType;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Map;

/**
 * End-to-end example showing the BigQuery parameter-driven report workflow.
 *
 * <h2>What this example does</h2>
 * <ol>
 *   <li><b>Fetch required keys</b> — reads {@code required_parameters_index} to discover
 *       which param_keys this report needs (no hard-coded key names in code).</li>
 *   <li><b>Fetch param values</b> — reads {@code parameter_store} to get the actual values
 *       for those keys for this specific {@code (reportName, subprocess, periodId)}.</li>
 *   <li><b>Read data source config</b> — uses {@link BigQueryReportRepository} to look up
 *       which BQ table holds the raw data for the required datasource.</li>
 *   <li><b>Run the transform</b> — executes a BigQuery query (from the params) using
 *       {@link BigQueryJobService}, materialising the result into a BQ table.</li>
 *   <li><b>Export to GCS</b> — uses a BQ extract job to write the result as a CSV
 *       to the GCS path from the params.</li>
 * </ol>
 *
 * <h2>BQ setup (run once before the example)</h2>
 * See {@code EXAMPLE.md} at the project root for:
 * <ul>
 *   <li>DDL to create {@code parameter_store} and {@code required_parameters_index}</li>
 *   <li>Sample INSERT statements for a trades-summary report</li>
 *   <li>DDL for the raw source table ({@code raw_trades})</li>
 * </ul>
 *
 * <h2>How to run</h2>
 * <pre>
 * mvn -pl beam-runner exec:java \
 *   -Dexec.mainClass=com.yourco.beam.runner.example.ExampleWorkflow \
 *   -Dexec.args="
 *     --project=my-gcp-project
 *     --paramBqProject=my-gcp-project
 *     --paramBqDataset=pipeline_config
 *     --paramStoreTable=parameter_store
 *     --paramRequiredTable=required_parameters_index
 *     --reportName=daily_trades_summary
 *     --reportSubprocess=eod
 *     --periodId=2024-01
 *     --periodStart=2024-01-01
 *     --periodEnd=2024-01-31
 *     --processType=REPORT_PROCESSING
 *     --sinkType=GCS"
 * </pre>
 */
public final class ExampleWorkflow {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleWorkflow.class);

    public static void main(String[] args) {
        FrameworkOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(FrameworkOptions.class);

        new ExampleWorkflow().run(options);
    }

    /**
     * Runs the full example workflow.  Call this from {@link #main} or from tests
     * with a pre-built {@link FrameworkOptions}.
     */
    public void run(FrameworkOptions options) {
        String reportName  = options.getReportName();
        String subprocess  = options.getReportSubprocess();
        String periodId    = options.getPeriodId();
        String periodStart = options.getPeriodStart();
        String periodEnd   = options.getPeriodEnd();

        LOG.info("=== ExampleWorkflow START ===");
        LOG.info("Report: {} / {} | Period: {} ({} → {})",
                 reportName, subprocess, periodId, periodStart, periodEnd);

        // ── Step 1: Fetch parameters from BigQuery ────────────────────────────
        //
        // BigQueryParameterAdapter wraps two BQ tables:
        //   required_parameters_index  — which keys are mandatory for this report
        //   parameter_store            — actual key-value rows for this period
        //
        // fetchRequiredParameters() does both in sequence and throws if any key is missing.

        BigQueryParameterAdapter paramAdapter = new BigQueryParameterAdapterImpl(options);
        Map<String, String> params = paramAdapter.fetchRequiredParameters(
            reportName, subprocess, periodId);

        LOG.info("Parameters loaded ({} key(s)):", params.size());
        params.forEach((k, v) -> LOG.info("  {} = {}", k, v));

        // ── Step 2: Extract specific params ──────────────────────────────────
        //
        // These keys are defined in required_parameters_index — the code doesn't
        // hard-code what's required; it just reads whatever the index says is needed.
        // For this example the index registers these five keys:

        String sourceBqTable      = require(params, "source_bq_table");
        String transformOutputTable = require(params, "transform_output_table");
        String transformQuery     = require(params, "transform_query");
        String outputGcsPath      = require(params, "output_gcs_path");
        String outputFileName     = require(params, "output_file_name");

        LOG.info("Source BQ table     : {}", sourceBqTable);
        LOG.info("Transform output    : {}", transformOutputTable);
        LOG.info("GCS output path     : {}", outputGcsPath);

        // ── Step 3: Resolve period tokens in the query ────────────────────────
        //
        // The query stored in parameter_store may contain {periodStart}, {periodEnd},
        // etc.  We substitute them before sending the SQL to BigQuery.

        String resolvedQuery = transformQuery
            .replace("{periodStart}", periodStart != null ? periodStart : "")
            .replace("{periodEnd}",   periodEnd   != null ? periodEnd   : "")
            .replace("{periodId}",    periodId    != null ? periodId    : "")
            .replace("{runDate}",     LocalDate.now().toString());

        LOG.info("Resolved SQL:\n{}", resolvedQuery);

        // ── Step 4: Run the transform query → BQ table ────────────────────────
        //
        // BigQueryJobService runs a standard BQ query job synchronously.
        // The result is materialised to transformOutputTable (WRITE_TRUNCATE).

        BigQueryJobService bqJobs = new BigQueryJobService();
        LOG.info("Running transform query → {}", transformOutputTable);
        bqJobs.runQueryToTable(resolvedQuery, transformOutputTable);
        LOG.info("Transform complete");

        // ── Step 5: Export the result table to GCS as CSV ────────────────────
        //
        // BigQueryJobService.exportToCsv() submits a BQ extract job:
        //   source  = transformOutputTable (the BQ table written in step 4)
        //   dest    = gs://<bucket>/<prefix>/<filename>
        // includeHeader=true writes column names on the first row.

        String gcsDir = outputGcsPath.endsWith("/") ? outputGcsPath : outputGcsPath + "/";
        String gcsUri = gcsDir + outputFileName;

        LOG.info("Exporting {} → {}", transformOutputTable, gcsUri);
        bqJobs.exportToCsv(transformOutputTable, gcsUri, true);
        LOG.info("Export complete: {}", gcsUri);

        LOG.info("=== ExampleWorkflow DONE — output at {} ===", gcsUri);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static String require(Map<String, String> params, String key) {
        String value = params.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                "Required param '" + key + "' is missing or blank in parameter_store");
        }
        return value;
    }
}
