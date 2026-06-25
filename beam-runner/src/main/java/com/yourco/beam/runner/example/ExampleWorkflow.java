package com.yourco.beam.runner.example;

import com.yourco.beam.io.params.BigQueryParameterAdapter;
import com.yourco.beam.io.params.BigQueryParameterAdapterImpl;
import com.yourco.beam.io.report.BigQueryJobService;
import com.yourco.beam.options.FrameworkOptions;
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
 *   <li><b>Fetch and validate parameters</b> — reads a single row from {@code parameter_store}
 *       by ({@code ParameterGroupName}, {@code ParameterDataSource}, {@code ParameterName}).
 *       Parses {@code SchemaOfJson} to discover required fields; parses {@code ParametersValJson}
 *       for the actual values; throws if any required field is missing.</li>
 *   <li><b>Run the transform</b> — executes a BigQuery query (from the params) using
 *       {@link BigQueryJobService}, materialising the result into a BQ table.</li>
 *   <li><b>Export to GCS</b> — uses a BQ extract job to write the result as a CSV
 *       to the GCS path from the params.</li>
 * </ol>
 *
 * <h2>parameter_store row for this example</h2>
 * <pre>
 * ParameterGroupName  = "TRADING"          ← --parentId (business group)
 * ParameterDataSource = "eod"             ← --reportSubprocess (child)
 * ParameterName       = "daily_trades_summary"  ← --reportName (name)
 * SchemaOfJson        = {
 *   "source_bq_table":       {"required": true,  "type": "string"},
 *   "transform_query":       {"required": true,  "type": "string"},
 *   "transform_output_table":{"required": true,  "type": "string"},
 *   "output_gcs_path":       {"required": true,  "type": "string"},
 *   "output_file_name":      {"required": true,  "type": "string"}
 * }
 * ParametersValJson   = {
 *   "source_bq_table":        "my-project.raw_data.trades",
 *   "transform_query":        "SELECT trade_date, SUM(amount) AS total FROM {source_bq_table} ...",
 *   "transform_output_table": "my-project.reports.daily_trades_summary",
 *   "output_gcs_path":        "gs://my-bucket/reports/",
 *   "output_file_name":       "daily_trades_{periodId}.csv"
 * }
 * </pre>
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
 *     --reportName=daily_trades_summary
 *     --reportSubprocess=eod
 *     --periodId=202401
 *     --periodStart=2024-01-01
 *     --periodEnd=2024-01-31
 *     --parentId=TRADING
 *     --processType=REPORT_PROCESSING"
 * </pre>
 *
 * See {@code EXAMPLE.md} for full DDL and sample INSERT statements.
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

    public void run(FrameworkOptions options) {
        String parentId    = options.getParentId();          // → ParameterGroupName column
        String reportName  = options.getReportName();       // → ParameterName column
        String subprocess  = options.getReportSubprocess(); // → ParameterDataSource column
        String periodId    = options.getPeriodId();
        String periodStart = options.getPeriodStart();
        String periodEnd   = options.getPeriodEnd();

        LOG.info("=== ExampleWorkflow START ===");
        LOG.info("Parent: {} | Report: {} / {} | Period: {} ({} → {})",
                 parentId, reportName, subprocess, periodId, periodStart, periodEnd);

        // ── Step 1: Fetch and validate parameters from BigQuery ───────────────
        //
        // fetchRequiredParameters() in one BQ round-trip:
        //   1. SELECT the row by (ParameterGroupName=parentId, ParameterDataSource=subprocess, ParameterName=reportName)
        //   2. Parse SchemaOfJson to find fields with "required": true
        //   3. Parse ParametersValJson into Map<String, String>
        //   4. Validate all required fields are present — throws if any are missing

        BigQueryParameterAdapter paramAdapter = new BigQueryParameterAdapterImpl(options);
        Map<String, String> params = paramAdapter.fetchRequiredParameters(
            parentId, subprocess, reportName);

        LOG.info("Parameters loaded ({} key(s)):", params.size());
        params.forEach((k, v) -> LOG.info("  {} = {}", k, v));

        // ── Step 2: Extract specific params ──────────────────────────────────
        //
        // These keys are declared as required in SchemaOfJson — if any were null,
        // fetchRequiredParameters() would have already thrown above.

        String sourceBqTable       = require(params, "source_bq_table");
        String transformOutputTable = require(params, "transform_output_table");
        String transformQuery      = require(params, "transform_query");
        String outputGcsPath       = require(params, "output_gcs_path");
        String outputFileName      = require(params, "output_file_name");

        // ── Step 3: Resolve period tokens in the query ────────────────────────
        //
        // Tokens in ParametersValJson values are replaced at runtime.
        // The query may reference {source_bq_table} which is another param value.

        String today = LocalDate.now().toString();
        String resolvedQuery = transformQuery
            .replace("{source_bq_table}",   sourceBqTable)
            .replace("{periodStart}",        periodStart  != null ? periodStart : "")
            .replace("{periodEnd}",          periodEnd    != null ? periodEnd   : "")
            .replace("{periodId}",           periodId     != null ? periodId    : "")
            .replace("{runDate}",            today);

        String resolvedFileName = outputFileName
            .replace("{periodId}", periodId != null ? periodId : "")
            .replace("{runDate}",  today);

        LOG.info("Resolved query → target table: {}", transformOutputTable);
        LOG.info("Output file: {}{}", outputGcsPath, resolvedFileName);

        // ── Step 4: Run BQ transform query → materialise output table ─────────

        BigQueryJobService bqJob = new BigQueryJobService();
        bqJob.runQueryToTable(resolvedQuery, transformOutputTable);
        LOG.info("Transform complete. Output in: {}", transformOutputTable);

        // ── Step 5: Export BQ table → GCS CSV ────────────────────────────────

        String gcsUri = outputGcsPath + resolvedFileName;
        bqJob.exportToCsv(transformOutputTable, gcsUri, true);
        LOG.info("=== ExampleWorkflow DONE. File: {} ===", gcsUri);
    }

    private static String require(Map<String, String> params, String key) {
        String v = params.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Required parameter '" + key + "' is missing or blank");
        }
        return v;
    }
}
