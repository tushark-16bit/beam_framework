package com.yourco.beam.runner;

import com.yourco.beam.io.report.BigQueryJobService;
import com.yourco.beam.model.ReportConfig;
import com.yourco.beam.model.ReportOutputConfig;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.options.ReportOutputSinkType;
import com.yourco.beam.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;

/**
 * Routes a single {@link ReportOutputConfig} to the right sink (GCS / BQ / API).
 *
 * <p>Called from {@link ReportPipelineFactory} after the transformation chain completes.
 * All methods run in the driver JVM — never inside a DoFn.
 */
public final class ReportOutputSinkRouter {

    private static final Logger LOG = LoggerFactory.getLogger(ReportOutputSinkRouter.class);

    private final BigQueryJobService bqJobService;

    public ReportOutputSinkRouter(BigQueryJobService bqJobService) {
        this.bqJobService = bqJobService;
    }

    /**
     * Routes one output config to its sink. Returns an {@link OutputResult} describing
     * what was produced.
     *
     * @param output      the output config row
     * @param sourceTable fully-qualified BQ table ref holding the transform result
     * @param config      full report config (provides reportName, periodId, etc.)
     * @param options     pipeline options
     */
    public OutputResult route(ReportOutputConfig output, String sourceTable,
                               ReportConfig config, FrameworkOptions options) {
        ReportOutputSinkType sinkType = output.sinkType != null
                                        ? output.sinkType : ReportOutputSinkType.GCS;

        return switch (sinkType) {
            case GCS -> routeToGcs(output, sourceTable, config, options);
            case BQ  -> routeToBq(output, sourceTable, options);
            case API -> routeToApi(output, sourceTable, options);
        };
    }

    // ── GCS ───────────────────────────────────────────────────────────────────

    private OutputResult routeToGcs(ReportOutputConfig output, String sourceTable,
                                     ReportConfig config, FrameworkOptions options) {
        LocalDate runDate = DateUtils.resolveRunDate(options);
        String fileName = (output.filePrefix != null ? output.filePrefix : "")
            + config.reportName + "_" + config.periodId + "_" + runDate
            + (output.fileSuffix != null ? output.fileSuffix : "")
            + (output.isCsv() ? ".csv" : ".json");

        String gcsUri = output.gcsPath + fileName;

        if (output.isCsv()) {
            bqJobService.exportToCsv(sourceTable, gcsUri, output.includeHeader);
        } else {
            bqJobService.exportToJson(sourceTable, gcsUri);
        }
        LOG.info("Exported {} → {}", sourceTable, gcsUri);

        String contentType = output.isCsv() ? "text/csv" : "application/json";
        return new OutputResult(gcsUri, fileName, contentType, null, true);
    }

    // ── BQ ────────────────────────────────────────────────────────────────────

    private OutputResult routeToBq(ReportOutputConfig output, String sourceTable,
                                    FrameworkOptions options) {
        String destTable = output.bqSinkTable;
        if (destTable == null || destTable.isBlank()) {
            throw new IllegalArgumentException(
                "bq_sink_table is required for BQ output (output_order=" + output.outputOrder + ")");
        }
        bqJobService.runQueryToTable(
            "SELECT * FROM `" + sourceTable + "`", destTable);
        LOG.info("Copied {} → BQ:{}", sourceTable, destTable);
        return new OutputResult(destTable, null, null, null, false);
    }

    // ── API ───────────────────────────────────────────────────────────────────

    private OutputResult routeToApi(ReportOutputConfig output, String sourceTable,
                                     FrameworkOptions options) {
        String endpoint = output.apiEndpoint;
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException(
                "api_endpoint is required for API output (output_order=" + output.outputOrder + ")");
        }
        // API delivery: export rows as JSON from BQ, then POST to endpoint.
        // Full implementation delegates to a dedicated HTTP adapter.
        // For now, log the intent and record the destination.
        LOG.warn("API output sink is not yet fully implemented — "
                 + "rows from '{}' would be POSTed to '{}'. Skipping.", sourceTable, endpoint);
        return new OutputResult(endpoint, null, null, null, false);
    }

    // ── Result record ─────────────────────────────────────────────────────────

    /**
     * Describes the outcome of one output routing step.
     *
     * @param destination GCS URI, BQ table ref, or API endpoint
     * @param fileName    file name only (GCS outputs only; null for BQ/API)
     * @param contentType MIME type (GCS outputs only; null for BQ/API)
     * @param rowCount    number of rows written (null if unknown)
     * @param hasAttachment true for GCS outputs eligible for email attachment
     */
    public record OutputResult(
        String  destination,
        String  fileName,
        String  contentType,
        Long    rowCount,
        boolean hasAttachment
    ) {}
}
