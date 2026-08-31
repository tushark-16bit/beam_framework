package com.yourco.beam.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.records.BigQueryDataSourceRecordAdapter;
import com.yourco.beam.io.report.BigQueryJobService;
import com.yourco.beam.io.util.FileHeaderLegend;
import com.yourco.beam.model.BncRule;
import com.yourco.beam.model.DataSourceCheckpoint;
import com.yourco.beam.model.DataTransformConfig;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.model.SourceFailureEmailConfig;
import com.yourco.beam.model.ValidationConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Final pipeline step for {@code DATA_SOURCE_DOWNLOAD}: validates row count and BnC rules
 * against the DaRec record table, runs the optional {@code data_transform_query}, updates the
 * DaRefer checkpoint to {@code COMPLETED} / {@code FAILED_BNC} / {@code FAILED_TRANSFORM} /
 * {@code FAILED}, cleans up superseded data under {@code --manualOverrun}, and sends a failure
 * email if configured.
 *
 * <p>Runs entirely inside a Beam worker (not the driver JVM), so it executes as part of the
 * Dataflow job — no external post-pipeline invocation or Classic Template multi-step DAG required.
 *
 * <p>Input: a {@code PCollection<Long>} with exactly one element (the count of rows committed
 * to DaRec), emitted by {@link com.yourco.beam.io.sink.DataSourceRecordSinkTransform} after
 * all streaming inserts are confirmed. Using this as input ensures {@link FinalizeDoFn} only
 * runs after every row is visible in BigQuery.
 *
 * <h2>Phase order inside {@link FinalizeDoFn#runValidation}</h2>
 * <ol>
 *   <li>row_count_mismatch (always-on) + min/max row bounds (optional) against the raw stored rows</li>
 *   <li>{@code data_transform_query} (optional) — only attempted if phase 1 passed; a {@code data}
 *       CTE reunifying this run's stored rows is always prepended before the operator's query
 *       runs (see {@link FinalizeDoFn#withDataCte}), so unnesting is never opt-in or skippable —
 *       the operator's SQL just references {@code data} as a plain table. Validates the output
 *       row count, and only then replaces the stored rows. A bounds failure or query error leaves
 *       the original rows untouched.</li>
 *   <li>BnC sum rules (optional) — against whatever is now stored (raw or transformed)</li>
 *   <li>Checkpoint update. Only on {@code COMPLETED}: if {@code --manualOverrun} superseded a
 *       previous COMPLETED run, that previous run's DaRec rows are deleted here — DaRefer keeps
 *       both rows; only the superseded bulk data is reclaimed, and only once the new run is
 *       confirmed good.</li>
 * </ol>
 */
public final class PostDownloadFinalizeTransform extends PTransform<PCollection<Long>, PDone> {

    private static final long serialVersionUID = 1L;

    private final long         daId;
    private final SourceConfig sourceConfig;
    private final String       daReferTableRef; // `project.dataset.DaRefer`
    private final String       daRecTableRef;   // `project.dataset.DaRec`
    private final long         previousDaId;    // -1 = no previous run to supersede

    PostDownloadFinalizeTransform(long daId, SourceConfig sourceConfig,
                                  String daReferTableRef, String daRecTableRef,
                                  long previousDaId) {
        this.daId            = daId;
        this.sourceConfig    = sourceConfig;
        this.daReferTableRef = daReferTableRef;
        this.daRecTableRef   = daRecTableRef;
        this.previousDaId    = previousDaId;
    }

    @Override
    public PDone expand(PCollection<Long> writtenCount) {
        writtenCount.apply("Finalize-" + sourceConfig.datasourceName,
            ParDo.of(new FinalizeDoFn(daId, sourceConfig, daReferTableRef, daRecTableRef, previousDaId)));
        return PDone.in(writtenCount.getPipeline());
    }

    // ── Named DoFn — required for Beam serialization safety ──────────────────

    private static final class FinalizeDoFn extends DoFn<Long, Void> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(FinalizeDoFn.class);
        private static final ObjectMapper MAPPER = new ObjectMapper();

        private final long         daId;
        private final SourceConfig sourceConfig;
        private final String       daReferTableRef;
        private final String       daRecTableRef;
        private final long         previousDaId;

        // Created in @Setup — BQ client is not serializable
        private transient BigQueryDataSourceCheckpointAdapter checkpointAdapter;
        private transient BigQueryDataSourceRecordAdapter     recordAdapter;
        private transient BigQueryJobService                  bqJobService;

        FinalizeDoFn(long daId, SourceConfig sourceConfig,
                     String daReferTableRef, String daRecTableRef, long previousDaId) {
            this.daId            = daId;
            this.sourceConfig    = sourceConfig;
            this.daReferTableRef = daReferTableRef;
            this.daRecTableRef   = daRecTableRef;
            this.previousDaId    = previousDaId;
        }

        @Setup
        public void setup() {
            checkpointAdapter = new BigQueryDataSourceCheckpointAdapter(daReferTableRef);
            recordAdapter     = new BigQueryDataSourceRecordAdapter(daRecTableRef);
            bqJobService      = new BigQueryJobService();
        }

        @ProcessElement
        public void processElement(@Element Long pipelineRowCount) {
            LOG.info("Finalizing da_id={} datasource='{}' (pipeline row count: {})",
                     daId, sourceConfig.datasourceName, pipelineRowCount);
            try {
                runValidation(pipelineRowCount);
            } catch (Exception e) {
                LOG.error("Finalize failed for '{}' (da_id={}): {}",
                          sourceConfig.datasourceName, daId, e.getMessage(), e);
                checkpointAdapter.updateStatus(daId, DataSourceCheckpoint.STA_FAILED, null);
                sendFailureEmail(DataSourceCheckpoint.STA_FAILED, e.getMessage(), null);
            }
        }

        private void runValidation(long pipelineRowCount) {
            ValidationConfig validation = sourceConfig.validationConfig;

            // Query DaRec for the actual committed count — streaming inserts are immediately
            // queryable, so by the time this DoFn runs all rows are visible.
            long rowCount = recordAdapter.countRecords(daId);
            boolean bqCountSucceeded = (rowCount != -1L);
            if (!bqCountSucceeded) {
                // BQ query itself failed (infra error); fall back to pipeline count so we still
                // set a terminal checkpoint status rather than leaving it stuck at LOADING.
                LOG.warn("DaRec count query failed for da_id={} — falling back to pipeline count={}",
                         daId, pipelineRowCount);
                rowCount = pipelineRowCount;
            }
            LOG.info("DaRec row count for '{}' (da_id={}): {} (pipeline processed: {})",
                     sourceConfig.datasourceName, daId, rowCount, pipelineRowCount);

            List<String> failures   = new ArrayList<>();
            boolean      infraError = false;

            // Row copy integrity check — always on, no config required.
            // Compares rows committed to DaRec against rows the pipeline processed.
            // Skipped only when the BQ count query itself failed (already fell back above).
            if (bqCountSucceeded && rowCount != pipelineRowCount) {
                failures.add("row_count_mismatch: stored " + rowCount
                    + " rows in DaRec but pipeline processed " + pipelineRowCount
                    + " — possible data loss during insert");
            }

            // Row count bounds (optional, from min_row_count / max_row_count config)
            if (validation.hasMinRowCheck() && rowCount < validation.minRowCount) {
                failures.add("row_count " + rowCount + " < min " + validation.minRowCount);
            }
            if (validation.hasMaxRowCheck() && rowCount > validation.maxRowCount) {
                failures.add("row_count " + rowCount + " > max " + validation.maxRowCount);
            }

            Map<String, Object> bncSummary = new LinkedHashMap<>();
            bncSummary.put("pipelineRowCount", pipelineRowCount);
            bncSummary.put("storedRowCount",   rowCount);

            // Optional post-storage query transform — only attempted once storage integrity
            // (row_count_mismatch + bounds above) is confirmed clean. Runs against a reunified
            // view of this run's stored rows (UNNEST across all pages, so pagination is invisible
            // to the operator's SQL); only replaces the stored rows once its own output passes
            // row-count bounds. On any failure the original rows are left untouched.
            boolean transformError = false;
            DataTransformConfig transform = sourceConfig.dataTransformConfig;
            if (failures.isEmpty() && transform != null && transform.hasQuery()) {
                transformError = !applyDataTransform(transform, bncSummary, failures);
            }

            // BnC sum checks — optional; only run when bnc_rules_json is configured.
            // Absent or empty bnc_rules_json is normal — sum checks are simply skipped.
            // Runs against whatever is now stored: raw rows, or the transform's output if applied.
            if (!validation.hasBncCheck()) {
                LOG.info("No BnC rules configured for '{}' — sum checks skipped",
                         sourceConfig.datasourceName);
            }

            for (BncRule rule : validation.bncRules) {
                double actual = recordAdapter.sumField(daId, rule.field);
                if (Double.isNaN(actual)) {
                    failures.add("BnC SUM(" + rule.field + ") query failed (infra error — check logs)");
                    infraError = true;
                } else {
                    bncSummary.put("srcAmount_" + rule.field, rule.expectedTotal);
                    bncSummary.put("dstAmount_" + rule.field, actual);
                    if (!rule.passes(actual)) {
                        failures.add("BnC SUM(" + rule.field + ") actual=" + actual
                            + " expected=" + rule.expectedTotal
                            + " ±" + (rule.tolerancePct * 100) + "%");
                    }
                }
            }

            String staCd;
            if (failures.isEmpty()) {
                bncSummary.put("status", "Matched");
                staCd = DataSourceCheckpoint.STA_COMPLETED;
                LOG.info("Validation PASSED for '{}'", sourceConfig.datasourceName);

                // manualOverrun cleanup — only once THIS run is confirmed COMPLETED. DaRefer is
                // never touched here (it only ever gains new rows, via createCheckpoint in the
                // driver JVM); this deletes only the superseded run's bulk DaRec data.
                if (previousDaId >= 0) {
                    recordAdapter.deleteRecords(previousDaId);
                    LOG.info("manualOverrun: deleted superseded DaRec rows for '{}' "
                             + "(previous da_id={}, new da_id={})",
                             sourceConfig.datasourceName, previousDaId, daId);
                }
            } else {
                bncSummary.put("status", "Not Matched");
                bncSummary.put("failures", failures);
                staCd = infraError ? DataSourceCheckpoint.STA_FAILED
                      : transformError ? DataSourceCheckpoint.STA_FAILED_TRANSFORM
                      : DataSourceCheckpoint.STA_FAILED_BNC;
                LOG.warn("Validation FAILED for '{}': {}", sourceConfig.datasourceName, failures);
            }

            String bncJson = toJson(bncSummary);
            checkpointAdapter.updateStatus(daId, staCd, bncJson);
            if (!DataSourceCheckpoint.STA_COMPLETED.equals(staCd)) {
                sendFailureEmail(staCd, String.join("; ", failures), bncJson);
            }
        }

        /**
         * Runs {@code transform.query} against a reunified view of this run's stored rows,
         * validates the output row count, and — only if that validation passes — replaces the
         * stored rows with the transform's output.
         *
         * <p>On any failure (query error, or output row count outside
         * {@code data_transform_min_row_count}/{@code data_transform_max_row_count}), appends to
         * {@code failures} and returns {@code false} without touching the stored rows.
         *
         * @return true if the transform ran and its output passed validation
         */
        private boolean applyDataTransform(DataTransformConfig transform,
                                           Map<String, Object> bncSummary, List<String> failures) {
            String resolvedQuery = withDataCte(transform.query);
            String tmpTable = datasetPrefix(daRecTableRef) + ".tmp_transform_" + daId;
            try {
                bqJobService.runQueryToTable(resolvedQuery, tmpTable);
                long transformedCount = bqJobService.countRows(tmpTable);
                bncSummary.put("transformOutputRowCount", transformedCount);

                List<String> transformFailures = new ArrayList<>();
                if (transform.hasMinRowCheck() && transformedCount < transform.minRowCount) {
                    transformFailures.add("data_transform_query output row_count " + transformedCount
                        + " < min " + transform.minRowCount);
                }
                if (transform.hasMaxRowCheck() && transformedCount > transform.maxRowCount) {
                    transformFailures.add("data_transform_query output row_count " + transformedCount
                        + " > max " + transform.maxRowCount);
                }

                if (!transformFailures.isEmpty()) {
                    failures.addAll(transformFailures);
                    LOG.warn("data_transform_query output failed row bounds for '{}' (da_id={}): {} "
                             + "— original stored data left untouched",
                             sourceConfig.datasourceName, daId, transformFailures);
                    return false;
                }

                replaceStoredRows(tmpTable);
                bncSummary.put("storedRowCount", transformedCount);
                LOG.info("data_transform_query applied for '{}' (da_id={}): {} → {} rows",
                         sourceConfig.datasourceName, daId, bncSummary.get("pipelineRowCount"),
                         transformedCount);
                return true;
            } catch (Exception e) {
                failures.add("data_transform_query failed: " + e.getMessage());
                LOG.error("data_transform_query failed for '{}' (da_id={}): {} — original data left untouched",
                          sourceConfig.datasourceName, daId, e.getMessage(), e);
                return false;
            } finally {
                bqJobService.dropTableIfExists(tmpTable);
            }
        }

        /**
         * Replaces this run's DaRec rows with {@code tmpTable}'s (re-paginated at 250 rows/page)
         * by running the delete and the insert as a single atomic BigQuery multi-statement
         * transaction — never as two separate jobs.
         *
         * <p>This matters for two reasons:
         * <ol>
         *   <li>This run's rows were just written via streaming inserts, which are immediately
         *       visible to {@code SELECT} but can remain in BigQuery's internal streaming buffer
         *       for a short while longer — and DML ({@code DELETE}/{@code UPDATE}/{@code MERGE})
         *       cannot touch rows still in that buffer. A transaction attempted while rows are
         *       still buffered fails outright and rolls back cleanly — it cannot partially
         *       delete some pages and leave others, the way two independent, unverified
         *       statements could (and once did: stale pages survived alongside freshly inserted
         *       ones, silently double-counted by every downstream reader since nothing filters
         *       by "which page is current" — every query here scopes only by {@code da_id}).</li>
         *   <li>If the delete succeeded but a separate, later insert then failed, the original
         *       rows would already be gone with nothing to restore them — a single dropped
         *       statement deletes without also inserting. Wrapping both in one transaction makes
         *       that impossible: either both happen, or neither does.</li>
         * </ol>
         *
         * <p>Retries the whole transaction (not a partial step) with backoff for up to ~30s if it
         * fails — safe to retry because a failed attempt is guaranteed to have changed nothing.
         * If it still hasn't succeeded after that, throws so the transform fails cleanly with the
         * original rows completely untouched.
         */
        private void replaceStoredRows(String tmpTable) {
            String sql = buildAtomicReplaceSql(tmpTable);
            int[] backoffMs = {2000, 4000, 8000, 8000, 8000};
            Exception lastError = null;
            for (int attempt = 1; attempt <= backoffMs.length + 1; attempt++) {
                try {
                    bqJobService.runQuery(sql);
                    return;
                } catch (Exception e) {
                    lastError = e;
                    LOG.warn("Atomic DaRec replace failed for da_id={} on attempt {}/{}: {} — likely "
                             + "rows are still in BigQuery's streaming buffer (original rows are "
                             + "untouched — the replace is atomic); retrying",
                             daId, attempt, backoffMs.length + 1, e.getMessage());
                    if (attempt <= backoffMs.length) {
                        try {
                            Thread.sleep(backoffMs[attempt - 1]);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(
                                "Interrupted while retrying DaRec replace for da_id=" + daId, ie);
                        }
                    }
                }
            }
            throw new IllegalStateException(
                "Could not replace DaRec rows for da_id=" + daId + " after " + (backoffMs.length + 1)
                + " attempts (original rows are untouched — the replace is atomic): "
                + lastError.getMessage(), lastError);
        }

        /**
         * Subquery reunifying every paginated DaRec page for this run into one flat rowset of
         * JSON row strings — the same {@link FileHeaderLegend#dataArrayExpr} pattern used by
         * {@code BigQueryDataSourceRecordAdapter.sumField()} and
         * {@code BigQueryReportCheckpointAdapter.stageFromDaRec()}, which extracts just the row
         * array whether a page is a flat JSON array or (FILE source with a header)
         * {@code {"Data":[...],"DataHeaders":[...]}}. Each row is a JSON object string; the
         * operator's query extracts fields with {@code JSON_VALUE(row_json, '$.field')}. The
         * header legend, when present, lives in the separate {@code DataHeaders} array and is
         * never unnested here, so operators never need to filter it out themselves in
         * {@code data_transform_query}.
         */
        private String dataSubquery() {
            return "(SELECT row_json FROM " + daRecTableRef
                + " CROSS JOIN UNNEST(" + FileHeaderLegend.dataArrayExpr("row_da_json_tx") + ") AS row_json"
                + " WHERE da_id = " + daId + ")";
        }

        /**
         * Prepends a {@code data} CTE built from {@link #dataSubquery()} onto the operator's
         * {@code data_transform_query}, so the reunified/un-nested row view is always computed
         * and always available as a plain table reference named {@code data} — the operator's
         * query just does {@code FROM data} (or joins it, etc.) like any other table; there is no
         * placeholder token to remember to include, and unnesting is never skipped by omission.
         *
         * <p>If the operator's own query already starts with a {@code WITH} clause, {@code data}
         * is inserted as its first CTE (comma-joined with theirs) rather than emitting a second,
         * illegal {@code WITH} keyword.
         */
        private String withDataCte(String userQuery) {
            String trimmed = userQuery.stripLeading();
            if (trimmed.regionMatches(true, 0, "WITH", 0, 4)) {
                return "WITH data AS " + dataSubquery() + ",\n" + trimmed.substring(4).stripLeading();
            }
            return "WITH data AS " + dataSubquery() + "\n" + userQuery;
        }

        /**
         * BigQuery script: deletes this run's existing DaRec rows and inserts {@code tmpTable}'s
         * rows re-paginated at 250 rows/page (matching
         * {@code DataSourceRecordSinkTransform.PaginateAndBuildDoFn.PAGE_SIZE}), both inside one
         * {@code BEGIN TRANSACTION ... COMMIT TRANSACTION} block. On any error, explicitly rolls
         * back and re-raises, so the whole script either fully commits or has no effect at all.
         */
        private String buildAtomicReplaceSql(String tmpTable) {
            return "BEGIN\n"
                + "  BEGIN TRANSACTION;\n"
                + "  DELETE FROM " + daRecTableRef + " WHERE da_id = " + daId + ";\n"
                + "  INSERT INTO " + daRecTableRef
                + "    (rec_id, da_id, page_no, row_da_json_tx, load_dt, lst_updt_ts)\n"
                + "  WITH transformed AS (SELECT * FROM `" + tmpTable + "`),\n"
                + "  numbered AS (\n"
                + "    SELECT TO_JSON_STRING(transformed) AS row_json, ROW_NUMBER() OVER () AS rn\n"
                + "    FROM transformed\n"
                + "  )\n"
                + "  SELECT\n"
                + "    GENERATE_UUID() AS rec_id,\n"
                + "    " + daId + " AS da_id,\n"
                + "    DIV(rn - 1, 250) + 1 AS page_no,\n"
                + "    CONCAT('[', STRING_AGG(row_json, ',' ORDER BY rn), ']') AS row_da_json_tx,\n"
                + "    CURRENT_DATE() AS load_dt,\n"
                + "    CURRENT_TIMESTAMP() AS lst_updt_ts\n"
                + "  FROM numbered\n"
                + "  GROUP BY page_no;\n"
                + "  COMMIT TRANSACTION;\n"
                + "EXCEPTION WHEN ERROR THEN\n"
                + "  ROLLBACK TRANSACTION;\n"
                + "  RAISE USING MESSAGE = @@error.message;\n"
                + "END;";
        }

        /** Strips backticks from a `project.dataset.Table` ref and drops the trailing `.Table`. */
        private static String datasetPrefix(String backtickedTableRef) {
            String stripped = backtickedTableRef.replace("`", "");
            int lastDot = stripped.lastIndexOf('.');
            return stripped.substring(0, lastDot);
        }

        private void sendFailureEmail(String staCd, String errorMessage, String bncSummary) {
            SourceFailureEmailConfig emailConfig = sourceConfig.failureEmailConfig;
            if (emailConfig == null || !emailConfig.isPresent()) return;
            try {
                String subject = resolveTokens(emailConfig.subjectTemplate, staCd, errorMessage, bncSummary);
                String body    = resolveTokens(emailConfig.bodyTemplate,    staCd, errorMessage, bncSummary);
                new SmtpReportEmailAdapter(
                    emailConfig.smtpHost, emailConfig.smtpPort,
                    emailConfig.smtpPasswordSecretId, emailConfig.fromAddress)
                    .send(subject, body, emailConfig.to, emailConfig.cc, java.util.List.of());
                LOG.info("Failure email sent for '{}' (staCd={})", sourceConfig.datasourceName, staCd);
            } catch (Exception e) {
                LOG.error("Failed to send failure email for '{}': {}",
                          sourceConfig.datasourceName, e.getMessage(), e);
            }
        }

        private String resolveTokens(String template, String staCd,
                                      String errorMessage, String bncSummary) {
            if (template == null) return "";
            return template
                .replace("{datasourceName}", sourceConfig.datasourceName)
                .replace("{periodId}",       String.valueOf(sourceConfig.periodId))
                .replace("{staCd}",          staCd        != null ? staCd        : "")
                .replace("{errorMessage}",   errorMessage != null ? errorMessage : "")
                .replace("{bncSummary}",     bncSummary   != null ? bncSummary   : "");
        }

        private static String toJson(Map<String, Object> map) {
            try {
                return MAPPER.writeValueAsString(map);
            } catch (Exception e) {
                return map.toString();
            }
        }
    }
}
