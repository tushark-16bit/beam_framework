package com.yourco.beam.runner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yourco.beam.io.checkpoint.BigQueryDataSourceCheckpointAdapter;
import com.yourco.beam.io.records.BigQueryDataSourceRecordAdapter;
import com.yourco.beam.model.BncRule;
import com.yourco.beam.model.DataSourceCheckpoint;
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
 * against the DaRec record table, updates the DaRefer checkpoint to
 * {@code COMPLETED} / {@code FAILED_BNC} / {@code FAILED}, and sends a failure email if
 * configured.
 *
 * <p>Runs entirely inside a Beam worker (not the driver JVM), so it executes as part of the
 * Dataflow job — no external post-pipeline invocation or Classic Template multi-step DAG required.
 *
 * <p>Input: a {@code PCollection<Long>} with exactly one element (the count of rows committed
 * to DaRec), emitted by {@link com.yourco.beam.io.sink.DataSourceRecordSinkTransform} after
 * all streaming inserts are confirmed. Using this as input ensures {@link FinalizeDoFn} only
 * runs after every row is visible in BigQuery.
 */
public final class PostDownloadFinalizeTransform extends PTransform<PCollection<Long>, PDone> {

    private static final long serialVersionUID = 1L;

    private final long         daId;
    private final SourceConfig sourceConfig;
    private final String       daReferTableRef; // `project.dataset.DaRefer`
    private final String       daRecTableRef;   // `project.dataset.DaRec`

    PostDownloadFinalizeTransform(long daId, SourceConfig sourceConfig,
                                  String daReferTableRef, String daRecTableRef) {
        this.daId            = daId;
        this.sourceConfig    = sourceConfig;
        this.daReferTableRef = daReferTableRef;
        this.daRecTableRef   = daRecTableRef;
    }

    @Override
    public PDone expand(PCollection<Long> writtenCount) {
        writtenCount.apply("Finalize-" + sourceConfig.datasourceName,
            ParDo.of(new FinalizeDoFn(daId, sourceConfig, daReferTableRef, daRecTableRef)));
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

        // Created in @Setup — BQ client is not serializable
        private transient BigQueryDataSourceCheckpointAdapter checkpointAdapter;
        private transient BigQueryDataSourceRecordAdapter     recordAdapter;

        FinalizeDoFn(long daId, SourceConfig sourceConfig,
                     String daReferTableRef, String daRecTableRef) {
            this.daId            = daId;
            this.sourceConfig    = sourceConfig;
            this.daReferTableRef = daReferTableRef;
            this.daRecTableRef   = daRecTableRef;
        }

        @Setup
        public void setup() {
            checkpointAdapter = new BigQueryDataSourceCheckpointAdapter(daReferTableRef);
            recordAdapter     = new BigQueryDataSourceRecordAdapter(daRecTableRef);
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

            // BnC sum checks — optional; only run when bnc_rules_json is configured.
            // Absent or empty bnc_rules_json is normal — sum checks are simply skipped.
            Map<String, Object> bncSummary = new LinkedHashMap<>();
            bncSummary.put("pipelineRowCount", pipelineRowCount);
            bncSummary.put("storedRowCount",   rowCount);

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
            } else {
                bncSummary.put("status", "Not Matched");
                bncSummary.put("failures", failures);
                staCd = infraError ? DataSourceCheckpoint.STA_FAILED : DataSourceCheckpoint.STA_FAILED_BNC;
                LOG.warn("Validation FAILED for '{}': {}", sourceConfig.datasourceName, failures);
            }

            String bncJson = toJson(bncSummary);
            checkpointAdapter.updateStatus(daId, staCd, bncJson);
            if (!DataSourceCheckpoint.STA_COMPLETED.equals(staCd)) {
                sendFailureEmail(staCd, String.join("; ", failures), bncJson);
            }
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
