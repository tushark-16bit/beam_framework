package com.yourco.beam.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * Status tracking record for one fetched data source in a {@code DATA_SOURCE_DOWNLOAD} run.
 *
 * <p>One row is written to the {@code process_status} BigQuery table per source per run.
 * The status transitions are:
 * <ol>
 *   <li>{@code PENDING} — written in the driver JVM before {@code pipeline.run()}</li>
 *   <li>{@code COMPLETED} — updated after the pipeline writes output AND all validation checks pass</li>
 *   <li>{@code FAILED} — updated if the pipeline throws an exception</li>
 *   <li>{@code VALIDATION_FAILED} — updated if the pipeline succeeds but a header, row-count,
 *       or BnC check fails</li>
 * </ol>
 *
 * <h2>BQ table schema ({@code process_status})</h2>
 * <pre>{@code
 * job_run_id          STRING   NOT NULL,
 * process_type        STRING,
 * datasource_name     STRING,
 * subprocess_name     STRING,
 * period_id           STRING,
 * period_start        STRING,
 * period_end          STRING,
 * status              STRING,   -- PENDING | COMPLETED | FAILED | VALIDATION_FAILED
 * row_count           INT64,
 * error_message       STRING,
 * validation_details  STRING,   -- JSON: header results, BnC results
 * started_at          TIMESTAMP,
 * completed_at        TIMESTAMP
 * }</pre>
 */
public final class ProcessStatusRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String STATUS_PENDING            = "PENDING";
    public static final String STATUS_COMPLETED          = "COMPLETED";
    public static final String STATUS_FAILED             = "FAILED";
    public static final String STATUS_VALIDATION_FAILED  = "VALIDATION_FAILED";

    public final String jobRunId;
    public final String processType;
    public final String datasourceName;
    public final String subprocessName;
    public final String periodId;
    public final String periodStart;
    public final String periodEnd;
    public final String status;
    public final long rowCount;
    public final String errorMessage;
    /** JSON blob describing header check and BnC results. */
    public final String validationDetails;
    public final Instant startedAt;
    public final Instant completedAt;

    private ProcessStatusRecord(Builder b) {
        this.jobRunId           = b.jobRunId;
        this.processType        = b.processType;
        this.datasourceName     = b.datasourceName;
        this.subprocessName     = b.subprocessName;
        this.periodId           = b.periodId;
        this.periodStart        = b.periodStart;
        this.periodEnd          = b.periodEnd;
        this.status             = b.status;
        this.rowCount           = b.rowCount;
        this.errorMessage       = b.errorMessage;
        this.validationDetails  = b.validationDetails;
        this.startedAt          = b.startedAt;
        this.completedAt        = b.completedAt;
    }

    // ── Factory helpers ───────────────────────────────────────────────────────

    /**
     * Creates a PENDING status record for a report run.
     * Use this instead of {@link #pending(String, String, SourceConfig, String, String)}
     * when the process is {@code REPORT_PROCESSING} (no {@code SourceConfig} involved).
     */
    public static ProcessStatusRecord pendingReport(String jobRunId, String processType,
                                                    String reportName, String reportSubprocess,
                                                    String periodId, String periodStart,
                                                    String periodEnd) {
        return builder()
            .jobRunId(jobRunId)
            .processType(processType)
            .datasourceName(reportName)
            .subprocessName(reportSubprocess)
            .periodId(periodId)
            .periodStart(periodStart)
            .periodEnd(periodEnd)
            .status(STATUS_PENDING)
            .startedAt(Instant.now())
            .build();
    }

    public static ProcessStatusRecord pending(String jobRunId, String processType,
                                              SourceConfig config, String periodStart, String periodEnd) {
        return builder()
            .jobRunId(jobRunId)
            .processType(processType)
            .datasourceName(config.datasourceName)
            .subprocessName(config.subprocessName)
            .periodId(config.periodId)
            .periodStart(periodStart)
            .periodEnd(periodEnd)
            .status(STATUS_PENDING)
            .startedAt(Instant.now())
            .build();
    }

    public static ProcessStatusRecord completed(ProcessStatusRecord pending, long rowCount,
                                                String validationDetails) {
        return builder()
            .jobRunId(pending.jobRunId)
            .processType(pending.processType)
            .datasourceName(pending.datasourceName)
            .subprocessName(pending.subprocessName)
            .periodId(pending.periodId)
            .periodStart(pending.periodStart)
            .periodEnd(pending.periodEnd)
            .status(STATUS_COMPLETED)
            .rowCount(rowCount)
            .validationDetails(validationDetails)
            .startedAt(pending.startedAt)
            .completedAt(Instant.now())
            .build();
    }

    public static ProcessStatusRecord failed(ProcessStatusRecord pending, String errorMessage) {
        return builder()
            .jobRunId(pending.jobRunId)
            .processType(pending.processType)
            .datasourceName(pending.datasourceName)
            .subprocessName(pending.subprocessName)
            .periodId(pending.periodId)
            .periodStart(pending.periodStart)
            .periodEnd(pending.periodEnd)
            .status(STATUS_FAILED)
            .errorMessage(errorMessage)
            .startedAt(pending.startedAt)
            .completedAt(Instant.now())
            .build();
    }

    public static ProcessStatusRecord validationFailed(ProcessStatusRecord pending,
                                                        long rowCount, String validationDetails) {
        return builder()
            .jobRunId(pending.jobRunId)
            .processType(pending.processType)
            .datasourceName(pending.datasourceName)
            .subprocessName(pending.subprocessName)
            .periodId(pending.periodId)
            .periodStart(pending.periodStart)
            .periodEnd(pending.periodEnd)
            .status(STATUS_VALIDATION_FAILED)
            .rowCount(rowCount)
            .validationDetails(validationDetails)
            .startedAt(pending.startedAt)
            .completedAt(Instant.now())
            .build();
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String jobRunId, processType, datasourceName, subprocessName;
        private String periodId, periodStart, periodEnd, status;
        private long rowCount;
        private String errorMessage, validationDetails;
        private Instant startedAt, completedAt;

        public Builder jobRunId(String v)          { jobRunId = v;          return this; }
        public Builder processType(String v)       { processType = v;       return this; }
        public Builder datasourceName(String v)    { datasourceName = v;    return this; }
        public Builder subprocessName(String v)    { subprocessName = v;    return this; }
        public Builder periodId(String v)          { periodId = v;          return this; }
        public Builder periodStart(String v)       { periodStart = v;       return this; }
        public Builder periodEnd(String v)         { periodEnd = v;         return this; }
        public Builder status(String v)            { status = v;            return this; }
        public Builder rowCount(long v)            { rowCount = v;          return this; }
        public Builder errorMessage(String v)      { errorMessage = v;      return this; }
        public Builder validationDetails(String v) { validationDetails = v; return this; }
        public Builder startedAt(Instant v)        { startedAt = v;         return this; }
        public Builder completedAt(Instant v)      { completedAt = v;       return this; }

        public ProcessStatusRecord build() { return new ProcessStatusRecord(this); }
    }
}
