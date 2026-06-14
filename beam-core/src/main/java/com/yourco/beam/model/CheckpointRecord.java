package com.yourco.beam.model;

import com.yourco.beam.options.SourceType;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents one row in the BigQuery pipeline checkpoint table.
 *
 * <p>The table must exist before the pipeline runs. Suggested DDL:
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.pipeline_checkpoints (
 *   job_run_id         STRING    NOT NULL,
 *   datasource_name    STRING    NOT NULL,
 *   period_id          STRING    NOT NULL,
 *   subprocess_name    STRING    NOT NULL,
 *   state              STRING    NOT NULL,   -- CheckpointState enum value
 *   source_type        STRING,
 *   created_at         TIMESTAMP NOT NULL,
 *   error_message      STRING,
 *   records_processed  INT64
 * );
 * }</pre>
 *
 * <p>Instances are created via {@link #started} / {@link #finished} / {@link #failed}
 * and written by {@link com.yourco.beam.io.checkpoint.BigQueryCheckpointAdapter}.
 */
public final class CheckpointRecord {

    public final String jobRunId;
    public final String datasourceName;
    public final String periodId;
    public final String subprocessName;
    public final CheckpointState state;
    public final SourceType sourceType;
    public final Instant createdAt;
    public final String errorMessage;     // null unless state == FAILED_DOWNLOADING
    public final long recordsProcessed;   // 0 unless state == FINISHED_ACCESSING

    private CheckpointRecord(String jobRunId, String datasourceName, String periodId,
                              String subprocessName, CheckpointState state, SourceType sourceType,
                              String errorMessage, long recordsProcessed) {
        this.jobRunId          = jobRunId;
        this.datasourceName    = datasourceName;
        this.periodId          = periodId;
        this.subprocessName    = subprocessName;
        this.state             = state;
        this.sourceType        = sourceType;
        this.createdAt         = Instant.now();
        this.errorMessage      = errorMessage;
        this.recordsProcessed  = recordsProcessed;
    }

    // ── Builder (used when reconstructing records read from BQ) ──────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String jobRunId;
        private String datasourceName;
        private String periodId;
        private String subprocessName;
        private CheckpointState state;
        private SourceType sourceType;
        private String errorMessage;
        private long recordsProcessed;

        public Builder jobRunId(String v)         { this.jobRunId = v; return this; }
        public Builder datasourceName(String v)   { this.datasourceName = v; return this; }
        public Builder periodId(String v)         { this.periodId = v; return this; }
        public Builder subprocessName(String v)   { this.subprocessName = v; return this; }
        public Builder state(CheckpointState v)   { this.state = v; return this; }
        public Builder sourceType(SourceType v)   { this.sourceType = v; return this; }
        public Builder errorMessage(String v)     { this.errorMessage = v; return this; }
        public Builder recordsProcessed(long v)   { this.recordsProcessed = v; return this; }

        public CheckpointRecord build() {
            return new CheckpointRecord(
                Objects.requireNonNull(jobRunId, "jobRunId"),
                Objects.requireNonNull(datasourceName, "datasourceName"),
                Objects.requireNonNull(periodId, "periodId"),
                Objects.requireNonNull(subprocessName, "subprocessName"),
                Objects.requireNonNull(state, "state"),
                sourceType, errorMessage, recordsProcessed);
        }
    }

    public static CheckpointRecord started(String jobRunId, SourceConfig config) {
        return new CheckpointRecord(
            jobRunId, config.datasourceName, config.periodId, config.subprocessName,
            CheckpointState.STARTED_ACCESSING, config.sourceType, null, 0L);
    }

    public static CheckpointRecord finished(String jobRunId, SourceConfig config, long recordsProcessed) {
        return new CheckpointRecord(
            jobRunId, config.datasourceName, config.periodId, config.subprocessName,
            CheckpointState.FINISHED_ACCESSING, config.sourceType, null, recordsProcessed);
    }

    public static CheckpointRecord failed(String jobRunId, SourceConfig config, String errorMessage) {
        return new CheckpointRecord(
            jobRunId, config.datasourceName, config.periodId, config.subprocessName,
            CheckpointState.FAILED_DOWNLOADING, config.sourceType, errorMessage, 0L);
    }

    @Override
    public String toString() {
        return "CheckpointRecord{jobRunId=" + jobRunId
            + ", datasource=" + datasourceName
            + ", period=" + periodId
            + ", subprocess=" + subprocessName
            + ", state=" + state + "}";
    }
}
