package com.yourco.beam.model;

import com.yourco.beam.options.SourceType;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Complete configuration for one data source in a {@code DATA_SOURCE_DOWNLOAD} run.
 *
 * <p>Fetched from the {@code source_config} BigQuery table by {@code BigQuerySourceConfigRepository}.
 * One {@link SourceConfig} corresponds to one independent Beam pipeline branch —
 * sources are <em>never</em> merged; each source reads, transforms, validates, and
 * writes all rows as JSON blobs to {@code DaRec} (keyed by {@code DaId}).
 *
 * <h2>Per-source pipeline shape</h2>
 * <pre>
 *   source read → {@link #queryConfig} applied → transform chain → DaRec (JSON blobs)
 *                                                     ↑
 *                              {@link #sourceTransforms} (LOOKUP, GROUP_BY, SORT_BY)
 * </pre>
 *
 * <p>Only one of {@link #apiConfig}, {@link #fileConfig}, or {@link #bqFetchConfig} will be
 * non-null, matching the value of {@link #sourceType}.
 */
public final class SourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String parentId;        // top-level business group (parent_id in source_config)
    public final String datasourceName;
    public final int    periodId;
    public final String subprocessName;
    public final SourceType sourceType;

    /** Non-null when sourceType == API. */
    public final ApiSourceConfig apiConfig;
    /** Non-null when sourceType == FILE. */
    public final FileSourceConfig fileConfig;
    /** Non-null when sourceType == BQ (in DATA_SOURCE_DOWNLOAD mode). */
    public final BqFetchConfig bqFetchConfig;

    /**
     * Query template and injectable parameters (period start/end, custom params).
     * Applied when building the query that fetches data from the source.
     */
    public final QueryConfig queryConfig;

    /**
     * Ordered list of per-source transforms to apply after fetching.
     * Applied left-to-right: GROUP_BY, SORT_BY, LOOKUP.
     * Empty list means no post-fetch transforms.
     */
    public final List<SourceTransformConfig> sourceTransforms;

    /**
     * Post-fetch validation rules: header check, row count bounds, BnC sums.
     * Evaluated in the driver JVM after the pipeline writes to the output table.
     */
    public final ValidationConfig validationConfig;

    /**
     * Optional failure-notification email config.
     * Null when {@code failure_email_to} is absent from {@code parameters_val_json}.
     */
    public final SourceFailureEmailConfig failureEmailConfig;

    /**
     * Optional post-storage SQL transform applied to this source's rows, within the same run.
     * Never null — defaults to {@link DataTransformConfig#none()} when
     * {@code data_transform_query} is absent from {@code parameters_val_json}.
     */
    public final DataTransformConfig dataTransformConfig;

    /**
     * Optional per-source run-scheduling config (which calendar date this source's run should
     * target). Never null — defaults to {@link RunScheduleConfig#none()} when
     * {@code run_details_json} is absent from {@code parameters_val_json}. See
     * {@link RunScheduleConfig} for the field meanings and {@code RunDateCalculator} (beam-utils)
     * for how a date is actually computed from it.
     */
    public final RunScheduleConfig runScheduleConfig;

    private SourceConfig(Builder b) {
        this.parentId            = b.parentId;
        this.datasourceName      = b.datasourceName;
        this.periodId            = b.periodId;
        this.subprocessName      = b.subprocessName;
        this.sourceType          = b.sourceType;
        this.apiConfig           = b.apiConfig;
        this.fileConfig          = b.fileConfig;
        this.bqFetchConfig       = b.bqFetchConfig;
        this.queryConfig         = b.queryConfig != null ? b.queryConfig : QueryConfig.empty();
        this.sourceTransforms    = b.sourceTransforms != null
                                   ? Collections.unmodifiableList(b.sourceTransforms)
                                   : Collections.emptyList();
        this.validationConfig    = b.validationConfig != null ? b.validationConfig : ValidationConfig.none();
        this.failureEmailConfig  = b.failureEmailConfig;
        this.dataTransformConfig = b.dataTransformConfig != null ? b.dataTransformConfig : DataTransformConfig.none();
        this.runScheduleConfig   = b.runScheduleConfig != null ? b.runScheduleConfig : RunScheduleConfig.none();
    }

    // ── Factory helpers (convenience wrappers around Builder) ─────────────────

    public static SourceConfig forApi(String datasourceName, int periodId,
                                      String subprocessName, ApiSourceConfig apiConfig) {
        return builder().datasourceName(datasourceName).periodId(periodId)
                        .subprocessName(subprocessName).sourceType(SourceType.API)
                        .apiConfig(apiConfig).build();
    }

    public static SourceConfig forFile(String datasourceName, int periodId,
                                       String subprocessName, FileSourceConfig fileConfig) {
        return builder().datasourceName(datasourceName).periodId(periodId)
                        .subprocessName(subprocessName).sourceType(SourceType.FILE)
                        .fileConfig(fileConfig).build();
    }

    public static SourceConfig forBq(String datasourceName, int periodId,
                                     String subprocessName, BqFetchConfig bqFetchConfig) {
        return builder().datasourceName(datasourceName).periodId(periodId)
                        .subprocessName(subprocessName).sourceType(SourceType.BQ)
                        .bqFetchConfig(bqFetchConfig).build();
    }

    public static Builder builder() { return new Builder(); }

    /**
     * A {@link Builder} pre-populated with every field of this instance — for rebuilding a
     * {@link SourceConfig} with one or two fields changed (e.g. {@code DataSourcePipelineFactory
     * .resolveQueryTokens()} swapping in a token-resolved {@link #bqFetchConfig}) without having
     * to re-list every other field by hand, where a newly added field is easy to forget to copy.
     */
    public Builder toBuilder() {
        return builder()
            .parentId(parentId)
            .datasourceName(datasourceName)
            .periodId(periodId)
            .subprocessName(subprocessName)
            .sourceType(sourceType)
            .apiConfig(apiConfig)
            .fileConfig(fileConfig)
            .bqFetchConfig(bqFetchConfig)
            .queryConfig(queryConfig)
            .sourceTransforms(new java.util.ArrayList<>(sourceTransforms))
            .validationConfig(validationConfig)
            .failureEmailConfig(failureEmailConfig)
            .dataTransformConfig(dataTransformConfig)
            .runScheduleConfig(runScheduleConfig);
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static final class Builder {
        private String parentId, datasourceName, subprocessName;
        private int    periodId;
        private SourceType sourceType;
        private ApiSourceConfig apiConfig;
        private FileSourceConfig fileConfig;
        private BqFetchConfig bqFetchConfig;
        private QueryConfig queryConfig;
        private List<SourceTransformConfig> sourceTransforms;
        private ValidationConfig validationConfig;
        private SourceFailureEmailConfig failureEmailConfig;
        private DataTransformConfig dataTransformConfig;
        private RunScheduleConfig runScheduleConfig;

        public Builder parentId(String v)                              { parentId = v;             return this; }
        public Builder datasourceName(String v)                        { datasourceName = v;       return this; }
        public Builder periodId(int v)                                 { periodId = v;             return this; }
        public Builder subprocessName(String v)                        { subprocessName = v;       return this; }
        public Builder sourceType(SourceType v)                        { sourceType = v;           return this; }
        public Builder apiConfig(ApiSourceConfig v)                    { apiConfig = v;            return this; }
        public Builder fileConfig(FileSourceConfig v)                  { fileConfig = v;           return this; }
        public Builder bqFetchConfig(BqFetchConfig v)                  { bqFetchConfig = v;        return this; }
        public Builder queryConfig(QueryConfig v)                      { queryConfig = v;          return this; }
        public Builder sourceTransforms(List<SourceTransformConfig> v) { sourceTransforms = v;     return this; }
        public Builder validationConfig(ValidationConfig v)            { validationConfig = v;     return this; }
        public Builder failureEmailConfig(SourceFailureEmailConfig v)  { failureEmailConfig = v;   return this; }
        public Builder dataTransformConfig(DataTransformConfig v)      { dataTransformConfig = v;  return this; }
        public Builder runScheduleConfig(RunScheduleConfig v)          { runScheduleConfig = v;    return this; }

        public SourceConfig build() { return new SourceConfig(this); }
    }

    @Override
    public String toString() {
        return "SourceConfig{parent=" + parentId
            + ", datasource=" + datasourceName
            + ", period=" + periodId
            + ", subprocess=" + subprocessName
            + ", type=" + sourceType
            + ", transforms=" + sourceTransforms.size() + "}";
    }
}
