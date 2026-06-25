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
    public final String periodId;
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

    private SourceConfig(Builder b) {
        this.parentId         = b.parentId;
        this.datasourceName   = b.datasourceName;
        this.periodId         = b.periodId;
        this.subprocessName   = b.subprocessName;
        this.sourceType       = b.sourceType;
        this.apiConfig        = b.apiConfig;
        this.fileConfig       = b.fileConfig;
        this.bqFetchConfig    = b.bqFetchConfig;
        this.queryConfig      = b.queryConfig != null ? b.queryConfig : QueryConfig.empty();
        this.sourceTransforms = b.sourceTransforms != null
                                ? Collections.unmodifiableList(b.sourceTransforms)
                                : Collections.emptyList();
        this.validationConfig = b.validationConfig != null ? b.validationConfig : ValidationConfig.none();
    }

    // ── Factory helpers (convenience wrappers around Builder) ─────────────────

    public static SourceConfig forApi(String datasourceName, String periodId,
                                      String subprocessName, ApiSourceConfig apiConfig) {
        return builder().datasourceName(datasourceName).periodId(periodId)
                        .subprocessName(subprocessName).sourceType(SourceType.API)
                        .apiConfig(apiConfig).build();
    }

    public static SourceConfig forFile(String datasourceName, String periodId,
                                       String subprocessName, FileSourceConfig fileConfig) {
        return builder().datasourceName(datasourceName).periodId(periodId)
                        .subprocessName(subprocessName).sourceType(SourceType.FILE)
                        .fileConfig(fileConfig).build();
    }

    public static SourceConfig forBq(String datasourceName, String periodId,
                                     String subprocessName, BqFetchConfig bqFetchConfig) {
        return builder().datasourceName(datasourceName).periodId(periodId)
                        .subprocessName(subprocessName).sourceType(SourceType.BQ)
                        .bqFetchConfig(bqFetchConfig).build();
    }

    public static Builder builder() { return new Builder(); }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static final class Builder {
        private String parentId, datasourceName, periodId, subprocessName;
        private SourceType sourceType;
        private ApiSourceConfig apiConfig;
        private FileSourceConfig fileConfig;
        private BqFetchConfig bqFetchConfig;
        private QueryConfig queryConfig;
        private List<SourceTransformConfig> sourceTransforms;
        private ValidationConfig validationConfig;

        public Builder parentId(String v)                         { parentId = v;           return this; }
        public Builder datasourceName(String v)                   { datasourceName = v;     return this; }
        public Builder periodId(String v)                         { periodId = v;           return this; }
        public Builder subprocessName(String v)                   { subprocessName = v;     return this; }
        public Builder sourceType(SourceType v)                   { sourceType = v;         return this; }
        public Builder apiConfig(ApiSourceConfig v)               { apiConfig = v;          return this; }
        public Builder fileConfig(FileSourceConfig v)             { fileConfig = v;         return this; }
        public Builder bqFetchConfig(BqFetchConfig v)             { bqFetchConfig = v;      return this; }
        public Builder queryConfig(QueryConfig v)                 { queryConfig = v;        return this; }
        public Builder sourceTransforms(List<SourceTransformConfig> v) { sourceTransforms = v; return this; }
        public Builder validationConfig(ValidationConfig v)       { validationConfig = v;  return this; }

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
