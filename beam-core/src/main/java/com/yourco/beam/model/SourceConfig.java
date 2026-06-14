package com.yourco.beam.model;

import com.yourco.beam.options.SourceType;

import java.io.Serializable;

/**
 * Complete configuration for one data source in a {@code DATA_SOURCE_DOWNLOAD} run.
 *
 * <p>Fetched from the parameter DB by {@link com.yourco.beam.utils.db.ParameterRepository}
 * using the composite key ({@code datasourceName}, {@code periodId}, {@code subprocessName}).
 * One {@link SourceConfig} corresponds to one parallel Beam source branch in
 * {@link com.yourco.beam.runner.DataSourcePipelineFactory}.
 *
 * <p>Only one of {@link #apiConfig}, {@link #fileConfig}, or {@link #bqFetchConfig} will be
 * non-null, matching the value of {@link #sourceType}.
 *
 * <p>This class is {@link Serializable} so instances can be passed as DoFn constructor arguments
 * and shipped to Dataflow workers inside the {@code ApiSourceTransform} and
 * {@code FileSourceTransform}.
 */
public final class SourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

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

    private SourceConfig(String datasourceName, String periodId, String subprocessName,
                         SourceType sourceType, ApiSourceConfig apiConfig,
                         FileSourceConfig fileConfig, BqFetchConfig bqFetchConfig) {
        this.datasourceName = datasourceName;
        this.periodId       = periodId;
        this.subprocessName = subprocessName;
        this.sourceType     = sourceType;
        this.apiConfig      = apiConfig;
        this.fileConfig     = fileConfig;
        this.bqFetchConfig  = bqFetchConfig;
    }

    public static SourceConfig forApi(String datasourceName, String periodId,
                                      String subprocessName, ApiSourceConfig apiConfig) {
        return new SourceConfig(datasourceName, periodId, subprocessName,
            SourceType.API, apiConfig, null, null);
    }

    public static SourceConfig forFile(String datasourceName, String periodId,
                                       String subprocessName, FileSourceConfig fileConfig) {
        return new SourceConfig(datasourceName, periodId, subprocessName,
            SourceType.FILE, null, fileConfig, null);
    }

    public static SourceConfig forBq(String datasourceName, String periodId,
                                     String subprocessName, BqFetchConfig bqFetchConfig) {
        return new SourceConfig(datasourceName, periodId, subprocessName,
            SourceType.BQ, null, null, bqFetchConfig);
    }

    @Override
    public String toString() {
        return "SourceConfig{datasource=" + datasourceName
            + ", period=" + periodId
            + ", subprocess=" + subprocessName
            + ", type=" + sourceType + "}";
    }
}
