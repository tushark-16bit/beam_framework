package com.yourco.beam.io.source;

import com.yourco.beam.model.BqFetchConfig;
import com.yourco.beam.model.PipelineRunConfig;
import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.time.LocalDate;
import java.util.Objects;

/**
 * Routes pipeline execution to the correct source connector.
 *
 * <p>Two routing modes:
 * <ul>
 *   <li>{@link #route(Pipeline, PipelineRunConfig)} — REPORT_PROCESSING legacy mode.
 *       Source type and config come from {@link PipelineRunConfig} loaded from parameter_store.</li>
 *   <li>{@link #routeFromConfig(Pipeline, SourceConfig, FrameworkOptions, LocalDate)} — DATA_SOURCE_DOWNLOAD
 *       mode. Source type and all configuration come from a {@link SourceConfig} fetched
 *       from the parameter DB. Called once per source in the parallel loop.</li>
 * </ul>
 *
 * <p>Stateless factory — never serialized. Runs only in the driver JVM.
 */
public final class SourceRouter {

    private SourceRouter() {}

    /**
     * REPORT_PROCESSING legacy mode: routes based on source type from {@link PipelineRunConfig}.
     * Reads from the configured source and returns a {@code PCollection<Row>}.
     */
    public static PCollection<Row> route(Pipeline pipeline, PipelineRunConfig runConfig) {
        return switch (runConfig.getSourceType()) {
            case GCS    -> pipeline.apply("Source-GCS",    new GcsSourceTransform(runConfig.getGcsSourcePath()));
            case BQ     -> pipeline.apply("Source-BQ",     new BigQuerySourceTransform(runConfig.getBqSourceTable(), runConfig.getBqSourceQuery()));
            case PUBSUB -> pipeline.apply("Source-PubSub", new PubSubSourceTransform(runConfig.getPubSubSubscription()));
            case API, FILE -> throw new IllegalArgumentException(
                "sourceType=" + runConfig.getSourceType()
                + " is only valid for DATA_SOURCE_DOWNLOAD. "
                + "Use routeFromConfig() with a SourceConfig from the parameter DB.");
        };
    }

    /**
     * DATA_SOURCE_DOWNLOAD mode: routes based on a {@link SourceConfig} fetched from
     * the parameter DB. Each source in the parallel loop calls this method once.
     *
     * <p>{@code runDate} must be resolved by the caller (e.g., {@code DateUtils.resolveRunDate(options)}
     * from beam-runner which has access to beam-utils). This keeps beam-io free of a
     * beam-utils dependency.
     *
     * <p>The node label includes the datasource name so each source branch appears
     * separately in the Dataflow UI for easy monitoring.
     */
    public static PCollection<Row> routeFromConfig(Pipeline pipeline, SourceConfig config,
                                                   FrameworkOptions options, LocalDate runDate) {
        String label = config.datasourceName + "-" + config.sourceType.name();

        return switch (config.sourceType) {
            case API  -> pipeline.apply("Source-" + label, new ApiSourceTransform(config));
            case FILE -> pipeline.apply("Source-" + label,
                             new FileSourceTransform(config,
                                 String.valueOf(options.getPeriodId()), runDate));
            case BQ   -> {
                BqFetchConfig bq = Objects.requireNonNull(config.bqFetchConfig,
                    "bqFetchConfig is required for sourceType=BQ in source: " + config.datasourceName);
                yield pipeline.apply("Source-" + label,
                    new BigQuerySourceTransform(bq.tableRef(), bq.query));
            }
            case GCS  -> pipeline.apply("Source-" + label,
                new GcsSourceTransform(config.fileConfig != null ? config.fileConfig.location : null));
            case PUBSUB -> throw new IllegalArgumentException(
                "PUBSUB is a streaming source and is not supported in DATA_SOURCE_DOWNLOAD mode.");
        };
    }
}
