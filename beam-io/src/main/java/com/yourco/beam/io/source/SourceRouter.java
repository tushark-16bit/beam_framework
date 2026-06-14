package com.yourco.beam.io.source;

import com.yourco.beam.model.SourceConfig;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.time.LocalDate;

/**
 * Routes pipeline execution to the correct source connector.
 *
 * <p>Two routing modes:
 * <ul>
 *   <li>{@link #route(Pipeline, FrameworkOptions)} — REPORT_PROCESSING mode. Source type
 *       comes from {@code --sourceType} CLI option.</li>
 *   <li>{@link #routeFromConfig(Pipeline, SourceConfig, FrameworkOptions)} — DATA_SOURCE_DOWNLOAD
 *       mode. Source type and all configuration come from a {@link SourceConfig} fetched
 *       from the parameter DB. Called once per source in the parallel loop.</li>
 * </ul>
 *
 * <p>Stateless factory — never serialized. Runs only in the driver JVM.
 */
public final class SourceRouter {

    private SourceRouter() {}

    /**
     * REPORT_PROCESSING mode: routes based on {@code --sourceType} CLI flag.
     * Reads from the configured source and returns a {@code PCollection<Row>}.
     */
    public static PCollection<Row> route(Pipeline pipeline, FrameworkOptions options) {
        if (options.getSourceType() == null) {
            throw new IllegalArgumentException(
                "--sourceType is required for REPORT_PROCESSING but was not provided.");
        }
        return switch (options.getSourceType()) {
            case GCS    -> pipeline.apply("Source-GCS",    new GcsSourceTransform(options));
            case BQ     -> pipeline.apply("Source-BQ",     new BigQuerySourceTransform(options));
            case PUBSUB -> pipeline.apply("Source-PubSub", new PubSubSourceTransform(options));
            case API, FILE -> throw new IllegalArgumentException(
                "sourceType=" + options.getSourceType()
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
                             new FileSourceTransform(config, options.getPeriodId(), runDate));
            case BQ   -> pipeline.apply("Source-" + label, new BigQuerySourceTransform(options));
            case GCS  -> pipeline.apply("Source-" + label, new GcsSourceTransform(options));
            case PUBSUB -> throw new IllegalArgumentException(
                "PUBSUB is a streaming source and is not supported in DATA_SOURCE_DOWNLOAD mode.");
        };
    }
}
