package com.yourco.beam.io.source;

import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Routes pipeline execution to the correct source connector based on
 * {@link FrameworkOptions#getSourceType()}.
 *
 * <p>Stateless factory — never serialized. Runs only in the driver JVM.
 */
public final class SourceRouter {

    private SourceRouter() {}

    /**
     * Reads from the configured source and returns a {@code PCollection<Row>}.
     * Uses {@code pipeline.apply()} so the source appears as a labelled node
     * in the Dataflow UI.
     */
    public static PCollection<Row> route(Pipeline pipeline, FrameworkOptions options) {
        return switch (options.getSourceType()) {
            case GCS    -> pipeline.apply("Source-GCS",    new GcsSourceTransform(options));
            case BQ     -> pipeline.apply("Source-BQ",     new BigQuerySourceTransform(options));
            case PUBSUB -> pipeline.apply("Source-PubSub", new PubSubSourceTransform(options));
        };
    }
}
