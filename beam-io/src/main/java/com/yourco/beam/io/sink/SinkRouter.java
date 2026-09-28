package com.yourco.beam.io.sink;

import com.yourco.beam.model.PipelineRunConfig;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Routes the final {@code PCollection<Row>} to the correct sink connector
 * based on the sink type in {@link PipelineRunConfig}.
 *
 * <p>Stateless factory — never serialized.
 *
 * <p>Uses a switch expression so the compiler enforces exhaustiveness —
 * adding a new {@link com.yourco.beam.options.SinkType} without a case is a
 * compile error, not a silent no-op.
 */
public final class SinkRouter {

    private SinkRouter() {}

    public static void route(PCollection<Row> data, PipelineRunConfig runConfig) {
        var sink = switch (runConfig.getSinkType()) {
            case GCS    -> new GcsSinkTransform(runConfig.getGcsSinkPath());
            case BQ     -> new BigQuerySinkTransform(runConfig.getBqSinkTable(), runConfig.getWriteDisposition());
            case PUBSUB -> new PubSubSinkTransform(runConfig.getPubSubTopic());
        };
        data.apply("Sink-" + runConfig.getSinkType(), sink);
    }
}
