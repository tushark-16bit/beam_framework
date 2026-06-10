package com.yourco.beam.io.sink;

import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Routes the final {@code PCollection<Row>} to the correct sink connector
 * based on {@link FrameworkOptions#getSinkType()}.
 *
 * <p>Stateless factory — never serialized.
 *
 * <h2>N4 fix</h2>
 * Uses a switch <em>expression</em> so the compiler enforces exhaustiveness —
 * adding a new {@link com.yourco.beam.options.SinkType} without a case is a
 * compile error, not a silent no-op.
 */
public final class SinkRouter {

    private SinkRouter() {}

    public static void route(PCollection<Row> data, FrameworkOptions options) {
        var sink = switch (options.getSinkType()) {
            case GCS    -> new GcsSinkTransform(options);
            case BQ     -> new BigQuerySinkTransform(options);
            case PUBSUB -> new PubSubSinkTransform(options);
        };
        data.apply("Sink-" + options.getSinkType(), sink);
    }
}
