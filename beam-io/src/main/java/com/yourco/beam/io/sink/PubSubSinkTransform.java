package com.yourco.beam.io.sink;

import com.yourco.beam.io.util.JsonUtils;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Objects;

/**
 * Publishes each {@link Row} as a JSON string message to a Pub/Sub topic.
 *
 * <h2>I3 fix</h2>
 * Uses shared {@link JsonUtils#rowToJson(Row)} — eliminates the duplicate
 * broken serializer that was copy-pasted from {@link GcsSinkTransform}.
 *
 * <p>Set {@code --pubSubTopic=projects/my-project/topics/my-topic}.
 */
public final class PubSubSinkTransform extends PTransform<PCollection<Row>, PDone> {

    private static final long serialVersionUID = 1L;

    private final String topic;

    public PubSubSinkTransform(FrameworkOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        this.topic = Objects.requireNonNull(
                options.getPubSubTopic(),
                "sinkType=PUBSUB requires --pubSubTopic");
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        return input
                .apply("Row-to-JSON", MapElements
                        .into(TypeDescriptors.strings())
                        .via(new RowToJsonFn()))
                .apply("PublishTo-PubSub", PubsubIO.writeStrings().to(topic));
    }

    /** Named static SerializableFunction — safe for Beam worker serialization. */
    private static final class RowToJsonFn implements SerializableFunction<Row, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String apply(Row row) {
            return JsonUtils.rowToJson(row);
        }
    }
}
