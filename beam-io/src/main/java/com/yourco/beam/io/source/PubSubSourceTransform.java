package com.yourco.beam.io.source;

import com.yourco.beam.model.Schemas;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Reads messages from a Pub/Sub subscription and wraps each payload as a
 * single-field {@link Row} using {@link Schemas#RAW_JSON}.
 *
 * <p>Set {@code --pubSubSubscription=projects/my-project/subscriptions/my-sub}.
 *
 * <h2>C5 fix</h2>
 * The previously untyped {@code msg -> ...} lambda is replaced by a named
 * {@code static final} {@link SerializableFunction} to ensure safe serialization
 * across JVM versions and Beam's custom worker class loaders.
 */
public final class PubSubSourceTransform extends PTransform<PBegin, PCollection<Row>> {

    private static final long serialVersionUID = 1L;

    private final String subscription;

    public PubSubSourceTransform(FrameworkOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        this.subscription = options.getPubSubSubscription();
        validateOptions();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        return input
                .apply("ReadFrom-PubSub",
                        PubsubIO.readMessages().fromSubscription(subscription))
                .apply("WrapAsRow", MapElements
                        .into(TypeDescriptor.of(Row.class))
                        .via(new MessageToRowFn()))
                .setRowSchema(Schemas.RAW_JSON);
    }

    private void validateOptions() {
        if (subscription == null || subscription.isBlank()) {
            throw new IllegalArgumentException(
                "sourceType=PUBSUB requires --pubSubSubscription");
        }
    }

    /** Named static SerializableFunction — replaces the previously untyped lambda. */
    private static final class MessageToRowFn
            implements SerializableFunction<PubsubMessage, Row> {

        private static final long serialVersionUID = 1L;

        @Override
        public Row apply(PubsubMessage message) {
            String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
            return Row.withSchema(Schemas.RAW_JSON)
                    .addValue(payload)
                    .build();
        }
    }
}
