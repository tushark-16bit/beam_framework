package com.yourco.beam.io.sink;

import com.yourco.beam.model.FailedRecord;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Objects;

/**
 * Writes {@link FailedRecord}s to a GCS dead-letter path as newline-delimited JSON.
 *
 * <p>Each line is a JSON object with fields: {@code payload}, {@code errorMessage},
 * {@code errorClass}, {@code attemptCount}, {@code failedAtUtc}.
 *
 * <p>Configured by {@code --deadLetterSink=gs://bucket/dlq/}.
 *
 * <h2>C2 fix</h2>
 * This transform is applied by {@link com.yourco.beam.runner.PipelineFactory}
 * to the flattened collection of all dead-letter outputs from every transform
 * in the chain, completing the end-to-end DLQ path.
 */
public final class DeadLetterSinkTransform extends PTransform<PCollection<FailedRecord>, PDone> {

    private static final long serialVersionUID = 1L;

    private final String dlqPath;

    public DeadLetterSinkTransform(FrameworkOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        this.dlqPath = Objects.requireNonNull(
                options.getDeadLetterSink(),
                "--deadLetterSink is required when retryPolicy != NONE");
    }

    @Override
    public PDone expand(PCollection<FailedRecord> input) {
        return input
                .apply("FailedRecord-to-JSON", MapElements
                        .into(TypeDescriptors.strings())
                        .via(new FailedRecordToJsonFn()))
                .apply("WriteDLQ-to-GCS", TextIO.write()
                        .to(dlqPath)
                        .withSuffix(".json")
                        .withNumShards(0));
    }

    private static final class FailedRecordToJsonFn
            implements SerializableFunction<FailedRecord, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public String apply(FailedRecord r) {
            return "{"
                   + "\"payload\":"      + quoted(r.getPayload())      + ","
                   + "\"errorMessage\":" + quoted(r.getErrorMessage())  + ","
                   + "\"errorClass\":"   + quoted(r.getErrorClass())    + ","
                   + "\"attemptCount\":" + r.getAttemptCount()          + ","
                   + "\"failedAtUtc\":"  + quoted(r.getFailedAtUtc())
                   + "}";
        }

        private static String quoted(String value) {
            if (value == null) return "null";
            return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
    }
}
