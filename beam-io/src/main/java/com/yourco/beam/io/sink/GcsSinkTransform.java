package com.yourco.beam.io.sink;

import com.yourco.beam.io.util.JsonUtils;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Objects;

/**
 * Writes a {@code PCollection<Row>} to GCS as newline-delimited JSON.
 *
 * <h2>I3 fix</h2>
 * Serialization now uses {@link JsonUtils#rowToJson(Row)}, which correctly
 * handles field types: numerics and booleans are unquoted, strings are quoted
 * and escaped, nulls are emitted as JSON {@code null}.
 *
 * <p>Set {@code --gcsSinkPath=gs://bucket/output/}.
 */
public final class GcsSinkTransform extends PTransform<PCollection<Row>, PDone> {

    private static final long serialVersionUID = 1L;

    private final String outputPath;

    public GcsSinkTransform(FrameworkOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        this.outputPath = Objects.requireNonNull(
                options.getGcsSinkPath(),
                "sinkType=GCS requires --gcsSinkPath, e.g. gs://bucket/output/");
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        return input
                .apply("Row-to-JSON", MapElements
                        .into(TypeDescriptors.strings())
                        .via(new RowToJsonFn()))
                .apply("WriteTo-GCS", TextIO.write()
                        .to(outputPath)
                        .withSuffix(".json")
                        .withNumShards(0));   // 0 = Beam chooses optimal shard count
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
