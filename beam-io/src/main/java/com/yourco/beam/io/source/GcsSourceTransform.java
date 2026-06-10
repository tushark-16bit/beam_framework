package com.yourco.beam.io.source;

import com.yourco.beam.model.Schemas;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Objects;

/**
 * Reads newline-delimited JSON files from GCS and produces a {@code PCollection<Row>}.
 *
 * <p>Each line is wrapped in a single-field {@link Row} using {@link Schemas#RAW_JSON}.
 * Downstream transforms (e.g., {@code flatten-json}) parse and expand this field.
 *
 * <p>Set {@code --gcsSourcePath=gs://bucket/prefix/*.json}.
 */
public final class GcsSourceTransform extends PTransform<PBegin, PCollection<Row>> {

    private static final long serialVersionUID = 1L;

    private final String sourcePath;

    public GcsSourceTransform(FrameworkOptions options) {
        Objects.requireNonNull(options, "options must not be null");
        this.sourcePath = options.getGcsSourcePath();
        validateOptions();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        return input
                .apply("ReadFrom-GCS", TextIO.read().from(sourcePath))
                .apply("WrapAsRow", MapElements
                        .into(TypeDescriptor.of(Row.class))
                        .via(new LineToRowFn()))
                .setRowSchema(Schemas.RAW_JSON);
    }

    private void validateOptions() {
        if (sourcePath == null || sourcePath.isBlank()) {
            throw new IllegalArgumentException(
                "sourceType=GCS requires --gcsSourcePath, e.g. gs://bucket/input/*.json");
        }
    }

    /** Named static SerializableFunction — safe for Beam worker serialization. */
    private static final class LineToRowFn implements SerializableFunction<String, Row> {

        private static final long serialVersionUID = 1L;

        @Override
        public Row apply(String line) {
            return Row.withSchema(Schemas.RAW_JSON)
                    .addValue(line)
                    .build();
        }
    }
}
