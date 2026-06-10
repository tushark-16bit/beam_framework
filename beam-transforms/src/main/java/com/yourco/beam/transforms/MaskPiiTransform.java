package com.yourco.beam.transforms;

import com.yourco.beam.model.FailedRecord;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.transform.BeamTransform;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

/**
 * Replaces the values of configured PII fields with their SHA-256 hash.
 *
 * <p>Token: {@code mask-pii}
 *
 * <h2>Fixes applied</h2>
 * <ul>
 *   <li>PII field list is now read from {@code --piiFields} (comma-separated)
 *       rather than hardcoded — any field names can be configured at runtime.</li>
 *   <li>Now implements the updated {@link BeamTransform} contract — returns
 *       {@link PCollectionTuple}.</li>
 *   <li>Rows that fail during hashing are routed to the dead-letter output.</li>
 * </ul>
 */
public final class MaskPiiTransform implements BeamTransform {

    private static final long serialVersionUID = 1L;

    @Override
    public String name() {
        return "mask-pii";
    }

    @Override
    public PTransform<PCollection<Row>, PCollectionTuple> toComposite(FrameworkOptions options) {
        // Parse the comma-separated PII field list from options (I4 fix)
        List<String> piiFields = Arrays.stream(options.getPiiFields().split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
        return new MaskPiiComposite(piiFields);
    }

    // ── Composite ────────────────────────────────────────────────────────────

    public static final class MaskPiiComposite
            extends PTransform<PCollection<Row>, PCollectionTuple> {

        private final List<String> piiFields;

        public MaskPiiComposite(List<String> piiFields) {
            this.piiFields = List.copyOf(piiFields);
        }

        @Override
        public PCollectionTuple expand(PCollection<Row> input) {
            PCollectionTuple result = input.apply("HashPiiFields",
                    ParDo.of(new MaskPiiDoFn(piiFields))
                         .withOutputTags(SUCCESS_TAG, TupleTagList.of(DEAD_LETTER_TAG)));

            result.get(SUCCESS_TAG).setRowSchema(input.getSchema());
            return result;
        }
    }

    // ── DoFn ─────────────────────────────────────────────────────────────────

    public static final class MaskPiiDoFn extends DoFn<Row, Row> {

        private final List<String> piiFields;

        private final Counter fieldsHashed =
                Metrics.counter("mask-pii", "pii_fields_hashed");

        public MaskPiiDoFn(List<String> piiFields) {
            this.piiFields = List.copyOf(piiFields);
        }

        @ProcessElement
        public void processElement(@Element Row row, MultiOutputReceiver out) {
            try {
                Schema schema = row.getSchema();
                Row.Builder builder = Row.withSchema(schema);

                for (Schema.Field field : schema.getFields()) {
                    Object value = row.getValue(field.getName());
                    if (piiFields.contains(field.getName()) && value != null) {
                        builder.addValue(sha256(value.toString()));
                        fieldsHashed.inc();
                    } else {
                        builder.addValue(value);
                    }
                }

                out.get(SUCCESS_TAG).output(builder.build());
            } catch (Exception e) {
                out.get(DEAD_LETTER_TAG).output(FailedRecord.of(row, e, 1));
            }
        }

        /** Lowercase hex SHA-256 of the input string. SHA-256 is JDK-guaranteed. */
        private static String sha256(String value) {
            try {
                byte[] hash = MessageDigest.getInstance("SHA-256")
                        .digest(value.getBytes(StandardCharsets.UTF_8));
                return HexFormat.of().formatHex(hash);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 not available", e);
            }
        }
    }
}
