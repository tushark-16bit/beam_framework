package com.yourco.beam.transforms;

import com.yourco.beam.model.FailedRecord;
import com.yourco.beam.options.FrameworkOptions;
import com.yourco.beam.transform.BeamTransform;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * Drops any {@link Row} that contains at least one null field value.
 *
 * <p>Token: {@code filter-nulls}
 *
 * <h2>Fixes applied</h2>
 * <ul>
 *   <li>Now implements the updated {@link BeamTransform} contract — returns
 *       {@link PCollectionTuple} with {@link BeamTransform#SUCCESS_TAG} and
 *       {@link BeamTransform#DEAD_LETTER_TAG} so dropped rows are auditable.</li>
 *   <li>Increments a {@link Metrics} counter for every dropped row so drops
 *       appear in Dataflow's metrics panel and Cloud Monitoring.</li>
 * </ul>
 */
public final class FilterNullsTransform implements BeamTransform {

    private static final long serialVersionUID = 1L;

    @Override
    public String name() {
        return "filter-nulls";
    }

    @Override
    public PTransform<PCollection<Row>, PCollectionTuple> toComposite(FrameworkOptions options) {
        return new FilterNullsComposite();
    }

    // ── Composite ────────────────────────────────────────────────────────────

    public static final class FilterNullsComposite
            extends PTransform<PCollection<Row>, PCollectionTuple> {

        @Override
        public PCollectionTuple expand(PCollection<Row> input) {
            PCollectionTuple result = input.apply("DropNullRows",
                    ParDo.of(new FilterNullsDoFn())
                         .withOutputTags(SUCCESS_TAG, TupleTagList.of(DEAD_LETTER_TAG)));

            // Propagate schema on the success branch
            result.get(SUCCESS_TAG).setRowSchema(input.getSchema());
            return result;
        }
    }

    // ── DoFn ─────────────────────────────────────────────────────────────────

    public static final class FilterNullsDoFn extends DoFn<Row, Row> {

        // Counter visible in Dataflow UI under namespace "filter-nulls"
        private final Counter nullRowsDropped =
                Metrics.counter("filter-nulls", "rows_dropped_null_field");

        @ProcessElement
        public void processElement(@Element Row row, MultiOutputReceiver out) {
            boolean hasNull = row.getSchema().getFields().stream()
                    .anyMatch(field -> row.getValue(field.getName()) == null);

            if (hasNull) {
                nullRowsDropped.inc();
                // Route to DLQ so dropped rows are auditable and replayable
                out.get(DEAD_LETTER_TAG).output(
                        FailedRecord.ofRaw(row.toString(),
                                new IllegalArgumentException("Row contains null field(s)"), 0));
            } else {
                out.get(SUCCESS_TAG).output(row);
            }
        }
    }
}
