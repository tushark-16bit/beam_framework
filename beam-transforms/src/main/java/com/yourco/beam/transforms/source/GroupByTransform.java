package com.yourco.beam.transforms.source;

import com.yourco.beam.model.AggregationConfig;
import com.yourco.beam.model.SourceTransformConfig;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Groups rows by a set of fields and applies aggregations (SUM, COUNT, AVG, MIN, MAX).
 *
 * <h2>Input</h2>
 * Any {@code PCollection<Row>} with a declared schema. The schema must contain all
 * fields listed in {@link SourceTransformConfig#groupByFields} and all fields referenced
 * by the {@link SourceTransformConfig#aggregations}.
 *
 * <h2>Output schema</h2>
 * Group-by key fields (STRING, preserving original values) + one field per aggregation
 * (DOUBLE for SUM/AVG/MIN/MAX, INT64 for COUNT).
 *
 * <h2>Beam implementation</h2>
 * <ol>
 *   <li>{@code MapElements} — extract group key as a pipe-delimited string of field values.</li>
 *   <li>{@code GroupByKey} — Beam groups all KV pairs with the same key.</li>
 *   <li>{@code ParDo(AggregateDoFn)} — iterates the group, computes aggregations, emits one Row.</li>
 * </ol>
 *
 * <p>Note: uses a string key to avoid schema complexity on the key side. This is efficient
 * for typical group-by cardinalities (< a few million groups).
 */
public final class GroupByTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

    private final List<String> groupByFields;
    private final List<AggregationConfig> aggregations;
    private final String label;

    public GroupByTransform(SourceTransformConfig config, String sourceLabel) {
        this.groupByFields = config.groupByFields;
        this.aggregations  = config.aggregations;
        this.label         = sourceLabel;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        // Build the output schema
        Schema outputSchema = buildOutputSchema();

        // Step 1: key each row by the group fields (pipe-delimited string key)
        PCollection<KV<String, Row>> keyed = input.apply(
            "KeyByGroupFields-" + label,
            MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                                                 TypeDescriptor.of(Row.class)))
                       .via((SerializableFunction<Row, KV<String, Row>>) row ->
                           KV.of(extractGroupKey(row, groupByFields), row)));

        // Step 2: group
        PCollection<KV<String, Iterable<Row>>> grouped = keyed.apply(
            "GroupByKey-" + label, GroupByKey.create());

        // Step 3: aggregate
        return grouped.apply("Aggregate-" + label,
                ParDo.of(new AggregateDoFn(groupByFields, aggregations, outputSchema)))
            .setRowSchema(outputSchema);
    }

    // ── Schema builder ────────────────────────────────────────────────────────

    private Schema buildOutputSchema() {
        Schema.Builder schemaBuilder = Schema.builder();
        for (String field : groupByFields) {
            schemaBuilder.addStringField(field);
        }
        for (AggregationConfig agg : aggregations) {
            if (AggregationConfig.COUNT.equals(agg.function)) {
                schemaBuilder.addInt64Field(agg.outputField);
            } else {
                schemaBuilder.addDoubleField(agg.outputField);
            }
        }
        return schemaBuilder.build();
    }

    // ── Key extractor ─────────────────────────────────────────────────────────

    static String extractGroupKey(Row row, List<String> fields) {
        StringBuilder key = new StringBuilder();
        for (String field : fields) {
            if (key.length() > 0) key.append("|");
            Object val = row.getValue(field);
            key.append(val != null ? val.toString() : "");
        }
        return key.toString();
    }

    // ── Aggregation DoFn ──────────────────────────────────────────────────────

    public static final class AggregateDoFn extends DoFn<KV<String, Iterable<Row>>, Row> {

        private final List<String> groupByFields;
        private final List<AggregationConfig> aggregations;
        private final Schema outputSchema;

        public AggregateDoFn(List<String> groupByFields,
                             List<AggregationConfig> aggregations,
                             Schema outputSchema) {
            this.groupByFields = groupByFields;
            this.aggregations  = aggregations;
            this.outputSchema  = outputSchema;
        }

        @ProcessElement
        public void processElement(@Element KV<String, Iterable<Row>> kv,
                                   OutputReceiver<Row> out) {
            String key = kv.getKey();
            Iterable<Row> rows = kv.getValue();

            // Split the pipe-delimited key back into field values
            String[] keyParts = key.split("\\|", -1);

            // Accumulators per aggregation
            double[] sums  = new double[aggregations.size()];
            double[] mins  = new double[aggregations.size()];
            double[] maxes = new double[aggregations.size()];
            long[]   counts = new long[aggregations.size()];
            boolean[] initialized = new boolean[aggregations.size()];

            for (Row row : rows) {
                for (int i = 0; i < aggregations.size(); i++) {
                    AggregationConfig agg = aggregations.get(i);
                    counts[i]++;
                    if (!AggregationConfig.COUNT.equals(agg.function)) {
                        double val = toDouble(row, agg.field);
                        sums[i] += val;
                        if (!initialized[i]) {
                            mins[i] = val;
                            maxes[i] = val;
                            initialized[i] = true;
                        } else {
                            mins[i]  = Math.min(mins[i],  val);
                            maxes[i] = Math.max(maxes[i], val);
                        }
                    }
                }
            }

            // Build output row
            Row.Builder rowBuilder = Row.withSchema(outputSchema);
            for (int i = 0; i < groupByFields.size() && i < keyParts.length; i++) {
                rowBuilder.addValue(keyParts[i]);
            }
            for (int i = 0; i < aggregations.size(); i++) {
                AggregationConfig agg = aggregations.get(i);
                switch (agg.function) {
                    case AggregationConfig.SUM   -> rowBuilder.addValue(sums[i]);
                    case AggregationConfig.COUNT -> rowBuilder.addValue(counts[i]);
                    case AggregationConfig.AVG   -> rowBuilder.addValue(counts[i] > 0 ? sums[i] / counts[i] : 0.0);
                    case AggregationConfig.MIN   -> rowBuilder.addValue(initialized[i] ? mins[i]  : 0.0);
                    case AggregationConfig.MAX   -> rowBuilder.addValue(initialized[i] ? maxes[i] : 0.0);
                    default -> rowBuilder.addValue(0.0);
                }
            }
            out.output(rowBuilder.build());
        }

        private static double toDouble(Row row, String field) {
            Object val = row.getValue(field);
            if (val instanceof Number n) return n.doubleValue();
            if (val instanceof String s) {
                try { return Double.parseDouble(s); } catch (NumberFormatException e) { return 0.0; }
            }
            return 0.0;
        }
    }
}
