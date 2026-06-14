package com.yourco.beam.transforms.source;

import com.yourco.beam.model.SourceTransformConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Sorts rows by a set of fields within each Beam bundle.
 *
 * <h2>Important: this is per-bundle ordering, not global ordering</h2>
 * Beam processes data in parallel bundles across worker machines. This transform
 * buffers and sorts all rows <em>within a single bundle</em> but cannot guarantee
 * a globally sorted output file or table.
 *
 * <h2>When you need global ordering</h2>
 * Use one of these approaches instead:
 * <ul>
 *   <li>Create a BigQuery view with {@code ORDER BY} on the output table.</li>
 *   <li>Use {@code Reshuffle.viaRandomKey()} to collapse to one shard, then sort —
 *       this is extremely expensive for large data sets and should be avoided.</li>
 *   <li>Sort within the downstream reporting query rather than in the pipeline.</li>
 * </ul>
 *
 * <p>For most use cases in this framework (downloading raw source data to BQ),
 * global sort is not needed — the ORDER BY can be applied at query time.
 */
public final class SortByTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

    private static final Logger LOG = LoggerFactory.getLogger(SortByTransform.class);

    private final List<String> sortByFields;
    private final boolean descending;
    private final String label;

    public SortByTransform(SourceTransformConfig config, String sourceLabel) {
        this.sortByFields = config.sortByFields;
        this.descending   = config.sortDescending;
        this.label        = sourceLabel;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        LOG.warn("SortByTransform ({}) applies per-bundle ordering only — output is NOT globally sorted. "
                 + "Use an ORDER BY in your downstream query for deterministic sort order.", label);

        return input.apply("SortWithinBundle-" + label,
            ParDo.of(new BundleSortDoFn(sortByFields, descending)));
    }

    // ── Bundle-level sort DoFn ────────────────────────────────────────────────

    public static final class BundleSortDoFn extends DoFn<Row, Row> {

        private final List<String> sortFields;
        private final boolean descending;

        // Buffer all rows in the bundle, then sort and emit in @FinishBundle.
        // transient: not serialized; recreated on the worker.
        private transient List<Row> buffer;

        public BundleSortDoFn(List<String> sortFields, boolean descending) {
            this.sortFields  = sortFields;
            this.descending  = descending;
        }

        @StartBundle
        public void startBundle() {
            buffer = new ArrayList<>();
        }

        @ProcessElement
        public void processElement(@Element Row row) {
            buffer.add(row);
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext ctx) {
            Comparator<Row> comparator = buildComparator(sortFields);
            if (descending) comparator = comparator.reversed();
            buffer.sort(comparator);
            for (Row row : buffer) {
                ctx.output(row, Instant.now(), GlobalWindow.INSTANCE);
            }
            buffer = null;
        }

        private static Comparator<Row> buildComparator(List<String> fields) {
            Comparator<Row> comparator = null;
            for (String field : fields) {
                Comparator<Row> fieldComp = Comparator.comparing(
                    row -> {
                        Object val = row.getValue(field);
                        return val != null ? val.toString() : "";
                    });
                comparator = (comparator == null) ? fieldComp : comparator.thenComparing(fieldComp);
            }
            return comparator != null ? comparator : Comparator.comparing(r -> "");
        }
    }
}
