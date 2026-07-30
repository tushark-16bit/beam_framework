package com.yourco.beam.io.sink;

import com.google.api.services.bigquery.model.TableRow;
import com.yourco.beam.io.util.JsonUtils;
import com.yourco.beam.options.FrameworkOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Writes a {@code PCollection<Row>} to the {@code DaRec} record table as paginated JSON array blobs.
 *
 * <h2>Storage format</h2>
 * All rows from one run are collected globally, then written as pages of at most
 * {@link PaginateAndBuildDoFn#PAGE_SIZE} rows per DaRec row. Each DaRec row stores one page:
 * {@code row_da_json_tx} = {@code [{...},{...},...]} — a JSON array of all source rows in the page.
 * For a 600-row source that means 3 DaRec rows: rows 1-250, 251-500, 501-600.
 *
 * <h2>Return value</h2>
 * Returns a {@code PCollection<Long>} with exactly one element — the <em>total number of source rows</em>
 * across all pages. This element is only emitted after all streaming inserts (successful and failed)
 * are confirmed, making it a safe {@link Wait} signal for {@code PostDownloadFinalizeTransform}.
 *
 * <h2>Why streaming inserts</h2>
 * Streaming inserts commit per-row and are immediately visible in {@code SELECT} queries.
 * {@code PostDownloadFinalizeTransform} queries DaRec (via {@code BigQueryDataSourceRecordAdapter})
 * from inside a Beam worker immediately after this signal fires — streaming inserts guarantee the
 * rows are already visible at that point. FILE_LOADS would not.
 *
 * <h2>DaRec schema</h2>
 * <pre>{@code
 * CREATE TABLE pipeline_metadata.DaRec (
 *   rec_id         STRING    NOT NULL,
 *   da_id          INT64     NOT NULL,
 *   page_no        INT64     NOT NULL,
 *   row_da_json_tx STRING,
 *   load_dt        DATE      NOT NULL,
 *   lst_updt_ts    TIMESTAMP NOT NULL
 * ) PARTITION BY load_dt;
 * }</pre>
 */
public final class DataSourceRecordSinkTransform extends PTransform<PCollection<Row>, PCollection<Long>> {

    private static final long serialVersionUID = 1L;

    private final String              recordTableRef; // project:dataset.table — BigQueryIO format
    private final ValueProvider<Long> daId;
    private final String              loadDt;         // shared across all rows in this run
    private final String              lstUpdtTs;      // shared across all rows in this run

    public DataSourceRecordSinkTransform(FrameworkOptions options, long daId) {
        this(options, ValueProvider.StaticValueProvider.of(daId));
    }

    public DataSourceRecordSinkTransform(FrameworkOptions options, ValueProvider<Long> daId) {
        String project = options.getCheckpointBqProject() != null
                         && !options.getCheckpointBqProject().isBlank()
                         ? options.getCheckpointBqProject() : options.getProject();
        this.recordTableRef = project + ":" + options.getCheckpointBqDataset()
                            + "." + options.getDaRecTable();
        this.daId      = daId;
        this.loadDt    = LocalDate.now(ZoneOffset.UTC).toString();
        this.lstUpdtTs = Instant.now().toString();
    }

    @Override
    public PCollection<Long> expand(PCollection<Row> input) {
        // Step 1: Convert each Row to its JSON string representation immediately.
        // Working with String (StringUtf8Coder) rather than Row avoids schema-coder
        // propagation issues through GroupByKey.
        PCollection<String> jsonRows = input
            .apply("RowsToJson", MapElements
                .into(TypeDescriptors.strings())
                .via(new RowToJsonFn()));

        // Step 2: Assign a constant group key so GroupByKey collects ALL rows into one bundle.
        PCollection<KV<String, String>> keyed = jsonRows
            .apply("KeyAll", MapElements
                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(new AddConstantKeyFn()));

        // Step 3: Group globally.
        PCollection<KV<String, Iterable<String>>> grouped = keyed
            .apply("GroupAll", GroupByKey.create());

        // Step 4: Chunk into pages; emit one TableRow per page (main) + total row count (side).
        PCollectionTuple paginatedOut = grouped
            .apply("PaginateAndBuild", ParDo
                .of(new PaginateAndBuildDoFn(daId, loadDt, lstUpdtTs))
                .withOutputTags(PaginateAndBuildDoFn.PAGES_TAG,
                                TupleTagList.of(PaginateAndBuildDoFn.TOTAL_ROWS_TAG)));

        PCollection<TableRow> pageRows   = paginatedOut.get(PaginateAndBuildDoFn.PAGES_TAG);
        PCollection<Long>     totalRows  = paginatedOut.get(PaginateAndBuildDoFn.TOTAL_ROWS_TAG);

        // Step 5: Write pages to DaRec using streaming inserts.
        WriteResult writeResult = pageRows
            .apply("WriteTo-DaRec", BigQueryIO.writeTableRows()
                .to(recordTableRef)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withExtendedErrorInfo()
                .withSuccessfulInsertsPropagation(true));

        // Step 6: Return total source-row count held until all streaming inserts complete.
        // PostDownloadFinalizeTransform.FinalizeDoFn receives this Long as its input element.
        return totalRows
            .apply("WaitForDaRecWrites",
                   Wait.on(writeResult.getSuccessfulInserts(),
                           writeResult.getFailedInsertsWithErr()));
    }

    // ── SerializableFunctions ─────────────────────────────────────────────────

    private static final class RowToJsonFn implements SerializableFunction<Row, String> {
        private static final long serialVersionUID = 1L;
        @Override public String apply(Row row) { return JsonUtils.rowToJson(row); }
    }

    private static final class AddConstantKeyFn implements SerializableFunction<String, KV<String, String>> {
        private static final long serialVersionUID = 1L;
        @Override public KV<String, String> apply(String json) { return KV.of("__all__", json); }
    }

    // ── Paginator DoFn ────────────────────────────────────────────────────────

    static final class PaginateAndBuildDoFn
            extends DoFn<KV<String, Iterable<String>>, TableRow> {

        private static final long serialVersionUID = 1L;

        /** Maximum source rows stored per DaRec row. */
        static final int PAGE_SIZE = 250;

        /** Main output: one TableRow per page. */
        static final TupleTag<TableRow> PAGES_TAG      = new TupleTag<TableRow>() {};
        /** Side output: one Long element = total source rows across all pages. */
        static final TupleTag<Long>     TOTAL_ROWS_TAG = new TupleTag<Long>() {};

        private final ValueProvider<Long> daId;
        private final String              loadDt;
        private final String              lstUpdtTs;

        PaginateAndBuildDoFn(ValueProvider<Long> daId, String loadDt, String lstUpdtTs) {
            this.daId      = daId;
            this.loadDt    = loadDt;
            this.lstUpdtTs = lstUpdtTs;
        }

        @ProcessElement
        public void processElement(
                @Element KV<String, Iterable<String>> element,
                MultiOutputReceiver out) {

            // Materialize all JSON strings — required to paginate.
            List<String> allJsonRows = new ArrayList<>();
            for (String json : element.getValue()) {
                allJsonRows.add(json);
            }

            long totalRows = allJsonRows.size();
            int  pageNo    = 1;

            for (int start = 0; start < allJsonRows.size(); start += PAGE_SIZE) {
                int end = Math.min(start + PAGE_SIZE, allJsonRows.size());
                List<String> pageSlice = allJsonRows.subList(start, end);

                // Build compact JSON array for this page: [{...},{...},...]
                String pageJson = "[" + String.join(",", pageSlice) + "]";

                out.get(PAGES_TAG).output(new TableRow()
                    .set("rec_id",         UUID.randomUUID().toString())
                    .set("da_id",          daId.get())
                    .set("page_no",        pageNo)
                    .set("row_da_json_tx", pageJson)
                    .set("load_dt",        loadDt)
                    .set("lst_updt_ts",    lstUpdtTs));

                pageNo++;
            }

            // Emit total source row count exactly once (one element, not one per page).
            out.get(TOTAL_ROWS_TAG).output(totalRows);
        }
    }
}
