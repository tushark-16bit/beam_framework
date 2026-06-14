package com.yourco.beam.io.source;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.yourco.beam.model.FileSourceConfig;
import com.yourco.beam.model.Schemas;
import com.yourco.beam.model.SourceConfig;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.List;

/**
 * Beam source transform that reads CSV or Excel files from GCS via {@link FileSourceAdapter}.
 *
 * <h2>Design: thin Beam wrapper around a testable adapter</h2>
 * All file-parsing logic lives in {@link FileSourceAdapter}. This class handles:
 * <ul>
 *   <li>Path resolution (substituting date/periodId placeholders)</li>
 *   <li>GCS download — transient {@link Storage} client created in {@code @Setup}</li>
 *   <li>Translating parsed rows into Beam {@link Row} objects</li>
 * </ul>
 *
 * <h2>Execution flow</h2>
 * <pre>
 *   Create.of(sourceConfig)            → single-element trigger PCollection
 *   → FileDoFn.@ProcessElement         → downloads file from GCS, parses CSV/Excel,
 *                                         emits one Row per data row
 *   → .setRowSchema(Schemas.RAW_JSON)  → downstream sees "raw_json STRING" schema
 * </pre>
 *
 * <h2>Why download then parse (not TextIO)?</h2>
 * TextIO works well for CSV-only, line-by-line reads. Excel files require the full
 * byte content (POI needs a seekable stream). Downloading to a byte array first
 * handles both formats uniformly and allows the same DoFn to serve both.
 */
public final class FileSourceTransform extends PTransform<PBegin, PCollection<Row>> {

    private static final long serialVersionUID = 1L;

    private final SourceConfig sourceConfig;
    private final String periodId;
    private final String runDateIso;

    public FileSourceTransform(SourceConfig sourceConfig, String periodId, LocalDate runDate) {
        this.sourceConfig = sourceConfig;
        this.periodId     = periodId;
        // LocalDate.toString() returns yyyy-MM-dd (ISO-8601) — no need for DateUtils here
        this.runDateIso   = runDate.toString();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
        return input
            .apply("CreateFileTrigger-" + sourceConfig.datasourceName,
                   Create.of(sourceConfig.datasourceName))
            .apply("ReadFile-" + sourceConfig.datasourceName,
                   ParDo.of(new FileDoFn(sourceConfig.fileConfig, periodId, runDateIso)))
            .setRowSchema(Schemas.RAW_JSON);
    }

    // ── DoFn — thin Beam wrapper around FileSourceAdapter ───────────────────

    private static final class FileDoFn extends DoFn<String, Row> {

        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(FileDoFn.class);

        // Serialized with the DoFn — all fields must be Serializable
        private final FileSourceConfig fileConfig;
        private final String periodId;
        private final String runDateIso;

        // Transient — recreated per worker in @Setup
        private transient Storage gcsClient;

        FileDoFn(FileSourceConfig fileConfig, String periodId, String runDateIso) {
            this.fileConfig  = fileConfig;
            this.periodId    = periodId;
            this.runDateIso  = runDateIso;
        }

        @Setup
        public void setup() {
            gcsClient = StorageOptions.getDefaultInstance().getService();
        }

        @ProcessElement
        public void processElement(@Element String datasourceName, OutputReceiver<Row> out) {
            LocalDate runDate = LocalDate.parse(runDateIso);
            String gcsPath = FileSourceAdapter.resolvePath(fileConfig, periodId, runDate);
            LOG.info("Downloading file: {}", gcsPath);

            byte[] fileBytes = downloadFromGcs(gcsPath);
            LOG.info("Downloaded {} bytes from {}", fileBytes.length, gcsPath);

            List<String> jsonRows = switch (fileConfig.fileType) {
                case "CSV"   -> FileSourceAdapter.parseCsv(fileBytes, fileConfig);
                case "EXCEL" -> FileSourceAdapter.parseExcel(fileBytes, fileConfig);
                default -> throw new IllegalArgumentException(
                    "Unsupported file type: " + fileConfig.fileType + ". Supported: CSV, EXCEL");
            };

            LOG.info("Parsed {} rows from {} ({})", jsonRows.size(), gcsPath, fileConfig.fileType);
            for (String json : jsonRows) {
                out.output(Row.withSchema(Schemas.RAW_JSON).addValue(json).build());
            }
        }

        @Teardown
        public void teardown() {
            gcsClient = null;
        }

        private byte[] downloadFromGcs(String gcsUri) {
            // Parse gs://bucket/path/to/file into bucket and object name
            if (!gcsUri.startsWith("gs://")) {
                throw new IllegalArgumentException("Expected GCS URI starting with gs://, got: " + gcsUri);
            }
            String withoutScheme = gcsUri.substring(5);
            int slashIndex = withoutScheme.indexOf('/');
            if (slashIndex < 0) {
                throw new IllegalArgumentException("GCS URI has no object path: " + gcsUri);
            }
            String bucket = withoutScheme.substring(0, slashIndex);
            String object = withoutScheme.substring(slashIndex + 1);

            byte[] bytes = gcsClient.readAllBytes(BlobId.of(bucket, object));
            if (bytes == null) {
                throw new FileSourceAdapter.FileSourceException(
                    "File not found in GCS: " + gcsUri, null);
            }
            return bytes;
        }
    }
}
