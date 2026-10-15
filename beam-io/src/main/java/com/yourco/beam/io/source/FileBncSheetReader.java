package com.yourco.beam.io.source;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.yourco.beam.model.FileSourceConfig;

import java.time.LocalDate;

/**
 * Downloads a FILE source's workbook from GCS and reads its Balance &amp; Control sheet — for
 * {@code PostDownloadFinalizeTransform}, which runs on a worker after the load and so cannot be
 * handed the control values by the (already finished) read step. The same file the source read is
 * fetched again; the path is resolved exactly as the read did
 * ({@link FileSourceAdapter#resolvePath}), from the run's own dates.
 *
 * <p>Only {@code EXCEL} files have a BnC sheet; calling this for another type is an error the
 * caller reports as a failed check (legacy: "CSV + Excel BnC Sheet: FAIL regardless of the flag").
 */
public final class FileBncSheetReader {

    private FileBncSheetReader() {}

    /**
     * @throws FileSourceAdapter.FileSourceException when the file or its BnC sheet cannot be read
     */
    public static FileSourceAdapter.BncSheet read(FileSourceConfig config, String periodId,
                                                  LocalDate runDate) {
        if (!"EXCEL".equals(config.fileType)) {
            throw new FileSourceAdapter.FileSourceException(
                "bnc.type EXCEL_SHEET needs an Excel file, but file_details.type is "
                + config.fileType, null);
        }
        String gcsUri = FileSourceAdapter.resolvePath(config, periodId, runDate);
        if (!gcsUri.startsWith("gs://") || gcsUri.indexOf('/', 5) < 0) {
            throw new FileSourceAdapter.FileSourceException("Not a GCS object path: " + gcsUri, null);
        }
        String withoutScheme = gcsUri.substring(5);
        int slash = withoutScheme.indexOf('/');
        Storage storage = StorageOptions.getDefaultInstance().getService();
        byte[] bytes = storage.readAllBytes(BlobId.of(withoutScheme.substring(0, slash),
                                                      withoutScheme.substring(slash + 1)));
        if (bytes == null) {
            throw new FileSourceAdapter.FileSourceException("File not found in GCS: " + gcsUri, null);
        }
        return FileSourceAdapter.readBncSheet(bytes, config.sheetIndex + 1);
    }
}
