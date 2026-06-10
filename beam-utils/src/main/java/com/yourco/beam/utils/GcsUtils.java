package com.yourco.beam.utils;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for common GCS operations needed at pipeline-assembly time or
 * inside non-Beam utility code.
 *
 * <h2>When to use this vs Beam's built-in GCS IOs</h2>
 * Use {@link com.yourco.beam.io.source.GcsSourceTransform} and
 * {@link com.yourco.beam.io.sink.GcsSinkTransform} for reading/writing
 * data <em>inside</em> the pipeline. Use this class for driver-JVM operations:
 * pre-flight path checks, writing metadata/manifest files, checking output
 * completeness after a run, or sending small control messages.
 *
 * <h2>Authentication</h2>
 * Uses Application Default Credentials (ADC). On GCP (Dataflow, Cloud Composer)
 * the service account is used automatically. Locally, run:
 * {@code gcloud auth application-default login}
 */
public final class GcsUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GcsUtils.class);

    private GcsUtils() {}

    /**
     * Returns {@code true} if at least one object exists under the given GCS prefix.
     *
     * <p>Useful as a pre-flight check before submitting a pipeline:
     * <pre>{@code
     * if (!GcsUtils.pathHasFiles("gs://my-bucket/input/2024-01-15/")) {
     *     throw new IllegalStateException("No input files found — aborting pipeline");
     * }
     * }</pre>
     *
     * @param gcsPath full GCS path, e.g. {@code gs://bucket/prefix/}
     */
    public static boolean pathHasFiles(String gcsPath) {
        BucketAndPrefix bp = parse(gcsPath);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        return storage.list(bp.bucket,
                Storage.BlobListOption.prefix(bp.prefix),
                Storage.BlobListOption.pageSize(1))
                .iterateAll()
                .iterator()
                .hasNext();
    }

    /**
     * Lists all object names (full GCS paths) under a given prefix.
     *
     * @param gcsPath prefix to list, e.g. {@code gs://bucket/output/}
     * @return list of full {@code gs://} paths, empty if nothing found
     */
    public static List<String> listFiles(String gcsPath) {
        BucketAndPrefix bp = parse(gcsPath);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        List<String> results = new ArrayList<>();
        storage.list(bp.bucket, Storage.BlobListOption.prefix(bp.prefix))
               .iterateAll()
               .forEach(blob -> results.add("gs://" + bp.bucket + "/" + blob.getName()));
        return results;
    }

    /**
     * Writes a small UTF-8 text file to GCS. Useful for writing pipeline
     * manifests, completion markers ({@code _SUCCESS}), or metadata files.
     *
     * <pre>{@code
     * GcsUtils.writeTextFile(
     *     "gs://my-bucket/output/2024-01-15/_metadata.json",
     *     "{\"runDate\":\"2024-01-15\",\"rowCount\":42000}"
     * );
     * }</pre>
     *
     * @param gcsPath destination GCS path
     * @param content UTF-8 string content to write
     */
    public static void writeTextFile(String gcsPath, String content) {
        BucketAndPrefix bp = parse(gcsPath);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bp.bucket, bp.prefix))
                .setContentType("text/plain; charset=utf-8")
                .build();
        storage.create(blobInfo, content.getBytes(StandardCharsets.UTF_8));
        LOG.info("Written {} byte(s) to {}", content.length(), gcsPath);
    }

    /**
     * Reads a small text file from GCS and returns its content as a UTF-8 string.
     * Not suitable for large files — intended for config or metadata files only.
     *
     * @param gcsPath path to the GCS object
     * @return file content as a string
     * @throws IllegalArgumentException if the object does not exist
     */
    public static String readTextFile(String gcsPath) {
        BucketAndPrefix bp = parse(gcsPath);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Blob blob = storage.get(BlobId.of(bp.bucket, bp.prefix));
        if (blob == null) {
            throw new IllegalArgumentException("GCS object not found: " + gcsPath);
        }
        return new String(blob.getContent(), StandardCharsets.UTF_8);
    }

    /**
     * Deletes all objects under a GCS prefix. Useful for clearing output paths
     * before a re-run to ensure idempotency (when not using WRITE_TRUNCATE).
     *
     * @param gcsPath prefix to clear
     * @return number of objects deleted
     */
    public static int deletePrefix(String gcsPath) {
        BucketAndPrefix bp = parse(gcsPath);
        Storage storage = StorageOptions.getDefaultInstance().getService();
        int count = 0;
        for (Blob blob : storage.list(bp.bucket, Storage.BlobListOption.prefix(bp.prefix))
                                .iterateAll()) {
            blob.delete();
            count++;
        }
        LOG.info("Deleted {} object(s) under {}", count, gcsPath);
        return count;
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /** Parsed bucket + prefix pair. */
    private record BucketAndPrefix(String bucket, String prefix) {}

    /**
     * Parses a {@code gs://bucket/prefix/path} string into bucket and prefix components.
     */
    private static BucketAndPrefix parse(String gcsPath) {
        if (gcsPath == null || !gcsPath.startsWith("gs://")) {
            throw new IllegalArgumentException(
                "Invalid GCS path (must start with gs://): " + gcsPath);
        }
        String withoutScheme = gcsPath.substring(5); // strip "gs://"
        int slash = withoutScheme.indexOf('/');
        if (slash < 0) {
            return new BucketAndPrefix(withoutScheme, "");
        }
        return new BucketAndPrefix(
                withoutScheme.substring(0, slash),
                withoutScheme.substring(slash + 1));
    }
}
