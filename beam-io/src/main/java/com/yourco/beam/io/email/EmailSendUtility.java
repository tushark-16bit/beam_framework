package com.yourco.beam.io.email;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.yourco.beam.model.email.EmailAttachment;
import com.yourco.beam.io.util.GcsFileMatcher;
import com.yourco.beam.model.email.EmailParams;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Port for sending email, independent of the transport (SMTP, an internal email gateway, etc).
 *
 * <p>This repository only defines the contract. The concrete implementation is expected to live
 * outside this codebase — e.g. an organization's existing internal email-gateway client — and be
 * supplied at runtime rather than committed here. Two ways to plug one in:
 * <ul>
 *   <li>Return it from {@code EmailSendUtilities.create()} in {@code beam-runner} — the one place
 *       the implementation is declared and constructed; {@code ReportPipelineFactory} and
 *       {@code FailureNotifier} both use it. Nothing is discovered via {@code ServiceLoader}.</li>
 *   <li>Construct it directly and pass it into {@code ReportPipelineFactory}'s constructor.</li>
 * </ul>
 *
 * <h2>Driver-JVM only</h2>
 * Like {@link ReportEmailAdapter}, this is called from {@code ReportPipelineFactory} after
 * outputs are exported — never from inside a Beam worker DoFn.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * EmailParams params = utility.SetEmailParams(fromAddress, subject, toList, ccList, encrypted);
 * List<EmailAttachment> attachments = files.stream()
 *     .map(f -> new EmailAttachment(f.fileName(), utility.FetchFileFromGcs(f.gcsUri()), f.contentType()))
 *     .toList();
 * utility.CreateEmailRequest(params, bodyHtml, attachments);
 * }</pre>
 */
public interface EmailSendUtility {

    /**
     * Builds the sender/recipient envelope for one email.
     *
     * @param fromAddress    sender address
     * @param subject        email subject line (already resolved, no template tokens)
     * @param toList         recipient addresses (at least one required)
     * @param ccList         CC addresses (may be empty)
     * @param encryptedOrNot whether this email must be sent encrypted
     * @return an {@link EmailParams} to pass into {@link #CreateEmailRequest}
     */
    EmailParams SetEmailParams(String fromAddress, String subject,
                               List<String> toList, List<String> ccList,
                               boolean encryptedOrNot);

    /**
     * Builds and sends the email.
     *
     * @param emailParams      envelope built by {@link #SetEmailParams}
     * @param emailBodyHtml    HTML email body (already resolved, no template tokens)
     * @param emailAttachments file attachments; each {@link EmailAttachment#content} stream is
     *                         read exactly once
     */
    void CreateEmailRequest(EmailParams emailParams, String emailBodyHtml,
                            List<EmailAttachment> emailAttachments);

    /**
     * Fetches a file previously written to GCS (e.g. a report output) and returns its content as
     * a stream, ready to wrap in an {@link EmailAttachment}.
     *
     * <p>Default implementation only — no wiring to {@code beam-utils}'s {@code GcsUtils}, since
     * {@code beam-io} may not depend on {@code beam-utils} (see {@code CLAUDE.md} §5). Uses the
     * GCS client directly, the same pattern already used in {@code FileSourceTransform}.
     *
     * @param fileLocation a {@code gs://bucket/object} URI
     * @return the file's full content as an in-memory stream
     * @throws IllegalArgumentException if {@code fileLocation} isn't a well-formed {@code gs://} URI
     * @throws IllegalStateException    if the object doesn't exist in GCS
     */
    default InputStream FetchFileFromGcs(String fileLocation) {
        if (fileLocation == null || !fileLocation.startsWith("gs://")) {
            throw new IllegalArgumentException(
                "Expected a GCS URI starting with gs://, got: " + fileLocation);
        }
        String withoutScheme = fileLocation.substring(5);
        int slashIndex = withoutScheme.indexOf('/');
        if (slashIndex < 0) {
            throw new IllegalArgumentException("GCS URI has no object path: " + fileLocation);
        }
        String bucket = withoutScheme.substring(0, slashIndex);
        String object = withoutScheme.substring(slashIndex + 1);

        Storage storage = StorageOptions.getDefaultInstance().getService();
        byte[] bytes = storage.readAllBytes(BlobId.of(bucket, object));
        if (bytes == null) {
            throw new IllegalStateException("File not found in GCS: " + fileLocation);
        }
        return new ByteArrayInputStream(bytes);
    }

    /**
     * Finds a report output in a GCS directory by the stored name stem and returns its content,
     * for when the object's real name is not exactly the one computed (extra prefix added, etc.).
     * An exact {@code expectedName} wins; otherwise the newest file in the directory whose name
     * contains {@code stem} and ends with {@code extension} (see {@link GcsFileMatcher}).
     *
     * @param directoryUri {@code gs://bucket/dir/} the output was written to
     * @throws IllegalStateException if no file in the directory matches
     */
    default InputStream FetchFileFromGcsByPrefix(String directoryUri, String expectedName,
                                                 String stem, String extension) {
        if (directoryUri == null || !directoryUri.startsWith("gs://")) {
            throw new IllegalArgumentException(
                "Expected a GCS URI starting with gs://, got: " + directoryUri);
        }
        String withoutScheme = directoryUri.substring(5);
        int slash = withoutScheme.indexOf('/');
        String bucket = slash < 0 ? withoutScheme : withoutScheme.substring(0, slash);
        String dir = slash < 0 ? "" : withoutScheme.substring(slash + 1);
        if (!dir.isEmpty() && !dir.endsWith("/")) dir += "/";

        Storage storage = StorageOptions.getDefaultInstance().getService();
        Page<Blob> page = storage.list(bucket,
            Storage.BlobListOption.prefix(dir), Storage.BlobListOption.currentDirectory());
        List<GcsFileMatcher.Candidate> candidates = new ArrayList<>();
        for (Blob b : page.iterateAll()) {
            String name = b.getName().substring(dir.length());
            if (name.isEmpty() || name.endsWith("/")) continue;
            candidates.add(new GcsFileMatcher.Candidate(
                name, b.getUpdateTimeOffsetDateTime() != null
                    ? b.getUpdateTimeOffsetDateTime().toInstant().toEpochMilli() : 0L));
        }
        String chosen = GcsFileMatcher.pick(candidates, expectedName, stem, extension);
        if (chosen == null) {
            throw new IllegalStateException("No file containing '" + stem + "' (" + extension
                + ") found in " + directoryUri);
        }
        return new ByteArrayInputStream(storage.readAllBytes(BlobId.of(bucket, dir + chosen)));
    }
}
