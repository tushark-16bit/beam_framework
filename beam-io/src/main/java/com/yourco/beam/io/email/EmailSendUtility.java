package com.yourco.beam.io.email;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.yourco.beam.model.EmailAttachment;
import com.yourco.beam.model.EmailParams;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

/**
 * Port for sending email, independent of the transport (SMTP, an internal email gateway, etc).
 *
 * <p>This repository only defines the contract. The concrete implementation is expected to live
 * outside this codebase — e.g. an organization's existing internal email-gateway client — and be
 * supplied at runtime rather than committed here. Two ways to plug one in:
 * <ul>
 *   <li>Register it via Java SPI: a JAR on the classpath containing
 *       {@code META-INF/services/com.yourco.beam.io.email.EmailSendUtility} with the
 *       implementation's fully-qualified class name — {@code ReportPipelineFactory} discovers it
 *       automatically via {@link java.util.ServiceLoader}, the same mechanism
 *       {@code TransformRegistry} uses for {@code BeamTransform}. No code change needed here.</li>
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
}
