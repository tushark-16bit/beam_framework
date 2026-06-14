package com.yourco.beam.io.email;

import java.io.InputStream;

/**
 * A single file attachment for a report email.
 *
 * <p>Used with {@link ReportEmailAdapter}. The {@code content} stream is read
 * exactly once by the adapter and should not be reused after calling
 * {@link ReportEmailAdapter#send}.
 */
public final class EmailAttachment {

    /** Raw file content — read once by the email adapter. */
    public final InputStream content;
    /** Attachment filename shown to the recipient (e.g. {@code daily_trades_2024-01-15.csv}). */
    public final String fileName;
    /** MIME type (e.g. {@code text/csv}, {@code application/json}). */
    public final String contentType;

    public EmailAttachment(InputStream content, String fileName, String contentType) {
        this.content     = content;
        this.fileName    = fileName;
        this.contentType = contentType;
    }

    /** Convenience factory for CSV attachments. */
    public static EmailAttachment csv(InputStream content, String fileName) {
        return new EmailAttachment(content, fileName, "text/csv");
    }

    /** Convenience factory for JSON attachments. */
    public static EmailAttachment json(InputStream content, String fileName) {
        return new EmailAttachment(content, fileName, "application/json");
    }
}
