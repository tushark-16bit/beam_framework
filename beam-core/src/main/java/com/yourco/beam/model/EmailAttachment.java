package com.yourco.beam.model;

import java.io.InputStream;

/**
 * One file attachment for {@code EmailSendUtility.CreateEmailRequest()}.
 *
 * <p>Distinct from {@code com.yourco.beam.io.email.EmailAttachment} — that one belongs to the
 * older {@code ReportEmailAdapter}/{@code SmtpReportEmailAdapter} SMTP path (still used for
 * DATA_SOURCE_DOWNLOAD failure email in {@code PostDownloadFinalizeTransform}); this one is the
 * {@code EmailSendUtility} contract's own attachment shape. Never import both unqualified in the
 * same file.
 *
 * <p>{@link #content} is read exactly once by the implementing utility and should not be reused
 * afterward.
 */
public final class EmailAttachment {

    /** Attachment filename shown to the recipient (e.g. {@code daily_trades_2024-01-15.csv}). */
    public final String      fileName;
    /** Raw file content — read once by the implementing utility. */
    public final InputStream content;
    /** MIME type (e.g. {@code text/csv}, {@code application/json}). */
    public final String      type;

    public EmailAttachment(String fileName, InputStream content, String type) {
        this.fileName = fileName;
        this.content  = content;
        this.type     = type;
    }
}
