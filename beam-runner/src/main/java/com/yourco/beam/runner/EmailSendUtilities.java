package com.yourco.beam.runner;

import com.yourco.beam.io.email.EmailSendUtility;

/**
 * The one place the {@link EmailSendUtility} implementation is declared and constructed.
 * {@code ReportPipelineFactory} (report-completion email) and {@code FailureNotifier} (ops
 * failure email) both get theirs from {@link #create()}; nothing is discovered via
 * {@code ServiceLoader} (CLAUDE.md §12).
 *
 * <p>This repository ships no implementation — the real one is an organization's own
 * email-gateway client. To enable email, add that class to the build and return an instance here,
 * e.g. {@code return new MyGatewayEmailSendUtility();}. Until then {@code null} is returned and
 * callers log a warning and skip sending rather than failing the run.
 */
final class EmailSendUtilities {

    private EmailSendUtilities() {}

    /** @return the email implementation in use, or {@code null} if none is configured */
    static EmailSendUtility create() {
        return null;
    }
}
