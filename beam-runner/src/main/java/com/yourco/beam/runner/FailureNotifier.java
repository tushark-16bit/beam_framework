package com.yourco.beam.runner;

import com.yourco.beam.exception.DataSourceDownloadException;
import com.yourco.beam.exception.PipelineException;
import com.yourco.beam.exception.ReportProcessingException;
import com.yourco.beam.io.email.EmailSendUtility;
import com.yourco.beam.model.EmailParams;
import com.yourco.beam.options.FrameworkOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * {@code Main}'s single, last-resort failure handler. Builds a subject/body template specific to
 * which of the framework's typed exceptions was caught (or a generic one for anything else),
 * always logs it, and — only if {@code --opsFailureEmail} is set and an {@link EmailSendUtility}
 * is available — attempts to email it.
 *
 * <h2>Why this exists as a single last-resort layer, not scattered per call site</h2>
 * {@code DataSourcePipelineFactory}, {@code ReportPipelineFactory}, and
 * {@code PipelineSequenceFactory} already classify failures into
 * {@link DataSourceDownloadException}, {@link ReportProcessingException}, and
 * {@link PipelineException} respectively, right at the point where they have the most context
 * (which report/datasource, which phase). This class's only job is the last mile: turn whichever
 * typed exception (or, for anything unclassified, the raw exception) reaches {@code Main} into a
 * human-readable notification, and never let a notification failure mask the original one.
 *
 * <h2>Recipients</h2>
 * This is deliberately a <em>fallback</em> address ({@code --opsFailureEmail}), not the
 * per-report ({@code ReportEmailConfig}) or per-source ({@code SourceFailureEmailConfig})
 * recipients — those are already used by {@code ReportPipelineFactory} and
 * {@code PostDownloadFinalizeTransform} respectively, at the point of failure, when that config
 * is available. By the time a failure reaches here, config may not have loaded at all (e.g. a
 * bad {@code parameter_store} row) — {@code --opsFailureEmail} is the one address that never
 * depends on any BQ config being reachable.
 */
final class FailureNotifier {

    private static final Logger LOG = LoggerFactory.getLogger(FailureNotifier.class);

    private FailureNotifier() {}

    static void notify(FrameworkOptions options, Throwable failure) {
        String subject;
        String body;

        if (failure instanceof DataSourceDownloadException e) {
            subject = "[DATA_SOURCE_DOWNLOAD FAILED] " + e.datasourceName + " (" + e.reason + ")";
            body = "Datasource   : " + e.datasourceName
                 + "\nSubprocess   : " + e.subprocessName
                 + "\nPeriod       : " + e.periodId
                 + "\nReason       : " + e.reason
                 + "\nMessage      : " + e.getMessage();
        } else if (failure instanceof ReportProcessingException e) {
            subject = "[REPORT_PROCESSING FAILED] " + e.reportName + " (" + e.reason + ")";
            body = "Report       : " + e.reportName
                 + "\nSubprocess   : " + e.reportSubprocess
                 + "\nPeriod       : " + e.periodId
                 + "\nReason       : " + e.reason
                 + "\nMessage      : " + e.getMessage();
        } else if (failure instanceof PipelineException e) {
            subject = "[PIPELINE FAILED] " + e.reportName + " (" + e.reason + ")";
            body = "Report       : " + e.reportName
                 + "\nSubprocess   : " + e.reportSubprocess
                 + "\nPeriod       : " + e.periodId
                 + "\nReason       : " + e.reason
                 + "\nMessage      : " + e.getMessage();
        } else {
            // Default template — anything not one of the three typed exceptions above.
            subject = "[BEAM PIPELINE FRAMEWORK FAILED] " + failure.getClass().getSimpleName();
            body = "Process type : " + options.getProcessType()
                 + "\nJob run ID   : " + options.getJobRunId()
                 + "\nMessage      : " + failure.getMessage();
        }

        LOG.error("FAILURE NOTIFICATION\nSubject: {}\nBody:\n{}", subject, body, failure);
        sendBestEffort(options, subject, body);
    }

    /**
     * Never throws — a failure here must never mask the original exception {@code Main} is
     * already in the middle of rethrowing.
     */
    private static void sendBestEffort(FrameworkOptions options, String subject, String body) {
        String opsEmail = options.getOpsFailureEmail();
        if (opsEmail == null || opsEmail.isBlank()) {
            LOG.info("--opsFailureEmail not set — failure notification logged above only");
            return;
        }
        try {
            EmailSendUtility emailUtility = discoverEmailUtility();
            if (emailUtility == null) {
                LOG.warn("--opsFailureEmail={} set, but no EmailSendUtility is available "
                         + "(none discovered via SPI) — cannot send failure notification", opsEmail);
                return;
            }
            List<String> toList = Arrays.stream(opsEmail.split(","))
                .map(String::trim).filter(s -> !s.isBlank()).toList();
            EmailParams params = emailUtility.SetEmailParams(
                options.getOpsFailureFromAddress(), subject, toList, List.of(), false);
            emailUtility.CreateEmailRequest(params, body, List.of());
            LOG.info("Failure notification sent to {}", opsEmail);
        } catch (Exception e) {
            LOG.error("Failed to send failure notification email to {}: {}", opsEmail, e.getMessage(), e);
        }
    }

    private static EmailSendUtility discoverEmailUtility() {
        Iterator<EmailSendUtility> found = ServiceLoader.load(EmailSendUtility.class).iterator();
        return found.hasNext() ? found.next() : null;
    }
}
