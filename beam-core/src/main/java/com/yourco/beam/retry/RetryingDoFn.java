package com.yourco.beam.retry;

import com.yourco.beam.model.FailedRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic {@link DoFn} that wraps any row-level operation with retry logic
 * and routes failures to a dead-letter output via a {@link TupleTag}.
 *
 * <h2>Delay handling</h2>
 * {@link RetryPolicy#delayMs(int)} is called between attempts. The returned
 * value is capped at {@link RetryPolicy#MAX_DELAY_MS} (200ms). The brief
 * sleep is safe for small, bounded delays, but for longer back-off strategies
 * use a retry-topic pattern (write the FailedRecord to Pub/Sub and re-process
 * on a scheduled interval) rather than blocking this thread.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * TupleTagList tags = TupleTagList.of(RetryingDoFn.DEAD_LETTER);
 * PCollectionTuple result = rows.apply(
 *     ParDo.of(new RetryingDoFn(myFn, new ExponentialRetryPolicy(3, 100)))
 *          .withOutputTags(RetryingDoFn.SUCCESS, tags));
 *
 * PCollection<Row>          good = result.get(RetryingDoFn.SUCCESS);
 * PCollection<FailedRecord> dlq  = result.get(RetryingDoFn.DEAD_LETTER);
 * }</pre>
 *
 * <h2>Serialization</h2>
 * Named static class — safe for Beam worker serialization.
 * {@link #SUCCESS} and {@link #DEAD_LETTER} are {@code static final} — required by Beam.
 * {@code fn} must be a {@link SerializableFunction}, not a plain lambda.
 */
public final class RetryingDoFn extends DoFn<Row, Row> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RetryingDoFn.class);

    public static final TupleTag<Row>          SUCCESS     = new TupleTag<Row>()     {};
    public static final TupleTag<FailedRecord> DEAD_LETTER = new TupleTag<FailedRecord>() {};

    private final SerializableFunction<Row, Row> fn;
    private final RetryPolicy retryPolicy;

    public RetryingDoFn(SerializableFunction<Row, Row> fn, RetryPolicy retryPolicy) {
        this.fn          = fn;
        this.retryPolicy = retryPolicy;
    }

    @ProcessElement
    public void processElement(@Element Row row, MultiOutputReceiver out) {
        int attempt = 0;

        while (true) {
            try {
                out.get(SUCCESS).output(fn.apply(row));
                return;
            } catch (Exception e) {
                attempt++;
                if (!retryPolicy.shouldRetry(attempt, e)) {
                    LOG.warn("Element failed after {} attempt(s) — routing to DLQ. Error: {}",
                             attempt, e.getMessage());
                    out.get(DEAD_LETTER).output(FailedRecord.of(row, e, attempt));
                    return;
                }

                long delay = retryPolicy.delayMs(attempt);
                LOG.debug("Attempt {} failed ({}); retrying in {}ms…", attempt, e.getMessage(), delay);
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);   // safe: capped at MAX_DELAY_MS (200ms)
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        out.get(DEAD_LETTER).output(FailedRecord.of(row, ie, attempt));
                        return;
                    }
                }
            }
        }
    }
}
