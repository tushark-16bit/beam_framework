package com.yourco.beam.retry;

import java.io.Serializable;

/**
 * Determines whether a failed operation should be retried and how long to
 * wait before the next attempt.
 *
 * <p>Implementations MUST be {@link Serializable} because they are held as
 * fields inside {@link RetryingDoFn} which is serialized to Beam workers.
 *
 * <h2>Delay constraint</h2>
 * {@link #delayMs(int)} must return a value {@code ≤ 200ms}. Longer delays
 * block the Beam worker's bundle-processing thread, starve other bundles on
 * the same worker, and can trigger the Dataflow harness watchdog. For
 * back-off strategies that require longer delays, use a retry-topic pattern
 * (write failures to Pub/Sub and re-process after a scheduled interval).
 */
public interface RetryPolicy extends Serializable {

    /** Maximum permitted in-DoFn delay. Enforced by all built-in implementations. */
    long MAX_DELAY_MS = 200L;

    /**
     * @param attemptNumber 1-based attempt count (1 = first retry after initial failure)
     * @param cause         the exception that triggered this retry evaluation
     * @return {@code true} if the operation should be retried
     */
    boolean shouldRetry(int attemptNumber, Exception cause);

    /**
     * Returns the number of milliseconds to wait before the next attempt.
     * Must return a value in {@code [0, MAX_DELAY_MS]}.
     *
     * @param attemptNumber 1-based attempt count
     */
    long delayMs(int attemptNumber);
}
