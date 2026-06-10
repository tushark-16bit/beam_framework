package com.yourco.beam.retry;

/**
 * Fixed-delay retry policy: waits the same duration between every attempt.
 * Delay is capped at {@link RetryPolicy#MAX_DELAY_MS} — see {@link RetryPolicy} Javadoc.
 */
public final class FixedRetryPolicy implements RetryPolicy {

    private static final long serialVersionUID = 1L;

    private final int  maxRetries;
    private final long delayMs;

    public FixedRetryPolicy(int maxRetries, long delayMs) {
        this.maxRetries = maxRetries;
        this.delayMs    = Math.min(delayMs, MAX_DELAY_MS);
    }

    @Override
    public boolean shouldRetry(int attemptNumber, Exception cause) {
        return attemptNumber <= maxRetries;
    }

    @Override
    public long delayMs(int attemptNumber) {
        return delayMs;
    }
}
