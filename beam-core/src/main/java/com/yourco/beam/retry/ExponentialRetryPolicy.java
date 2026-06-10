package com.yourco.beam.retry;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential back-off retry policy with jitter, capped at {@link RetryPolicy#MAX_DELAY_MS}.
 *
 * <p>Delay for attempt <em>n</em>:
 * <pre>delay = min(baseDelayMs * 2^(n-1), MAX_DELAY_MS) + uniform jitter in [0, 10% of delay]</pre>
 *
 * <p>The 200ms cap is a hard DoFn constraint — see {@link RetryPolicy} Javadoc.
 * For longer back-off, use a retry-topic pattern outside the DoFn.
 */
public final class ExponentialRetryPolicy implements RetryPolicy {

    private static final long serialVersionUID = 1L;

    private final int  maxRetries;
    private final long baseDelayMs;

    public ExponentialRetryPolicy(int maxRetries, long baseDelayMs) {
        this.maxRetries  = maxRetries;
        // Clamp base delay so it can never exceed the cap even on attempt 1
        this.baseDelayMs = Math.min(baseDelayMs, MAX_DELAY_MS);
    }

    @Override
    public boolean shouldRetry(int attemptNumber, Exception cause) {
        return attemptNumber <= maxRetries;
    }

    @Override
    public long delayMs(int attemptNumber) {
        long exponential = baseDelayMs * (1L << (attemptNumber - 1));
        long capped      = Math.min(exponential, MAX_DELAY_MS);
        // Add up to 10% jitter using ThreadLocalRandom — thread-safe and testable
        long jitter      = (long) (capped * 0.1 * ThreadLocalRandom.current().nextDouble());
        return capped + jitter;
    }
}
