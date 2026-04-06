package com.bonitasoft.connectors.kafka;

import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Retry policy with exponential backoff and jitter for transient messaging errors.
 */
@Slf4j
public class RetryPolicy {

    private static final long INITIAL_WAIT_MS = 1000L;
    private static final long MAX_WAIT_MS = 64000L;
    private static final Set<Integer> RETRYABLE_STATUS_CODES = Set.of(429, 500, 502, 503);

    private final int maxRetries;

    public RetryPolicy(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public <T> T execute(Callable<T> action) throws KafkaException {
        int attempt = 0;
        while (true) {
            try {
                return action.call();
            } catch (KafkaException e) {
                if (!e.isRetryable() || attempt >= maxRetries) {
                    throw e;
                }
                long waitMs = calculateWait(attempt);
                log.warn("Retryable error (attempt {}/{}), waiting {}ms: {}",
                        attempt + 1, maxRetries, waitMs, e.getMessage());
                sleep(waitMs);
                attempt++;
            } catch (Exception e) {
                throw new KafkaException("Unexpected error during messaging operation", e);
            }
        }
    }

    long calculateWait(int attempt) {
        long exponentialWait = INITIAL_WAIT_MS * (1L << attempt);
        long cappedWait = Math.min(exponentialWait, MAX_WAIT_MS);
        long jitter = ThreadLocalRandom.current().nextLong(0, cappedWait / 2);
        return cappedWait + jitter;
    }

    public static boolean isRetryableStatusCode(int statusCode) {
        return RETRYABLE_STATUS_CODES.contains(statusCode);
    }

    void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retry interrupted", e);
        }
    }
}
