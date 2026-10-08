package com.JobController.failure;

/**
 * Abstraction over the system clock used by failure detection components.
 *
 * <p>Inject {@link SystemTimeSource} in production and a controllable fake in tests so
 * that failure-detection logic can be exercised without real sleeps.
 */
public interface TimeSource {

    /** Returns the current wall-clock time in milliseconds since the Unix epoch. */
    long currentTimeMillis();
}
