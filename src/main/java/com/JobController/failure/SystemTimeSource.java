package com.JobController.failure;

/**
 * Production {@link TimeSource} that delegates to {@link System#currentTimeMillis()}.
 */
public final class SystemTimeSource implements TimeSource {

    /** Singleton instance – stateless, safe to share. */
    public static final SystemTimeSource INSTANCE = new SystemTimeSource();

    private SystemTimeSource() {}

    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
