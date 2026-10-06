package com.JobController.dag;

public final class RetryPolicy {
    private final int maxRetries;

    public RetryPolicy(int maxRetries) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries cannot be negative");
        }
        this.maxRetries = maxRetries;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public boolean canRetry(int retryCount) {
        if (retryCount < 0) {
            throw new IllegalArgumentException("retryCount cannot be negative");
        }
        return retryCount < maxRetries;
    }
}
