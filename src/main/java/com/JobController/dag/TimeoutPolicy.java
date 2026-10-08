package com.JobController.dag;

public final class TimeoutPolicy {
    private final Long taskTimeoutMillis;

    public TimeoutPolicy(Long taskTimeoutMillis) {
        if (taskTimeoutMillis != null && taskTimeoutMillis <= 0) {
            throw new IllegalArgumentException("taskTimeoutMillis must be positive");
        }
        this.taskTimeoutMillis = taskTimeoutMillis;
    }

    public Long getTaskTimeoutMillis() {
        return taskTimeoutMillis;
    }

    public boolean hasTimeout() {
        return taskTimeoutMillis != null;
    }
}
