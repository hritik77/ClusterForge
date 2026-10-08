package com.JobController.dag;

public final class TaskFailure {
    private final String taskId;
    private final RetryReason reason;
    private final TimeoutReason timeoutReason;
    private final String message;

    public TaskFailure(String taskId, RetryReason reason, String message) {
        this(taskId, reason, null, message);
    }

    public TaskFailure(
            String taskId, RetryReason reason, TimeoutReason timeoutReason, String message) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId cannot be blank");
        }
        if (reason == null) {
            throw new IllegalArgumentException("reason cannot be null");
        }
        if (timeoutReason != null && reason != RetryReason.TIMEOUT) {
            throw new IllegalArgumentException("timeoutReason requires a TIMEOUT failure");
        }
        this.taskId = taskId;
        this.reason = reason;
        this.timeoutReason = timeoutReason;
        this.message = message;
    }

    public String getTaskId() {
        return taskId;
    }

    public RetryReason getReason() {
        return reason;
    }

    public TimeoutReason getTimeoutReason() {
        return timeoutReason;
    }

    public String getMessage() {
        return message;
    }
}
