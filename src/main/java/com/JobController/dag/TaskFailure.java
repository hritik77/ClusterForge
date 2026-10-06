package com.JobController.dag;

public final class TaskFailure {
    private final String taskId;
    private final RetryReason reason;
    private final String message;

    public TaskFailure(String taskId, RetryReason reason, String message) {
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId cannot be blank");
        }
        if (reason == null) {
            throw new IllegalArgumentException("reason cannot be null");
        }
        this.taskId = taskId;
        this.reason = reason;
        this.message = message;
    }

    public String getTaskId() {
        return taskId;
    }

    public RetryReason getReason() {
        return reason;
    }

    public String getMessage() {
        return message;
    }
}
