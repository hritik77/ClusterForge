package com.JobController.dag;

import java.util.Map;

public final class TaskAttemptSnapshot {
    private final String runId;
    private final String attemptId;
    private final String taskId;
    private final int attemptNumber;
    private final Long jobId;
    private final String workerId;
    private final Map<String, String> workerLabels;
    private final long createdAtMillis;
    private final Long startedAtMillis;
    private final Long finishedAtMillis;
    private final AttemptState state;
    private final RetryReason failureReason;
    private final String failureMessage;

    TaskAttemptSnapshot(
            String runId, String attemptId, String taskId, int attemptNumber, Long jobId,
            String workerId, Map<String, String> workerLabels, long createdAtMillis,
            Long startedAtMillis, Long finishedAtMillis, AttemptState state,
            RetryReason failureReason, String failureMessage) {
        this.runId = runId;
        this.attemptId = attemptId;
        this.taskId = taskId;
        this.attemptNumber = attemptNumber;
        this.jobId = jobId;
        this.workerId = workerId;
        this.workerLabels = Map.copyOf(workerLabels);
        this.createdAtMillis = createdAtMillis;
        this.startedAtMillis = startedAtMillis;
        this.finishedAtMillis = finishedAtMillis;
        this.state = state;
        this.failureReason = failureReason;
        this.failureMessage = failureMessage;
    }

    public String getRunId() {
        return runId;
    }

    public String getAttemptId() {
        return attemptId;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public Long getJobId() {
        return jobId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public Map<String, String> getWorkerLabels() {
        return workerLabels;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public Long getStartedAtMillis() {
        return startedAtMillis;
    }

    public Long getFinishedAtMillis() {
        return finishedAtMillis;
    }

    public AttemptState getState() {
        return state;
    }

    public RetryReason getFailureReason() {
        return failureReason;
    }

    public String getFailureMessage() {
        return failureMessage;
    }
}
