package com.JobController.dag;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class TaskAttempt {
    private final String runId;
    private final String taskId;
    private final int attemptNumber;
    private final long createdAtMillis;
    private Long jobId;
    private String workerId;
    private Map<String, String> workerLabels = Map.of();
    private Long startedAtMillis;
    private Long finishedAtMillis;
    private AttemptState state = AttemptState.CREATED;
    private RetryReason failureReason;
    private String failureMessage;

    TaskAttempt(String runId, String taskId, int attemptNumber, long createdAtMillis) {
        if (runId == null || runId.isBlank() || taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("runId and taskId must not be blank");
        }
        if (attemptNumber < 0 || createdAtMillis <= 0) {
            throw new IllegalArgumentException("attemptNumber and createdAtMillis are invalid");
        }
        this.runId = runId;
        this.taskId = taskId;
        this.attemptNumber = attemptNumber;
        this.createdAtMillis = createdAtMillis;
    }

    public String getAttemptId() {
        return runId + ":" + taskId + ":" + attemptNumber;
    }

    public String getRunId() {
        return runId;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getAttemptNumber() {
        return attemptNumber;
    }

    public synchronized Long getJobId() {
        return jobId;
    }

    public synchronized String getWorkerId() {
        return workerId;
    }

    public synchronized Map<String, String> getWorkerLabels() {
        return workerLabels;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public synchronized Long getStartedAtMillis() {
        return startedAtMillis;
    }

    public synchronized Long getFinishedAtMillis() {
        return finishedAtMillis;
    }

    public synchronized AttemptState getState() {
        return state;
    }

    public synchronized RetryReason getFailureReason() {
        return failureReason;
    }

    public synchronized String getFailureMessage() {
        return failureMessage;
    }

    synchronized TaskAttemptSnapshot snapshot() {
        return new TaskAttemptSnapshot(
                runId, getAttemptId(), taskId, attemptNumber, jobId, workerId, workerLabels,
                createdAtMillis, startedAtMillis, finishedAtMillis, state,
                failureReason, failureMessage);
    }

    synchronized boolean markSubmitted(long jobId) {
        if (jobId <= 0 || state != AttemptState.CREATED) {
            return false;
        }
        this.jobId = jobId;
        state = AttemptState.SUBMITTED;
        return true;
    }

    synchronized boolean markAllocated(String workerId, Map<String, String> workerLabels) {
        if (state.isTerminal() || state == AttemptState.RUNNING) {
            return false;
        }
        if (state == AttemptState.ALLOCATED) {
            return Objects.equals(this.workerId, workerId)
                    && this.workerLabels.equals(workerLabels);
        }
        if (state != AttemptState.SUBMITTED || workerId == null || workerId.isBlank()) {
            return false;
        }
        this.workerId = workerId;
        this.workerLabels = Collections.unmodifiableMap(new LinkedHashMap<>(
                Objects.requireNonNull(workerLabels, "workerLabels must not be null")));
        state = AttemptState.ALLOCATED;
        return true;
    }

    synchronized boolean markRunning(long timestampMillis) {
        if (state == AttemptState.RUNNING) {
            return false;
        }
        if ((state != AttemptState.SUBMITTED && state != AttemptState.ALLOCATED)
                || timestampMillis <= 0) {
            return false;
        }
        state = AttemptState.RUNNING;
        if (startedAtMillis == null) {
            startedAtMillis = timestampMillis;
        }
        return true;
    }

    synchronized boolean markTerminal(
            AttemptState terminalState, RetryReason reason, String message, long timestampMillis) {
        if (terminalState == null || !terminalState.isTerminal()) {
            throw new IllegalArgumentException("Attempt terminal state is required");
        }
        if (state.isTerminal() || timestampMillis <= 0) {
            return false;
        }
        state = terminalState;
        finishedAtMillis = timestampMillis;
        failureReason = reason;
        failureMessage = message;
        return true;
    }
}
