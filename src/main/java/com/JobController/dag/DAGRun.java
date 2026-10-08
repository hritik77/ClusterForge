package com.JobController.dag;

import java.util.Collections;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DAGRun {
    private final String runId;
    private final String dagId;
    private volatile DAGRunState state = DAGRunState.RUNNING;
    private final long startTimeMillis;
    private final Long deadlineMillis;
    private volatile TimeoutReason timeoutReason;
    private volatile CancellationReason cancellationReason;
    private volatile Boolean cancellationConfirmed;
    private final Map<String, TaskState> taskStates = new LinkedHashMap<>();
    private final Map<String, Long> jobIds = new LinkedHashMap<>();
    private final Map<String, Integer> retryCounts = new LinkedHashMap<>();
    private final Map<String, Long> taskStartTimes = new LinkedHashMap<>();
    private final Map<String, TaskFailure> taskFailures = new LinkedHashMap<>();
    private final Map<String, List<TaskAttempt>> taskAttempts = new LinkedHashMap<>();
    public DAGRun(String runId, DAG dag) {
        if (runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("Run ID must not be null or blank");
        }
        Objects.requireNonNull(dag, "DAG must not be null");

        this.runId = runId;
        this.dagId = dag.getId();
        this.startTimeMillis = System.currentTimeMillis();
        try {
            this.deadlineMillis = dag.hasDagTimeout()
                    ? Math.addExact(startTimeMillis, dag.getDagTimeoutMillis())
                    : null;
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("DAG deadline exceeds supported timestamp range", e);
        }
        dag.getTasks().forEach((taskId, task) -> {
            taskStates.put(taskId,
                    task.getDependencies().isEmpty() ? TaskState.READY : TaskState.BLOCKED);
            retryCounts.put(taskId, 0);
            taskAttempts.put(taskId, new ArrayList<>());
        });
    }

    public String getRunId() {
        return runId;
    }

    public String getDagId() {
        return dagId;
    }

    public synchronized DAGRunState getState() {
        return state;
    }

    public synchronized boolean setState(DAGRunState state) {
        Objects.requireNonNull(state, "DAG run state must not be null");
        if (this.state != DAGRunState.RUNNING) {
            return false;
        }
        this.state = state;
        return true;
    }

    public long getStartTimeMillis() {
        return startTimeMillis;
    }

    public Long getDeadlineMillis() {
        return deadlineMillis;
    }

    public boolean hasDeadline() {
        return deadlineMillis != null;
    }

    public TimeoutReason getTimeoutReason() {
        return timeoutReason;
    }

    public synchronized void setTimeoutReason(TimeoutReason timeoutReason) {
        this.timeoutReason = timeoutReason;
    }

    public CancellationReason getCancellationReason() {
        return cancellationReason;
    }

    public synchronized void setCancellationReason(CancellationReason cancellationReason) {
        this.cancellationReason = cancellationReason;
    }

    public Boolean getCancellationConfirmed() {
        return cancellationConfirmed;
    }

    public synchronized void setCancellationConfirmed(boolean cancellationConfirmed) {
        this.cancellationConfirmed = cancellationConfirmed;
    }

    public synchronized TaskState getTaskState(String taskId) {
        validateTaskId(taskId);
        return taskStates.get(taskId);
    }

    public synchronized void setTaskState(String taskId, TaskState state) {
        validateTaskId(taskId);
        Objects.requireNonNull(state, "Task state must not be null");
        ensureTaskCanTransition(taskId, state);
        taskStates.put(taskId, state);
    }

    public synchronized boolean transitionTaskState(
            String taskId, TaskState expected, TaskState next) {
        validateTaskId(taskId);
        Objects.requireNonNull(expected, "Expected task state must not be null");
        Objects.requireNonNull(next, "Next task state must not be null");
        if (taskStates.get(taskId) != expected || isImmutableTerminal(taskStates.get(taskId))) {
            return false;
        }
        taskStates.put(taskId, next);
        return true;
    }

    public synchronized Map<String, TaskState> getTaskStates() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(taskStates));
    }

    public synchronized Long getJobId(String taskId) {
        validateTaskId(taskId);
        return jobIds.get(taskId);
    }

    public synchronized void setJobId(String taskId, long jobId) {
        validateTaskId(taskId);
        if (jobId <= 0) {
            throw new IllegalArgumentException("Job ID must be positive");
        }
        jobIds.put(taskId, jobId);
    }

    public synchronized Map<String, Long> getJobIds() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(jobIds));
    }

    public synchronized int getRetryCount(String taskId) {
        validateTaskId(taskId);
        return retryCounts.get(taskId);
    }

    public synchronized void incrementRetryCount(String taskId) {
        validateTaskId(taskId);
        retryCounts.put(taskId, retryCounts.get(taskId) + 1);
    }

    public synchronized Map<String, Integer> getRetryCounts() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(retryCounts));
    }

    public synchronized TaskAttempt createAttempt(String taskId) {
        validateTaskId(taskId);
        List<TaskAttempt> attempts = taskAttempts.get(taskId);
        TaskAttempt attempt = new TaskAttempt(
                runId, taskId, attempts.size(), System.currentTimeMillis());
        attempts.add(attempt);
        return attempt;
    }

    public synchronized List<TaskAttemptSnapshot> getAttempts(String taskId) {
        validateTaskId(taskId);
        return taskAttempts.get(taskId).stream().map(TaskAttempt::snapshot).toList();
    }

    public synchronized TaskAttemptSnapshot getCurrentAttempt(String taskId) {
        validateTaskId(taskId);
        List<TaskAttempt> attempts = taskAttempts.get(taskId);
        return attempts.isEmpty() ? null : attempts.get(attempts.size() - 1).snapshot();
    }

    synchronized TaskAttempt getAttemptInternal(String taskId, int attemptNumber) {
        validateTaskId(taskId);
        List<TaskAttempt> attempts = taskAttempts.get(taskId);
        return attemptNumber < 0 || attemptNumber >= attempts.size()
                ? null : attempts.get(attemptNumber);
    }

    synchronized TaskAttempt getCurrentAttemptInternal(String taskId) {
        validateTaskId(taskId);
        List<TaskAttempt> attempts = taskAttempts.get(taskId);
        return attempts.isEmpty() ? null : attempts.get(attempts.size() - 1);
    }

    public synchronized void markTaskStarted(String taskId, long timestamp) {
        validateTaskId(taskId);
        if (taskStates.get(taskId) != TaskState.RUNNING) {
            throw new IllegalStateException("Task must be RUNNING before its start time is recorded");
        }
        if (timestamp <= 0) {
            throw new IllegalArgumentException("Task start timestamp must be positive");
        }
        taskStartTimes.put(taskId, timestamp);
    }

    public synchronized Long getTaskStartTime(String taskId) {
        validateTaskId(taskId);
        return taskStartTimes.get(taskId);
    }

    public synchronized Map<String, Long> getTaskStartTimes() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(taskStartTimes));
    }

    public synchronized void recordTaskFailure(TaskFailure failure) {
        Objects.requireNonNull(failure, "Task failure must not be null");
        validateTaskId(failure.getTaskId());
        taskFailures.put(failure.getTaskId(), failure);
    }

    public synchronized TaskFailure getTaskFailure(String taskId) {
        validateTaskId(taskId);
        return taskFailures.get(taskId);
    }

    public synchronized Map<String, TaskFailure> getTaskFailures() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(taskFailures));
    }

    private void validateTaskId(String taskId) {
        if (!taskStates.containsKey(taskId)) {
            throw new IllegalArgumentException("Unknown task ID: " + taskId);
        }
    }

    private void ensureTaskCanTransition(String taskId, TaskState next) {
        TaskState current = taskStates.get(taskId);
        if (isImmutableTerminal(current) && current != next) {
            throw new IllegalStateException(
                    "Task " + taskId + " is terminal in state " + current);
        }
    }

    private static boolean isImmutableTerminal(TaskState state) {
        return state == TaskState.COMPLETED || state == TaskState.CANCELLED;
    }
}
