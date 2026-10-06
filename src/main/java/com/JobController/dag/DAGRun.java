package com.JobController.dag;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class DAGRun {
    private final String runId;
    private final String dagId;
    private volatile DAGRunState state = DAGRunState.RUNNING;
    private final Map<String, TaskState> taskStates = new LinkedHashMap<>();
    private final Map<String, Long> jobIds = new LinkedHashMap<>();
    private final Map<String, Integer> retryCounts = new LinkedHashMap<>();
    private final Map<String, TaskState> readOnlyTaskStates =
            Collections.unmodifiableMap(taskStates);
    private final Map<String, Long> readOnlyJobIds = Collections.unmodifiableMap(jobIds);
    private final Map<String, Integer> readOnlyRetryCounts =
            Collections.unmodifiableMap(retryCounts);

    public DAGRun(String runId, DAG dag) {
        if (runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("Run ID must not be null or blank");
        }
        Objects.requireNonNull(dag, "DAG must not be null");

        this.runId = runId;
        this.dagId = dag.getId();
        dag.getTasks().forEach((taskId, task) -> {
            taskStates.put(taskId,
                    task.getDependencies().isEmpty() ? TaskState.READY : TaskState.BLOCKED);
            retryCounts.put(taskId, 0);
        });
    }

    public String getRunId() {
        return runId;
    }

    public String getDagId() {
        return dagId;
    }

    public DAGRunState getState() {
        return state;
    }

    public void setState(DAGRunState state) {
        this.state = Objects.requireNonNull(state, "DAG run state must not be null");
    }

    public synchronized TaskState getTaskState(String taskId) {
        validateTaskId(taskId);
        return taskStates.get(taskId);
    }

    public synchronized void setTaskState(String taskId, TaskState state) {
        validateTaskId(taskId);
        taskStates.put(taskId, Objects.requireNonNull(state, "Task state must not be null"));
    }

    public synchronized Map<String, TaskState> getTaskStates() {
        return readOnlyTaskStates;
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
        return readOnlyJobIds;
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
        return readOnlyRetryCounts;
    }

    private void validateTaskId(String taskId) {
        if (!taskStates.containsKey(taskId)) {
            throw new IllegalArgumentException("Unknown task ID: " + taskId);
        }
    }
}
