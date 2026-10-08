package com.JobController.job.spec;

import com.JobController.job.JobType;

import java.util.List;
import java.util.Objects;

public final class CustomDAGJobSpecification extends JobSpecification {
    private final List<CustomDAGTaskSpecification> tasks;
    private final Long dagTimeoutMillis;

    public CustomDAGJobSpecification(
            String jobId, String name, List<CustomDAGTaskSpecification> tasks) {
        this(jobId, name, tasks, null);
    }

    public CustomDAGJobSpecification(
            String jobId, String name, List<CustomDAGTaskSpecification> tasks,
            Long dagTimeoutMillis) {
        super(jobId, name);
        if (dagTimeoutMillis != null && dagTimeoutMillis <= 0) {
            throw new IllegalArgumentException("dagTimeoutMillis must be positive");
        }
        this.tasks = List.copyOf(
                Objects.requireNonNull(tasks, "Custom DAG tasks must not be null"));
        this.dagTimeoutMillis = dagTimeoutMillis;
    }

    public List<CustomDAGTaskSpecification> getTasks() {
        return tasks;
    }

    public Long getDagTimeoutMillis() {
        return dagTimeoutMillis;
    }

    @Override
    public JobType getJobType() {
        return JobType.CUSTOM_DAG;
    }
}
