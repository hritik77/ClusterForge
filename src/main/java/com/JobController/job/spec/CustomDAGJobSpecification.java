package com.JobController.job.spec;

import com.JobController.job.JobType;

import java.util.List;
import java.util.Objects;

public final class CustomDAGJobSpecification extends JobSpecification {
    private final List<CustomDAGTaskSpecification> tasks;

    public CustomDAGJobSpecification(
            String jobId, String name, List<CustomDAGTaskSpecification> tasks) {
        super(jobId, name);
        this.tasks = List.copyOf(
                Objects.requireNonNull(tasks, "Custom DAG tasks must not be null"));
    }

    public List<CustomDAGTaskSpecification> getTasks() {
        return tasks;
    }

    @Override
    public JobType getJobType() {
        return JobType.CUSTOM_DAG;
    }
}
