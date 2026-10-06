package com.JobController.job.spec;

import com.JobController.job.JobType;

import java.util.List;
import java.util.Objects;

public final class PipelineJobSpecification extends JobSpecification {
    private final List<PipelineTaskSpecification> tasks;

    public PipelineJobSpecification(
            String jobId, String name, List<PipelineTaskSpecification> tasks) {
        super(jobId, name);
        Objects.requireNonNull(tasks, "Pipeline tasks must not be null");
        this.tasks = List.copyOf(tasks);
    }

    public List<PipelineTaskSpecification> getTasks() {
        return tasks;
    }

    @Override
    public JobType getJobType() {
        return JobType.PIPELINE;
    }
}
