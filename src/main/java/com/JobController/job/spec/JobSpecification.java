package com.JobController.job.spec;

import com.JobController.job.JobType;

public abstract class JobSpecification {
    private final String jobId;
    private final String name;

    protected JobSpecification(String jobId, String name) {
        if (jobId == null || jobId.isBlank()) {
            throw new IllegalArgumentException("Job ID must not be null or blank");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Job name must not be null or blank");
        }
        this.jobId = jobId;
        this.name = name;
    }

    public final String getJobId() {
        return jobId;
    }

    public final String getName() {
        return name;
    }

    public abstract JobType getJobType();
}
