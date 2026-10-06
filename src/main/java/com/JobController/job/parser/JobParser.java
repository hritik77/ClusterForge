package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.job.JobType;
import com.JobController.job.spec.JobSpecification;

public interface JobParser {
    JobType getJobType();

    DAG parse(JobSpecification specification);
}
