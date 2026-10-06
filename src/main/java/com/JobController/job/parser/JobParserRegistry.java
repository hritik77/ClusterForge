package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.job.JobType;
import com.JobController.job.spec.JobSpecification;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

public final class JobParserRegistry {
    private final Map<JobType, JobParser> parsers = new EnumMap<>(JobType.class);

    public JobParserRegistry() {
        register(new BatchJobParser());
        register(new MapReduceJobParser());
        register(new PipelineJobParser());
        register(new ParameterSweepJobParser());
        register(new CustomDAGJobParser());
    }

    public synchronized void register(JobParser parser) {
        Objects.requireNonNull(parser, "Job parser must not be null");
        JobType jobType = Objects.requireNonNull(
                parser.getJobType(), "Job parser type must not be null");
        if (parsers.putIfAbsent(jobType, parser) != null) {
            throw new IllegalArgumentException("A parser is already registered for " + jobType);
        }
    }

    public synchronized DAG parse(JobSpecification specification) {
        Objects.requireNonNull(specification, "Job specification must not be null");
        JobType jobType = Objects.requireNonNull(
                specification.getJobType(), "Job specification type must not be null");
        JobParser parser = parsers.get(jobType);
        if (parser == null) {
            throw new IllegalArgumentException(
                    "No parser is registered for " + jobType);
        }
        return parser.parse(specification);
    }
}
