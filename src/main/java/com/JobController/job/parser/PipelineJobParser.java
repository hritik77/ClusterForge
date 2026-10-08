package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.dag.RetryPolicy;
import com.JobController.dag.TimeoutPolicy;
import com.JobController.job.JobType;
import com.JobController.job.spec.JobSpecification;
import com.JobController.job.spec.PipelineJobSpecification;
import com.JobController.job.spec.PipelineTaskSpecification;

public final class PipelineJobParser implements JobParser {
    @Override
    public JobType getJobType() {
        return JobType.PIPELINE;
    }

    @Override
    public DAG parse(JobSpecification specification) {
        if (!(specification instanceof PipelineJobSpecification pipeline)) {
            throw new IllegalArgumentException("PIPELINE parser requires a PipelineJobSpecification");
        }
        if (pipeline.getTasks().isEmpty()) {
            throw new IllegalArgumentException("Pipeline must contain at least one task");
        }

        DAG dag = new DAG(pipeline.getJobId(), pipeline.getName());
        for (PipelineTaskSpecification task : pipeline.getTasks()) {
            if (task == null) {
                throw new IllegalArgumentException("Pipeline must not contain null tasks");
            }
            if (task.getId() == null || task.getId().isBlank()) {
                throw new IllegalArgumentException("Pipeline task ID must not be null or blank");
            }
            if (task.getCommand() == null || task.getCommand().isBlank()) {
                throw new IllegalArgumentException(
                        "Command for pipeline task '" + task.getId() + "' must not be null or blank");
            }
            if (task.getCpu() <= 0) {
                throw new IllegalArgumentException(
                        "CPU for pipeline task '" + task.getId() + "' must be positive");
            }
            if (task.getMemory() <= 0) {
                throw new IllegalArgumentException(
                        "Memory for pipeline task '" + task.getId() + "' must be positive");
            }
            dag.addTask(new DAGTask(
                    task.getId(),
                    task.getCpu(),
                    task.getMemory(),
                    task.getDiskMb(),
                    task.getGpuCount(),
                    task.getGpuMemoryMbPerGpu(),
                    task.getDependencies(),
                    task.getCommand(),
                    new RetryPolicy(0),
                    new TimeoutPolicy(null),
                    task.getPlacementRequirements()));
        }
        return dag;
    }
}
