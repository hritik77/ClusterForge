package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.dag.DAGValidator;
import com.JobController.dag.RetryPolicy;
import com.JobController.dag.TimeoutPolicy;
import com.JobController.job.JobType;
import com.JobController.job.spec.CustomDAGJobSpecification;
import com.JobController.job.spec.CustomDAGTaskSpecification;
import com.JobController.job.spec.JobSpecification;

public final class CustomDAGJobParser implements JobParser {
    @Override
    public JobType getJobType() {
        return JobType.CUSTOM_DAG;
    }

    @Override
    public DAG parse(JobSpecification specification) {
        if (!(specification instanceof CustomDAGJobSpecification customDAG)) {
            throw new IllegalArgumentException(
                    "CUSTOM_DAG parser requires a CustomDAGJobSpecification");
        }
        if (customDAG.getTasks().isEmpty()) {
            throw new IllegalArgumentException("Custom DAG must contain at least one task");
        }

        DAG dag = new DAG(customDAG.getJobId(), customDAG.getName(),
                customDAG.getDagTimeoutMillis());
        for (CustomDAGTaskSpecification task : customDAG.getTasks()) {
            if (task.getId() == null || task.getId().isBlank()) {
                throw new IllegalArgumentException("Custom DAG task ID must not be null or blank");
            }
            if (task.getCommand() == null || task.getCommand().isBlank()) {
                throw new IllegalArgumentException(
                        "Command for custom DAG task '" + task.getId()
                                + "' must not be null or blank");
            }
            if (task.getCpu() <= 0) {
                throw new IllegalArgumentException(
                        "CPU for custom DAG task '" + task.getId() + "' must be positive");
            }
            if (task.getMemory() <= 0) {
                throw new IllegalArgumentException(
                        "Memory for custom DAG task '" + task.getId() + "' must be positive");
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
                    new TimeoutPolicy(task.getTimeoutMillis()),
                    task.getPlacementRequirements()));
        }

        DAGValidator.validate(dag);
        return dag;
    }
}
