package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.job.JobType;
import com.JobController.job.spec.BatchJobSpecification;
import com.JobController.job.spec.JobSpecification;

import java.util.Set;

public final class BatchJobParser implements JobParser {
    @Override
    public JobType getJobType() {
        return JobType.BATCH;
    }

    @Override
    public DAG parse(JobSpecification specification) {
        if (!(specification instanceof BatchJobSpecification batch)) {
            throw new IllegalArgumentException(
                    "BATCH parser requires a BatchJobSpecification");
        }

        DAG dag = new DAG(batch.getJobId(), batch.getName());
        for (int index = 1; index <= batch.getCount(); index++) {
            String taskId = "%s-%03d".formatted(batch.getName(), index);
            dag.addTask(new DAGTask(
                    taskId, batch.getCpu(), batch.getMemory(),
                    batch.getDiskMb(), batch.getGpuCount(), batch.getGpuMemoryMbPerGpu(),
                    Set.of(), batch.getCommand()));
        }
        return dag;
    }
}
