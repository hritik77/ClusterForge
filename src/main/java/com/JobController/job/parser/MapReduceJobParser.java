package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.job.JobType;
import com.JobController.job.spec.JobSpecification;
import com.JobController.job.spec.MapReduceJobSpecification;

import java.util.LinkedHashSet;
import java.util.Set;

public final class MapReduceJobParser implements JobParser {
    @Override
    public JobType getJobType() {
        return JobType.MAP_REDUCE;
    }

    @Override
    public DAG parse(JobSpecification specification) {
        if (!(specification instanceof MapReduceJobSpecification mapReduce)) {
            throw new IllegalArgumentException(
                    "MAP_REDUCE parser requires a MapReduceJobSpecification");
        }

        DAG dag = new DAG(mapReduce.getJobId(), mapReduce.getName());
        Set<String> mapTaskIds = new LinkedHashSet<>();
        for (int index = 1; index <= mapReduce.getPartitions(); index++) {
            String mapTaskId = "map-%03d".formatted(index);
            mapTaskIds.add(mapTaskId);
            dag.addTask(new DAGTask(
                    mapTaskId,
                    mapReduce.getMapCpu(),
                    mapReduce.getMapMemory(),
                    mapReduce.getMapDiskMb(),
                    mapReduce.getMapGpuCount(),
                    mapReduce.getMapGpuMemoryMbPerGpu(),
                    Set.of(),
                    mapReduce.getMapperCommand()));
        }

        dag.addTask(new DAGTask(
                "reduce",
                mapReduce.getReduceCpu(),
                mapReduce.getReduceMemory(),
                mapReduce.getReduceDiskMb(),
                mapReduce.getReduceGpuCount(),
                mapReduce.getReduceGpuMemoryMbPerGpu(),
                mapTaskIds,
                mapReduce.getReducerCommand()));
        return dag;
    }
}
