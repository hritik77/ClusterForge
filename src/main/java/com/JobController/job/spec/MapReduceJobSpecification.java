package com.JobController.job.spec;

import com.JobController.job.JobType;

public final class MapReduceJobSpecification extends JobSpecification {
    private final String input;
    private final String mapperCommand;
    private final String reducerCommand;
    private final int partitions;
    private final int mapCpu;
    private final int mapMemory;
    private final int reduceCpu;
    private final int reduceMemory;
    private final long mapDiskMb;
    private final int mapGpuCount;
    private final long mapGpuMemoryMbPerGpu;
    private final long reduceDiskMb;
    private final int reduceGpuCount;
    private final long reduceGpuMemoryMbPerGpu;

    public MapReduceJobSpecification(
            String jobId,
            String name,
            String input,
            String mapperCommand,
            String reducerCommand,
            int partitions,
            int mapCpu,
            int mapMemory,
            int reduceCpu,
            int reduceMemory) {
        this(jobId, name, input, mapperCommand, reducerCommand, partitions,
                mapCpu, mapMemory, reduceCpu, reduceMemory, 0, 0, 0, 0, 0, 0);
    }

    public MapReduceJobSpecification(
            String jobId,
            String name,
            String input,
            String mapperCommand,
            String reducerCommand,
            int partitions,
            int mapCpu,
            int mapMemory,
            int reduceCpu,
            int reduceMemory,
            long mapDiskMb,
            int mapGpuCount,
            long mapGpuMemoryMbPerGpu,
            long reduceDiskMb,
            int reduceGpuCount,
            long reduceGpuMemoryMbPerGpu) {
        super(jobId, name);
        if (input == null || input.isBlank()) {
            throw new IllegalArgumentException("MapReduce input must not be null or blank");
        }
        if (mapperCommand == null || mapperCommand.isBlank()) {
            throw new IllegalArgumentException(
                    "MapReduce mapper command must not be null or blank");
        }
        if (reducerCommand == null || reducerCommand.isBlank()) {
            throw new IllegalArgumentException(
                    "MapReduce reducer command must not be null or blank");
        }
        if (partitions <= 0) {
            throw new IllegalArgumentException("MapReduce partition count must be positive");
        }
        if (mapCpu <= 0) {
            throw new IllegalArgumentException("Map task CPU must be positive");
        }
        if (mapMemory <= 0) {
            throw new IllegalArgumentException("Map task memory must be positive");
        }
        if (reduceCpu <= 0) {
            throw new IllegalArgumentException("Reduce task CPU must be positive");
        }
        if (reduceMemory <= 0) {
            throw new IllegalArgumentException("Reduce task memory must be positive");
        }
        if (mapDiskMb < 0 || mapGpuCount < 0 || mapGpuMemoryMbPerGpu < 0
                || reduceDiskMb < 0 || reduceGpuCount < 0 || reduceGpuMemoryMbPerGpu < 0) {
            throw new IllegalArgumentException("Additional resource requests cannot be negative");
        }
        if ((mapGpuCount == 0 && mapGpuMemoryMbPerGpu != 0)
                || (reduceGpuCount == 0 && reduceGpuMemoryMbPerGpu != 0)) {
            throw new IllegalArgumentException(
                    "GPU memory request requires at least one requested GPU");
        }

        this.input = input;
        this.mapperCommand = mapperCommand;
        this.reducerCommand = reducerCommand;
        this.partitions = partitions;
        this.mapCpu = mapCpu;
        this.mapMemory = mapMemory;
        this.reduceCpu = reduceCpu;
        this.reduceMemory = reduceMemory;
        this.mapDiskMb = mapDiskMb;
        this.mapGpuCount = mapGpuCount;
        this.mapGpuMemoryMbPerGpu = mapGpuMemoryMbPerGpu;
        this.reduceDiskMb = reduceDiskMb;
        this.reduceGpuCount = reduceGpuCount;
        this.reduceGpuMemoryMbPerGpu = reduceGpuMemoryMbPerGpu;
    }

    public String getInput() {
        return input;
    }

    public String getMapperCommand() {
        return mapperCommand;
    }

    public String getReducerCommand() {
        return reducerCommand;
    }

    public int getPartitions() {
        return partitions;
    }

    public int getMapCpu() {
        return mapCpu;
    }

    public int getMapMemory() {
        return mapMemory;
    }

    public int getReduceCpu() {
        return reduceCpu;
    }

    public int getReduceMemory() {
        return reduceMemory;
    }

    public long getMapDiskMb() {
        return mapDiskMb;
    }

    public int getMapGpuCount() {
        return mapGpuCount;
    }

    public long getMapGpuMemoryMbPerGpu() {
        return mapGpuMemoryMbPerGpu;
    }

    public long getReduceDiskMb() {
        return reduceDiskMb;
    }

    public int getReduceGpuCount() {
        return reduceGpuCount;
    }

    public long getReduceGpuMemoryMbPerGpu() {
        return reduceGpuMemoryMbPerGpu;
    }

    @Override
    public JobType getJobType() {
        return JobType.MAP_REDUCE;
    }
}
