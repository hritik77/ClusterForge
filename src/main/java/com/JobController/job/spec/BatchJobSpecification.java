package com.JobController.job.spec;

import com.JobController.job.JobType;

public final class BatchJobSpecification extends JobSpecification {
    private final String command;
    private final int count;
    private final int cpu;
    private final int memory;
    private final long diskMb;
    private final int gpuCount;
    private final long gpuMemoryMbPerGpu;

    public BatchJobSpecification(
            String jobId, String name, String command, int count, int cpu, int memory) {
        this(jobId, name, command, count, cpu, memory, 0, 0, 0);
    }

    public BatchJobSpecification(
            String jobId, String name, String command, int count, int cpu, int memory,
            long diskMb, int gpuCount, long gpuMemoryMbPerGpu) {
        super(jobId, name);
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("Batch command must not be null or blank");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("Batch count must be positive");
        }
        if (cpu <= 0) {
            throw new IllegalArgumentException("Batch CPU must be positive");
        }
        if (memory <= 0) {
            throw new IllegalArgumentException("Batch memory must be positive");
        }
        if (diskMb < 0 || gpuCount < 0 || gpuMemoryMbPerGpu < 0) {
            throw new IllegalArgumentException("Additional resource requests cannot be negative");
        }
        if (gpuCount == 0 && gpuMemoryMbPerGpu != 0) {
            throw new IllegalArgumentException(
                    "GPU memory request requires at least one requested GPU");
        }
        this.command = command;
        this.count = count;
        this.cpu = cpu;
        this.memory = memory;
        this.diskMb = diskMb;
        this.gpuCount = gpuCount;
        this.gpuMemoryMbPerGpu = gpuMemoryMbPerGpu;
    }

    public String getCommand() {
        return command;
    }

    public int getCount() {
        return count;
    }

    public int getCpu() {
        return cpu;
    }

    public int getMemory() {
        return memory;
    }

    public long getDiskMb() {
        return diskMb;
    }

    public int getGpuCount() {
        return gpuCount;
    }

    public long getGpuMemoryMbPerGpu() {
        return gpuMemoryMbPerGpu;
    }

    @Override
    public JobType getJobType() {
        return JobType.BATCH;
    }
}
