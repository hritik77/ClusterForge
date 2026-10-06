package com.JobController.job.spec;

import com.JobController.job.JobType;

public final class BatchJobSpecification extends JobSpecification {
    private final String command;
    private final int count;
    private final int cpu;
    private final int memory;

    public BatchJobSpecification(
            String jobId, String name, String command, int count, int cpu, int memory) {
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
        this.command = command;
        this.count = count;
        this.cpu = cpu;
        this.memory = memory;
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

    @Override
    public JobType getJobType() {
        return JobType.BATCH;
    }
}
