package com.JobController.job.spec;

import com.JobController.scheduling.PlacementRequirements;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class PipelineTaskSpecification {
    private final String id;
    private final String command;
    private final int cpu;
    private final int memory;
    private final long diskMb;
    private final int gpuCount;
    private final long gpuMemoryMbPerGpu;
    private final Set<String> dependencies;
    private final PlacementRequirements placementRequirements;

    public PipelineTaskSpecification(
            String id, String command, int cpu, int memory, Set<String> dependencies) {
        this(id, command, cpu, memory, 0, 0, 0, dependencies);
    }

    public PipelineTaskSpecification(
            String id, String command, int cpu, int memory, long diskMb, int gpuCount,
            long gpuMemoryMbPerGpu, Set<String> dependencies) {
        this(id, command, cpu, memory, diskMb, gpuCount, gpuMemoryMbPerGpu,
                dependencies, PlacementRequirements.none());
    }

    public PipelineTaskSpecification(
            String id, String command, int cpu, int memory, long diskMb, int gpuCount,
            long gpuMemoryMbPerGpu, Set<String> dependencies,
            PlacementRequirements placementRequirements) {
        if (placementRequirements == null) {
            throw new IllegalArgumentException("placementRequirements must not be null");
        }
        if (diskMb < 0 || gpuCount < 0 || gpuMemoryMbPerGpu < 0) {
            throw new IllegalArgumentException("Additional resource requests cannot be negative");
        }
        if (gpuCount == 0 && gpuMemoryMbPerGpu != 0) {
            throw new IllegalArgumentException(
                    "GPU memory request requires at least one requested GPU");
        }
        this.id = id;
        this.command = command;
        this.cpu = cpu;
        this.memory = memory;
        this.diskMb = diskMb;
        this.gpuCount = gpuCount;
        this.gpuMemoryMbPerGpu = gpuMemoryMbPerGpu;
        this.placementRequirements = placementRequirements;
        this.dependencies = Collections.unmodifiableSet(
                dependencies == null ? new LinkedHashSet<>() : new LinkedHashSet<>(dependencies));
    }

    public String getId() {
        return id;
    }

    public String getCommand() {
        return command;
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

    public Set<String> getDependencies() {
        return dependencies;
    }

    public PlacementRequirements getPlacementRequirements() {
        return placementRequirements;
    }
}
