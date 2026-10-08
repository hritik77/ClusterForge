package com.JobController.dag;

import com.JobController.scheduling.PlacementRequirements;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class DAGTask {
    private final String id;
    private final int cpuRequested;
    private final int memRequested;
    private final long diskMbRequested;
    private final int gpuCountRequested;
    private final long gpuMemoryMbPerGpu;
    private final Set<String> dependencies;
    private final String command;
    private final RetryPolicy retryPolicy;
    private final TimeoutPolicy timeoutPolicy;
    private final PlacementRequirements placementRequirements;

    public DAGTask(String id, int cpuRequested, int memRequested, Set<String> dependencies) {
        this(id, cpuRequested, memRequested, dependencies, "", new RetryPolicy(0),
                new TimeoutPolicy(null));
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, Set<String> dependencies,
            RetryPolicy retryPolicy) {
        this(id, cpuRequested, memRequested, dependencies, "", retryPolicy,
                new TimeoutPolicy(null));
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, Set<String> dependencies,
            String command) {
        this(id, cpuRequested, memRequested, dependencies, command, new RetryPolicy(0),
                new TimeoutPolicy(null));
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, Set<String> dependencies,
            String command, RetryPolicy retryPolicy) {
        this(id, cpuRequested, memRequested, dependencies, command, retryPolicy,
                new TimeoutPolicy(null));
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, long diskMbRequested,
            int gpuCountRequested, long gpuMemoryMbPerGpu, Set<String> dependencies,
            String command) {
        this(id, cpuRequested, memRequested, diskMbRequested, gpuCountRequested,
                gpuMemoryMbPerGpu, dependencies, command, new RetryPolicy(0),
                new TimeoutPolicy(null));
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, Set<String> dependencies,
            TimeoutPolicy timeoutPolicy) {
        this(id, cpuRequested, memRequested, dependencies, "", new RetryPolicy(0), timeoutPolicy);
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, Set<String> dependencies,
            String command, RetryPolicy retryPolicy, TimeoutPolicy timeoutPolicy) {
        this(id, cpuRequested, memRequested, 0, 0, 0, dependencies, command, retryPolicy,
                timeoutPolicy);
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, long diskMbRequested,
            int gpuCountRequested, long gpuMemoryMbPerGpu, Set<String> dependencies,
            String command, RetryPolicy retryPolicy, TimeoutPolicy timeoutPolicy) {
        this(id, cpuRequested, memRequested, diskMbRequested, gpuCountRequested,
                gpuMemoryMbPerGpu, dependencies, command, retryPolicy, timeoutPolicy,
                PlacementRequirements.none());
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, long diskMbRequested,
            int gpuCountRequested, long gpuMemoryMbPerGpu, Set<String> dependencies,
            String command, RetryPolicy retryPolicy, TimeoutPolicy timeoutPolicy,
            PlacementRequirements placementRequirements) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Task ID must not be null or blank");
        }
        if (cpuRequested <= 0) {
            throw new IllegalArgumentException("Requested CPU must be positive");
        }
        if (memRequested <= 0) {
            throw new IllegalArgumentException("Requested memory must be positive");
        }
        if (diskMbRequested < 0 || gpuCountRequested < 0 || gpuMemoryMbPerGpu < 0) {
            throw new IllegalArgumentException("Additional resource requests cannot be negative");
        }
        if (gpuCountRequested == 0 && gpuMemoryMbPerGpu != 0) {
            throw new IllegalArgumentException(
                    "GPU memory request requires at least one requested GPU");
        }

        Set<String> copiedDependencies = dependencies == null
                ? new LinkedHashSet<>()
                : new LinkedHashSet<>(dependencies);
        if (copiedDependencies.contains(id)) {
            throw new IllegalArgumentException("A task cannot depend on itself");
        }

        this.id = id;
        this.cpuRequested = cpuRequested;
        this.memRequested = memRequested;
        this.diskMbRequested = diskMbRequested;
        this.gpuCountRequested = gpuCountRequested;
        this.gpuMemoryMbPerGpu = gpuMemoryMbPerGpu;
        this.dependencies = Collections.unmodifiableSet(copiedDependencies);
        this.command = Objects.requireNonNull(command, "Task command must not be null");
        if (retryPolicy == null) {
            throw new IllegalArgumentException("retryPolicy cannot be null");
        }
        if (timeoutPolicy == null) {
            throw new IllegalArgumentException("timeoutPolicy cannot be null");
        }
        if (placementRequirements == null) {
            throw new IllegalArgumentException("placementRequirements cannot be null");
        }
        this.retryPolicy = retryPolicy;
        this.timeoutPolicy = timeoutPolicy;
        this.placementRequirements = placementRequirements;
    }

    public String getId() {
        return id;
    }

    public int getCpuRequested() {
        return cpuRequested;
    }

    public int getMemRequested() {
        return memRequested;
    }

    public long getDiskMbRequested() {
        return diskMbRequested;
    }

    public int getGpuCountRequested() {
        return gpuCountRequested;
    }

    public long getGpuMemoryMbPerGpu() {
        return gpuMemoryMbPerGpu;
    }

    public Set<String> getDependencies() {
        return dependencies;
    }

    public String getCommand() {
        return command;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public TimeoutPolicy getTimeoutPolicy() {
        return timeoutPolicy;
    }

    public PlacementRequirements getPlacementRequirements() {
        return placementRequirements;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DAGTask dagTask)) {
            return false;
        }
        return id.equals(dagTask.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        String format = "DAGTask{id='%s', cpuRequested=%d, memRequested=%d, diskMbRequested=%d, "
                + "gpuCountRequested=%d, gpuMemoryMbPerGpu=%d, dependencies=%s, command='%s', "
                + "maxRetries=%d}";
        return format.formatted(id, cpuRequested, memRequested, diskMbRequested,
                gpuCountRequested, gpuMemoryMbPerGpu, dependencies, command,
                retryPolicy.getMaxRetries());
    }
}
