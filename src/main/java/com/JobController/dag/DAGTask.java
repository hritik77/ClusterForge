package com.JobController.dag;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public final class DAGTask {
    private final String id;
    private final int cpuRequested;
    private final int memRequested;
    private final Set<String> dependencies;
    private final String command;
    private final RetryPolicy retryPolicy;

    public DAGTask(String id, int cpuRequested, int memRequested, Set<String> dependencies) {
        this(id, cpuRequested, memRequested, dependencies, "", new RetryPolicy(0));
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, Set<String> dependencies,
            RetryPolicy retryPolicy) {
        this(id, cpuRequested, memRequested, dependencies, "", retryPolicy);
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, Set<String> dependencies,
            String command) {
        this(id, cpuRequested, memRequested, dependencies, command, new RetryPolicy(0));
    }

    public DAGTask(
            String id, int cpuRequested, int memRequested, Set<String> dependencies,
            String command, RetryPolicy retryPolicy) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Task ID must not be null or blank");
        }
        if (cpuRequested <= 0) {
            throw new IllegalArgumentException("Requested CPU must be positive");
        }
        if (memRequested <= 0) {
            throw new IllegalArgumentException("Requested memory must be positive");
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
        this.dependencies = Collections.unmodifiableSet(copiedDependencies);
        this.command = Objects.requireNonNull(command, "Task command must not be null");
        if (retryPolicy == null) {
            throw new IllegalArgumentException("retryPolicy cannot be null");
        }
        this.retryPolicy = retryPolicy;
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

    public Set<String> getDependencies() {
        return dependencies;
    }

    public String getCommand() {
        return command;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
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
        return "DAGTask{id='%s', cpuRequested=%d, memRequested=%d, dependencies=%s, command='%s', maxRetries=%d}"
                .formatted(id, cpuRequested, memRequested, dependencies, command,
                        retryPolicy.getMaxRetries());
    }
}
