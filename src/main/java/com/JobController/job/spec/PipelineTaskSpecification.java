package com.JobController.job.spec;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class PipelineTaskSpecification {
    private final String id;
    private final String command;
    private final int cpu;
    private final int memory;
    private final Set<String> dependencies;

    public PipelineTaskSpecification(
            String id, String command, int cpu, int memory, Set<String> dependencies) {
        this.id = id;
        this.command = command;
        this.cpu = cpu;
        this.memory = memory;
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

    public Set<String> getDependencies() {
        return dependencies;
    }
}
