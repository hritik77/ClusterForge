package com.JobController.job.spec;

import com.JobController.job.JobType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ParameterSweepJobSpecification extends JobSpecification {
    private final String command;
    private final Map<String, List<String>> parameters;
    private final int cpu;
    private final int memory;
    private final long diskMb;
    private final int gpuCount;
    private final long gpuMemoryMbPerGpu;

    public ParameterSweepJobSpecification(
            String jobId,
            String name,
            String command,
            Map<String, List<String>> parameters,
            int cpu,
            int memory) {
        this(jobId, name, command, parameters, cpu, memory, 0, 0, 0);
    }

    public ParameterSweepJobSpecification(
            String jobId,
            String name,
            String command,
            Map<String, List<String>> parameters,
            int cpu,
            int memory,
            long diskMb,
            int gpuCount,
            long gpuMemoryMbPerGpu) {
        super(jobId, name);
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("Parameter sweep command must not be null or blank");
        }
        if (parameters == null || parameters.isEmpty()) {
            throw new IllegalArgumentException("Parameter sweep parameters must not be empty");
        }
        if (cpu <= 0) {
            throw new IllegalArgumentException("Parameter sweep CPU must be positive");
        }
        if (memory <= 0) {
            throw new IllegalArgumentException("Parameter sweep memory must be positive");
        }
        if (diskMb < 0 || gpuCount < 0 || gpuMemoryMbPerGpu < 0) {
            throw new IllegalArgumentException("Additional resource requests cannot be negative");
        }
        if (gpuCount == 0 && gpuMemoryMbPerGpu != 0) {
            throw new IllegalArgumentException(
                    "GPU memory request requires at least one requested GPU");
        }

        Map<String, List<String>> copiedParameters = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : parameters.entrySet()) {
            String parameterName = entry.getKey();
            List<String> values = entry.getValue();
            if (parameterName == null || parameterName.isBlank()) {
                throw new IllegalArgumentException("Parameter name must not be null or blank");
            }
            if (values == null || values.isEmpty()) {
                throw new IllegalArgumentException(
                        "Values for parameter '" + parameterName + "' must not be empty");
            }
            if (values.stream().anyMatch(value -> value == null)) {
                throw new IllegalArgumentException(
                        "Values for parameter '" + parameterName + "' must not contain null");
            }
            copiedParameters.put(parameterName, List.copyOf(new ArrayList<>(values)));
        }
        this.command = command;
        this.parameters = Collections.unmodifiableMap(copiedParameters);
        this.cpu = cpu;
        this.memory = memory;
        this.diskMb = diskMb;
        this.gpuCount = gpuCount;
        this.gpuMemoryMbPerGpu = gpuMemoryMbPerGpu;
    }

    public String getCommand() {
        return command;
    }

    public Map<String, List<String>> getParameters() {
        return parameters;
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
        return JobType.PARAMETER_SWEEP;
    }
}
