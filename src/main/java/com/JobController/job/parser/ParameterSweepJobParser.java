package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.job.JobType;
import com.JobController.job.spec.JobSpecification;
import com.JobController.job.spec.ParameterSweepJobSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ParameterSweepJobParser implements JobParser {
    @Override
    public JobType getJobType() {
        return JobType.PARAMETER_SWEEP;
    }

    @Override
    public DAG parse(JobSpecification specification) {
        if (!(specification instanceof ParameterSweepJobSpecification sweep)) {
            throw new IllegalArgumentException(
                    "PARAMETER_SWEEP parser requires a ParameterSweepJobSpecification");
        }

        List<String> parameterNames = sweep.getParameters().keySet().stream().sorted().toList();
        List<Map<String, String>> combinations = new ArrayList<>();
        expandCombinations(sweep.getParameters(), parameterNames, 0, new java.util.LinkedHashMap<>(),
                combinations);

        DAG dag = new DAG(sweep.getJobId(), sweep.getName());
        for (int index = 0; index < combinations.size(); index++) {
            Map<String, String> combination = combinations.get(index);
            String taskId = "experiment-%03d".formatted(index + 1);
            String generatedCommand = generateCommand(sweep.getCommand(), parameterNames, combination);
            dag.addTask(new DAGTask(
                    taskId, sweep.getCpu(), sweep.getMemory(), sweep.getDiskMb(),
                    sweep.getGpuCount(), sweep.getGpuMemoryMbPerGpu(), Set.of(), generatedCommand));
        }
        return dag;
    }

    private static void expandCombinations(
            Map<String, List<String>> parameters,
            List<String> parameterNames,
            int parameterIndex,
            Map<String, String> combination,
            List<Map<String, String>> combinations) {
        if (parameterIndex == parameterNames.size()) {
            combinations.add(Map.copyOf(combination));
            return;
        }

        String parameterName = parameterNames.get(parameterIndex);
        for (String value : parameters.get(parameterName)) {
            combination.put(parameterName, value);
            expandCombinations(
                    parameters, parameterNames, parameterIndex + 1, combination, combinations);
        }
        combination.remove(parameterName);
    }

    private static String generateCommand(
            String command, List<String> parameterNames, Map<String, String> combination) {
        StringBuilder generated = new StringBuilder(command);
        for (String parameterName : parameterNames) {
            generated.append(" --")
                    .append(parameterName)
                    .append(' ')
                    .append(combination.get(parameterName));
        }
        return generated.toString();
    }
}
