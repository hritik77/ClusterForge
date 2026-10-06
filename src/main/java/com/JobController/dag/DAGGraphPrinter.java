package com.JobController.dag;

import com.JobController.Job;

import java.util.List;
import java.util.Objects;

public final class DAGGraphPrinter {
    private DAGGraphPrinter() {}

    public static String renderStandaloneJob(Job job) {
        Objects.requireNonNull(job, "Job must not be null");
        return """
                Job DAG (standalone submission)
                  [%s] Job #%d: %s (CPU=%d, memory=%d)
                    -> (no dependencies or dependents)
                """.formatted(
                job.getState(),
                job.getId(),
                singleLine(job.getDescription()),
                job.getCpuRequested(),
                job.getMemRequested()).stripTrailing();
    }

    public static String renderRun(DAG dag, DAGRun run) {
        DAGValidator.validate(dag);
        Objects.requireNonNull(run, "DAG run must not be null");
        if (!dag.getId().equals(run.getDagId())
                || !dag.getTasks().keySet().equals(run.getTaskStates().keySet())) {
            throw new IllegalArgumentException(
                    "DAG run does not match DAG '" + dag.getId() + "'");
        }

        StringBuilder graph = new StringBuilder()
                .append("DAG \"").append(singleLine(dag.getName()))
                .append("\" (").append(singleLine(dag.getId()))
                .append(") / run ").append(singleLine(run.getRunId()))
                .append(" [").append(run.getState()).append("]");
        List<String> taskIds = DAGValidator.topologicalOrder(dag);
        for (String taskId : taskIds) {
            Long jobId = run.getJobId(taskId);
            graph.append(System.lineSeparator())
                    .append("  [").append(run.getTaskState(taskId)).append("] ")
                    .append(singleLine(taskId));
            if (jobId != null) {
                graph.append(" (job #").append(jobId).append(')');
            }

            List<String> dependents = dag.getDependents(taskId).stream().sorted().toList();
            if (dependents.isEmpty()) {
                graph.append(System.lineSeparator()).append("    -> (end)");
            } else {
                for (String dependentId : dependents) {
                    graph.append(System.lineSeparator())
                            .append("    -> [")
                            .append(run.getTaskState(dependentId))
                            .append("] ")
                            .append(singleLine(dependentId));
                }
            }
        }
        return graph.toString();
    }

    private static String singleLine(String value) {
        return value.replaceAll("[\\p{Cntrl}]", " ");
    }
}
