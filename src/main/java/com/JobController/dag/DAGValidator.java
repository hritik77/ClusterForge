package com.JobController.dag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

public final class DAGValidator {
    private DAGValidator() {}

    public static void validate(DAG dag) {
        if (dag == null) {
            throw new IllegalArgumentException("DAG must not be null");
        }
        if (dag.getId() == null || dag.getId().isBlank()) {
            throw new IllegalArgumentException("DAG ID must not be null or blank");
        }
        if (dag.getName() == null || dag.getName().isBlank()) {
            throw new IllegalArgumentException("DAG name must not be null or blank");
        }

        Map<String, DAGTask> tasks = dag.getTasks();
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("DAG must contain at least one task");
        }

        Set<String> taskIds = new LinkedHashSet<>();
        for (Map.Entry<String, DAGTask> entry : tasks.entrySet()) {
            String taskId = entry.getKey();
            DAGTask task = entry.getValue();
            if (task == null) {
                throw new IllegalArgumentException("DAG contains a null task for ID '" + taskId + "'");
            }
            if (taskId == null || taskId.isBlank()) {
                throw new IllegalArgumentException("Task ID must not be null or blank");
            }
            if (!taskIds.add(taskId)) {
                throw new IllegalArgumentException("Duplicate task ID: " + taskId);
            }
            if (!taskId.equals(task.getId())) {
                throw new IllegalArgumentException(
                        "Task map key '" + taskId + "' does not match task ID '" + task.getId() + "'");
            }
            if (task.getCpuRequested() <= 0) {
                throw new IllegalArgumentException(
                        "Task '" + taskId + "' must request positive CPU");
            }
            if (task.getMemRequested() <= 0) {
                throw new IllegalArgumentException(
                        "Task '" + taskId + "' must request positive memory");
            }

            Set<String> dependencies = task.getDependencies();
            if (dependencies == null) {
                throw new IllegalArgumentException(
                        "Task '" + taskId + "' has null dependencies");
            }
            for (String dependencyId : dependencies) {
                if (taskId.equals(dependencyId)) {
                    throw new IllegalArgumentException(
                            "Task '" + taskId + "' cannot depend on itself");
                }
                if (!tasks.containsKey(dependencyId)) {
                    throw new IllegalArgumentException("Task '" + taskId
                            + "' depends on unknown task '" + dependencyId + "'");
                }
            }
        }

        List<String> order = topologicalOrderUnchecked(dag);
        if (order.size() != tasks.size()) {
            throw new IllegalArgumentException("DAG contains a cycle");
        }
    }

    public static boolean isValid(DAG dag) {
        try {
            validate(dag);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public static List<String> topologicalOrder(DAG dag) {
        validate(dag);
        return topologicalOrderUnchecked(dag);
    }

    public static List<String> getRootTasks(DAG dag) {
        validate(dag);
        return dag.getTasks().values().stream()
                .filter(task -> task.getDependencies().isEmpty())
                .map(DAGTask::getId)
                .sorted()
                .toList();
    }

    public static List<String> getLeafTasks(DAG dag) {
        validate(dag);
        return dag.getTasks().keySet().stream()
                .filter(taskId -> dag.getDependents(taskId).isEmpty())
                .sorted()
                .toList();
    }

    public static Set<String> findReadyTasks(DAG dag, DAGRun run) {
        validate(dag);
        validateRun(dag, run);

        Set<String> ready = new LinkedHashSet<>();
        dag.getTasks().values().stream()
                .filter(task -> run.getTaskState(task.getId()) == TaskState.BLOCKED)
                .filter(task -> task.getDependencies().stream()
                        .allMatch(dependencyId ->
                                run.getTaskState(dependencyId) == TaskState.COMPLETED))
                .map(DAGTask::getId)
                .sorted()
                .forEach(ready::add);
        return Collections.unmodifiableSet(ready);
    }

    public static boolean isComplete(DAGRun run) {
        Objects.requireNonNull(run, "DAG run must not be null");
        return !run.getTaskStates().isEmpty()
                && run.getTaskStates().values().stream()
                        .allMatch(state -> state == TaskState.COMPLETED);
    }

    public static boolean hasFailed(DAGRun run) {
        Objects.requireNonNull(run, "DAG run must not be null");
        return run.getTaskStates().values().stream()
                .anyMatch(state -> state == TaskState.FAILED);
    }

    private static List<String> topologicalOrderUnchecked(DAG dag) {
        Map<String, Integer> indegrees = new HashMap<>();
        PriorityQueue<String> ready = new PriorityQueue<>();

        for (DAGTask task : dag.getTasks().values()) {
            int indegree = task.getDependencies().size();
            indegrees.put(task.getId(), indegree);
            if (indegree == 0) {
                ready.add(task.getId());
            }
        }

        List<String> order = new ArrayList<>(dag.getTasks().size());
        while (!ready.isEmpty()) {
            String taskId = ready.remove();
            order.add(taskId);
            for (String dependentId : dag.getDependents(taskId)) {
                int remaining = indegrees.compute(dependentId,
                        (ignored, indegree) -> indegree - 1);
                if (remaining == 0) {
                    ready.add(dependentId);
                }
            }
        }
        return List.copyOf(order);
    }

    private static void validateRun(DAG dag, DAGRun run) {
        if (run == null) {
            throw new IllegalArgumentException("DAG run must not be null");
        }
        if (!dag.getId().equals(run.getDagId())) {
            throw new IllegalArgumentException("DAG run '" + run.getRunId()
                    + "' belongs to DAG '" + run.getDagId()
                    + "', not DAG '" + dag.getId() + "'");
        }
        if (!dag.getTasks().keySet().equals(run.getTaskStates().keySet())) {
            throw new IllegalArgumentException("DAG run '" + run.getRunId()
                    + "' does not contain the same tasks as DAG '" + dag.getId() + "'");
        }
    }
}
