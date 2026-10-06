package com.JobController.dag;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class DAG {
    private final String id;
    private final String name;
    private final Map<String, DAGTask> tasks = new LinkedHashMap<>();
    private final Map<String, Set<String>> dependents = new LinkedHashMap<>();
    private final Map<String, DAGTask> readOnlyTasks = Collections.unmodifiableMap(tasks);
    private final Map<String, Set<String>> readOnlyDependents =
            Collections.unmodifiableMap(dependents);

    public DAG(String id, String name) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("DAG ID must not be null or blank");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("DAG name must not be null or blank");
        }
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void addTask(DAGTask task) {
        Objects.requireNonNull(task, "Task must not be null");
        String taskId = task.getId();
        if (tasks.containsKey(taskId)) {
            throw new IllegalArgumentException("Duplicate task ID: " + taskId);
        }

        tasks.put(taskId, task);
        dependents.putIfAbsent(taskId, Set.of());
        for (String dependencyId : task.getDependencies()) {
            Set<String> updatedDependents =
                    new LinkedHashSet<>(dependents.getOrDefault(dependencyId, Set.of()));
            updatedDependents.add(taskId);
            dependents.put(dependencyId, Collections.unmodifiableSet(updatedDependents));
        }
    }

    public DAGTask getTask(String taskId) {
        return tasks.get(taskId);
    }

    public Map<String, DAGTask> getTasks() {
        return readOnlyTasks;
    }

    public Set<String> getDependencies(String taskId) {
        DAGTask task = tasks.get(taskId);
        return task == null ? Set.of() : task.getDependencies();
    }

    public Set<String> getDependents(String taskId) {
        Set<String> taskDependents = dependents.get(taskId);
        return taskDependents == null ? Set.of() : taskDependents;
    }

    public Map<String, Set<String>> getDependents() {
        return readOnlyDependents;
    }

    public boolean hasTask(String taskId) {
        return tasks.containsKey(taskId);
    }
}
