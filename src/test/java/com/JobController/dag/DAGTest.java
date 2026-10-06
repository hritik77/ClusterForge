package com.JobController.dag;

import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DAGTest {
    @Test
    public void rejectsBlankIdAndName() {
        assertThrows(IllegalArgumentException.class, () -> new DAG(null, "workflow"));
        assertThrows(IllegalArgumentException.class, () -> new DAG(" ", "workflow"));
        assertThrows(IllegalArgumentException.class, () -> new DAG("workflow", null));
        assertThrows(IllegalArgumentException.class, () -> new DAG("workflow", " "));
    }

    @Test
    public void addsTasksAndBuildsReverseDependencies() {
        DAG dag = new DAG("dag-1", "example");
        DAGTask root = task("A");
        DAGTask childB = task("B", "A");
        DAGTask childC = task("C", "A");

        dag.addTask(root);
        dag.addTask(childB);
        dag.addTask(childC);

        assertEquals("dag-1", dag.getId());
        assertEquals("example", dag.getName());
        assertEquals(root, dag.getTask("A"));
        assertTrue(dag.hasTask("B"));
        assertFalse(dag.hasTask("missing"));
        assertEquals(Set.of("A"), dag.getDependencies("B"));
        assertEquals(Set.of("A"), dag.getDependencies("C"));
        assertEquals(Set.of("B", "C"), dag.getDependents("A"));
        assertEquals(Set.of(), dag.getDependents("B"));
        assertEquals(Set.of("B", "C"), dag.getDependents().get("A"));
        assertNotNull(dag.getDependents().get("B"));
    }

    @Test
    public void allowsUnresolvedDependencyReferencesUntilLaterPhase() {
        DAG dag = new DAG("dag-1", "example");
        dag.addTask(task("child", "not-added-yet"));

        assertEquals(Set.of("child"), dag.getDependents("not-added-yet"));
        assertFalse(dag.hasTask("not-added-yet"));
    }

    @Test
    public void rejectsDuplicateTaskIds() {
        DAG dag = new DAG("dag-1", "example");
        dag.addTask(task("task"));

        assertThrows(IllegalArgumentException.class, () -> dag.addTask(task("task")));
    }

    @Test
    public void returnedCollectionsCannotMutateDag() {
        DAG dag = new DAG("dag-1", "example");
        dag.addTask(task("root"));
        dag.addTask(task("child", "root"));

        Map<String, DAGTask> tasks = dag.getTasks();
        Map<String, Set<String>> dependents = dag.getDependents();
        assertThrows(UnsupportedOperationException.class,
                () -> tasks.put("other", task("other")));
        assertThrows(UnsupportedOperationException.class,
                () -> dependents.put("other", Set.of()));
        assertThrows(UnsupportedOperationException.class,
                () -> dependents.get("root").add("other"));
        assertThrows(UnsupportedOperationException.class,
                () -> dag.getDependents("root").clear());

        assertFalse(dag.hasTask("other"));
        assertEquals(Set.of("child"), dag.getDependents("root"));
    }

    @Test
    public void storesExampleWorkflowDefinition() {
        DAG dag = new DAG("example-run", "training workflow");
        dag.addTask(task("preprocess"));
        dag.addTask(task("augment", "preprocess"));
        dag.addTask(task("validate", "preprocess"));
        dag.addTask(task("train", "augment", "validate"));

        assertEquals(Set.of(), dag.getDependencies("preprocess"));
        assertEquals(Set.of("preprocess"), dag.getDependencies("augment"));
        assertEquals(Set.of("preprocess"), dag.getDependencies("validate"));
        assertEquals(Set.of("augment", "validate"), dag.getDependencies("train"));

        DAGRun run = new DAGRun("run-1", dag);
        assertEquals(TaskState.READY, run.getTaskState("preprocess"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("augment"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("validate"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("train"));
    }

    private static DAGTask task(String id, String... dependencies) {
        return new DAGTask(id, 1, 1, Set.of(dependencies));
    }
}
