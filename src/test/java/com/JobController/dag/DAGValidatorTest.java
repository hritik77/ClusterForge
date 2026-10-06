package com.JobController.dag;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DAGValidatorTest {
    @Test
    public void validatesChainAndFindsRootsLeavesAndTopologicalOrder() {
        DAG dag = dag("chain", "A", "B", "C");

        assertTrue(DAGValidator.isValid(dag));
        assertEquals(List.of("A"), DAGValidator.getRootTasks(dag));
        assertEquals(List.of("C"), DAGValidator.getLeafTasks(dag));
        assertEquals(List.of("A", "B", "C"), DAGValidator.topologicalOrder(dag));
        DAGValidator.validate(dag);
    }

    @Test
    public void analyzesDiamondDag() {
        DAG dag = new DAG("diamond", "diamond");
        dag.addTask(task("A"));
        dag.addTask(task("B", "A"));
        dag.addTask(task("C", "A"));
        dag.addTask(task("D", "B", "C"));

        assertTrue(DAGValidator.isValid(dag));
        assertEquals(List.of("A"), DAGValidator.getRootTasks(dag));
        assertEquals(List.of("D"), DAGValidator.getLeafTasks(dag));
        assertTopologicalEdges(dag, DAGValidator.topologicalOrder(dag));
        assertEquals(List.of("A", "B", "C", "D"), DAGValidator.topologicalOrder(dag));
    }

    @Test
    public void findsMultipleRootsAndLeavesDeterministically() {
        DAG dag = new DAG("multiple-roots", "multiple roots");
        dag.addTask(task("A"));
        dag.addTask(task("B"));
        dag.addTask(task("C", "A"));
        dag.addTask(task("D", "B"));

        assertEquals(List.of("A", "B"), DAGValidator.getRootTasks(dag));
        assertEquals(List.of("C", "D"), DAGValidator.getLeafTasks(dag));
    }

    @Test
    public void rejectsNullAndEmptyDag() {
        assertThrows(IllegalArgumentException.class, () -> DAGValidator.validate(null));
        assertFalse(DAGValidator.isValid(null));
        assertThrows(IllegalArgumentException.class,
                () -> DAGValidator.validate(new DAG("empty", "empty")));
    }

    @Test
    public void rejectsSelfDependencyAtTaskConstruction() {
        assertThrows(IllegalArgumentException.class, () -> task("A", "A"));
    }

    @Test
    public void rejectsSimpleCycle() {
        DAG dag = new DAG("cycle", "cycle");
        dag.addTask(task("A", "C"));
        dag.addTask(task("B", "A"));
        dag.addTask(task("C", "B"));

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> DAGValidator.validate(dag));
        assertEquals("DAG contains a cycle", exception.getMessage());
        assertFalse(DAGValidator.isValid(dag));
    }

    @Test
    public void rejectsLargerCycle() {
        DAG dag = new DAG("larger-cycle", "larger cycle");
        dag.addTask(task("A"));
        dag.addTask(task("B", "A", "D"));
        dag.addTask(task("C", "B"));
        dag.addTask(task("D", "C"));

        assertThrows(IllegalArgumentException.class, () -> DAGValidator.validate(dag));
    }

    @Test
    public void rejectsMissingDependencyWithTaskAndDependencyInMessage() {
        DAG dag = new DAG("missing", "missing dependency");
        dag.addTask(task("B", "X"));

        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> DAGValidator.validate(dag));
        assertEquals("Task 'B' depends on unknown task 'X'", exception.getMessage());
    }

    @Test
    public void topologicalOrderIsDeterministicAndRespectsEveryEdge() {
        DAG dag = new DAG("unordered", "unordered insertion");
        dag.addTask(task("D", "B", "C"));
        dag.addTask(task("C", "A"));
        dag.addTask(task("B", "A"));
        dag.addTask(task("A"));

        List<String> first = DAGValidator.topologicalOrder(dag);
        for (int attempt = 0; attempt < 10; attempt++) {
            assertEquals(first, DAGValidator.topologicalOrder(dag));
        }
        assertEquals(List.of("A", "B", "C", "D"), first);
        assertTopologicalEdges(dag, first);
    }

    @Test
    public void findsSingleReadyTaskWithoutChangingRun() {
        DAG dag = dag("ready-one", "A", "B");
        DAGRun run = new DAGRun("run-one", dag);
        run.setTaskState("A", TaskState.COMPLETED);
        var before = run.getTaskStates();

        assertEquals(Set.of("B"), DAGValidator.findReadyTasks(dag, run));
        assertEquals(before, run.getTaskStates());
        assertEquals(TaskState.BLOCKED, run.getTaskState("B"));
    }

    @Test
    public void findsParallelReadyTasksInDeterministicOrder() {
        DAG dag = new DAG("parallel", "parallel");
        dag.addTask(task("A"));
        dag.addTask(task("C", "A"));
        dag.addTask(task("B", "A"));
        DAGRun run = new DAGRun("parallel-run", dag);
        run.setTaskState("A", TaskState.COMPLETED);

        Set<String> ready = DAGValidator.findReadyTasks(dag, run);
        assertEquals(List.of("B", "C"), new ArrayList<>(ready));
    }

    @Test
    public void doesNotReadyTaskUntilAllDependenciesComplete() {
        DAG dag = dag("blocked", "A", "B", "C");
        DAGRun run = new DAGRun("blocked-run", dag);
        run.setTaskState("A", TaskState.COMPLETED);
        run.setTaskState("B", TaskState.RUNNING);

        assertTrue(DAGValidator.findReadyTasks(dag, run).isEmpty());
    }

    @Test
    public void rejectsReadinessQueryForMismatchedRun() {
        DAG dag = dag("first", "A");
        DAG otherDag = dag("other", "A");

        assertThrows(IllegalArgumentException.class,
                () -> DAGValidator.findReadyTasks(dag, new DAGRun("other-run", otherDag)));
        assertThrows(IllegalArgumentException.class,
                () -> DAGValidator.findReadyTasks(dag, null));
    }

    @Test
    public void detectsCompleteAndIncompleteRuns() {
        DAG dag = dag("completion", "A", "B");
        DAGRun complete = new DAGRun("complete", dag);
        complete.setTaskState("A", TaskState.COMPLETED);
        complete.setTaskState("B", TaskState.COMPLETED);

        DAGRun incomplete = new DAGRun("incomplete", dag);
        incomplete.setTaskState("A", TaskState.COMPLETED);
        incomplete.setTaskState("B", TaskState.RUNNING);

        assertTrue(DAGValidator.isComplete(complete));
        assertFalse(DAGValidator.isComplete(incomplete));
    }

    @Test
    public void detectsFailureWithoutFailurePropagation() {
        DAG dag = dag("failure", "A", "B");
        DAGRun failed = new DAGRun("failed", dag);
        failed.setTaskState("A", TaskState.FAILED);
        assertTrue(DAGValidator.hasFailed(failed));
        assertEquals(TaskState.BLOCKED, failed.getTaskState("B"));

        DAGRun noFailure = new DAGRun("no-failure", dag);
        noFailure.setTaskState("A", TaskState.COMPLETED);
        noFailure.setTaskState("B", TaskState.RUNNING);
        assertFalse(DAGValidator.hasFailed(noFailure));
    }

    @Test
    public void taskStateAnalysisRejectsNullRuns() {
        assertThrows(NullPointerException.class, () -> DAGValidator.isComplete(null));
        assertThrows(NullPointerException.class, () -> DAGValidator.hasFailed(null));
    }

    private static DAG dag(String id, String... taskIds) {
        DAG dag = new DAG(id, id);
        String previous = null;
        for (String taskId : taskIds) {
            dag.addTask(previous == null ? task(taskId) : task(taskId, previous));
            previous = taskId;
        }
        return dag;
    }

    private static DAGTask task(String id, String... dependencies) {
        return new DAGTask(id, 1, 1, Set.of(dependencies));
    }

    private static void assertTopologicalEdges(DAG dag, List<String> order) {
        assertEquals(dag.getTasks().size(), order.size());
        assertEquals(order.size(), order.stream().distinct().count());
        for (DAGTask task : dag.getTasks().values()) {
            for (String dependency : task.getDependencies()) {
                assertTrue(dependency + " must come before " + task.getId(),
                        order.indexOf(dependency) < order.indexOf(task.getId()));
            }
        }
    }
}
