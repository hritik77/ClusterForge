package com.JobController.dag;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DAGRunTest {
    @Test
    public void initializesTaskStatesFromDagDependencies() {
        DAG dag = chain();
        DAGRun run = new DAGRun("run-1", dag);

        assertEquals("run-1", run.getRunId());
        assertEquals("dag-1", run.getDagId());
        assertEquals(TaskState.READY, run.getTaskState("A"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("B"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("C"));
        assertEquals(3, run.getTaskStates().size());
    }

    @Test
    public void rejectsInvalidRunAndDag() {
        DAG dag = chain();
        assertThrows(IllegalArgumentException.class, () -> new DAGRun(null, dag));
        assertThrows(IllegalArgumentException.class, () -> new DAGRun(" ", dag));
        assertThrows(NullPointerException.class, () -> new DAGRun("run-1", null));
    }

    @Test
    public void runStateDefaultsToRunningAndRejectsNull() {
        DAGRun run = new DAGRun("run-1", chain());

        assertEquals(DAGRunState.RUNNING, run.getState());
        run.setState(DAGRunState.COMPLETED);
        assertEquals(DAGRunState.COMPLETED, run.getState());
        assertThrows(NullPointerException.class, () -> run.setState(null));
    }

    @Test
    public void changesStateAndRejectsInvalidTaskOrState() {
        DAGRun run = new DAGRun("run-1", chain());
        run.setTaskState("B", TaskState.RUNNING);

        assertEquals(TaskState.RUNNING, run.getTaskState("B"));
        assertThrows(IllegalArgumentException.class, () -> run.getTaskState("missing"));
        assertThrows(IllegalArgumentException.class,
                () -> run.setTaskState("missing", TaskState.READY));
        assertThrows(NullPointerException.class, () -> run.setTaskState("A", null));
    }

    @Test
    public void mapsDagTasksToPositiveJobIds() {
        DAGRun run = new DAGRun("run-1", chain());

        assertNull(run.getJobId("A"));
        run.setJobId("A", 101);
        run.setJobId("B", 102);

        assertEquals(Long.valueOf(101), run.getJobId("A"));
        assertEquals(Long.valueOf(102), run.getJobId("B"));
        assertEquals(Map.of("A", 101L, "B", 102L), run.getJobIds());
        assertThrows(IllegalArgumentException.class, () -> run.setJobId("A", 0));
        assertThrows(IllegalArgumentException.class, () -> run.setJobId("A", -1));
        assertThrows(IllegalArgumentException.class, () -> run.getJobId("missing"));
        assertThrows(IllegalArgumentException.class, () -> run.setJobId("missing", 103));
    }

    @Test
    public void returnedMapsCannotMutateRun() {
        DAGRun run = new DAGRun("run-1", chain());
        assertThrows(UnsupportedOperationException.class,
                () -> run.getTaskStates().put("A", TaskState.FAILED));
        assertThrows(UnsupportedOperationException.class,
                () -> run.getJobIds().put("A", 101L));

        assertEquals(TaskState.READY, run.getTaskState("A"));
        assertNull(run.getJobId("A"));
    }

    @Test
    public void initializesAndTracksRetryCountsPerTask() {
        DAGRun run = new DAGRun("run-retries", chain());

        assertEquals(0, run.getRetryCount("A"));
        assertEquals(0, run.getRetryCount("B"));
        run.incrementRetryCount("A");
        assertEquals(1, run.getRetryCount("A"));
        assertEquals(Map.of("A", 1, "B", 0, "C", 0), run.getRetryCounts());
        assertThrows(UnsupportedOperationException.class,
                () -> run.getRetryCounts().put("A", 10));
        assertThrows(IllegalArgumentException.class, () -> run.getRetryCount("missing"));
        assertThrows(IllegalArgumentException.class, () -> run.incrementRetryCount("missing"));
    }

    @Test
    public void keepsZeroBasedAttemptHistoryPerTask() {
        DAGRun run = new DAGRun("attempt-run", chain());

        assertNull(run.getCurrentAttempt("A"));
        TaskAttempt first = run.createAttempt("A");
        assertEquals(0, first.getAttemptNumber());
        assertTrue(first.markSubmitted(101));
        var firstView = run.getAttempts("A").get(0);
        TaskAttempt second = run.createAttempt("A");
        assertEquals(1, second.getAttemptNumber());
        assertTrue(second.markSubmitted(102));

        assertEquals(2, run.getAttempts("A").size());
        assertEquals(0, run.getAttempts("B").size());
        assertEquals(second.getAttemptId(), run.getCurrentAttempt("A").getAttemptId());
        assertThrows(UnsupportedOperationException.class,
                () -> run.getAttempts("A").clear());
        assertEquals(first.getCreatedAtMillis(), run.getAttempts("A").get(0).getCreatedAtMillis());
        assertTrue(first.markTerminal(
                AttemptState.FAILED, RetryReason.JOB_FAILURE, "failure", 200L));
        assertEquals(AttemptState.SUBMITTED, firstView.getState());
        assertEquals(AttemptState.FAILED, run.getAttempts("A").get(0).getState());
        assertThrows(UnsupportedOperationException.class,
                () -> run.getAttempts("A").get(0).getWorkerLabels().put("region", "west"));
        assertThrows(IllegalArgumentException.class, () -> run.createAttempt("missing"));
    }

    @Test
    public void runUsesSnapshotOfDagTasks() {
        DAG dag = new DAG("dag-1", "chain");
        dag.addTask(task("A"));
        DAGRun run = new DAGRun("run-1", dag);
        dag.addTask(task("B", "A"));

        assertEquals(1, run.getTaskStates().size());
        assertThrows(IllegalArgumentException.class, () -> run.getTaskState("B"));
    }

    @Test
    public void storesRunDeadlineAndTaskStartTime() {
        DAG dag = new DAG("timed", "timed", 10_000L);
        dag.addTask(task("A"));
        DAGRun run = new DAGRun("timed-run", dag);

        assertTrue(run.getStartTimeMillis() > 0);
        assertEquals(Long.valueOf(run.getStartTimeMillis() + 10_000L),
                run.getDeadlineMillis());
        assertTrue(run.hasDeadline());
        run.setTaskState("A", TaskState.RUNNING);
        run.markTaskStarted("A", 1234L);
        assertEquals(Long.valueOf(1234), run.getTaskStartTime("A"));
        assertEquals(Map.of("A", 1234L), run.getTaskStartTimes());
        assertThrows(UnsupportedOperationException.class,
                () -> run.getTaskStartTimes().put("A", 456L));
    }

    @Test
    public void terminalRunStateCannotTransitionBackToRunning() {
        DAGRun run = new DAGRun("run-1", chain());

        assertTrue(run.setState(DAGRunState.COMPLETED));
        assertFalse(run.setState(DAGRunState.RUNNING));
        assertEquals(DAGRunState.COMPLETED, run.getState());
    }

    @Test
    public void completedAndCancelledTasksCannotBeChangedByLateEvents() {
        DAGRun run = new DAGRun("run-1", chain());
        run.setTaskState("A", TaskState.COMPLETED);
        run.setTaskState("B", TaskState.CANCELLED);

        assertThrows(IllegalStateException.class,
                () -> run.setTaskState("A", TaskState.FAILED));
        assertThrows(IllegalStateException.class,
                () -> run.setTaskState("B", TaskState.COMPLETED));
        assertFalse(run.transitionTaskState("A", TaskState.COMPLETED, TaskState.RUNNING));
        assertEquals(TaskState.COMPLETED, run.getTaskState("A"));
        assertEquals(TaskState.CANCELLED, run.getTaskState("B"));
    }

    private static DAG chain() {
        DAG dag = new DAG("dag-1", "chain");
        dag.addTask(task("A"));
        dag.addTask(task("B", "A"));
        dag.addTask(task("C", "B"));
        return dag;
    }

    private static DAGTask task(String id, String... dependencies) {
        return new DAGTask(id, 1, 1, java.util.Set.of(dependencies));
    }
}
