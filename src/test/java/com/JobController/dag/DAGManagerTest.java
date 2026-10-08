package com.JobController.dag;

import com.JobController.Job;
import com.JobController.JobState;
import com.JobController.JobSubmissionService;
import com.JobController.scheduling.PlacementConstraint;
import com.JobController.scheduling.PlacementOperator;
import com.JobController.scheduling.PlacementRequirements;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DAGManagerTest {
    @Test
    public void carriesPlacementRequirementsIntoSubmittedJobAndRetry() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        DAG dag = new DAG("placement-retry", "placement retry");
        PlacementRequirements requirements = new PlacementRequirements(List.of(
                new PlacementConstraint("architecture", PlacementOperator.EQUALS, "x86_64")));
        dag.addTask(new DAGTask(
                "A", 1, 1, 0, 0, 0, Set.of(), "run", new RetryPolicy(1),
                new TimeoutPolicy(null), requirements));
        manager.registerDAG(dag);

        DAGRun run = manager.startRun("placement-retry");
        Long firstJobId = run.getJobId("A");
        assertEquals("x86_64", submissions.job(firstJobId)
                .getPlacementConstraints(0).getValue());
        manager.onJobLost(terminalJob(firstJobId, JobState.LOST));

        Long retryJobId = run.getJobId("A");
        assertNotEquals(firstJobId, retryJobId);
        assertEquals(submissions.job(firstJobId).getPlacementConstraintsList(),
                submissions.job(retryJobId).getPlacementConstraintsList());
    }

    @Test
    public void runsChainThroughExistingJobSubmissionService() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        manager.registerDAG(chain("chain", "A", "B", "C"));

        DAGRun run = manager.startRun("chain");
        assertEquals(DAGRunState.RUNNING, run.getState());
        assertEquals(TaskState.SUBMITTED, run.getTaskState("A"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("B"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("C"));
        assertEquals(Set.of("A"), submissions.taskIdsForRun(run));

        manager.onJobCompleted(completedJob(run.getJobId("A")));
        assertEquals(TaskState.SUBMITTED, run.getTaskState("B"));
        assertEquals(Set.of("A", "B"), submissions.taskIdsForRun(run));

        manager.onJobCompleted(completedJob(run.getJobId("B")));
        assertEquals(TaskState.SUBMITTED, run.getTaskState("C"));
        manager.onJobCompleted(completedJob(run.getJobId("C")));
        assertEquals(DAGRunState.COMPLETED, run.getState());
        assertTrue(DAGValidator.isComplete(run));
        assertEquals(3, submissions.jobs().size());
    }

    @Test
    public void diamondSubmitsIndependentReadyTasksInParallelAndUnlocksJoin() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        DAG diamond = new DAG("diamond", "diamond");
        diamond.addTask(task("A"));
        diamond.addTask(task("B", "A"));
        diamond.addTask(task("C", "A"));
        diamond.addTask(task("D", "B", "C"));
        manager.registerDAG(diamond);

        DAGRun run = manager.startRun("diamond");
        manager.onJobCompleted(completedJob(run.getJobId("A")));

        assertEquals(TaskState.SUBMITTED, run.getTaskState("B"));
        assertEquals(TaskState.SUBMITTED, run.getTaskState("C"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("D"));
        assertEquals(Set.of("A", "B", "C"), submissions.taskIdsForRun(run));

        manager.onJobCompleted(completedJob(run.getJobId("B")));
        assertEquals(TaskState.BLOCKED, run.getTaskState("D"));
        manager.onJobCompleted(completedJob(run.getJobId("C")));
        assertEquals(TaskState.SUBMITTED, run.getTaskState("D"));

        manager.onJobCompleted(completedJob(run.getJobId("D")));
        assertEquals(DAGRunState.COMPLETED, run.getState());
    }

    @Test
    public void submitsMultipleRootTasksWithoutSelectingWorkers() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        DAG dag = new DAG("multiple-roots", "multiple roots");
        dag.addTask(task("A"));
        dag.addTask(task("B"));
        dag.addTask(task("C", "A"));
        dag.addTask(task("D", "B"));
        manager.registerDAG(dag);

        DAGRun run = manager.startRun("multiple-roots");

        assertEquals(Set.of("A", "B"), submissions.taskIdsForRun(run));
        assertEquals(TaskState.BLOCKED, run.getTaskState("C"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("D"));
    }

    @Test
    public void failureStopsDownstreamAndFailsRun() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        manager.registerDAG(chain("failure", "A", "B", "C"));

        DAGRun run = manager.startRun("failure");
        manager.onJobCompleted(completedJob(run.getJobId("A")));
        manager.onJobFailed(terminalJob(run.getJobId("B"), JobState.FAILED));
        manager.onJobFailed(terminalJob(run.getJobId("B"), JobState.FAILED));

        assertEquals(TaskState.FAILED, run.getTaskState("B"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("C"));
        assertEquals(DAGRunState.FAILED, run.getState());
        assertEquals(Set.of("A", "B"), submissions.taskIdsForRun(run));
    }

    @Test
    public void retriesJobFailureWithNewJobIdAndIgnoresStaleEvents() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        DAG dag = new DAG("retry", "retry");
        dag.addTask(taskWithRetries("A", 1));
        dag.addTask(task("B", "A"));
        manager.registerDAG(dag);

        DAGRun run = manager.startRun("retry");
        Long firstJobId = run.getJobId("A");
        manager.onJobFailed(terminalJob(firstJobId, JobState.FAILED));
        Long retryJobId = run.getJobId("A");

        assertNotEquals(firstJobId, retryJobId);
        assertEquals(1, run.getRetryCount("A"));
        assertEquals(TaskState.SUBMITTED, run.getTaskState("A"));
        assertEquals(JobState.PENDING, submissions.job(retryJobId).getState());
        assertEquals(2, run.getAttempts("A").size());
        assertEquals(0, run.getAttempts("A").get(0).getAttemptNumber());
        assertEquals(AttemptState.FAILED, run.getAttempts("A").get(0).getState());
        assertEquals(RetryReason.JOB_FAILURE,
                run.getAttempts("A").get(0).getFailureReason());
        assertEquals(Long.valueOf(firstJobId),
                run.getAttempts("A").get(0).getJobId());
        assertEquals(1, run.getCurrentAttempt("A").getAttemptNumber());
        assertEquals(Long.valueOf(retryJobId), run.getCurrentAttempt("A").getJobId());

        manager.onJobCompleted(completedJob(firstJobId));
        manager.onJobFailed(terminalJob(firstJobId, JobState.FAILED));
        assertEquals(TaskState.SUBMITTED, run.getTaskState("A"));
        assertEquals(1, run.getRetryCount("A"));
        assertEquals(2, submissions.jobs().size());

        manager.onJobCompleted(completedJob(retryJobId));
        assertEquals(AttemptState.COMPLETED, run.getCurrentAttempt("A").getState());
        assertEquals(2, run.getAttempts("A").size());
        assertEquals(TaskState.SUBMITTED, run.getTaskState("B"));
        assertEquals(3, submissions.jobs().size());
    }

    @Test
    public void retriesLostJobAndFailsAfterRetryLimit() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        DAG dag = new DAG("lost-retry", "lost-retry");
        dag.addTask(taskWithRetries("A", 1));
        manager.registerDAG(dag);
        DAGRun run = manager.startRun("lost-retry");

        Long firstJobId = run.getJobId("A");
        manager.onJobLost(terminalJob(firstJobId, JobState.LOST));
        Long retryJobId = run.getJobId("A");
        assertNotEquals(firstJobId, retryJobId);
        assertEquals(1, run.getRetryCount("A"));
        assertEquals(AttemptState.LOST, run.getAttempts("A").get(0).getState());
        assertEquals(RetryReason.WORKER_FAILURE,
                run.getAttempts("A").get(0).getFailureReason());
        assertEquals(1, run.getCurrentAttempt("A").getAttemptNumber());
        manager.onJobLost(terminalJob(firstJobId, JobState.LOST));
        assertEquals(retryJobId, run.getJobId("A"));
        assertEquals(1, run.getRetryCount("A"));

        manager.onJobLost(terminalJob(retryJobId, JobState.LOST));
        manager.onJobLost(terminalJob(retryJobId, JobState.LOST));
        assertEquals(1, run.getRetryCount("A"));
        assertEquals(TaskState.FAILED, run.getTaskState("A"));
        assertEquals(DAGRunState.FAILED, run.getState());
        assertEquals(2, submissions.jobs().size());
    }

    @Test
    public void jobFailureWithoutRetryFailsRunOnce() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        manager.registerDAG(chain("no-retry", "A", "B"));
        DAGRun run = manager.startRun("no-retry");
        Long jobId = run.getJobId("A");

        manager.onJobFailed(terminalJob(jobId, JobState.FAILED));
        manager.onJobFailed(terminalJob(jobId, JobState.FAILED));

        assertEquals(TaskState.FAILED, run.getTaskState("A"));
        assertEquals(DAGRunState.FAILED, run.getState());
        assertEquals(0, run.getRetryCount("A"));
        assertEquals(1, submissions.jobs().size());
    }

    @Test
    public void inFlightIndependentTaskCanCompleteAfterFailFast() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        DAG dag = new DAG("fail-fast", "fail-fast");
        dag.addTask(task("A"));
        dag.addTask(task("B"));
        dag.addTask(task("C", "A", "B"));
        manager.registerDAG(dag);
        DAGRun run = manager.startRun("fail-fast");

        Long failedJobId = run.getJobId("A");
        Long runningJobId = run.getJobId("B");
        manager.onJobFailed(terminalJob(failedJobId, JobState.FAILED));
        manager.onJobCompleted(completedJob(runningJobId));

        assertEquals(DAGRunState.FAILED, run.getState());
        assertEquals(TaskState.FAILED, run.getTaskState("A"));
        assertEquals(TaskState.COMPLETED, run.getTaskState("B"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("C"));
        assertEquals(2, submissions.jobs().size());
    }

    @Test
    public void inFlightSiblingCancellationIsRecordedAfterRunFailure() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        DAG dag = new DAG("failed-sibling-cancel", "failed sibling cancel");
        dag.addTask(task("A"));
        dag.addTask(task("B"));
        dag.addTask(task("C", "A", "B"));
        manager.registerDAG(dag);
        DAGRun run = manager.startRun(dag.getId());

        manager.onJobFailed(terminalJob(run.getJobId("A"), JobState.FAILED));
        manager.onJobCancelled(terminalJob(run.getJobId("B"), JobState.CANCELLED));

        assertEquals(DAGRunState.FAILED, run.getState());
        assertEquals(TaskState.FAILED, run.getTaskState("A"));
        assertEquals(TaskState.CANCELLED, run.getTaskState("B"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("C"));
        assertEquals(AttemptState.CANCELLED, run.getCurrentAttempt("B").getState());
        assertEquals("Job cancelled", run.getCurrentAttempt("B").getFailureMessage());
        assertNotNull(run.getCurrentAttempt("B").getFinishedAtMillis());
    }

    @Test
    public void taskTimeoutStartsOnlyAfterRunningAndRetriesWithANewJobId() throws Exception {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        try (DAGManager manager = new DAGManager(submissions)) {
            DAG dag = new DAG("task-timeout-retry", "task timeout retry");
            dag.addTask(new DAGTask("A", 1, 1, Set.of(), "run",
                    new RetryPolicy(1), new TimeoutPolicy(120L)));
            manager.registerDAG(dag);
            DAGRun run = manager.startRun(dag.getId());
            Long firstJobId = run.getJobId("A");

            Thread.sleep(150);
            assertEquals(TaskState.SUBMITTED, run.getTaskState("A"));
            assertNull(run.getTaskStartTime("A"));

            manager.onJobStateChanged(terminalJob(firstJobId, JobState.RUNNING));
            assertEquals(TaskState.RUNNING, run.getTaskState("A"));
            assertTrue(run.getTaskStartTime("A") > 0);
            await(() -> run.getRetryCount("A") == 1
                    && !firstJobId.equals(run.getJobId("A")));

            Long retryJobId = run.getJobId("A");
            assertNotEquals(firstJobId, retryJobId);
            assertEquals(List.of(firstJobId), submissions.cancelledJobIds());
            manager.onJobCompleted(completedJob(firstJobId));
            assertEquals(TaskState.SUBMITTED, run.getTaskState("A"));

            manager.onJobStateChanged(terminalJob(retryJobId, JobState.RUNNING));
            manager.onJobCompleted(completedJob(retryJobId));
            assertEquals(DAGRunState.COMPLETED, run.getState());
        }
    }

    @Test
    public void taskTimeoutWithoutRetriesFailsRunAndRecordsReason() throws Exception {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        try (DAGManager manager = new DAGManager(submissions)) {
            DAG dag = new DAG("task-timeout-failure", "task timeout failure");
            dag.addTask(new DAGTask("A", 1, 1, Set.of(), "run",
                    new RetryPolicy(0), new TimeoutPolicy(50L)));
            manager.registerDAG(dag);
            DAGRun run = manager.startRun(dag.getId());
            manager.onJobStateChanged(
                    terminalJob(run.getJobId("A"), JobState.RUNNING));

            await(() -> run.getState() == DAGRunState.FAILED);
            assertEquals(TaskState.FAILED, run.getTaskState("A"));
            assertEquals(RetryReason.TIMEOUT, run.getTaskFailure("A").getReason());
            assertEquals(TimeoutReason.TASK_TIMEOUT,
                    run.getTaskFailure("A").getTimeoutReason());
            assertEquals(0, run.getRetryCount("A"));
            assertEquals(1, submissions.jobs().size());
        }
    }

    @Test
    public void jobFailureBeforeTimeoutCancelsOldTimerAndRetriesNormally() throws Exception {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        try (DAGManager manager = new DAGManager(submissions)) {
            DAG dag = new DAG("failure-before-timeout", "failure before timeout");
            dag.addTask(new DAGTask("A", 1, 1, Set.of(), "run",
                    new RetryPolicy(1), new TimeoutPolicy(120L)));
            manager.registerDAG(dag);
            DAGRun run = manager.startRun(dag.getId());
            Long firstJobId = run.getJobId("A");
            manager.onJobStateChanged(terminalJob(firstJobId, JobState.RUNNING));
            manager.onJobFailed(terminalJob(firstJobId, JobState.FAILED));
            Long retryJobId = run.getJobId("A");

            assertNotEquals(firstJobId, retryJobId);
            Thread.sleep(150);
            assertEquals(DAGRunState.RUNNING, run.getState());
            assertEquals(TaskState.SUBMITTED, run.getTaskState("A"));
            assertEquals(1, run.getRetryCount("A"));
            assertTrue(submissions.cancelledJobIds().isEmpty());
        }
    }

    @Test
    public void dagTimeoutCancelsActiveAndBlockedTasksWithoutRetry() throws Exception {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        try (DAGManager manager = new DAGManager(submissions)) {
            DAG dag = new DAG("dag-timeout", "dag timeout", 300L);
            dag.addTask(task("A"));
            dag.addTask(taskWithRetries("B", 2));
            dag.addTask(task("C", "A", "B"));
            manager.registerDAG(dag);
            DAGRun run = manager.startRun(dag.getId());
            Long activeJobId = run.getJobId("B");
            manager.onJobCompleted(completedJob(run.getJobId("A")));

            await(() -> run.getState() == DAGRunState.FAILED);
            assertEquals(TimeoutReason.DAG_TIMEOUT, run.getTimeoutReason());
            assertEquals(TaskState.COMPLETED, run.getTaskState("A"));
            assertEquals(TaskState.CANCELLED, run.getTaskState("B"));
            assertEquals(TaskState.CANCELLED, run.getTaskState("C"));
            assertEquals(0, run.getRetryCount("B"));
            assertEquals(List.of(activeJobId), submissions.cancelledJobIds());
            assertEquals(2, submissions.jobs().size());
        }
    }

    @Test
    public void cancellingRunKeepsCompletedTasksAndIgnoresLateCompletion() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        try (DAGManager manager = new DAGManager(submissions)) {
            DAG dag = new DAG("cancel", "cancel");
            dag.addTask(task("A"));
            dag.addTask(task("B"));
            manager.registerDAG(dag);
            DAGRun run = manager.startRun(dag.getId());
            Long completedId = run.getJobId("A");
            Long runningId = run.getJobId("B");
            manager.onJobCompleted(completedJob(completedId));
            manager.onJobStateChanged(terminalJob(runningId, JobState.RUNNING));

            assertTrue(manager.cancelRun(run.getRunId()));
            assertTrue(manager.cancelRun(run.getRunId()));
            manager.onJobCompleted(completedJob(runningId));

            assertEquals(DAGRunState.CANCELLED, run.getState());
            assertEquals(TaskState.COMPLETED, run.getTaskState("A"));
            assertEquals(TaskState.CANCELLED, run.getTaskState("B"));
            assertEquals(List.of(runningId), submissions.cancelledJobIds());
            assertEquals(2, submissions.jobs().size());
        }
    }

    @Test
    public void cancellationFailureRemainsVisibleOnRepeatedRequest() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService(false);
        DAGManager manager = new DAGManager(submissions);
        manager.registerDAG(chain("cancel-failure", "A", "B"));
        DAGRun run = manager.startRun("cancel-failure");

        assertFalse(manager.cancelRun(run.getRunId()));
        assertFalse(manager.cancelRun(run.getRunId()));
        assertEquals(DAGRunState.CANCELLED, run.getState());
        assertEquals(Boolean.FALSE, run.getCancellationConfirmed());
        assertEquals(TaskState.CANCELLED, run.getTaskState("A"));
        assertEquals(TaskState.CANCELLED, run.getTaskState("B"));
    }

    @Test
    public void completionBeforeTimeoutRemainsCompleted() throws Exception {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        try (DAGManager manager = new DAGManager(submissions)) {
            DAG dag = new DAG("completion-race", "completion race");
            dag.addTask(new DAGTask("A", 1, 1, Set.of(), "run",
                    new RetryPolicy(1), new TimeoutPolicy(100L)));
            manager.registerDAG(dag);
            DAGRun run = manager.startRun(dag.getId());
            Long jobId = run.getJobId("A");
            manager.onJobStateChanged(terminalJob(jobId, JobState.RUNNING));
            manager.onJobCompleted(completedJob(jobId));

            Thread.sleep(150);
            assertEquals(DAGRunState.COMPLETED, run.getState());
            assertEquals(TaskState.COMPLETED, run.getTaskState("A"));
            assertTrue(submissions.cancelledJobIds().isEmpty());
        }
    }

    @Test
    public void shutdownCancelsActiveRunWithShutdownReason() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        manager.registerDAG(chain("shutdown", "A", "B"));
        DAGRun run = manager.startRun("shutdown");
        manager.close();

        assertEquals(DAGRunState.CANCELLED, run.getState());
        assertEquals(CancellationReason.SHUTDOWN, run.getCancellationReason());
        assertEquals(TaskState.CANCELLED, run.getTaskState("A"));
        assertEquals(TaskState.CANCELLED, run.getTaskState("B"));
        assertEquals(List.of(run.getJobId("A")), submissions.cancelledJobIds());
    }

    @Test
    public void cancellationCancelsRunWithoutSubmittingDependents() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        manager.registerDAG(chain("cancel", "A", "B"));
        DAGRun run = manager.startRun("cancel");

        manager.onJobCancelled(terminalJob(run.getJobId("A"), JobState.CANCELLED));

        assertEquals(TaskState.CANCELLED, run.getTaskState("A"));
        assertEquals(TaskState.CANCELLED, run.getTaskState("B"));
        assertEquals(DAGRunState.CANCELLED, run.getState());
        assertEquals(Set.of("A"), submissions.taskIdsForRun(run));
    }

    @Test
    public void duplicateCompletionDoesNotResubmitTask() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        manager.registerDAG(chain("duplicate", "A", "B"));
        DAGRun run = manager.startRun("duplicate");
        Job completion = completedJob(run.getJobId("A"));

        manager.onJobCompleted(completion);
        Long childJobId = run.getJobId("B");
        manager.onJobCompleted(completion);

        assertNotEquals(run.getJobId("A"), childJobId);
        assertEquals(childJobId, run.getJobId("B"));
        assertEquals(2, submissions.jobs().size());
    }

    @Test
    public void rejectsInvalidOrDuplicateDagsAndUnknownRuns() {
        DAGManager manager = new DAGManager(new FakeJobSubmissionService());
        DAG cycle = new DAG("cycle", "cycle");
        cycle.addTask(task("A", "B"));
        cycle.addTask(task("B", "A"));

        assertThrows(IllegalArgumentException.class, () -> manager.registerDAG(cycle));
        DAG valid = chain("valid", "A");
        manager.registerDAG(valid);
        assertThrows(IllegalArgumentException.class, () -> manager.registerDAG(valid));
        assertThrows(IllegalArgumentException.class, () -> manager.startRun("missing"));
        assertTrue(manager.findRun("missing").isEmpty());
    }

    @Test
    public void registrationCopiesDagDefinitionAndJobsCarryTaskResources() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        DAG dag = new DAG("snapshot", "snapshot");
        dag.addTask(new DAGTask("root", 3, 5, Set.of()));
        manager.registerDAG(dag);
        dag.addTask(task("later"));

        DAGRun run = manager.startRun("snapshot");
        Job submitted = submissions.job(run.getJobId("root"));
        assertEquals("root", submitted.getDescription());
        assertEquals(3, submitted.getCpuRequested());
        assertEquals(5, submitted.getMemRequested());
        assertEquals(JobState.PENDING, submitted.getState());
        assertFalse(run.getTaskStates().containsKey("later"));
    }

    private static DAG chain(String dagId, String... taskIds) {
        DAG dag = new DAG(dagId, dagId);
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

    private static DAGTask taskWithRetries(String id, int maxRetries) {
        return new DAGTask(id, 1, 1, Set.of(), new RetryPolicy(maxRetries));
    }

    private static Job completedJob(Long id) {
        return terminalJob(id, JobState.COMPLETED);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue("Timed out waiting for DAG event", condition.getAsBoolean());
    }

    private static Job terminalJob(Long id, JobState state) {
        return Job.newBuilder().setId(Math.toIntExact(id)).setState(state).build();
    }

    private static final class FakeJobSubmissionService implements JobSubmissionService {
        private final AtomicLong nextId = new AtomicLong(1);
        private final Map<Long, Job> jobs = new HashMap<>();
        private final List<Long> cancelled = new ArrayList<>();
        private final boolean cancellationAccepted;

        FakeJobSubmissionService() {
            this(true);
        }

        FakeJobSubmissionService(boolean cancellationAccepted) {
            this.cancellationAccepted = cancellationAccepted;
        }

        @Override
        public synchronized long submit(Job job) {
            long id = nextId.getAndIncrement();
            jobs.put(id, job.toBuilder().setId(Math.toIntExact(id)).build());
            return id;
        }

        @Override
        public synchronized boolean cancel(long jobId) {
            cancelled.add(jobId);
            return cancellationAccepted;
        }

        synchronized Job job(Long id) {
            return jobs.get(id);
        }

        synchronized List<Job> jobs() {
            return new ArrayList<>(jobs.values());
        }

        synchronized List<Long> cancelledJobIds() {
            return new ArrayList<>(cancelled);
        }

        synchronized Set<String> taskIdsForRun(DAGRun run) {
            return run.getJobIds().keySet();
        }
    }
}
