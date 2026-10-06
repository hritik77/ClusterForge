package com.JobController.dag;

import com.JobController.Job;
import com.JobController.JobState;
import com.JobController.JobSubmissionService;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DAGManagerTest {
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

        manager.onJobCompleted(completedJob(firstJobId));
        manager.onJobFailed(terminalJob(firstJobId, JobState.FAILED));
        assertEquals(TaskState.SUBMITTED, run.getTaskState("A"));
        assertEquals(1, run.getRetryCount("A"));
        assertEquals(2, submissions.jobs().size());

        manager.onJobCompleted(completedJob(retryJobId));
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
    public void cancellationCancelsRunWithoutSubmittingDependents() {
        FakeJobSubmissionService submissions = new FakeJobSubmissionService();
        DAGManager manager = new DAGManager(submissions);
        manager.registerDAG(chain("cancel", "A", "B"));
        DAGRun run = manager.startRun("cancel");

        manager.onJobCancelled(terminalJob(run.getJobId("A"), JobState.CANCELLED));

        assertEquals(TaskState.CANCELLED, run.getTaskState("A"));
        assertEquals(TaskState.BLOCKED, run.getTaskState("B"));
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

    private static Job terminalJob(Long id, JobState state) {
        return Job.newBuilder().setId(Math.toIntExact(id)).setState(state).build();
    }

    private static final class FakeJobSubmissionService implements JobSubmissionService {
        private final AtomicLong nextId = new AtomicLong(1);
        private final Map<Long, Job> jobs = new HashMap<>();

        @Override
        public synchronized long submit(Job job) {
            long id = nextId.getAndIncrement();
            jobs.put(id, job.toBuilder().setId(Math.toIntExact(id)).build());
            return id;
        }

        synchronized Job job(Long id) {
            return jobs.get(id);
        }

        synchronized List<Job> jobs() {
            return new ArrayList<>(jobs.values());
        }

        synchronized Set<String> taskIdsForRun(DAGRun run) {
            return run.getJobIds().keySet();
        }
    }
}
