package com.JobController.dag;

import com.JobController.Job;
import com.JobController.JobEventListener;
import com.JobController.JobState;
import com.JobController.JobSubmissionService;
import com.JobController.scheduling.PlacementConstraint;
import com.JobController.scheduling.PlacementOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public final class DAGManager implements JobEventListener, AutoCloseable {
    private final JobSubmissionService jobSubmissionService;
    private final TaskTimerManager timerManager;
    private final Map<String, DAG> dags = new HashMap<>();
    private final Map<String, DAGRun> runs = new HashMap<>();
    private final Map<Long, TaskAttemptReference> jobToAttempt = new HashMap<>();
    private final Map<TaskReference, Long> taskToCurrentJob = new HashMap<>();

    public DAGManager(JobSubmissionService jobSubmissionService) {
        this(jobSubmissionService, new TaskTimerManager());
    }

    public DAGManager(
            JobSubmissionService jobSubmissionService, TaskTimerManager timerManager) {
        this.jobSubmissionService = Objects.requireNonNull(
                jobSubmissionService, "Job submission service must not be null");
        this.timerManager = Objects.requireNonNull(
                timerManager, "Task timer manager must not be null");
    }

    public synchronized void registerDAG(DAG dag) {
        DAGValidator.validate(dag);
        if (dags.containsKey(dag.getId())) {
            throw new IllegalArgumentException("DAG ID is already registered: " + dag.getId());
        }

        DAG storedDAG = new DAG(dag.getId(), dag.getName(), dag.getDagTimeoutMillis());
        dag.getTasks().values().stream()
                .sorted((left, right) -> left.getId().compareTo(right.getId()))
                .map(task -> new DAGTask(
                        task.getId(),
                        task.getCpuRequested(),
                        task.getMemRequested(),
                        task.getDiskMbRequested(),
                        task.getGpuCountRequested(),
                        task.getGpuMemoryMbPerGpu(),
                        task.getDependencies(),
                        task.getCommand(),
                        task.getRetryPolicy(),
                        task.getTimeoutPolicy(),
                        task.getPlacementRequirements()))
                .forEach(storedDAG::addTask);
        dags.put(storedDAG.getId(), storedDAG);
    }

    public synchronized DAGRun startRun(String dagId) {
        DAG dag = dags.get(dagId);
        if (dag == null) {
            throw new IllegalArgumentException("Unknown DAG ID: " + dagId);
        }

        String runId;
        do {
            runId = UUID.randomUUID().toString();
        } while (runs.containsKey(runId));
        DAGRun run = new DAGRun(runId, dag);
        runs.put(run.getRunId(), run);
        if (run.hasDeadline()) {
            String timerRunId = run.getRunId();
            long remainingMillis = Math.max(1, run.getDeadlineMillis() - System.currentTimeMillis());
            timerManager.scheduleDagTimeout(
                    timerRunId, remainingMillis, () -> handleDagTimeout(timerRunId));
        }
        for (String rootTaskId : DAGValidator.getRootTasks(dag)) {
            if (run.getState() != DAGRunState.RUNNING) {
                break;
            }
            submitTask(dag, run, rootTaskId);
        }
        return run;
    }

    public synchronized Optional<DAGRun> findRun(String runId) {
        return Optional.ofNullable(runs.get(runId));
    }

    public synchronized Optional<DAG> findDAG(String dagId) {
        return Optional.ofNullable(dags.get(dagId));
    }

    public synchronized boolean cancelRun(String runId) {
        DAGRun run = runs.get(runId);
        if (run == null) {
            return false;
        }
        if (run.getState() == DAGRunState.CANCELLED) {
            return Boolean.TRUE.equals(run.getCancellationConfirmed());
        }
        if (run.getState() != DAGRunState.RUNNING) {
            return false;
        }
        return terminateRun(run, DAGRunState.CANCELLED, CancellationReason.USER_REQUEST,
                null, "DAG run cancelled");
    }

    @Override
    public synchronized void onJobStateChanged(Job job) {
        TaskAttemptReference attemptReference =
                findAttemptReference(job, job == null ? null : job.getState());
        if (attemptReference == null) {
            return;
        }
        TaskReference reference = attemptReference.taskReference();
        DAGRun run = runs.get(reference.dagRunId());
        TaskAttempt attempt = findAttempt(run, attemptReference);
        if (run == null || attempt == null) {
            return;
        }
        if (job.getState() == JobState.ALLOCATED || job.getState() == JobState.RUNNING) {
            attempt.markAllocated(job.getAssignedWorkerId(), job.getAssignedWorkerLabelsMap());
        }
        if (job.getState() == JobState.RUNNING) {
            attempt.markRunning(System.currentTimeMillis());
        }
        if (expireDagIfOverdue(run) || run.getState() != DAGRunState.RUNNING
                || !isCurrentAttempt(run, reference, job)) {
            return;
        }

        TaskState current = run.getTaskState(reference.taskId());
        if (job.getState() == JobState.ALLOCATED) {
            if (current == TaskState.SUBMITTED) {
                run.setTaskState(reference.taskId(), TaskState.ALLOCATED);
            }
            return;
        }
        if (job.getState() != JobState.RUNNING
                || (current != TaskState.SUBMITTED && current != TaskState.ALLOCATED)) {
            return;
        }

        run.setTaskState(reference.taskId(), TaskState.RUNNING);
        long startedAt = System.currentTimeMillis();
        run.markTaskStarted(reference.taskId(), startedAt);
        DAGTask task = dags.get(run.getDagId()).getTask(reference.taskId());
        if (task.getTimeoutPolicy().hasTimeout()) {
            long timeoutMillis = task.getTimeoutPolicy().getTaskTimeoutMillis();
            System.out.println("Task " + task.getId() + " in run " + run.getRunId()
                    + " started its " + timeoutMillis + " ms timeout");
            timerManager.scheduleTaskTimeout(
                    run.getRunId(), task.getId(), timeoutMillis,
                    () -> handleTaskTimeout(run.getRunId(), task.getId(), job.getId()));
        }
        printRun(dags.get(run.getDagId()), run);
    }

    @Override
    public synchronized void onJobCompleted(Job job) {
        TaskAttemptReference attemptReference = findAttemptReference(job, JobState.COMPLETED);
        if (attemptReference == null) {
            return;
        }
        TaskReference reference = attemptReference.taskReference();
        DAGRun run = runs.get(reference.dagRunId());
        if (expireDagIfOverdue(run)) {
            return;
        }
        TaskAttempt attempt = findAttempt(run, attemptReference);
        if (attempt == null || !attempt.markTerminal(
                AttemptState.COMPLETED, null, null, System.currentTimeMillis())) {
            return;
        }
        if (!canProcessTerminalEvent(run, reference, job)) {
            return;
        }

        timerManager.cancelTaskTimeout(run.getRunId(), reference.taskId());
        run.setTaskState(reference.taskId(), TaskState.COMPLETED);
        if (run.getState() == DAGRunState.RUNNING && DAGValidator.isComplete(run)) {
            run.setState(DAGRunState.COMPLETED);
            timerManager.cancelRunTimers(run.getRunId());
            printRun(dags.get(run.getDagId()), run);
            return;
        }

        DAG dag = dags.get(run.getDagId());
        printRun(dag, run);
        if (run.getState() != DAGRunState.RUNNING) {
            return;
        }
        Set<String> readyTaskIds = DAGValidator.findReadyTasks(dag, run);
        for (String taskId : readyTaskIds) {
            submitTask(dag, run, taskId);
            if (run.getState() != DAGRunState.RUNNING) {
                return;
            }
        }
    }

    @Override
    public synchronized void onJobFailed(Job job) {
        TaskAttemptReference attemptReference = findAttemptReference(job, JobState.FAILED);
        if (attemptReference == null) {
            return;
        }
        TaskReference reference = attemptReference.taskReference();
        DAGRun run = runs.get(reference.dagRunId());
        if (expireDagIfOverdue(run)) {
            return;
        }
        TaskAttempt attempt = findAttempt(run, attemptReference);
        if (attempt == null || !attempt.markTerminal(
                AttemptState.FAILED, RetryReason.JOB_FAILURE, "Job failed",
                System.currentTimeMillis())) {
            return;
        }
        if (!canProcessTerminalEvent(run, reference, job)) {
            return;
        }

        handleTaskFailure(run, dags.get(run.getDagId()).getTask(reference.taskId()),
                reference, job, RetryReason.JOB_FAILURE, "Job failed");
    }

    @Override
    public synchronized void onJobCancelled(Job job) {
        TaskAttemptReference attemptReference = findAttemptReference(job, JobState.CANCELLED);
        if (attemptReference == null) {
            return;
        }
        TaskReference reference = attemptReference.taskReference();
        DAGRun run = runs.get(reference.dagRunId());
        if (run == null || run.getState() == DAGRunState.RUNNING && expireDagIfOverdue(run)) {
            return;
        }
        TaskAttempt attempt = findAttempt(run, attemptReference);
        if (attempt == null || !attempt.markTerminal(
                AttemptState.CANCELLED, null, "Job cancelled", System.currentTimeMillis())) {
            return;
        }
        if (run.getState() == DAGRunState.CANCELLED
                || run.getState() == DAGRunState.COMPLETED) {
            return;
        }
        if (!canProcessTerminalEvent(run, reference, job)) {
            return;
        }
        if (run.getState() == DAGRunState.RUNNING) {
            cancelRun(run.getRunId());
        } else {
            timerManager.cancelTaskTimeout(run.getRunId(), reference.taskId());
            run.setTaskState(reference.taskId(), TaskState.CANCELLED);
            printRun(dags.get(run.getDagId()), run);
        }
    }

    @Override
    public synchronized void onJobLost(Job job) {
        TaskAttemptReference attemptReference = findAttemptReference(job, JobState.LOST);
        if (attemptReference == null) {
            return;
        }
        TaskReference reference = attemptReference.taskReference();
        DAGRun run = runs.get(reference.dagRunId());
        if (expireDagIfOverdue(run)) {
            return;
        }
        TaskAttempt attempt = findAttempt(run, attemptReference);
        if (attempt == null || !attempt.markTerminal(
                AttemptState.LOST, RetryReason.WORKER_FAILURE, "Worker was lost",
                System.currentTimeMillis())) {
            return;
        }
        if (!canProcessTerminalEvent(run, reference, job)) {
            return;
        }

        handleTaskFailure(run, dags.get(run.getDagId()).getTask(reference.taskId()),
                reference, job, RetryReason.WORKER_FAILURE, "Worker was lost");
    }

    @Override
    public void close() {
        synchronized (this) {
            for (DAGRun run : runs.values()) {
                if (run.getState() == DAGRunState.RUNNING) {
                    terminateRun(run, DAGRunState.CANCELLED, CancellationReason.SHUTDOWN,
                            null, "Controller is shutting down");
                }
            }
        }
        timerManager.shutdown();
    }

    public void shutdown() {
        close();
    }

    private void handleTaskTimeout(String runId, String taskId, long timedOutJobId) {
        synchronized (this) {
            DAGRun run = runs.get(runId);
            if (expireDagIfOverdue(run)) {
                return;
            }
            TaskReference reference = new TaskReference(runId, taskId);
            if (run == null || run.getState() != DAGRunState.RUNNING
                    || run.getTaskState(taskId) != TaskState.RUNNING
                    || !Objects.equals(taskToCurrentJob.get(reference), timedOutJobId)) {
                System.out.println("Ignoring stale timeout for task " + taskId
                        + " in run " + runId);
                return;
            }
            TaskAttempt currentAttempt = run.getCurrentAttemptInternal(taskId);
            if (currentAttempt == null
                    || !Objects.equals(currentAttempt.getJobId(), timedOutJobId)
                    || !currentAttempt.markTerminal(
                            AttemptState.FAILED, RetryReason.TIMEOUT,
                            "Task timed out", System.currentTimeMillis())) {
                return;
            }

            DAGTask task = dags.get(run.getDagId()).getTask(taskId);
            long timeoutMillis = task.getTimeoutPolicy().getTaskTimeoutMillis();
            run.setTaskState(taskId, TaskState.FAILED);
            run.recordTaskFailure(new TaskFailure(
                    taskId, RetryReason.TIMEOUT, TimeoutReason.TASK_TIMEOUT,
                    "Task timed out after "
                            + timeoutMillis + " ms"));
            timerManager.cancelTaskTimeout(runId, taskId);
            System.out.println("Task " + taskId + " in run " + runId
                    + " timed out after " + timeoutMillis + " ms");

            if (!cancelUnderlyingJob(timedOutJobId, runId, taskId)) {
                failRunAfterTimeout(run, taskId,
                        "Could not confirm cancellation of timed-out job " + timedOutJobId);
                return;
            }
            retryOrFail(run, task, reference, RetryReason.TIMEOUT, "Task timeout");
        }
    }

    private void handleDagTimeout(String runId) {
        synchronized (this) {
            DAGRun run = runs.get(runId);
            if (run == null || run.getState() != DAGRunState.RUNNING) {
                return;
            }
            long remainingMillis = run.getDeadlineMillis() - System.currentTimeMillis();
            if (remainingMillis > 0) {
                timerManager.scheduleDagTimeout(
                        runId, remainingMillis, () -> handleDagTimeout(runId));
                return;
            }
            expireDagIfOverdue(run);
        }
    }

    private boolean expireDagIfOverdue(DAGRun run) {
        if (run == null || run.getState() != DAGRunState.RUNNING || !run.hasDeadline()
                || System.currentTimeMillis() < run.getDeadlineMillis()) {
            return false;
        }
        run.setTimeoutReason(TimeoutReason.DAG_TIMEOUT);
        System.out.println("DAG run " + run.getRunId() + " timed out");
        terminateRun(run, DAGRunState.FAILED, CancellationReason.DAG_TIMEOUT,
                TimeoutReason.DAG_TIMEOUT, "DAG deadline expired");
        return true;
    }

    private boolean terminateRun(
            DAGRun run, DAGRunState terminalState, CancellationReason cancellationReason,
            TimeoutReason timeoutReason, String message) {
        List<Long> activeJobIds = new ArrayList<>();
        synchronized (run) {
            if (run.getState() != DAGRunState.RUNNING) {
                return false;
            }
            if (cancellationReason != null) {
                run.setCancellationReason(cancellationReason);
            }
            if (timeoutReason != null) {
                run.setTimeoutReason(timeoutReason);
            }
            if (!run.setState(terminalState)) {
                return false;
            }

            timerManager.cancelRunTimers(run.getRunId());
            for (String taskId : run.getTaskStates().keySet()) {
                TaskState state = run.getTaskState(taskId);
                if (isTaskTerminal(state)) {
                    continue;
                }
                Long jobId = run.getJobId(taskId);
                if (jobId != null && (state == TaskState.SUBMITTED
                        || state == TaskState.ALLOCATED || state == TaskState.RUNNING)) {
                    activeJobIds.add(jobId);
                }
                run.setTaskState(taskId, TaskState.CANCELLED);
                if (timeoutReason != null) {
                    run.recordTaskFailure(new TaskFailure(
                            taskId, RetryReason.TIMEOUT, timeoutReason, message));
                }
                System.out.println("Task " + taskId + " in run " + run.getRunId()
                        + " cancelled: " + message);
            }
        }

        boolean cancellationConfirmed = true;
        for (Long jobId : activeJobIds) {
            boolean cancelled = cancelUnderlyingJob(jobId, run.getRunId(), "run");
            cancellationConfirmed &= cancelled;
            if (cancelled) {
                TaskAttemptReference attemptReference = jobToAttempt.get(jobId);
                if (attemptReference != null) {
                    TaskAttempt attempt = findAttempt(
                            run, attemptReference);
                    if (attempt != null) {
                        attempt.markTerminal(AttemptState.CANCELLED, null, message,
                                System.currentTimeMillis());
                    }
                }
            }
        }
        if (terminalState == DAGRunState.CANCELLED) {
            run.setCancellationConfirmed(cancellationConfirmed);
        }
        printRun(dags.get(run.getDagId()), run);
        return cancellationConfirmed;
    }

    private void handleTaskFailure(
            DAGRun run, DAGTask task, TaskReference reference, Job job,
            RetryReason reason, String message) {
        if (!isCurrentAttempt(run, reference, job)) {
            return;
        }
        timerManager.cancelTaskTimeout(run.getRunId(), task.getId());
        run.recordTaskFailure(new TaskFailure(task.getId(), reason, message));
        run.setTaskState(task.getId(), TaskState.FAILED);
        if (run.getState() == DAGRunState.RUNNING) {
            retryOrFail(run, task, reference, reason, message);
        } else {
            printRun(dags.get(run.getDagId()), run);
        }
    }

    private void retryOrFail(
            DAGRun run, DAGTask task, TaskReference reference,
            RetryReason reason, String message) {
        int retryCount = run.getRetryCount(task.getId());
        if (run.getState() == DAGRunState.RUNNING
                && task.getRetryPolicy().canRetry(retryCount)) {
            run.incrementRetryCount(task.getId());
            run.setTaskState(task.getId(), TaskState.READY);
            System.out.println("Retrying DAG task " + task.getId()
                    + " after " + reason + " (retry " + run.getRetryCount(task.getId())
                    + "/" + task.getRetryPolicy().getMaxRetries() + ")");
            submitTask(dags.get(run.getDagId()), run, task.getId());
            return;
        }

        run.setTaskState(task.getId(), TaskState.FAILED);
        if (run.setState(DAGRunState.FAILED)) {
            timerManager.cancelDagTimeout(run.getRunId());
        }
        System.err.println("DAG task " + task.getId() + " failed in run "
                + run.getRunId() + ": " + message);
        printRun(dags.get(run.getDagId()), run);
    }

    private void failRunAfterTimeout(DAGRun run, String taskId, String message) {
        run.recordTaskFailure(new TaskFailure(
                taskId, RetryReason.TIMEOUT, TimeoutReason.TASK_TIMEOUT, message));
        if (run.setState(DAGRunState.FAILED)) {
            timerManager.cancelDagTimeout(run.getRunId());
        }
        System.err.println("DAG task " + taskId + " could not be safely retried: " + message);
        printRun(dags.get(run.getDagId()), run);
    }

    private boolean cancelUnderlyingJob(long jobId, String runId, String taskId) {
        try {
            boolean cancelled = jobSubmissionService.cancel(jobId);
            if (!cancelled) {
                System.err.println("Could not confirm cancellation for job " + jobId
                        + " (" + taskId + ") in run " + runId);
            }
            return cancelled;
        } catch (RuntimeException e) {
            System.err.println("Cancellation failed for job " + jobId + " in run "
                    + runId + ": " + e.getMessage());
            return false;
        }
    }

    private void submitTask(DAG dag, DAGRun run, String taskId) {
        if (expireDagIfOverdue(run) || run.getState() != DAGRunState.RUNNING) {
            return;
        }

        TaskState state = run.getTaskState(taskId);
        boolean ready = state == TaskState.READY
                || state == TaskState.BLOCKED
                        && DAGValidator.findReadyTasks(dag, run).contains(taskId);
        if (!ready || expireDagIfOverdue(run)
                || run.getState() != DAGRunState.RUNNING) {
            return;
        }

        TaskAttempt attempt = null;
        try {
            Job taskJob = createJob(dag.getTask(taskId), run);
            if (expireDagIfOverdue(run) || run.getState() != DAGRunState.RUNNING) {
                return;
            }
            attempt = run.createAttempt(taskId);
            long jobId = jobSubmissionService.submit(taskJob);
            if (jobId <= 0) {
                throw new IllegalStateException(
                        "Job submission service returned an invalid job ID: " + jobId);
            }
            if (!attempt.markSubmitted(jobId)) {
                throw new IllegalStateException("Could not associate submitted job with task attempt");
            }
            TaskReference reference = new TaskReference(run.getRunId(), taskId);
            TaskAttemptReference attemptReference = new TaskAttemptReference(
                    run.getRunId(), taskId, attempt.getAttemptNumber());
            if (jobToAttempt.putIfAbsent(jobId, attemptReference) != null) {
                throw new IllegalStateException(
                        "Job submission service returned duplicate job ID: " + jobId);
            }
            taskToCurrentJob.put(reference, jobId);
            run.setJobId(taskId, jobId);
            run.setTaskState(taskId, TaskState.SUBMITTED);
            jobSubmissionService.find(jobId).ifPresent(this::onJobStateChanged);
            printRun(dag, run);
        } catch (RuntimeException e) {
            if (attempt != null) {
                attempt.markTerminal(AttemptState.FAILED, RetryReason.JOB_FAILURE,
                        "Job submission failed: " + e.getMessage(), System.currentTimeMillis());
            }
            timerManager.cancelRunTimers(run.getRunId());
            run.setTaskState(taskId, TaskState.FAILED);
            run.recordTaskFailure(new TaskFailure(taskId, RetryReason.JOB_FAILURE,
                    "Job submission failed: " + e.getMessage()));
            run.setState(DAGRunState.FAILED);
            printRun(dag, run);
            throw e;
        }
    }

    private static Job createJob(DAGTask task, DAGRun run) {
        return Job.newBuilder()
                .setOwner("dag:" + run.getDagId() + ":" + run.getRunId())
                .setDescription(task.getId())
                .setCpuRequested(task.getCpuRequested())
                .setMemRequested(task.getMemRequested())
                .setDiskMbRequested(task.getDiskMbRequested())
                .setGpuCountRequested(task.getGpuCountRequested())
                .setGpuMemoryMbPerGpu(task.getGpuMemoryMbPerGpu())
                .addAllPlacementConstraints(task.getPlacementRequirements().getConstraints()
                        .stream()
                        .map(DAGManager::toProtoConstraint)
                        .toList())
                .setState(JobState.PENDING)
                .build();
    }

    private static com.JobController.PlacementConstraint toProtoConstraint(
            PlacementConstraint constraint) {
        return com.JobController.PlacementConstraint.newBuilder()
                .setKey(constraint.getKey())
                .setOperator(com.JobController.PlacementOperator.valueOf(
                        constraint.getOperator().name()))
                .setValue(constraint.getValue())
                .build();
    }

    private static boolean isTaskTerminal(TaskState state) {
        return state == TaskState.COMPLETED || state == TaskState.FAILED
                || state == TaskState.CANCELLED;
    }

    private static void printRun(DAG dag, DAGRun run) {
        System.out.println(DAGGraphPrinter.renderRun(dag, run));
    }

    private TaskAttemptReference findAttemptReference(Job job, JobState expectedState) {
        if (job == null || job.getId() <= 0 || job.getState() != expectedState) {
            return null;
        }
        return jobToAttempt.get((long) job.getId());
    }

    private static TaskAttempt findAttempt(DAGRun run, TaskAttemptReference reference) {
        if (run == null || !run.getRunId().equals(reference.runId())) {
            return null;
        }
        return run.getAttemptInternal(reference.taskId(), reference.attemptNumber());
    }

    private boolean canProcessTerminalEvent(DAGRun run, TaskReference reference, Job job) {
        if (run == null || run.getState() == DAGRunState.COMPLETED
                || run.getState() == DAGRunState.CANCELLED
                || !isCurrentAttempt(run, reference, job)) {
            return false;
        }
        TaskState state = run.getTaskState(reference.taskId());
        return state == TaskState.SUBMITTED || state == TaskState.ALLOCATED
                || state == TaskState.RUNNING;
    }

    private boolean isCurrentAttempt(DAGRun run, TaskReference reference, Job job) {
        if (run == null || job == null) {
            return false;
        }
        TaskAttemptReference attemptReference = jobToAttempt.get((long) job.getId());
        TaskAttempt currentAttempt = run.getCurrentAttemptInternal(reference.taskId());
        return attemptReference != null
                && attemptReference.runId().equals(reference.dagRunId())
                && attemptReference.taskId().equals(reference.taskId())
                && currentAttempt != null
                && currentAttempt.getAttemptNumber() == attemptReference.attemptNumber()
                && Objects.equals(currentAttempt.getJobId(), (long) job.getId())
                && Objects.equals(taskToCurrentJob.get(reference), (long) job.getId());
    }

    private record TaskReference(String dagRunId, String taskId) {}
    private record TaskAttemptReference(String runId, String taskId, int attemptNumber) {
        private TaskReference taskReference() {
            return new TaskReference(runId, taskId);
        }
    }
}
