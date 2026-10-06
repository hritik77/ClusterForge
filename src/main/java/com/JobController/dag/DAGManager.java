package com.JobController.dag;

import com.JobController.Job;
import com.JobController.JobEventListener;
import com.JobController.JobState;
import com.JobController.JobSubmissionService;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public final class DAGManager implements JobEventListener {
    private final JobSubmissionService jobSubmissionService;
    private final Map<String, DAG> dags = new HashMap<>();
    private final Map<String, DAGRun> runs = new HashMap<>();
    private final Map<Long, TaskReference> jobToTask = new HashMap<>();
    private final Map<TaskReference, Long> taskToCurrentJob = new HashMap<>();

    public DAGManager(JobSubmissionService jobSubmissionService) {
        this.jobSubmissionService = Objects.requireNonNull(
                jobSubmissionService, "Job submission service must not be null");
    }

    public synchronized void registerDAG(DAG dag) {
        DAGValidator.validate(dag);
        if (dags.containsKey(dag.getId())) {
            throw new IllegalArgumentException("DAG ID is already registered: " + dag.getId());
        }

        DAG storedDAG = new DAG(dag.getId(), dag.getName());
        dag.getTasks().values().stream()
                .sorted((left, right) -> left.getId().compareTo(right.getId()))
                .map(task -> new DAGTask(
                        task.getId(),
                        task.getCpuRequested(),
                        task.getMemRequested(),
                        task.getDependencies(),
                        task.getCommand(),
                        task.getRetryPolicy()))
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

    @Override
    public synchronized void onJobCompleted(Job job) {
        TaskReference reference = findTaskReference(job, JobState.COMPLETED);
        if (reference == null) {
            return;
        }
        DAGRun run = runs.get(reference.dagRunId());
        if (!canProcessTerminalEvent(run, reference, job)) {
            return;
        }

        run.setTaskState(reference.taskId(), TaskState.COMPLETED);
        if (run.getState() == DAGRunState.RUNNING && DAGValidator.isComplete(run)) {
            run.setState(DAGRunState.COMPLETED);
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
        TaskReference reference = findTaskReference(job, JobState.FAILED);
        if (reference == null) {
            return;
        }
        DAGRun run = runs.get(reference.dagRunId());
        if (!canProcessTerminalEvent(run, reference, job)) {
            return;
        }

        handleTaskFailure(run, dags.get(run.getDagId()).getTask(reference.taskId()),
                reference, job, RetryReason.JOB_FAILURE);
    }

    @Override
    public synchronized void onJobCancelled(Job job) {
        TaskReference reference = findTaskReference(job, JobState.CANCELLED);
        if (reference == null) {
            return;
        }
        DAGRun run = runs.get(reference.dagRunId());
        if (!canProcessTerminalEvent(run, reference, job)) {
            return;
        }

        run.setTaskState(reference.taskId(), TaskState.CANCELLED);
        if (run.getState() == DAGRunState.RUNNING) {
            run.setState(DAGRunState.CANCELLED);
        }
        printRun(dags.get(run.getDagId()), run);
    }

    @Override
    public synchronized void onJobLost(Job job) {
        TaskReference reference = findTaskReference(job, JobState.LOST);
        if (reference == null) {
            return;
        }
        DAGRun run = runs.get(reference.dagRunId());
        if (!canProcessTerminalEvent(run, reference, job)) {
            return;
        }

        handleTaskFailure(run, dags.get(run.getDagId()).getTask(reference.taskId()),
                reference, job, RetryReason.WORKER_FAILURE);
    }

    private void handleTaskFailure(
            DAGRun run, DAGTask task, TaskReference reference, Job job, RetryReason reason) {
        Long currentJob = taskToCurrentJob.get(reference);
        if (!Objects.equals(currentJob, (long) job.getId())) {
            return;
        }

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
        if (run.getState() == DAGRunState.RUNNING) {
            run.setState(DAGRunState.FAILED);
        }
        printRun(dags.get(run.getDagId()), run);
    }

    private void submitTask(DAG dag, DAGRun run, String taskId) {
        if (run.getState() != DAGRunState.RUNNING) {
            return;
        }

        TaskState state = run.getTaskState(taskId);
        boolean ready = state == TaskState.READY
                || state == TaskState.BLOCKED
                        && DAGValidator.findReadyTasks(dag, run).contains(taskId);
        if (!ready) {
            return;
        }

        try {
            Job taskJob = createJob(dag.getTask(taskId), run);
            long jobId = jobSubmissionService.submit(taskJob);
            if (jobId <= 0) {
                throw new IllegalStateException(
                        "Job submission service returned an invalid job ID: " + jobId);
            }
            TaskReference reference = new TaskReference(run.getRunId(), taskId);
            if (jobToTask.putIfAbsent(jobId, reference) != null) {
                throw new IllegalStateException(
                        "Job submission service returned duplicate job ID: " + jobId);
            }
            taskToCurrentJob.put(reference, jobId);
            run.setJobId(taskId, jobId);
            run.setTaskState(taskId, TaskState.SUBMITTED);
            printRun(dag, run);
        } catch (RuntimeException e) {
            run.setTaskState(taskId, TaskState.FAILED);
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
                .setState(JobState.PENDING)
                .build();
    }

    private static void printRun(DAG dag, DAGRun run) {
        System.out.println(DAGGraphPrinter.renderRun(dag, run));
    }

    private TaskReference findTaskReference(Job job, JobState expectedState) {
        if (job == null || job.getId() <= 0 || job.getState() != expectedState) {
            return null;
        }
        return jobToTask.get((long) job.getId());
    }

    private boolean canProcessTerminalEvent(DAGRun run, TaskReference reference, Job job) {
        return run != null
                && run.getState() != DAGRunState.COMPLETED
                && run.getJobId(reference.taskId()) != null
                && run.getJobId(reference.taskId()) == job.getId()
                && Objects.equals(taskToCurrentJob.get(reference), (long) job.getId())
                && run.getTaskState(reference.taskId()) == TaskState.SUBMITTED;
    }

    private record TaskReference(String dagRunId, String taskId) {}
}
