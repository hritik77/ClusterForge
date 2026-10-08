package com.JobController;

import com.JobController.dag.DAGGraphPrinter;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class JobManager implements JobSubmissionService {
    private final Scheduler scheduler;
    private final Scheduler firstFitScheduler=new FirstFitSchedular();
    private final Scheduler bestFitScheduler=new BestFitSchedular();
    private final Scheduler roundRobinScheduler=new RoundRobinSchedular();
    private final CopyOnWriteArrayList<JobEventListener> jobEventListeners =
            new CopyOnWriteArrayList<>();
    private final JobQ jobQueue;
    private final ScheduledExecutorService dispatcher=Executors.newSingleThreadScheduledExecutor(
            runnable -> {
                Thread thread=new Thread(runnable, "job-queue-dispatcher");
                thread.setDaemon(true);
                return thread;
            });

    JobManager(Scheduler scheduler) {
        this.scheduler=scheduler;
        this.jobQueue=new JobQ(Integer.getInteger("clusterforge.queue.capacity", 10_000));
    }

    Optional<Job> submitJob(String owner, String description, int cpuRequested, int memRequested) {
        return submitJob(owner, description, cpuRequested, memRequested, 0, 0, 0, List.of());
    }

    Optional<Job> submitJob(String owner, String description, int cpuRequested, int memRequested,
                            long diskMbRequested, int gpuCountRequested,
                            long gpuMemoryMbPerGpu,
                            List<com.JobController.PlacementConstraint> constraints) {
        if (cpuRequested <= 0 || memRequested <= 0 || diskMbRequested < 0
                || gpuCountRequested < 0 || gpuMemoryMbPerGpu < 0) {
            throw new IllegalArgumentException(
                    "CPU/memory must be positive and disk/GPU requests non-negative");
        }
        if (gpuCountRequested == 0 && gpuMemoryMbPerGpu != 0) {
            throw new IllegalArgumentException(
                    "GPU memory request requires at least one requested GPU");
        }
        validatePlacementConstraints(constraints);
        Job job=Job.newBuilder()
                .setOwner(owner)
                .setDescription(description)
                .setCpuRequested(cpuRequested)
                .setMemRequested(memRequested)
                .setDiskMbRequested(diskMbRequested)
                .setGpuCountRequested(gpuCountRequested)
                .setGpuMemoryMbPerGpu(gpuMemoryMbPerGpu)
                .addAllPlacementConstraints(constraints)
                .setState(JobState.PENDING)
                .build();
        return enqueueJob(job, true);
    }

    @Override
    public long submit(Job job) {
        if (job == null) {
            throw new IllegalArgumentException("Job must not be null");
        }
        if (job.getId() != 0 || job.getState() != JobState.PENDING) {
            throw new IllegalArgumentException("Job ID and state must be assigned by the controller");
        }
        if (job.getOwner().isBlank() || job.getDescription().isBlank()) {
            throw new IllegalArgumentException("Job owner and description are required");
        }
        if (job.getCpuRequested() <= 0 || job.getMemRequested() <= 0
                || job.getDiskMbRequested() < 0 || job.getGpuCountRequested() < 0
                || job.getGpuMemoryMbPerGpu() < 0) {
            throw new IllegalArgumentException(
                    "Job CPU/memory must be positive and disk/GPU requests non-negative");
        }
        if (job.getGpuCountRequested() == 0 && job.getGpuMemoryMbPerGpu() != 0) {
            throw new IllegalArgumentException(
                    "GPU memory request requires at least one requested GPU");
        }
        validatePlacementConstraints(job.getPlacementConstraintsList());

        return enqueueJob(job).map(Job::getId)
                .orElseThrow(() -> new IllegalStateException("Job queue is full"));
    }

    private static void validatePlacementConstraints(
            List<com.JobController.PlacementConstraint> constraints) {
        for (com.JobController.PlacementConstraint constraint : constraints) {
            if (constraint.getKey().isBlank() || constraint.getValue().isBlank()
                    || constraint.getOperator()
                            == com.JobController.PlacementOperator.UNRECOGNIZED) {
                throw new IllegalArgumentException("Invalid job placement constraint");
            }
        }
    }

    void addJobEventListener(JobEventListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Job event listener must not be null");
        }
        jobEventListeners.addIfAbsent(listener);
    }

    void publishTerminalJobEvent(Job job) {
        if (job == null) {
            throw new IllegalArgumentException("Job must not be null");
        }

        for (JobEventListener listener : jobEventListeners) {
            try {
                switch (job.getState()) {
                    case COMPLETED -> listener.onJobCompleted(job);
                    case FAILED -> listener.onJobFailed(job);
                    case CANCELLED -> listener.onJobCancelled(job);
                    case LOST -> listener.onJobLost(job);
                    default -> { return; }
                }
            } catch (RuntimeException e) {
                System.err.println("Job event listener failed for job " + job.getId()
                        + " in state " + job.getState() + ": " + e.getMessage());
            }
        }
    }

    void publishJobStateChanged(Job job) {
        if (job == null) {
            throw new IllegalArgumentException("Job must not be null");
        }
        for (JobEventListener listener : jobEventListeners) {
            try {
                listener.onJobStateChanged(job);
            } catch (RuntimeException e) {
                System.err.println("Job state listener failed for job " + job.getId()
                        + " in state " + job.getState() + ": " + e.getMessage());
            }
        }
    }

    @Override
    public boolean cancel(long jobId) {
        if (jobId <= 0 || jobId > Integer.MAX_VALUE) {
            return false;
        }
        RestApiServer.CancellationResult result =
                cancelJob((int) jobId);
        return result.outcome() == RestApiServer.CancellationOutcome.CANCELLED
                || result.outcome() == RestApiServer.CancellationOutcome.ALREADY_CANCELLED;
    }

    RestApiServer.CancellationResult cancelJob(int jobId) {
        JobEntry entry = Discover.jobs.get(jobId);
        if (entry == null) {
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.NOT_FOUND, null);
        }

        try {
            if (!entry.awaitAllocation(5_500)) {
                return new RestApiServer.CancellationResult(
                        RestApiServer.CancellationOutcome.UNAVAILABLE, entry.job());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.UNAVAILABLE, entry.job());
        }

        Job current = entry.job();
        if (current.getState() == JobState.CANCELLED) {
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.ALREADY_CANCELLED, current);
        }
        if (current.getState() == JobState.COMPLETED || current.getState() == JobState.FAILED
                || current.getState() == JobState.LOST) {
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.NOT_CANCELLABLE, current);
        }

        if (entry.cancelIfQueued()) {
            jobQueue.remove(jobId);
            publishTerminalJobEvent(entry.job());
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.CANCELLED, entry.job());
        }

        if (entry.node() == null) {
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.UNAVAILABLE, entry.job());
        }

        try {
            WorkerClientRegistry.WorkerClient client = Discover.workers.getOrConnect(
                    entry.node().nodeId(), entry.node().node().getAgentEndpoint());
            Cnf reply = client.stub()
                    .withDeadlineAfter(15, TimeUnit.SECONDS)
                    .cancelAllocated(JobRef.newBuilder().setId(jobId).build());
            if (!reply.getSuccess() || reply.getState() != JobState.CANCELLED) {
                System.err.println("Worker rejected cancellation for job " + jobId + ": "
                        + reply.getMessage());
                return refreshCancellationState(jobId, entry);
            }
            if (!entry.tryCancel()) {
                Job latest = entry.job();
                System.err.println("Job " + jobId + " changed state during cancellation to "
                        + latest.getState());
                RestApiServer.CancellationOutcome outcome =
                        latest.getState() == JobState.CANCELLED
                                ? RestApiServer.CancellationOutcome.ALREADY_CANCELLED
                                : latest.getState() == JobState.COMPLETED
                                        || latest.getState() == JobState.FAILED
                                        || latest.getState() == JobState.LOST
                                        ? RestApiServer.CancellationOutcome.NOT_CANCELLABLE
                                        : RestApiServer.CancellationOutcome.UNAVAILABLE;
                return new RestApiServer.CancellationResult(outcome, latest);
            }
            entry.releaseResources();
            dispatchQueuedJobs();
            publishTerminalJobEvent(entry.job());
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.CANCELLED, entry.job());
        } catch (RuntimeException e) {
            System.err.println("Cancel failed for job " + jobId + ": " + e.getMessage());
            return refreshCancellationState(jobId, entry);
        }
    }

    private RestApiServer.CancellationResult refreshCancellationState(
            int jobId, JobEntry entry) {
        try {
            WorkerClientRegistry.WorkerClient client = Discover.workers.getOrConnect(
                    entry.node().nodeId(), entry.node().node().getAgentEndpoint());
            Job workerJob = client.stub()
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getJobStatus(Job.newBuilder().setId(jobId).build());
            JobState workerState = workerJob.getState();
            if (workerState == JobState.CANCELLED || workerState == JobState.COMPLETED
                    || workerState == JobState.FAILED || workerState == JobState.LOST) {
                boolean transitioned = entry.setState(workerState);
                entry.releaseResources();
                if (transitioned) {
                    publishTerminalJobEvent(entry.job());
                }
            }
        } catch (RuntimeException e) {
            System.err.println("Could not refresh job state after cancellation failure: "
                    + e.getMessage());
        }

        Job current = entry.job();
        RestApiServer.CancellationOutcome outcome =
                current.getState() == JobState.CANCELLED
                        ? RestApiServer.CancellationOutcome.ALREADY_CANCELLED
                        : current.getState() == JobState.COMPLETED
                                || current.getState() == JobState.FAILED
                                || current.getState() == JobState.LOST
                                ? RestApiServer.CancellationOutcome.NOT_CANCELLABLE
                                : RestApiServer.CancellationOutcome.UNAVAILABLE;
        return new RestApiServer.CancellationResult(outcome, current);
    }

    @Override
    public Optional<Job> find(long jobId) {
        if (jobId <= 0 || jobId > Integer.MAX_VALUE) {
            return Optional.empty();
        }
        JobEntry entry = Discover.jobs.get((int) jobId);
        return entry == null ? Optional.empty() : Optional.of(entry.job());
    }

    private Optional<Job> enqueueJob(Job request) {
        return enqueueJob(request, false);
    }

    private Optional<Job> enqueueJob(Job request, boolean printStandaloneDAG) {
        Job job=request.toBuilder()
                .setId(Discover.nextJobId.incrementAndGet())
                .setState(JobState.PENDING)
                .build();
        JobEntry entry=new JobEntry(job);
        if (Discover.jobs.putIfAbsent(job.getId(), entry)!=null) {
            throw new IllegalStateException("Generated duplicate job ID " + job.getId());
        }
        if (!jobQueue.add(job.getId())) {
            Discover.jobs.remove(job.getId(), entry);
            return Optional.empty();
        }
        if (printStandaloneDAG) {
            System.out.println(DAGGraphPrinter.renderStandaloneJob(job));
        }
        dispatchQueuedJobs();
        return Optional.of(job);
    }

    List<Job> listJobs() {
        return Discover.jobs.values().stream()
                .map(JobEntry::job)
                .sorted(Comparator.comparingInt(Job::getId))
                .toList();
    }

    Optional<Job> findJob(int id) {
        JobEntry entry=Discover.jobs.get(id);
        return entry==null ? Optional.empty() : Optional.of(entry.job());
    }

    boolean removeQueuedJob(long jobId) {
        return jobQueue.remove(jobId);
    }

    void shutdown() {
        dispatcher.shutdownNow();
    }

    void dispatchQueuedJobs() {
        if (!dispatcher.isShutdown()) {
            dispatcher.execute(this::drainQueue);
        }
    }

    void onWorkerDown(String workerUUID) {
        if (workerUUID == null || workerUUID.isBlank()) {
            throw new IllegalArgumentException("Worker UUID must not be blank");
        }
        for (JobEntry entry : Discover.jobs.values()) {
            NodeEntry assignedNode = entry.node();
            if (assignedNode != null && workerUUID.equals(assignedNode.workerUUID())
                    && entry.setState(JobState.LOST)) {
                entry.releaseResources();
                publishTerminalJobEvent(entry.job());
            }
        }
        dispatchQueuedJobs();
    }

    void onWorkerRecovered(String workerUUID) {
        if (workerUUID == null || workerUUID.isBlank()) {
            throw new IllegalArgumentException("Worker UUID must not be blank");
        }
        dispatchQueuedJobs();
    }

    private void drainQueue() {
        for (Integer jobId : jobQueue.snapshot()) {
            JobEntry entry=Discover.jobs.get(jobId);
            if (entry==null || entry.job().getState()!=JobState.PENDING) {
                jobQueue.remove(jobId);
                continue;
            }
            if (!tryAllocatePending(entry)) {
                continue;
            }
            jobQueue.remove(jobId);
        }
    }

    private boolean tryAllocatePending(JobEntry entry) {
        Job pending=entry.job();
        List<NodeEntry> candidates=Discover.nodes.values().stream()
                .sorted(Comparator.comparingInt(NodeEntry::nodeId))
                .toList();
        Optional<NodeEntry> selected=scheduler.selectNode(pending, candidates);
        if (selected.isEmpty()) {
            return false;
        }

        Optional<NodeEntry> firstFit=firstFitScheduler.selectNode(pending, candidates);
        Optional<NodeEntry> bestFit=bestFitScheduler.selectNode(pending, candidates);
        Optional<NodeEntry> roundRobin=roundRobinScheduler.selectNode(pending, candidates);
        NodeEntry chosen=selected.get();
        List<String> assignedGpuIds = chosen.tally().tryReserve(pending);
        if (assignedGpuIds == null) {
            return false;
        }

        int allocationId=Discover.allocationKey.incrementAndGet();
        if (!entry.beginAllocation(chosen)) {
            chosen.tally().release(pending.getId());
            return false;
        }

        com.JobController.worker.WorkerMembership membership =
                Discover.membershipManager.getWorker(chosen.workerUUID());
        if (membership != null && !membership.isEligibleForScheduling()) {
            chosen.tally().release(pending.getId());
            entry.abortAllocation();
            return entry.job().getState() != JobState.PENDING;
        }

        try {
            WorkerClientRegistry.WorkerClient client=Discover.workers.getOrConnect(
                    chosen.nodeId(), chosen.node().getAgentEndpoint());
            Job allocationJob = entry.job().toBuilder()
                    .setAssignedWorkerId(chosen.workerId())
                    .putAllAssignedWorkerLabels(chosen.labels())
                    .build();
            Cnf reply=client.stub()
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .allocate(AllocationCommand.newBuilder()
                            .setAllocationId(allocationId)
                            .setJob(allocationJob)
                            .addAllGpuDeviceIds(assignedGpuIds)
                            .build());
            if (!reply.getSuccess()) {
                throw new IllegalStateException(
                        "Worker rejected job " + pending.getId() + ": " + reply.getMessage());
            }
            entry.finishAllocation(chosen, allocationId, true, allocationJob);
            if (entry.job().getState() == JobState.ALLOCATED) {
                publishJobStateChanged(entry.job());
            }
            if (entry.job().getState()==JobState.COMPLETED
                    || entry.job().getState()==JobState.FAILED
                    || entry.job().getState()==JobState.CANCELLED
                    || entry.job().getState()==JobState.LOST) {
                entry.releaseResources();
                Discover.dispatchQueuedJobs();
            }
            System.out.println("Job " + pending.getId() + " allocated to node " + chosen.nodeId());
            System.out.println("Scheduling comparison for job " + pending.getId()
                    + ": LeastLoaded=node " + chosen.nodeId()
                    + ", FirstFit=" + nodeId(firstFit)
                    + ", BestFit=" + nodeId(bestFit)
                    + ", RoundRobin=" + nodeId(roundRobin));
            return true;
        } catch (RuntimeException e) {
            boolean failed=entry.finishAllocation(null, allocationId, false, entry.job());
            chosen.tally().release(pending.getId());
            if (failed) {
                publishTerminalJobEvent(entry.job());
            }
            System.err.println("Allocation failed for job " + pending.getId() + ": " + e.getMessage());
            return true;
        }
    }

    private static String nodeId(Optional<NodeEntry> selection) {
        return selection.map(node -> Integer.toString(node.nodeId())).orElse("none");
    }
}
