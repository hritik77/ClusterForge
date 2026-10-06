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
        Job job=Job.newBuilder()
                .setOwner(owner)
                .setDescription(description)
                .setCpuRequested(cpuRequested)
                .setMemRequested(memRequested)
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
        if (job.getCpuRequested() <= 0 || job.getMemRequested() <= 0) {
            throw new IllegalArgumentException("Job CPU and memory requests must be positive");
        }

        return enqueueJob(job).map(Job::getId)
                .orElseThrow(() -> new IllegalStateException("Job queue is full"));
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
                    default -> {
                        return;
                    }
                }
            } catch (RuntimeException e) {
                System.err.println("Job event listener failed for job " + job.getId()
                        + " in state " + job.getState() + ": " + e.getMessage());
            }
        }
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

    void removeQueuedJob(int jobId) {
        jobQueue.remove(jobId);
    }

    void shutdown() {
        dispatcher.shutdownNow();
    }

    void dispatchQueuedJobs() {
        if (!dispatcher.isShutdown()) {
            dispatcher.execute(this::drainQueue);
        }
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
        NodeState state=chosen.node().getState();
        if (state==NodeState.DOWN || state==NodeState.MAINTENANCE
                || !chosen.tally().tryReserve(pending.getCpuRequested(), pending.getMemRequested())) {
            return false;
        }

        int allocationId=Discover.allocationKey.incrementAndGet();
        if (!entry.beginAllocation()) {
            chosen.tally().release(pending.getCpuRequested(), pending.getMemRequested());
            return false;
        }

        try {
            WorkerClientRegistry.WorkerClient client=Discover.workers.getOrConnect(
                    chosen.nodeId(), chosen.node().getAgentEndpoint());
            Cnf reply=client.stub()
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .allocate(AllocationCommand.newBuilder()
                            .setAllocationId(allocationId)
                            .setJob(entry.job())
                            .build());
            if (!reply.getSuccess()) {
                throw new IllegalStateException(
                        "Worker rejected job " + pending.getId() + ": " + reply.getMessage());
            }
            entry.finishAllocation(chosen, allocationId, true);
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
            boolean failed=entry.finishAllocation(null, allocationId, false);
            chosen.tally().release(pending.getCpuRequested(), pending.getMemRequested());
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
