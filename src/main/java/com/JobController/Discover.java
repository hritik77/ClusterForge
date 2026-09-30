package com.JobController;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Total / available CPU and memory of one node. Thread-safe. */
final class NodeResourceTally {
    private final int nodeId;
    private final int totalCpu;
    private final int totalMem;
    private int availableCpu;
    private int availableMem;

    NodeResourceTally(int nodeId, int cpu, int mem) {
        this.nodeId=nodeId;
        this.totalCpu=cpu;
        this.totalMem=mem;
        this.availableCpu=cpu;
        this.availableMem=mem;
    }

    synchronized boolean tryReserve(int cpu, int mem) {
        if (cpu > availableCpu || mem > availableMem) return false;
        availableCpu-=cpu;
        availableMem-=mem;
        return true;
    }

    synchronized void release(int cpu, int mem) {
        availableCpu=Math.min(totalCpu, availableCpu + cpu);
        availableMem=Math.min(totalMem, availableMem + mem);
    }

    int nodeId()               { return nodeId; }
    int totalCpu()             { return totalCpu; }
    int totalMem()             { return totalMem; }
    synchronized int availableCpu() { return availableCpu; }
    synchronized int availableMem() { return availableMem; }
}

/** Map value #1: nodeId -> [node, nodeResourceTally, lastHeartBeat] */
final class NodeEntry {
    private volatile Node node;
    private final NodeResourceTally tally;
    private volatile long lastHeartBeatNanos;

    NodeEntry(Node node, NodeResourceTally tally) {
        this.node=node;
        this.tally=tally;
        this.lastHeartBeatNanos=System.nanoTime();
    }

    Node node()                 { return node; }
    NodeResourceTally tally()   { return tally; }
    long lastHeartBeatNanos()   { return lastHeartBeatNanos; }
    int nodeId()                { return node.getId(); }   

    void beat(NodeState reportedState) {
        this.node=node.toBuilder().setState(reportedState).build();
        this.lastHeartBeatNanos=System.nanoTime();
    }

    void markDown() {
        this.node=node.toBuilder().setState(NodeState.DOWN).build();
    }
}

/** Map value #2: jobId -> job details + the node it is allocated on */
final class JobEntry {
    private volatile Job job;
    private final NodeEntry node;          // live reference to the hosting node
    private final int allocationId;
    private final long allocatedAt=System.currentTimeMillis();
    private final AtomicBoolean resourcesReleased=new AtomicBoolean(false);

    JobEntry(Job job, NodeEntry node, int allocationId) {
        this.job=job;
        this.node=node;
        this.allocationId=allocationId;
    }

    Job job()               { return job; }
    NodeEntry node()        { return node; }
    int allocationId()      { return allocationId; }
    long allocatedAt()      { return allocatedAt; }

    synchronized void setState(JobState state) {
        if (!isTerminal(job.getState())) {
            this.job=job.toBuilder().setState(state).build();
        }
    }

    synchronized boolean tryCancel() {
        if (isTerminal(job.getState())) return false;
        this.job=job.toBuilder().setState(JobState.CANCELLED).build();
        return true;
    }

    private static boolean isTerminal(JobState state) {
        return state==JobState.COMPLETED || state==JobState.FAILED
                || state==JobState.CANCELLED;
    }

    /** Gives CPU/mem back to the node exactly once, however many times it is called. */
    void releaseResources() {
        if (resourcesReleased.compareAndSet(false, true)) {
            node.tally().release(job.getCpuRequested(), job.getMemRequested());
        }
    }
}

public class Discover implements RestApiServer.JobOperations {
    private static final AtomicInteger nextNodeId=new AtomicInteger();
    private static final AtomicInteger nextJobId=new AtomicInteger();
    private static final AtomicInteger allocationKey=new AtomicInteger();

    private static final WorkerClientRegistry workers=new WorkerClientRegistry();

    // nodeId -> [node, nodeResourceTally, lastHeartBeat]
    private static final ConcurrentHashMap<Integer, NodeEntry> nodes=new ConcurrentHashMap<>();

    // jobId -> [job, node it is allocated on, ...]
    private static final ConcurrentHashMap<Integer, JobEntry> jobs=new ConcurrentHashMap<>();

    @Override
    public Optional<Job> submitJob(String owner, String description, int cpuRequested,
                                  int memRequested) {
        Job job=Job.newBuilder()
                .setId(nextJobId.incrementAndGet())
                .setOwner(owner)
                .setDescription(description)
                .setCpuRequested(cpuRequested)
                .setMemRequested(memRequested)
                .setState(JobState.PENDING)
                .build();
        if (!allocater(job)) return Optional.empty();
        return findJob(job.getId());
    }

    @Override
    public List<Job> listJobs() {
        return jobs.values().stream()
                .map(JobEntry::job)
                .sorted(Comparator.comparingInt(Job::getId))
                .toList();
    }

    @Override
    public Optional<Job> findJob(int id) {
        JobEntry entry=jobs.get(id);
        return entry==null ? Optional.empty() : Optional.of(entry.job());
    }

    @Override
    public RestApiServer.CancellationResult cancelJob(int id) {
        JobEntry entry=jobs.get(id);
        if (entry==null) {
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.NOT_FOUND, null);
        }

        Job current=entry.job();
        if (current.getState()==JobState.CANCELLED) {
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.ALREADY_CANCELLED, current);
        }
        if (current.getState()==JobState.COMPLETED || current.getState()==JobState.FAILED) {
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.NOT_CANCELLABLE, current);
        }

        try {
            WorkerClientRegistry.WorkerClient client=workers.getOrConnect(
                    entry.node().nodeId(), entry.node().node().getAgentEndpoint());
            Cnf reply=client.stub()
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .cancelAllocated(JobRef.newBuilder().setId(id).build());
            if (!reply.getSuccess()) {
                return refreshCancellationState(id, entry);
            }
            if (!entry.tryCancel()) {
                Job latest=entry.job();
                RestApiServer.CancellationOutcome outcome =
                        latest.getState()==JobState.CANCELLED
                                ? RestApiServer.CancellationOutcome.ALREADY_CANCELLED
                                : latest.getState()==JobState.COMPLETED
                                        || latest.getState()==JobState.FAILED
                                        ? RestApiServer.CancellationOutcome.NOT_CANCELLABLE
                                        : RestApiServer.CancellationOutcome.UNAVAILABLE;
                return new RestApiServer.CancellationResult(outcome, latest);
            }
            entry.releaseResources();
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.CANCELLED, entry.job());
        } catch (RuntimeException e) {
            System.out.println("Cancel failed: " + e.getMessage());
            return refreshCancellationState(id, entry);
        }
    }

    private static RestApiServer.CancellationResult refreshCancellationState(int id, JobEntry entry) {
        try {
            WorkerClientRegistry.WorkerClient client=workers.getOrConnect(
                    entry.node().nodeId(), entry.node().node().getAgentEndpoint());
            Job workerJob=client.stub()
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getJobStatus(Job.newBuilder().setId(id).build());
            JobState workerState=workerJob.getState();
            if (workerState==JobState.CANCELLED || workerState==JobState.COMPLETED
                    || workerState==JobState.FAILED) {
                entry.setState(workerState);
                entry.releaseResources();
            }
        } catch (RuntimeException e) {
            System.out.println("Could not refresh job state after cancellation failure: "
                    + e.getMessage());
        }

        Job current=entry.job();
        RestApiServer.CancellationOutcome outcome =
                current.getState()==JobState.CANCELLED
                        ? RestApiServer.CancellationOutcome.ALREADY_CANCELLED
                        : current.getState()==JobState.COMPLETED
                                || current.getState()==JobState.FAILED
                                ? RestApiServer.CancellationOutcome.NOT_CANCELLABLE
                                : RestApiServer.CancellationOutcome.UNAVAILABLE;
        return new RestApiServer.CancellationResult(outcome, current);
    }

    private static Cnf fail(String message) {
        return Cnf.newBuilder().setSuccess(false).setMessage(message).build();
    }

    private static final long HEARTBEAT_INTERVAL_MS=5_000;
    private static final long HEARTBEAT_TIMEOUT_NS=3 * HEARTBEAT_INTERVAL_MS * 1_000_000L;

    private static final ScheduledExecutorService reaper =
            Executors.newSingleThreadScheduledExecutor();

    static void startFailureDetector() {
        reaper.scheduleWithFixedDelay(() -> {
            try {
                long now=System.nanoTime();
                for (NodeEntry n : nodes.values()) {
                    boolean stale=now-n.lastHeartBeatNanos() > HEARTBEAT_TIMEOUT_NS;
                    if (stale && n.node().getState()!=NodeState.DOWN) {
                        markNodeDown(n);
                    }
                }
            } catch (RuntimeException e) {
                e.printStackTrace();   // an uncaught exception would silently cancel future runs
            }
        }, 1, 2, TimeUnit.SECONDS);
    }

    private static void markNodeDown(NodeEntry n) {
       n.markDown();
       System.out.println("Node " + n.nodeId() + " missed heartbeats; marked DOWN");

       for (JobEntry e : jobs.values()) {
           if (e.node()==n) {
               JobState s=e.job().getState();
               if (s!=JobState.COMPLETED && s!=JobState.FAILED && s!=JobState.CANCELLED) {
                   e.setState(JobState.FAILED);   // or reset to PENDING and reallocate
                   e.releaseResources();
               }
           }
       }
   }

    public static class NodeManager extends WorkerToControllerGrpc.WorkerToControllerImplBase {

        @Override
        public void registerNode(Node request, StreamObserver<Cnf> obs) {
            if (request.getHostname().isBlank()
                    || request.getAgentEndpoint().isBlank()
                    || request.getCpu()<=0
                    || request.getMem()<=0) {
                obs.onNext(fail("hostname, endpoint, CPU, and memory are required"));
                obs.onCompleted();
                return;
            }

            int nodeId=nextNodeId.incrementAndGet();
            Node node=request.toBuilder()
                    .setId(nodeId)
                    .setState(NodeState.AVAILABLE)
                    .build();
            try {
                workers.getOrConnect(nodeId, node.getAgentEndpoint());
                // Published only after the connection succeeded, so no half-registered nodes are visible.
                nodes.put(nodeId, new NodeEntry(node,
                        new NodeResourceTally(nodeId, node.getCpu(), node.getMem())));
                obs.onNext(Cnf.newBuilder().setSuccess(true).setNodeId(nodeId).build());
                System.out.println("Node " + nodeId + " added");
            } catch (RuntimeException e) {
                workers.remove(nodeId);
                obs.onNext(fail("Could not connect to worker: " + e.getMessage()));
            }
            obs.onCompleted();
        }

        @Override
        public void heartBeat(Node request, StreamObserver<Cnf> obs) {
            NodeEntry entry=nodes.get(request.getId());
            if (entry==null) {
                obs.onNext(fail("Unknown node"));
                System.out.println("Unknown Node Approached");
            } else {
                entry.beat(request.getState());
                System.out.println("Heartbeat from node " + request.getId() + " recorded");
                obs.onNext(Cnf.newBuilder().setSuccess(true).build());
            }
            obs.onCompleted();
        }

        @Override
        public void reportJobStatus(Job j, StreamObserver<Cnf> obs) {
            JobEntry entry=jobs.get(j.getId());
            if (j.getId()<=0 || entry==null) {
                obs.onNext(fail("Invalid or unknown job ID"));
                obs.onCompleted();
                return;
            }

            entry.setState(j.getState());
            switch (j.getState()) {
                case COMPLETED, FAILED, CANCELLED -> entry.releaseResources();
                default -> { }
            }

            obs.onNext(Cnf.newBuilder()
                    .setSuccess(true)
                    .setJobId(j.getId())
                    .setNodeId(entry.node().nodeId())
                    .setMessage("Successfully Updated Job Status")
                    .build());
            obs.onCompleted();
        }
    }

    /** Picks a node with enough free CPU/mem, reserves it, and tells the worker. */
    public static boolean allocater(Job j) {
        if (j.getId()<=0 || j.getCpuRequested()<=0 || j.getMemRequested()<=0
                || jobs.containsKey(j.getId())) {
            System.out.println("Job Allocation Unsuccessful: invalid or duplicate job id");
            return false;
        }

        // 1. Find a node and reserve resources on it atomically.
        NodeEntry chosen=null;
        for (NodeEntry n : nodes.values()) {
            NodeState s=n.node().getState();
            if (s==NodeState.DOWN || s==NodeState.MAINTENANCE) continue;
            if (n.tally().tryReserve(j.getCpuRequested(), j.getMemRequested())) {
                chosen=n;
                break;
            }
        }
        if (chosen==null) {
            System.out.println("Job Allocation Unsuccessful: no node has enough capacity");
            return false;
        }

        // 2. Record the job, then call the worker.
        int id=allocationKey.incrementAndGet();
        Job allocated=j.toBuilder().setState(JobState.ALLOCATED).build();
        JobEntry entry=new JobEntry(allocated, chosen, id);
        if (jobs.putIfAbsent(j.getId(), entry)!=null) {
            entry.releaseResources();
            return false;
        }

        try {
            WorkerClientRegistry.WorkerClient client =
                    workers.getOrConnect(chosen.nodeId(), chosen.node().getAgentEndpoint());
            Cnf reply=client.stub()
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .allocate(AllocationCommand.newBuilder()
                            .setAllocationId(id)
                            .setJob(allocated)
                            .build());
            if (!reply.getSuccess()) throw new RuntimeException(reply.getMessage());
            System.out.println("Job " + j.getId() + " allocated to node " + chosen.nodeId());
            return true;
        } catch (RuntimeException e) {
            jobs.remove(j.getId());
            entry.releaseResources();
            System.out.println("Job Allocation Unsuccessful: " + e.getMessage());
            return false;
        }
    }

    public static boolean canceller(Job j) {
        RestApiServer.CancellationOutcome outcome=new Discover()
                .cancelJob(j.getId()).outcome();
        return outcome==RestApiServer.CancellationOutcome.CANCELLED
                || outcome==RestApiServer.CancellationOutcome.ALREADY_CANCELLED;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int grpcPort=Integer.getInteger("clusterforge.controller.port", 9999);
        Server server=ServerBuilder.forPort(grpcPort)
                .addService(new NodeManager())
                .build();
        String restHost=System.getProperty("clusterforge.rest.host", "127.0.0.1");
        int restPort=Integer.getInteger("clusterforge.rest.port", 8080);
        RestApiServer restServer=new RestApiServer(restHost, restPort, new Discover());
        startFailureDetector();
        System.out.println("Reaper Started");
        server.start();
        restServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            restServer.close();
            server.shutdown();
            reaper.shutdownNow();
            workers.close();
        }));
        System.out.println("Controller gRPC started on port " + grpcPort);
        System.out.println("REST API started on " + restHost + ":" + restServer.port());
        server.awaitTermination();
    }
}