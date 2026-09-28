package com.java.JobController;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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

    void setState(JobState state) {
        this.job=job.toBuilder().setState(state).build();
    }

    /** Gives CPU/mem back to the node exactly once, however many times it is called. */
    void releaseResources() {
        if (resourcesReleased.compareAndSet(false, true)) {
            node.tally().release(job.getCpuRequested(), job.getMemRequested());
        }
    }
}

class Discover {
    private static final AtomicInteger nextNodeId=new AtomicInteger();
    private static final AtomicInteger allocationKey=new AtomicInteger();

    private static final WorkerClientRegistry workers=new WorkerClientRegistry();

    // nodeId -> [node, nodeResourceTally, lastHeartBeat]
    private static final ConcurrentHashMap<Integer, NodeEntry> nodes=new ConcurrentHashMap<>();

    // jobId -> [job, node it is allocated on, ...]
    private static final ConcurrentHashMap<Integer, JobEntry> jobs=new ConcurrentHashMap<>();

    private static Cnf fail(String message) {
        return Cnf.newBuilder().setSuccess(false).setMessage(message).build();
    }

    private static final long HEARTBEAT_INTERVAL_MS=5_000;               // what the worker uses
    private static final long HEARTBEAT_TIMEOUT_NS =3 * HEARTBEAT_INTERVAL_MS * 1_000_000L; // 15 s

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
        if (j.getId()<=0 || jobs.containsKey(j.getId())) {
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
        JobEntry entry=jobs.get(j.getId());
        if (entry==null) {
            System.out.println("Cancel failed: job " + j.getId() + " is not allocated");
            return false;
        }
        try {
            WorkerClientRegistry.WorkerClient client=workers.getOrConnect(
                    entry.node().nodeId(), entry.node().node().getAgentEndpoint());
            Cnf reply=client.stub()
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .cancelAllocated(JobRef.newBuilder().setId(j.getId()).build());
            if (reply.getSuccess()) {
                entry.setState(JobState.CANCELLED);
                entry.releaseResources();
            }
            return reply.getSuccess();
        } catch (RuntimeException e) {
            System.out.println("Cancel failed: " + e.getMessage());
            return false;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Server server=ServerBuilder.forPort(9999)
                .addService(new NodeManager())
                .build();
        startFailureDetector();
        System.out.println("Reaper Started");
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(workers::close));
        System.out.println("Broker started on port 9999");
        server.awaitTermination();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            reaper.shutdownNow();
            workers.close();
        }));
    }
}