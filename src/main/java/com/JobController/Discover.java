package com.JobController;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import com.JobController.dag.DAG;
import com.JobController.dag.DAGRun;
import com.JobController.dag.DAGManager;
import com.JobController.dag.DAGRunState;
import com.JobController.failure.FailureDetector;
import com.JobController.failure.FailureDetectorConfig;
import com.JobController.failure.SystemTimeSource;
import com.JobController.job.parser.JobParserRegistry;
import com.JobController.job.spec.CustomDAGJobSpecification;
import com.JobController.job.spec.CustomDAGTaskSpecification;
import com.JobController.worker.WorkerFailureListener;
import com.JobController.worker.WorkerMembership;
import com.JobController.worker.WorkerMembershipManager;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Total / available resources of one node. Thread-safe. */
final class NodeResourceTally {
    private final int nodeId;
    private int totalCpu;
    private int totalMem;
    private long totalDiskMb;
    private final Map<String, GpuResource> gpuResources = new LinkedHashMap<>();
    private final Map<Integer, Reservation> reservations = new HashMap<>();
    private int availableCpu;
    private int availableMem;
    private long availableDiskMb;

    NodeResourceTally(int nodeId, int cpu, int mem) {
        this(nodeId, cpu, mem, 0, List.of());
    }

    NodeResourceTally(int nodeId, int cpu, int mem, long diskMb,
                      List<com.JobController.GPUDevice> gpuDevices) {
        if (diskMb < 0) {
            throw new IllegalArgumentException("Node disk capacity cannot be negative");
        }
        this.nodeId=nodeId;
        this.totalCpu=cpu;
        this.totalMem=mem;
        this.totalDiskMb=diskMb;
        this.availableCpu=cpu;
        this.availableMem=mem;
        this.availableDiskMb=diskMb;
        for (com.JobController.GPUDevice gpu : gpuDevices) {
            if (gpu.getId().isBlank() || gpu.getMemoryMb() <= 0
                    || gpuResources.putIfAbsent(
                            gpu.getId(), new GpuResource(gpu.getMemoryMb())) != null) {
                throw new IllegalArgumentException(
                        "GPU inventory requires unique non-blank IDs and positive memory");
            }
        }
    }

    synchronized boolean canFit(Job job) {
        return job.getCpuRequested() > 0
                && job.getMemRequested() > 0
                && job.getDiskMbRequested() >= 0
                && job.getGpuCountRequested() >= 0
                && job.getGpuMemoryMbPerGpu() >= 0
                && (job.getGpuCountRequested() > 0 || job.getGpuMemoryMbPerGpu() == 0)
                && job.getCpuRequested() <= availableCpu
                && job.getMemRequested() <= availableMem
                && job.getDiskMbRequested() <= availableDiskMb
                && selectGpuDevices(job) != null;
    }

    synchronized List<String> tryReserve(Job job) {
        if (reservations.containsKey(job.getId()) || !canFit(job)) {
            return null;
        }
        List<String> selectedGpuIds = selectGpuDevices(job);
        for (String gpuId : selectedGpuIds) {
            GpuResource gpu = gpuResources.get(gpuId);
            gpu.allocated = true;
            gpu.availableMemoryMb = 0;
        }
        availableCpu -= job.getCpuRequested();
        availableMem -= job.getMemRequested();
        availableDiskMb -= job.getDiskMbRequested();
        reservations.put(job.getId(), new Reservation(job, selectedGpuIds));
        return selectedGpuIds;
    }

    synchronized void reconfigure(
            int cpu, int mem, long diskMb, List<com.JobController.GPUDevice> gpuDevices) {
        if (cpu <= 0 || mem <= 0 || diskMb < 0) {
            throw new IllegalArgumentException("Worker capacity values are invalid");
        }
        Map<String, Long> newGpuMemory = new LinkedHashMap<>();
        for (com.JobController.GPUDevice gpu : gpuDevices) {
            if (gpu.getId().isBlank() || gpu.getMemoryMb() <= 0
                    || newGpuMemory.putIfAbsent(gpu.getId(), gpu.getMemoryMb()) != null) {
                throw new IllegalArgumentException(
                        "GPU inventory requires unique non-blank IDs and positive memory");
            }
        }

        long usedCpu = 0;
        long usedMem = 0;
        long usedDisk = 0;
        Map<String, Long> reservedGpuMemory = new HashMap<>();
        for (Reservation reservation : reservations.values()) {
            Job job = reservation.job();
            usedCpu += job.getCpuRequested();
            usedMem += job.getMemRequested();
            usedDisk += job.getDiskMbRequested();
            for (String gpuId : reservation.gpuIds()) {
                Long capacity = newGpuMemory.get(gpuId);
                if (capacity == null || capacity < job.getGpuMemoryMbPerGpu()) {
                    throw new IllegalArgumentException(
                            "New GPU inventory cannot satisfy existing reservation " + gpuId);
                }
                reservedGpuMemory.put(gpuId, capacity);
            }
        }
        if (usedCpu > cpu || usedMem > mem || usedDisk > diskMb) {
            throw new IllegalArgumentException(
                    "New worker capacity is below resources reserved by active jobs");
        }

        Map<String, GpuResource> updatedGpus = new LinkedHashMap<>();
        newGpuMemory.forEach((id, memoryMb) -> {
            GpuResource gpu = new GpuResource(memoryMb);
            if (reservedGpuMemory.containsKey(id)) {
                gpu.allocated = true;
                gpu.availableMemoryMb = 0;
            }
            updatedGpus.put(id, gpu);
        });
        totalCpu = cpu;
        totalMem = mem;
        totalDiskMb = diskMb;
        availableCpu = cpu - (int) usedCpu;
        availableMem = mem - (int) usedMem;
        availableDiskMb = diskMb - usedDisk;
        gpuResources.clear();
        gpuResources.putAll(updatedGpus);
    }

    synchronized void release(int jobId) {
        Reservation reservation = reservations.remove(jobId);
        if (reservation == null) {
            return;
        }
        Job job = reservation.job();
        availableCpu = Math.min(totalCpu, availableCpu + job.getCpuRequested());
        availableMem = Math.min(totalMem, availableMem + job.getMemRequested());
        availableDiskMb = Math.min(totalDiskMb, availableDiskMb + job.getDiskMbRequested());
        for (String gpuId : reservation.gpuIds()) {
            GpuResource gpu = gpuResources.get(gpuId);
            gpu.allocated = false;
            gpu.availableMemoryMb = gpu.totalMemoryMb;
        }
    }

    private List<String> selectGpuDevices(Job job) {
        if (job.getGpuCountRequested() == 0) {
            return List.of();
        }
        if (job.getGpuCountRequested() > gpuResources.size()) {
            return null;
        }
        List<String> selected = new ArrayList<>(job.getGpuCountRequested());
        for (Map.Entry<String, GpuResource> entry : gpuResources.entrySet()) {
            if (!entry.getValue().allocated
                    && entry.getValue().totalMemoryMb >= job.getGpuMemoryMbPerGpu()) {
                selected.add(entry.getKey());
                if (selected.size() == job.getGpuCountRequested()) {
                    return List.copyOf(selected);
                }
            }
        }
        return null;
    }

    int nodeId()               { return nodeId; }
    int totalCpu()             { return totalCpu; }
    int totalMem()             { return totalMem; }
    long totalDiskMb()          { return totalDiskMb; }
    synchronized int availableCpu() { return availableCpu; }
    synchronized int availableMem() { return availableMem; }
    synchronized long availableDiskMb() { return availableDiskMb; }
    synchronized int totalGpuCount() { return gpuResources.size(); }
    synchronized int availableGpuCount() {
        return (int) gpuResources.values().stream()
                .filter(gpu -> gpu.availableMemoryMb > 0).count();
    }
    synchronized long totalGpuMemoryMb() {
        return gpuResources.values().stream().mapToLong(gpu -> gpu.totalMemoryMb).sum();
    }
    synchronized long availableGpuMemoryMb() {
        return gpuResources.values().stream().mapToLong(gpu -> gpu.availableMemoryMb).sum();
    }

    private record Reservation(Job job, List<String> gpuIds) {}

    private static final class GpuResource {
        private final long totalMemoryMb;
        private long availableMemoryMb;
        private boolean allocated;

        private GpuResource(long memoryMb) {
            totalMemoryMb = memoryMb;
            availableMemoryMb = memoryMb;
        }
    }
}

/** Registered worker endpoint and resource tally for a controller-assigned node ID. */
final class NodeEntry {
    private volatile Node node;
    private final NodeResourceTally tally;
    /** Stable logical identity used by WorkerMembershipManager. */
    private final String workerUUID;

    NodeEntry(Node node, NodeResourceTally tally) {
        this.node=node;
        this.tally=tally;
        // Prefer the proto worker_uuid field; fall back to legacy "node-N" scheme.
        String uuid = node.getWorkerUuid();
        this.workerUUID = (uuid != null && !uuid.isBlank()) ? uuid : "node-" + node.getId();
    }

    Node node()                 { return node; }
    NodeResourceTally tally()   { return tally; }
    int nodeId()                { return node.getId(); }
    /** Stable logical identity for membership tracking. */
    String workerUUID()         { return workerUUID; }
    /** Legacy alias kept for backward-compatibility. */
    String workerId()           { return workerUUID; }

    Map<String, String> labels() {
        Map<String, String> labels = new LinkedHashMap<>(node.getLabelsMap());
        labels.put("hostname", node.getHostname());
        return Collections.unmodifiableMap(labels);
    }

    synchronized boolean beat(Node reportedNode) {
        Map<String, String> previousLabels = labels();
        this.node=node.toBuilder()
                .setState(reportedNode.getState())
                .clearLabels()
                .putAllLabels(reportedNode.getLabelsMap())
                .build();
        return !previousLabels.equals(labels());
    }

    synchronized boolean updateRegistration(Node registeredNode) {
        if (!workerUUID.equals(registeredNode.getWorkerUuid())
                || registeredNode.getId() != nodeId()) {
            throw new IllegalArgumentException("Worker registration identity changed");
        }
        tally.reconfigure(registeredNode.getCpu(), registeredNode.getMem(),
                registeredNode.getDiskMb(), registeredNode.getGpuDevicesList());
        Map<String, String> previousLabels = labels();
        node = registeredNode.toBuilder().setState(NodeState.AVAILABLE).build();
        return !previousLabels.equals(labels());
    }

    synchronized void markDown() {
        this.node=node.toBuilder().setState(NodeState.DOWN).build();
    }
}

/** Map value #2: jobId -> job details + the node it is allocated on */
final class JobEntry {
    private volatile Job job;
    private volatile NodeEntry node;
    private volatile int allocationId;
    private boolean dispatching;
    private final long allocatedAt=System.currentTimeMillis();
    private final AtomicBoolean resourcesReleased=new AtomicBoolean(false);

    JobEntry(Job job) {
        this.job=job;
    }

    Job job()               { return job; }
    NodeEntry node()        { return node; }
    int allocationId()      { return allocationId; }
    long allocatedAt()      { return allocatedAt; }

    synchronized boolean setState(JobState state) {
        if (!isTerminal(job.getState())) {
            this.job=job.toBuilder().setState(state).build();
            return true;
        }
        return false;
    }

    synchronized boolean beginAllocation(NodeEntry target) {
        if (job.getState()!=JobState.PENDING || node!=null || dispatching) return false;
        if (target == null) {
            throw new IllegalArgumentException("Allocation target must not be null");
        }
        node=target;
        dispatching=true;
        return true;
    }

    synchronized void abortAllocation() {
        if (dispatching && job.getState() == JobState.PENDING) {
            node = null;
            dispatching = false;
            notifyAll();
        }
    }

    synchronized boolean finishAllocation(
            NodeEntry node, int allocationId, boolean successful, Job allocatedJob) {
        boolean failed=false;
        if (successful) {
            this.node=node;
            this.allocationId=allocationId;
            job=job.toBuilder()
                    .setAssignedWorkerId(allocatedJob.getAssignedWorkerId())
                    .clearAssignedWorkerLabels()
                    .putAllAssignedWorkerLabels(allocatedJob.getAssignedWorkerLabelsMap())
                    .setState(job.getState() == JobState.PENDING
                            ? JobState.ALLOCATED : job.getState())
                    .build();
        } else if (job.getState()==JobState.PENDING) {
            node=null;
            job=job.toBuilder().setState(JobState.FAILED).build();
            failed=true;
        }
        dispatching=false;
        notifyAll();
        return failed;
    }

    synchronized boolean awaitAllocation(long timeoutMillis) throws InterruptedException {
        long deadline=System.nanoTime()+TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (dispatching) {
            long remainingNanos=deadline-System.nanoTime();
            if (remainingNanos<=0) return false;
            TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
        }
        return true;
    }

    synchronized boolean cancelIfQueued() {
        if (job.getState()!=JobState.PENDING || node!=null || dispatching) return false;
        job=job.toBuilder().setState(JobState.CANCELLED).build();
        return true;
    }

    synchronized boolean tryCancel() {
        if (isTerminal(job.getState())) return false;
        this.job=job.toBuilder().setState(JobState.CANCELLED).build();
        return true;
    }

    private static boolean isTerminal(JobState state) {
        return state==JobState.COMPLETED || state==JobState.FAILED
                || state==JobState.CANCELLED || state==JobState.LOST;
    }

    /** Gives the job's complete reservation back exactly once. */
    void releaseResources() {
        NodeEntry allocatedNode=node;
        if (allocatedNode!=null && resourcesReleased.compareAndSet(false, true)) {
            allocatedNode.tally().release(job.getId());
        }
    }
}

public class Discover implements RestApiServer.JobOperations {
    private static final AtomicInteger nextNodeId=new AtomicInteger();
    static final AtomicInteger nextJobId=new AtomicInteger();
    static final AtomicInteger allocationKey=new AtomicInteger();
    static final WorkerClientRegistry workers=new WorkerClientRegistry();
    static final JobManager jobManager=new JobManager(new LeastLoadedSchedular());
    static final DAGManager dagManager=new DAGManager(jobManager);
    private static final JobParserRegistry jobParserRegistry=new JobParserRegistry();

    static final WorkerMembershipManager membershipManager = new WorkerMembershipManager(SystemTimeSource.INSTANCE);
    static final FailureDetector failureDetector = new FailureDetector(
            membershipManager, FailureDetectorConfig.fromSystemProperties(), SystemTimeSource.INSTANCE);
    private static final Object workerRegistrationLock = new Object();

    static {
        jobManager.addJobEventListener(dagManager);
        membershipManager.addListener(new WorkerFailureListener() {
            @Override
            public void onWorkerDown(String workerUUID) {
                com.JobController.worker.WorkerMembership membership =
                        membershipManager.getWorker(workerUUID);
                if (membership != null
                        && membership.state()
                                == com.JobController.worker.WorkerMembershipState.DOWN) {
                    nodes.values().stream()
                            .filter(entry -> entry.workerUUID().equals(workerUUID))
                            .forEach(NodeEntry::markDown);
                }
                jobManager.onWorkerDown(workerUUID);
            }

            @Override
            public void onWorkerRecovered(String workerUUID) {
                jobManager.onWorkerRecovered(workerUUID);
            }
        });
    }

    // nodeId -> worker endpoint and resource tally.
    static final ConcurrentHashMap<Integer, NodeEntry> nodes=new ConcurrentHashMap<>();

    // jobId -> [job, node it is allocated on, ...]
    static final ConcurrentHashMap<Integer, JobEntry> jobs=new ConcurrentHashMap<>();

    @Override
    public Optional<Job> submitJob(String owner, String description, int cpuRequested,
                                  int memRequested, long diskMbRequested,
                                  int gpuCountRequested, long gpuMemoryMbPerGpu,
                                  List<com.JobController.PlacementConstraint> constraints) {
        return jobManager.submitJob(owner, description, cpuRequested, memRequested,
                diskMbRequested, gpuCountRequested, gpuMemoryMbPerGpu, constraints);
    }

    @Override
    public RestApiServer.DAGSubmission submitDAG(
            String name, List<CustomDAGTaskSpecification> tasks, Long dagTimeoutMillis) {
        String dagId="custom-dag-" + UUID.randomUUID();
        CustomDAGJobSpecification specification =
                new CustomDAGJobSpecification(dagId, name, tasks, dagTimeoutMillis);
        DAG dag=jobParserRegistry.parse(specification);
        dagManager.registerDAG(dag);
        DAGRun run=dagManager.startRun(dag.getId());
        return new RestApiServer.DAGSubmission(dag, run);
    }

    @Override
    public Optional<RestApiServer.DAGSubmission> findDAGRun(String runId) {
        return dagManager.findRun(runId).flatMap(run -> dagManager.findDAG(run.getDagId())
                .map(dag -> new RestApiServer.DAGSubmission(dag, run)));
    }

    @Override
    public Optional<RestApiServer.DAGCancellation> cancelDAGRun(String dagId, String runId) {
        Optional<DAGRun> run=dagManager.findRun(runId);
        if (run.isEmpty() || !run.get().getDagId().equals(dagId)) {
            return Optional.empty();
        }
        boolean cancellationConfirmed;
        if (run.get().getState() == DAGRunState.RUNNING
                || run.get().getState() == DAGRunState.CANCELLED) {
            cancellationConfirmed = dagManager.cancelRun(runId);
        } else {
            cancellationConfirmed = true;
        }
        return dagManager.findDAG(dagId).map(dag -> new RestApiServer.DAGCancellation(
                new RestApiServer.DAGSubmission(dag, run.get()), cancellationConfirmed));
    }

    @Override
    public List<Job> listJobs() {
        return jobManager.listJobs();
    }

    @Override
    public Optional<Job> findJob(int id) {
        return jobManager.findJob(id);
    }

    @Override
    public RestApiServer.CancellationResult cancelJob(int id) {
        return jobManager.cancelJob(id);
    }

    private static Cnf fail(String message) {
        return Cnf.newBuilder().setSuccess(false).setMessage(message).build();
    }

    static void dispatchQueuedJobs() {
        jobManager.dispatchQueuedJobs();
    }


    public static class NodeManager extends WorkerToControllerGrpc.WorkerToControllerImplBase {

        @Override
        public void registerNode(Node request, StreamObserver<Cnf> obs) {
            if (request.getHostname().isBlank()
                    || request.getAgentEndpoint().isBlank()
                    || request.getCpu()<=0
                    || request.getMem()<=0
                    || request.getDiskMb() < 0) {
                obs.onNext(fail(
                        "hostname, endpoint, positive CPU/memory, and non-negative disk are required"));
                obs.onCompleted();
                return;
            }
            if (request.getLabelsMap().entrySet().stream().anyMatch(entry ->
                    entry.getKey().isBlank() || entry.getValue().isBlank())) {
                obs.onNext(fail("Worker label keys and values must not be blank"));
                obs.onCompleted();
                return;
            }

            try {
                new NodeResourceTally(0, request.getCpu(), request.getMem(),
                        request.getDiskMb(), request.getGpuDevicesList());
            } catch (IllegalArgumentException e) {
                obs.onNext(fail(e.getMessage()));
                obs.onCompleted();
                return;
            }

            String workerUuid = request.getWorkerUuid();
            if (workerUuid == null || workerUuid.isBlank()) {
                workerUuid = "node-" + (nextNodeId.get() + 1); // Legacy worker identity.
            } else {
                try {
                    UUID.fromString(workerUuid);
                } catch (IllegalArgumentException e) {
                    obs.onNext(fail("worker_uuid must be a valid UUID"));
                    obs.onCompleted();
                    return;
                }
            }
            final String registeredWorkerUuid = workerUuid;
            String incarnationId = request.getIncarnationId();
            if (incarnationId == null || incarnationId.isBlank()) {
                incarnationId = "legacy-" + System.currentTimeMillis();
            }

            synchronized (workerRegistrationLock) {
                NodeEntry existing = nodes.values().stream()
                        .filter(entry -> entry.workerUUID().equals(registeredWorkerUuid))
                        .findFirst().orElse(null);
                int nodeId = existing == null ? nextNodeId.incrementAndGet() : existing.nodeId();
                Node node=request.toBuilder()
                        .setId(nodeId)
                        .setState(NodeState.AVAILABLE)
                        .setWorkerUuid(workerUuid)
                        .setIncarnationId(incarnationId)
                        .build();
                try {
                    membershipManager.registerWorker(registeredWorkerUuid, incarnationId);
                    if (existing == null) {
                        workers.getOrConnect(nodeId, node.getAgentEndpoint());
                        nodes.put(nodeId, new NodeEntry(node,
                                new NodeResourceTally(nodeId, node.getCpu(), node.getMem(),
                                        node.getDiskMb(), node.getGpuDevicesList())));
                    } else {
                        workers.getOrConnect(nodeId, node.getAgentEndpoint());
                        existing.updateRegistration(node);
                    }
                    dispatchQueuedJobs();
                    obs.onNext(Cnf.newBuilder().setSuccess(true).setNodeId(nodeId).build());
                } catch (RuntimeException e) {
                    if (existing == null) {
                        workers.remove(nodeId);
                    }
                    obs.onNext(fail("Could not register worker: " + e.getMessage()));
                }
            }
            obs.onCompleted();
        }

        @Override
        public void heartBeat(Node request, StreamObserver<Cnf> obs) {
            NodeEntry entry=nodes.get(request.getId());
            if (entry==null) {
                obs.onNext(fail("Unknown node"));
            } else {
                if (request.getLabelsMap().entrySet().stream().anyMatch(label ->
                        label.getKey().isBlank() || label.getValue().isBlank())) {
                    obs.onNext(fail("Worker label keys and values must not be blank"));
                    obs.onCompleted();
                    return;
                }

                String uuid = request.getWorkerUuid();
                if (uuid == null || uuid.isBlank()) uuid = entry.workerUUID();
                if (!entry.workerUUID().equals(uuid)) {
                    obs.onNext(fail("Heartbeat worker UUID does not match registered node"));
                    obs.onCompleted();
                    return;
                }
                String inc = request.getIncarnationId();
                if (inc == null || inc.isBlank()) {
                    inc = entry.node().getIncarnationId();
                }
                long seq = request.getHeartbeatSequence();

                boolean accepted = membershipManager.processHeartbeat(uuid, inc, seq);
                if (!accepted) {
                    obs.onNext(fail("Stale or duplicate heartbeat"));
                    obs.onCompleted();
                    return;
                }

                boolean labelsChanged = entry.beat(request);
                obs.onNext(Cnf.newBuilder().setSuccess(true).build());
                if (labelsChanged) {
                    dispatchQueuedJobs();
                }
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

            boolean transitioned=entry.setState(j.getState());
            switch (j.getState()) {
                case COMPLETED, FAILED, CANCELLED, LOST -> {
                    entry.releaseResources();
                    dispatchQueuedJobs();
                    if (transitioned) {
                        jobManager.publishTerminalJobEvent(entry.job());
                    }
                }
                default -> { }
            }
            if (transitioned && (j.getState()==JobState.ALLOCATED
                    || j.getState()==JobState.RUNNING)) {
                jobManager.publishJobStateChanged(entry.job());
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
        failureDetector.start();
        server.start();
        restServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            restServer.close();
            server.shutdown();
            failureDetector.shutdown();
            dagManager.shutdown();
            jobManager.shutdown();
            workers.close();
        }));
        System.out.println("Controller gRPC started on port " + grpcPort);
        System.out.println("REST API started on " + restHost + ":" + restServer.port());
        server.awaitTermination();
    }
}