package com.JobController;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGRun;
import com.JobController.dag.DAGRunState;
import com.JobController.dag.DAGTask;
import com.JobController.dag.DAGManager;
import com.JobController.dag.RetryPolicy;
import com.JobController.dag.TimeoutPolicy;
import com.JobController.dag.TaskState;
import com.JobController.dag.TaskAttemptSnapshot;
import com.JobController.dag.AttemptState;
import com.JobController.scheduling.PlacementConstraint;
import com.JobController.scheduling.PlacementOperator;
import com.JobController.scheduling.PlacementRequirements;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JobManagerDagIntegrationTest {
    @Test
    public void pendingTaskWaitsForMatchingWorkerLabelAndRecordsWorkerAttempt()
            throws Exception {
        String dagId = uniqueId("placement-label");
        try (TestCluster cluster = new TestCluster(
                0, List.of(), Map.of("region", "east"), 8)) {
            DAG dag = new DAG(dagId, "region placement");
            PlacementRequirements requirements = new PlacementRequirements(List.of(
                    new PlacementConstraint("region", PlacementOperator.EQUALS, "west")));
            dag.addTask(new DAGTask(
                    "task", 2, 4, 0, 0, 0, Set.of(), "run",
                    new RetryPolicy(0), new TimeoutPolicy(null), requirements));
            Discover.dagManager.registerDAG(dag);

            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);
            Long jobId = run.getJobId("task");
            await(() -> jobState(jobId) == JobState.PENDING);
            assertNull(Discover.jobs.get(Math.toIntExact(jobId)).node());

            NodeEntry node = Discover.nodes.get(TestCluster.NODE_ID_BASE);
            Node changedLabels = node.node().toBuilder()
                    .putLabels("region", "west")
                    .setHeartbeatSequence(1)
                    .build();
            new Discover.NodeManager().heartBeat(changedLabels, new StreamObserver<>() {
                @Override
                public void onNext(Cnf value) {
                    assertTrue(value.getSuccess());
                }

                @Override
                public void onError(Throwable error) {
                    throw new AssertionError(error);
                }

                @Override
                public void onCompleted() {}
            });
            await(() -> jobState(jobId) == JobState.ALLOCATED);

            Job allocatedJob = Discover.jobs.get(Math.toIntExact(jobId)).job();
            assertEquals(Discover.nodes.get(TestCluster.NODE_ID_BASE).workerUUID(),
                    allocatedJob.getAssignedWorkerId());
            assertEquals("west", allocatedJob.getAssignedWorkerLabelsMap().get("region"));
            TaskAttemptSnapshot attempt = run.getCurrentAttempt("task");
            assertEquals(AttemptState.ALLOCATED, attempt.getState());
            assertEquals(allocatedJob.getAssignedWorkerId(), attempt.getWorkerId());
            assertEquals("west", attempt.getWorkerLabels().get("region"));

            complete(jobId);
            assertEquals(AttemptState.ALLOCATED, attempt.getState());
            TaskAttemptSnapshot completedAttempt = run.getCurrentAttempt("task");
            assertEquals(AttemptState.COMPLETED, completedAttempt.getState());
            assertTrue(completedAttempt.getFinishedAtMillis() > 0);
        }
    }

    @Test
    public void recoveredWorkerDoesNotResurrectItsLostAttempt() throws Exception {
        String dagId = uniqueId("recovered-worker");
        try (TestCluster cluster = new TestCluster(8, 8)) {
            DAG dag = new DAG(dagId, "recovered worker");
            dag.addTask(new DAGTask("A", 4, 1, Set.of(), new RetryPolicy(1)));
            Discover.dagManager.registerDAG(dag);
            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);

            Long firstJobId = run.getJobId("A");
            await(() -> jobState(firstJobId) == JobState.ALLOCATED);
            NodeEntry failedWorker = Discover.nodes.get(TestCluster.NODE_ID_BASE);
            Discover.membershipManager.markDown(
                    failedWorker.workerUUID(), System.currentTimeMillis());

            Long retryJobId = run.getJobId("A");
            await(() -> retryJobId != null && jobState(retryJobId) == JobState.ALLOCATED);
            assertEquals(JobState.LOST, jobState(firstJobId));
            assertEquals(AttemptState.LOST, run.getAttempts("A").get(0).getState());

            Node registration = failedWorker.node().toBuilder()
                    .setIncarnationId("2")
                    .build();
            registerWorker(registration);

            assertEquals(com.JobController.worker.WorkerMembershipState.AVAILABLE,
                    Discover.membershipManager.getWorker(failedWorker.workerUUID()).state());
            assertEquals(JobState.LOST, jobState(firstJobId));
            assertEquals(retryJobId, run.getJobId("A"));
            assertEquals(2, run.getAttempts("A").size());
            assertEquals(AttemptState.ALLOCATED, run.getCurrentAttempt("A").getState());
            assertEquals(1, Discover.nodes.values().stream()
                    .filter(node -> node.workerUUID().equals(failedWorker.workerUUID())).count());
        }
    }

    @Test
    public void schedulesGpuTaskOnMatchingDeviceAndReleasesAllResourceReservations()
            throws Exception {
        String dagId = uniqueId("gpu-task");
        List<GPUDevice> gpus = List.of(
                GPUDevice.newBuilder().setId("gpu-small").setMemoryMb(4_096).build(),
                GPUDevice.newBuilder().setId("gpu-large").setMemoryMb(16_384).build());
        try (TestCluster cluster = new TestCluster(1_024, gpus, 8)) {
            DAG dag = new DAG(dagId, "gpu training");
            dag.addTask(new DAGTask(
                    "train", 2, 4, 512, 1, 8_192, Set.of(), "python train.py"));
            Discover.dagManager.registerDAG(dag);

            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);
            Long jobId = run.getJobId("train");
            await(() -> jobState(jobId) == JobState.ALLOCATED);
            NodeEntry node = Discover.nodes.get(TestCluster.NODE_ID_BASE);

            assertEquals(List.of("gpu-large"), cluster.assignedGpus(jobId));
            assertEquals(512, node.tally().availableDiskMb());
            assertEquals(1, node.tally().availableGpuCount());
            assertEquals(512, Discover.jobs.get(Math.toIntExact(jobId))
                    .job().getDiskMbRequested());
            assertEquals(8_192, Discover.jobs.get(Math.toIntExact(jobId))
                    .job().getGpuMemoryMbPerGpu());

            complete(jobId);
            assertEquals(1_024, node.tally().availableDiskMb());
            assertEquals(2, node.tally().availableGpuCount());
            assertEquals(DAGRunState.COMPLETED, run.getState());
        }
    }

    @Test
    public void jobStatusCompletionUnlocksChainThroughExistingDispatcher() throws Exception {
        String dagId = uniqueId("chain");
        try (TestCluster cluster = new TestCluster(8)) {
            DAG dag = new DAG(dagId, "chain");
            dag.addTask(task("A", 2, Set.of()));
            dag.addTask(task("B", 2, Set.of("A")));
            dag.addTask(task("C", 2, Set.of("B")));
            Discover.dagManager.registerDAG(dag);

            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);
            await(() -> jobState(run.getJobId("A")) == JobState.ALLOCATED);
            complete(run.getJobId("A"));
            await(() -> jobState(run.getJobId("B")) == JobState.ALLOCATED);
            complete(run.getJobId("B"));
            await(() -> jobState(run.getJobId("C")) == JobState.ALLOCATED);
            complete(run.getJobId("C"));

            assertEquals(DAGRunState.COMPLETED, run.getState());
            assertEquals(TaskState.COMPLETED, run.getTaskState("A"));
            assertEquals(TaskState.COMPLETED, run.getTaskState("B"));
            assertEquals(TaskState.COMPLETED, run.getTaskState("C"));
        }
    }

    @Test
    public void existingSchedulerKeepsSecondRootPendingWhenOneWorkerLacksCapacity()
            throws Exception {
        String dagId = uniqueId("single-worker");
        try (TestCluster cluster = new TestCluster(8)) {
            DAG dag = new DAG(dagId, "parallel roots");
            dag.addTask(task("B", 8, Set.of()));
            dag.addTask(task("C", 8, Set.of()));
            Discover.dagManager.registerDAG(dag);

            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);
            await(() -> allocatedCount(run) == 1 && pendingCount(run) == 1);

            assertTrue(Set.of(TaskState.SUBMITTED, TaskState.ALLOCATED)
                    .contains(run.getTaskState("B")));
            assertTrue(Set.of(TaskState.SUBMITTED, TaskState.ALLOCATED)
                    .contains(run.getTaskState("C")));
            assertTrue(run.getTaskState("B") != run.getTaskState("C"));
            assertEquals(1, allocatedCount(run));
            assertEquals(1, pendingCount(run));
        }
    }

    @Test
    public void existingSchedulerAllocatesParallelRootsAcrossWorkers() throws Exception {
        String dagId = uniqueId("two-workers");
        try (TestCluster cluster = new TestCluster(8, 8)) {
            DAG dag = new DAG(dagId, "parallel roots");
            dag.addTask(task("B", 8, Set.of()));
            dag.addTask(task("C", 8, Set.of()));
            Discover.dagManager.registerDAG(dag);

            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);
            await(() -> allocatedCount(run) == 2);

            assertEquals(2, allocatedCount(run));
            assertEquals(0, pendingCount(run));
        }
    }

    @Test
    public void lostWorkerReleasesReservationBeforeRetryIsAllocated() throws Exception {
        String dagId = uniqueId("lost-worker");
        try (TestCluster cluster = new TestCluster(8, 8)) {
            DAG dag = new DAG(dagId, "worker recovery");
            dag.addTask(new DAGTask("A", 4, 1, Set.of(), new RetryPolicy(1)));
            Discover.dagManager.registerDAG(dag);

            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);
            Long firstJobId = run.getJobId("A");
            await(() -> jobState(firstJobId) == JobState.ALLOCATED);
            NodeEntry failedNode = Discover.nodes.get(TestCluster.NODE_ID_BASE);

            Discover.membershipManager.markDown(failedNode.workerUUID(), System.currentTimeMillis());

            Long retryJobId = run.getJobId("A");
            assertTrue(retryJobId != null && !retryJobId.equals(firstJobId));
            await(() -> jobState(retryJobId) == JobState.ALLOCATED);
            assertEquals(JobState.LOST, jobState(firstJobId));
            assertEquals(8, failedNode.tally().availableCpu());
            assertEquals(1, run.getRetryCount("A"));
            Discover.jobs.remove(Math.toIntExact(firstJobId));
        }
    }

    @Test
    public void suspectedWorkerKeepsExistingJobsAndValidHeartbeatRecoversIt() throws Exception {
        String dagId = uniqueId("suspected-worker");
        try (TestCluster cluster = new TestCluster(8)) {
            DAG dag = new DAG(dagId, "suspected worker");
            dag.addTask(new DAGTask("A", 4, 1, Set.of(), new RetryPolicy(1)));
            Discover.dagManager.registerDAG(dag);
            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);

            Long jobId = run.getJobId("A");
            await(() -> jobState(jobId) == JobState.ALLOCATED);
            NodeEntry worker = Discover.nodes.get(TestCluster.NODE_ID_BASE);
            Discover.membershipManager.markSuspected(
                    worker.workerUUID(), System.currentTimeMillis());

            assertEquals(JobState.ALLOCATED, jobState(jobId));
            assertEquals(0, run.getRetryCount("A"));
            assertEquals(com.JobController.worker.WorkerMembershipState.SUSPECTED,
                    Discover.membershipManager.getWorker(worker.workerUUID()).state());

            Node heartbeat = worker.node().toBuilder().setHeartbeatSequence(1).build();
            new Discover.NodeManager().heartBeat(heartbeat, new StreamObserver<>() {
                @Override
                public void onNext(Cnf value) {
                    assertTrue(value.getSuccess());
                }

                @Override
                public void onError(Throwable error) {
                    throw new AssertionError(error);
                }

                @Override
                public void onCompleted() {}
            });

            assertEquals(com.JobController.worker.WorkerMembershipState.AVAILABLE,
                    Discover.membershipManager.getWorker(worker.workerUUID()).state());
            assertEquals(JobState.ALLOCATED, jobState(jobId));
            assertEquals(0, run.getRetryCount("A"));
        }
    }

    @Test
    public void taskTimeoutCancelsFirstAttemptAndRetriesThroughJobManager() throws Exception {
        String dagId = uniqueId("timeout-retry");
        try (TestCluster cluster = new TestCluster(8)) {
            DAG dag = new DAG(dagId, "timeout retry");
            dag.addTask(new DAGTask("A", 4, 1, Set.of(), "run",
                    new RetryPolicy(1), new TimeoutPolicy(80L)));
            Discover.dagManager.registerDAG(dag);

            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);
            Long firstJobId = run.getJobId("A");
            await(() -> jobState(firstJobId) == JobState.ALLOCATED);
            reportState(firstJobId, JobState.RUNNING);

            await(() -> run.getRetryCount("A") == 1
                    && !firstJobId.equals(run.getJobId("A")));
            Long retryJobId = run.getJobId("A");
            await(() -> jobState(retryJobId) == JobState.ALLOCATED);
            assertEquals(JobState.CANCELLED, jobState(firstJobId));
            assertEquals(4, Discover.nodes.get(TestCluster.NODE_ID_BASE).tally().availableCpu());

            complete(retryJobId);
            assertEquals(DAGRunState.COMPLETED, run.getState());
            assertEquals(TaskState.COMPLETED, run.getTaskState("A"));
            assertEquals(1, run.getRetryCount("A"));
            assertEquals(8, Discover.nodes.get(TestCluster.NODE_ID_BASE).tally().availableCpu());
            Discover.jobs.remove(Math.toIntExact(firstJobId));
        }
    }

    @Test
    public void dagCancellationCancelsQueuedAndAllocatedJobsExactlyOnce() throws Exception {
        String dagId = uniqueId("cancel");
        try (TestCluster cluster = new TestCluster(8)) {
            DAG dag = new DAG(dagId, "cancel running tasks");
            dag.addTask(task("A", 8, Set.of()));
            dag.addTask(task("B", 8, Set.of()));
            Discover.dagManager.registerDAG(dag);

            DAGRun run = Discover.dagManager.startRun(dagId);
            cluster.trackRun(run);
            await(() -> allocatedCount(run) == 1 && pendingCount(run) == 1);
            NodeEntry node = Discover.nodes.get(TestCluster.NODE_ID_BASE);

            assertTrue(Discover.dagManager.cancelRun(run.getRunId()));
            assertEquals(DAGRunState.CANCELLED, run.getState());
            assertEquals(TaskState.CANCELLED, run.getTaskState("A"));
            assertEquals(TaskState.CANCELLED, run.getTaskState("B"));
            assertEquals(JobState.CANCELLED, jobState(run.getJobId("A")));
            assertEquals(JobState.CANCELLED, jobState(run.getJobId("B")));
            assertEquals(8, node.tally().availableCpu());
            assertEquals(8, node.tally().availableMem());
            assertTrue(Discover.dagManager.cancelRun(run.getRunId()));
            assertEquals(8, node.tally().availableCpu());
            assertEquals(8, node.tally().availableMem());
        }
    }

    private static void reportState(Long jobId, JobState state) {
        Job current = Discover.jobs.get(Math.toIntExact(jobId)).job();
        Discover.NodeManager nodeManager = new Discover.NodeManager();
        nodeManager.reportJobStatus(current.toBuilder().setState(state).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(Cnf value) {
                        assertTrue(value.getSuccess());
                    }

                    @Override
                    public void onError(Throwable error) {
                        throw new AssertionError(error);
                    }

                    @Override
                    public void onCompleted() {}
                });
    }

    private static int registerWorker(Node registration) {
        AtomicReference<Cnf> response = new AtomicReference<>();
        new Discover.NodeManager().registerNode(registration, new StreamObserver<>() {
            @Override
            public void onNext(Cnf value) {
                response.set(value);
            }

            @Override
            public void onError(Throwable error) {
                throw new AssertionError(error);
            }

            @Override
            public void onCompleted() {}
        });
        assertTrue(response.get().getSuccess());
        return response.get().getNodeId();
    }

    private static void complete(Long jobId) {
        Job current = Discover.jobs.get(Math.toIntExact(jobId)).job();
        Discover.NodeManager nodeManager = new Discover.NodeManager();
        nodeManager.reportJobStatus(current.toBuilder().setState(JobState.COMPLETED).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(Cnf value) {
                        assertTrue(value.getSuccess());
                    }

                    @Override
                    public void onError(Throwable error) {
                        throw new AssertionError(error);
                    }

                    @Override
                    public void onCompleted() {}
                });
    }

    private static JobState jobState(Long jobId) {
        if (jobId == null) {
            return null;
        }
        JobEntry entry = Discover.jobs.get(Math.toIntExact(jobId));
        return entry == null ? null : entry.job().getState();
    }

    private static int allocatedCount(DAGRun run) {
        return (int) run.getJobIds().values().stream()
                .filter(id -> jobState(id) == JobState.ALLOCATED)
                .count();
    }

    private static int pendingCount(DAGRun run) {
        return (int) run.getJobIds().values().stream()
                .filter(id -> jobState(id) == JobState.PENDING)
                .count();
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue("Timed out waiting for job dispatcher", condition.getAsBoolean());
    }

    private static DAGTask task(String id, int cpu, Set<String> dependencies) {
        return new DAGTask(id, cpu, 1, dependencies);
    }

    private static String uniqueId(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    private static final class TestCluster implements AutoCloseable {
        static final int NODE_ID_BASE = 700_000;

        private final List<Integer> nodeIds = new ArrayList<>();
        private final List<Server> servers = new ArrayList<>();
        private final List<DAGRun> runs = new ArrayList<>();
        private final Map<Integer, List<String>> assignedGpuIds = new ConcurrentHashMap<>();
        private final Map<Integer, String> assignedWorkerIds = new ConcurrentHashMap<>();

        private TestCluster(int... cpuCapacities) throws IOException {
            this(0, List.of(), Map.of(), cpuCapacities);
        }

        private TestCluster(long diskMb, List<GPUDevice> gpuDevices, int... cpuCapacities)
                throws IOException {
            this(diskMb, gpuDevices, Map.of(), cpuCapacities);
        }

        private TestCluster(
                long diskMb, List<GPUDevice> gpuDevices, Map<String, String> labels,
                int... cpuCapacities) throws IOException {
            for (int index = 0; index < cpuCapacities.length; index++) {
                Server server = ServerBuilder.forPort(0)
                        .addService(new ControllerToWorkerGrpc.ControllerToWorkerImplBase() {
                            @Override
                            public void allocate(AllocationCommand request,
                                                 StreamObserver<Cnf> observer) {
                                assignedGpuIds.put(
                                        request.getJob().getId(), request.getGpuDeviceIdsList());
                                assignedWorkerIds.put(request.getJob().getId(),
                                        request.getJob().getAssignedWorkerId());
                                observer.onNext(Cnf.newBuilder()
                                        .setSuccess(true)
                                        .setJobId(request.getJob().getId())
                                        .setMessage("Accepted for integration test")
                                        .build());
                                observer.onCompleted();
                            }

                            @Override
                            public void cancelAllocated(JobRef request,
                                                        StreamObserver<Cnf> observer) {
                                observer.onNext(Cnf.newBuilder()
                                        .setSuccess(true)
                                        .setJobId(request.getId())
                                        .setState(JobState.CANCELLED)
                                        .setMessage("Cancelled for integration test")
                                        .build());
                                observer.onCompleted();
                            }
                        })
                        .build()
                        .start();
                servers.add(server);

                int nodeId = NODE_ID_BASE + index;
                nodeIds.add(nodeId);
                String endpoint = "127.0.0.1:" + server.getPort();
                String uuid = UUID.randomUUID().toString();
                String inc = "1";
                Node node = Node.newBuilder()
                        .setId(nodeId)
                        .setHostname("dag-test-" + index)
                        .setState(NodeState.AVAILABLE)
                        .setCpu(cpuCapacities[index])
                        .setMem(8)
                        .setDiskMb(diskMb)
                        .addAllGpuDevices(gpuDevices)
                        .putAllLabels(labels)
                        .putLabels("hostname", "dag-test-" + index)
                        .setAgentEndpoint(endpoint)
                        .setWorkerUuid(uuid)
                        .setIncarnationId(inc)
                        .build();
                NodeEntry entry = new NodeEntry(node,
                        new NodeResourceTally(nodeId, cpuCapacities[index], 8, diskMb, gpuDevices));
                Discover.nodes.put(nodeId, entry);
                Discover.membershipManager.registerWorker(uuid, inc);
            }
        }

        private List<String> assignedGpus(Long jobId) {
            return assignedGpuIds.get(Math.toIntExact(jobId));
        }

        private String assignedWorkerId(Long jobId) {
            return assignedWorkerIds.get(Math.toIntExact(jobId));
        }

        private void trackRun(DAGRun run) {
            runs.add(run);
        }

        @Override
        public void close() {
            for (Server server : servers) {
                server.shutdownNow();
            }
            for (int nodeId : nodeIds) {
                Discover.workers.remove(nodeId);
                Discover.nodes.remove(nodeId);
            }
            for (DAGRun run : runs) {
                run.getJobIds().values().forEach(jobId -> {
                    Discover.jobManager.removeQueuedJob(Math.toIntExact(jobId));
                    Discover.jobs.remove(Math.toIntExact(jobId));
                });
            }
        }
    }
}
