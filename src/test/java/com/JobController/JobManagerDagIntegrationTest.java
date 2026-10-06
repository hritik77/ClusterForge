package com.JobController;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGRun;
import com.JobController.dag.DAGRunState;
import com.JobController.dag.DAGTask;
import com.JobController.dag.DAGManager;
import com.JobController.dag.RetryPolicy;
import com.JobController.dag.TaskState;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JobManagerDagIntegrationTest {
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
            await(() -> allocatedCount(run) == 1);

            assertEquals(TaskState.SUBMITTED, run.getTaskState("B"));
            assertEquals(TaskState.SUBMITTED, run.getTaskState("C"));
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

            Discover.markNodeDown(failedNode);

            Long retryJobId = run.getJobId("A");
            assertTrue(retryJobId != null && !retryJobId.equals(firstJobId));
            await(() -> jobState(retryJobId) == JobState.ALLOCATED);
            assertEquals(JobState.LOST, jobState(firstJobId));
            assertEquals(8, failedNode.tally().availableCpu());
            assertEquals(1, run.getRetryCount("A"));
            Discover.jobs.remove(Math.toIntExact(firstJobId));
        }
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

        private TestCluster(int... cpuCapacities) throws IOException {
            for (int index = 0; index < cpuCapacities.length; index++) {
                Server server = ServerBuilder.forPort(0)
                        .addService(new ControllerToWorkerGrpc.ControllerToWorkerImplBase() {
                            @Override
                            public void allocate(AllocationCommand request,
                                                 StreamObserver<Cnf> observer) {
                                observer.onNext(Cnf.newBuilder()
                                        .setSuccess(true)
                                        .setJobId(request.getJob().getId())
                                        .setMessage("Accepted for integration test")
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
                Node node = Node.newBuilder()
                        .setId(nodeId)
                        .setHostname("dag-test-" + index)
                        .setState(NodeState.AVAILABLE)
                        .setCpu(cpuCapacities[index])
                        .setMem(8)
                        .setAgentEndpoint(endpoint)
                        .build();
                Discover.nodes.put(nodeId, new NodeEntry(node,
                        new NodeResourceTally(nodeId, cpuCapacities[index], 8)));
            }
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
