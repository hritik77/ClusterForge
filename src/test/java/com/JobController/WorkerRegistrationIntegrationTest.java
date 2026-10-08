package com.JobController;

import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WorkerRegistrationIntegrationTest {
    @Test
    public void repeatedRegistrationReusesOneNodeAndUpdatesItsSession() {
        String workerUuid = UUID.randomUUID().toString();
        Discover.NodeManager manager = new Discover.NodeManager();
        int initialNodeCount = Discover.nodes.size();
        int firstNodeId = register(manager, workerUuid, "10");
        int secondNodeId = register(manager, workerUuid, "11");

        assertEquals(firstNodeId, secondNodeId);
        assertEquals(initialNodeCount + 1, Discover.nodes.size());
        assertEquals(1, Discover.nodes.values().stream()
                .filter(node -> node.workerUUID().equals(workerUuid)).count());
        assertEquals("11", Discover.membershipManager.getWorker(workerUuid)
                .currentSession().incarnationId());

        Discover.nodes.remove(firstNodeId);
        Discover.workers.remove(firstNodeId);
    }

    private static int register(Discover.NodeManager manager, String uuid, String incarnation) {
        Node node = Node.newBuilder()
                .setHostname("registration-test")
                .setAgentEndpoint("127.0.0.1:1")
                .setState(NodeState.AVAILABLE)
                .setCpu(2)
                .setMem(4)
                .setWorkerUuid(uuid)
                .setIncarnationId(incarnation)
                .build();
        AtomicReference<Cnf> response = new AtomicReference<>();
        manager.registerNode(node, new StreamObserver<>() {
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
}
