package com.java.JobController;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.ConcurrentHashMap;

public final class WorkerClientRegistry implements AutoCloseable {

    private final ConcurrentHashMap<Integer, WorkerClient> clients =
            new ConcurrentHashMap<>();

    public WorkerClient getOrConnect(int nodeId, String endpoint) {
        return clients.compute(nodeId, (id, existing) -> {

            if (existing != null && existing.endpoint().equals(endpoint)) {
                return existing;
            }

            if (existing != null) {
                existing.channel().shutdown();
            }

            ManagedChannel channel = ManagedChannelBuilder
                    .forTarget(endpoint)
                    .usePlaintext()
                    .build();

            return new WorkerClient(
                    endpoint,
                    channel,
                    ControllerToWorkerGrpc.newBlockingStub(channel)
            );
        });
    }

    public void remove(int nodeId) {
        WorkerClient client = clients.remove(nodeId);

        if (client != null) {
            client.channel().shutdown();
        }
    }

    @Override
    public void close() {
        clients.values()
                .forEach(client -> client.channel().shutdown());

        clients.clear();
    }

    public record WorkerClient(
            String endpoint,
            ManagedChannel channel,
            ControllerToWorkerGrpc.ControllerToWorkerBlockingStub stub
    ) {}
}