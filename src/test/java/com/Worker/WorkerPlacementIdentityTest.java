package com.Worker;

import com.JobController.AllocationCommand;
import com.JobController.Cnf;
import com.JobController.Job;
import com.JobController.JobState;
import com.JobController.Node;
import com.JobController.NodeState;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WorkerPlacementIdentityTest {
    @Test
    public void workerAcceptsAllocationUsingStableUuidAndAdvertisedLabels() {
        Worker.nodeId = 73;
        Node registered = Node.newBuilder()
                .setId(Worker.nodeId)
                .setHostname(Worker.Hostname)
                .setState(NodeState.AVAILABLE)
                .setCpu(Worker.cpu)
                .setMem(Worker.mem)
                .putAllLabels(Worker.workerLabels())
                .build();
        Job job = Job.newBuilder()
                .setId(987_700)
                .setState(JobState.PENDING)
                .setCpuRequested(1)
                .setMemRequested(1)
                .setAssignedWorkerId(Worker.workerUuid)
                .putAllAssignedWorkerLabels(Worker.workerLabels())
                .build();

        Cnf accepted = allocate(job);
        assertTrue(accepted.getSuccess());
        JobMap.jobs.remove(job.getId());

        Job wrongIdentity = job.toBuilder().setId(987_701)
                .setAssignedWorkerId("node-" + registered.getId())
                .build();
        Cnf rejected = allocate(wrongIdentity);
        assertFalse(rejected.getSuccess());
        assertTrue(rejected.getMessage().contains("placement metadata"));
    }

    private static Cnf allocate(Job job) {
        AtomicReference<Cnf> response = new AtomicReference<>();
        new Worker.ControllerToWorkerService().allocate(
                AllocationCommand.newBuilder().setJob(job).build(),
                new StreamObserver<>() {
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
        return response.get();
    }
}
