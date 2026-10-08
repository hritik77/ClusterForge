package com.Worker;

import com.JobController.Cnf;
import com.JobController.Job;
import com.JobController.JobRef;
import com.JobController.JobState;
import org.junit.Test;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WorkerCancellationTest {
    @Test
    public void cancellationReturnsCancelledStateAndIsIdempotent() throws Exception {
        int jobId = 987_654;
        JobMap.ManagedJob allocated = new JobMap.ManagedJob(Job.newBuilder()
                .setId(jobId).setState(JobState.ALLOCATED).build());
        JobMap.jobs.put(jobId, allocated);
        try {
            Cnf first = cancel(jobId);
            Cnf second = cancel(jobId);

            assertTrue(first.getSuccess());
            assertEquals(JobState.CANCELLED, first.getState());
            assertTrue(second.getSuccess());
            assertEquals(JobState.CANCELLED, allocated.snapshot().getState());
        } finally {
            JobMap.jobs.remove(jobId, allocated);
        }
    }

    @Test
    public void cancellationDoesNotChangeCompletedWorkerJob() throws Exception {
        int jobId = 987_655;
        JobMap.ManagedJob completed = new JobMap.ManagedJob(Job.newBuilder()
                .setId(jobId).setState(JobState.COMPLETED).build());
        JobMap.jobs.put(jobId, completed);
        try {
            Cnf response = cancel(jobId);

            assertFalse(response.getSuccess());
            assertEquals(JobState.COMPLETED, response.getState());
            assertEquals(JobState.COMPLETED, completed.snapshot().getState());
        } finally {
            JobMap.jobs.remove(jobId, completed);
        }
    }

    private static Cnf cancel(int jobId) throws InterruptedException {
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Cnf> response = new AtomicReference<>();
        new Worker.ControllerToWorkerService().cancelAllocated(
                JobRef.newBuilder().setId(jobId).build(), new StreamObserver<>() {
                    @Override
                    public void onNext(Cnf value) {
                        response.set(value);
                    }

                    @Override
                    public void onError(Throwable error) {
                        throw new AssertionError(error);
                    }

                    @Override
                    public void onCompleted() {
                        completed.countDown();
                    }
                });
        assertTrue(completed.await(2, TimeUnit.SECONDS));
        return response.get();
    }
}
