package com.JobController.failure;

import com.JobController.worker.WorkerMembership;
import com.JobController.worker.WorkerMembershipManager;
import com.JobController.worker.WorkerMembershipState;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.JobController.worker.WorkerFailureListener;
import java.util.concurrent.atomic.AtomicInteger;

public class FailureDetectorTest {
    private FakeTimeSource clock;
    private WorkerMembershipManager membershipManager;
    private FailureDetector detector;
    private WorkerMembership membership;

    @Before
    public void setup() {
        clock = new FakeTimeSource();
        membershipManager = new WorkerMembershipManager(clock);

        FailureDetectorConfig config = new FailureDetectorConfig(
                5000, 15000, 30000, 1000);
        detector = new FailureDetector(membershipManager, config, clock);

        clock.set(1000);
        membership = membershipManager.registerWorker("worker-1", "inc-1");
    }

    @Test
    public void testWorkerRemainsAvailableIfHeartbeatRecent() {
        clock.set(10000); // 9000ms elapsed, less than suspect timeout (15000)
        detector.sweep();
        assertEquals(WorkerMembershipState.AVAILABLE, membership.state());
        assertEquals(0, detector.workersSuspectedTotal());
    }

    @Test
    public void testWorkerBecomesSuspectedAfterTimeout() {
        clock.set(16000); // 15000ms elapsed, exactly at suspect timeout
        detector.sweep();

        assertEquals(WorkerMembershipState.SUSPECTED, membership.state());
        assertEquals(1, detector.workersSuspectedTotal());
        assertEquals(0, detector.workersDownTotal());
    }

    @Test
    public void testWorkerBecomesDownAfterFurtherTimeout() {
        // Suspect it first
        clock.set(16000);
        detector.sweep();
        assertEquals(WorkerMembershipState.SUSPECTED, membership.state());

        // Wait longer until down timeout (30000ms from last heartbeat)
        // Last heartbeat was at 1000. So at 31000 it should be DOWN.
        clock.set(31000);
        detector.sweep();
        assertEquals(WorkerMembershipState.DOWN, membership.state());
        assertEquals(1, detector.workersSuspectedTotal());
        assertEquals(1, detector.workersDownTotal());
        assertEquals(15_000, membership.lastSuspectLatencyMillis());
        assertEquals(30_000, membership.lastDownLatencyMillis());
        assertEquals(15_000, detector.suspectLatencyTotalMillis());
        assertEquals(30_000, detector.downLatencyTotalMillis());
    }

    @Test
    public void transitionsAreEmittedOnceAndHeartbeatBeforeSweepPreventsSuspicion() {
        AtomicInteger suspectedEvents = new AtomicInteger();
        AtomicInteger downEvents = new AtomicInteger();
        membershipManager.addListener(new WorkerFailureListener() {
            @Override
            public void onWorkerSuspected(String workerUUID) {
                suspectedEvents.incrementAndGet();
            }

            @Override
            public void onWorkerDown(String workerUUID) {
                downEvents.incrementAndGet();
            }
        });

        clock.set(16_000);
        assertTrue(membershipManager.processHeartbeat("worker-1", "inc-1", 1));
        detector.sweep();
        assertEquals(WorkerMembershipState.AVAILABLE, membership.state());
        assertEquals(0, suspectedEvents.get());

        clock.set(31_000);
        detector.sweep();
        detector.sweep();
        assertEquals(WorkerMembershipState.SUSPECTED, membership.state());
        assertEquals(1, suspectedEvents.get());

        clock.set(46_000);
        detector.sweep();
        detector.sweep();
        assertEquals(WorkerMembershipState.DOWN, membership.state());
        assertEquals(1, downEvents.get());
        assertEquals(1, detector.workersSuspectedTotal());
        assertEquals(1, detector.workersDownTotal());
    }
}
