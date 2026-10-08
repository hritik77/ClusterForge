package com.JobController.worker;

import com.JobController.failure.FakeTimeSource;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.concurrent.atomic.AtomicInteger;

public class HeartbeatProcessingTest {
    private FakeTimeSource clock;
    private WorkerMembershipManager manager;
    private WorkerMembership membership;

    @Before
    public void setup() {
        clock = new FakeTimeSource();
        manager = new WorkerMembershipManager(clock);
        clock.set(1000);
        membership = manager.registerWorker("worker-1", "inc-1");
    }

    @Test
    public void testValidHeartbeatAdvancesSequenceAndTimestamp() {
        clock.set(2000);
        boolean accepted = manager.processHeartbeat("worker-1", "inc-1", 1);
        assertTrue(accepted);
        assertEquals(2000, membership.lastHeartbeatMillis());
        assertEquals(1, membership.currentSession().latestSequence());

        clock.set(3000);
        accepted = manager.processHeartbeat("worker-1", "inc-1", 2);
        assertTrue(accepted);
        assertEquals(3000, membership.lastHeartbeatMillis());
        assertEquals(2, membership.currentSession().latestSequence());
    }

    @Test
    public void testDuplicateHeartbeatIgnored() {
        clock.set(2000);
        manager.processHeartbeat("worker-1", "inc-1", 1);

        clock.set(3000);
        boolean accepted = manager.processHeartbeat("worker-1", "inc-1", 1);
        assertFalse(accepted);
        // Timestamp should NOT advance on duplicate
        assertEquals(2000, membership.lastHeartbeatMillis());
        assertEquals(1, manager.heartbeatsDuplicate());
    }

    @Test
    public void testStaleHeartbeatIgnored() {
        clock.set(2000);
        manager.processHeartbeat("worker-1", "inc-1", 5);

        clock.set(3000);
        boolean accepted = manager.processHeartbeat("worker-1", "inc-1", 2); // Older sequence
        assertFalse(accepted);
        assertEquals(2000, membership.lastHeartbeatMillis());
        assertEquals(1, manager.heartbeatsStale());
    }

    @Test
    public void testHeartbeatFromNewIncarnationResetsSession() {
        clock.set(2000);
        manager.processHeartbeat("worker-1", "inc-1", 10);

        // Simulating restart with new incarnation id (lexicographically greater or numerically)
        clock.set(3000);
        boolean accepted = manager.processHeartbeat("worker-1", "inc-2", 1);
        assertTrue(accepted);
        assertEquals("inc-2", membership.currentSession().incarnationId());
        assertEquals(1, membership.currentSession().latestSequence());
        assertEquals(3000, membership.lastHeartbeatMillis());
    }

    @Test
    public void testHeartbeatRecoversSuspectedWorker() {
        manager.markSuspected("worker-1", 2000);
        assertEquals(WorkerMembershipState.SUSPECTED, membership.state());

        clock.set(3000);
        boolean accepted = manager.processHeartbeat("worker-1", "inc-1", 1);
        assertTrue(accepted);

        assertEquals(WorkerMembershipState.AVAILABLE, membership.state());
    }

    @Test
    public void newerIncarnationRecoversAndNotifiesExactlyOnce() {
        AtomicInteger recovered = new AtomicInteger();
        manager.addListener(new WorkerFailureListener() {
            @Override
            public void onWorkerDown(String workerUUID) {}

            @Override
            public void onWorkerRecovered(String workerUUID) {
                recovered.incrementAndGet();
            }
        });
        membership.markDown(2000);

        clock.set(3000);
        assertTrue(manager.processHeartbeat("worker-1", "inc-2", 1));
        assertEquals(WorkerMembershipState.AVAILABLE, membership.state());
        assertEquals(1, recovered.get());

        clock.set(4000);
        assertFalse(manager.processHeartbeat("worker-1", "inc-1", 100));
        assertEquals(WorkerMembershipState.AVAILABLE, membership.state());
        assertEquals(1, recovered.get());
    }
}
