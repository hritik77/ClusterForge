package com.JobController.worker;

import com.JobController.failure.FakeTimeSource;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

public class WorkerMembershipManagerTest {
    private FakeTimeSource clock;
    private WorkerMembershipManager manager;

    @Before
    public void setup() {
        clock = new FakeTimeSource();
        manager = new WorkerMembershipManager(clock);
    }

    @Test
    public void testRegisterWorkerCreatesNewMembership() {
        clock.set(1000);
        WorkerMembership m = manager.registerWorker("worker-1", "inc-1");
        assertNotNull(m);
        assertEquals("worker-1", m.workerUUID());
        assertEquals(WorkerMembershipState.AVAILABLE, m.state());
        assertEquals("inc-1", m.currentSession().incarnationId());
        assertEquals(1000, m.registeredAtMillis());

        assertTrue(manager.isAlive("worker-1"));
        assertEquals(1, manager.getWorkers().size());
        assertEquals(1, manager.totalRegistrations());
    }

    @Test
    public void testIdempotentRegistration() {
        clock.set(1000);
        WorkerMembership m1 = manager.registerWorker("worker-1", "inc-1");

        clock.set(2000);
        WorkerMembership m2 = manager.registerWorker("worker-1", "inc-1");

        // Same object returned
        assertTrue(m1 == m2);
        assertEquals(1, manager.totalRegistrations());
        assertEquals(1, manager.totalReconnections());
        // Since it's the same incarnation, sequence doesn't reset but it's re-marked AVAILABLE.
        assertEquals(WorkerMembershipState.AVAILABLE, m2.state());
    }

    @Test
    public void testNewIncarnationRegistration() {
        clock.set(1000);
        WorkerMembership m1 = manager.registerWorker("worker-1", "inc-1");

        clock.set(2000);
        WorkerMembership m2 = manager.registerWorker("worker-1", "inc-2");

        assertTrue(m1 == m2);
        assertEquals("inc-2", m2.currentSession().incarnationId());
        assertEquals(1, manager.totalReconnections());
    }

    @Test
    public void duplicateRegistrationRefreshesLivenessWithoutResettingSequence() {
        clock.set(1000);
        WorkerMembership membership = manager.registerWorker("worker-1", "1");
        assertTrue(manager.processHeartbeat("worker-1", "1", 7));

        clock.set(9000);
        assertSame(membership, manager.registerWorker("worker-1", "1"));
        assertEquals(9000, membership.lastHeartbeatMillis());
        assertEquals(7, membership.currentSession().latestSequence());
    }

    @Test
    public void staleRegistrationCannotReplaceCurrentWorkerSession() {
        clock.set(1000);
        WorkerMembership membership = manager.registerWorker("worker-1", "2");
        assertTrue(manager.processHeartbeat("worker-1", "2", 10));

        clock.set(2000);
        assertThrows(IllegalArgumentException.class,
                () -> manager.registerWorker("worker-1", "1"));
        assertEquals("2", membership.currentSession().incarnationId());
        assertEquals(1000, membership.lastHeartbeatMillis());
    }

    @Test
    public void testMarkSuspectedAndDown() {
        clock.set(1000);
        WorkerMembership m = manager.registerWorker("worker-1", "inc-1");

        clock.set(5000);
        manager.markSuspected("worker-1", 5000);
        assertEquals(WorkerMembershipState.SUSPECTED, m.state());

        clock.set(10000);
        manager.markDown("worker-1", 10000);
        assertEquals(WorkerMembershipState.DOWN, m.state());
        assertFalse(manager.isAlive("worker-1"));
    }

    private static void assertSame(Object expected, Object actual) {
        org.junit.Assert.assertSame(expected, actual);
    }
}
