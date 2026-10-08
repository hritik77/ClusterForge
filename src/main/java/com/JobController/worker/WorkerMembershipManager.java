package com.JobController.worker;

import com.JobController.failure.TimeSource;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Central registry for all worker membership records.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Creating and updating membership records on registration.</li>
 *   <li>Processing incoming heartbeats (including duplicate/stale/incarnation checks).</li>
 *   <li>Exposing membership state to the {@link com.JobController.failure.FailureDetector}
 *       and the scheduler.</li>
 * </ul>
 *
 * <p>This class is <strong>not</strong> responsible for job retries, DAG logic, or
 * directly triggering resource release — those belong to the {@code JobManager} and
 * {@code DAGManager}.
 *
 * <p>Thread-safe: all public methods may be called concurrently from heartbeat-RPC
 * threads, registration-RPC threads, and the failure-detector sweep thread.
 */
public final class WorkerMembershipManager {
    private static final Logger LOGGER =
            Logger.getLogger(WorkerMembershipManager.class.getName());


    private final ConcurrentHashMap<String, WorkerMembership> memberships =
            new ConcurrentHashMap<>();

    private final CopyOnWriteArrayList<WorkerFailureListener> globalListeners =
            new CopyOnWriteArrayList<>();

    private final TimeSource clock;

    // Metrics counters (volatile for visibility; approximate under contention is fine).
    private final LongAdder totalRegistrations = new LongAdder();
    private final LongAdder totalReconnections = new LongAdder();
    private final LongAdder heartbeatsAccepted = new LongAdder();
    private final LongAdder heartbeatsIgnored = new LongAdder();
    private final LongAdder heartbeatsStale = new LongAdder();
    private final LongAdder heartbeatsDuplicate = new LongAdder();
    private final LongAdder heartbeatsStaleIncarnation = new LongAdder();
    private final LongAdder workersRecovered = new LongAdder();

    public WorkerMembershipManager(TimeSource clock) {
        if (clock == null) throw new IllegalArgumentException("clock must not be null");
        this.clock = clock;
    }

    // -------------------------------------------------------------------------
    // Global listener management
    // -------------------------------------------------------------------------

    /**
     * Registers a {@link WorkerFailureListener} that will be notified for
     * <em>every</em> worker's state changes.
     */
    public void addListener(WorkerFailureListener listener) {
        if (listener == null) throw new IllegalArgumentException("listener must not be null");
        globalListeners.addIfAbsent(listener);
        memberships.values().forEach(membership -> membership.addListener(listener));
    }

    public void removeListener(WorkerFailureListener listener) {
        globalListeners.remove(listener);
        memberships.values().forEach(membership -> membership.removeListener(listener));
    }

    // -------------------------------------------------------------------------
    // Registration
    // -------------------------------------------------------------------------

    /**
     * Registers or re-registers a worker.
     *
     * <p>Registration is <em>idempotent</em> by {@code workerUUID}: calling this
     * multiple times for the same UUID updates the existing record rather than
     * creating duplicates.
     *
     * <p>The same incarnation refreshes liveness without resetting its heartbeat
     * sequence. A newer incarnation replaces the session; stale incarnations are
     * rejected.
     *
     * @param workerUUID    stable logical identity
     * @param incarnationId monotonically increasing process incarnation
     * @return the updated (or newly created) membership record
     */
    public WorkerMembership registerWorker(String workerUUID, String incarnationId) {
        if (workerUUID == null || workerUUID.isBlank()) {
            throw new IllegalArgumentException("workerUUID must not be blank");
        }
        if (incarnationId == null || incarnationId.isBlank()) {
            throw new IllegalArgumentException("incarnationId must not be blank");
        }

        long now = clock.currentTimeMillis();
        AtomicBoolean created = new AtomicBoolean();

        WorkerMembership membership = memberships.compute(workerUUID, (uuid, existing) -> {
            if (existing == null) {
                // Brand-new worker.
                WorkerMembership m = new WorkerMembership(
                        uuid, new WorkerSession(uuid, incarnationId, now), now);
                addGlobalListenersTo(m);
                totalRegistrations.increment();
                created.set(true);
                return m;
            }
            return existing;
        });

        WorkerMembershipState stateBefore = membership.state();
        if (!membership.registerSession(incarnationId, now)) {
            throw new IllegalArgumentException(
                    "Worker registration has a stale incarnation: " + incarnationId);
        }
        if (!created.get()) {
            totalReconnections.increment();
            if (stateBefore == WorkerMembershipState.SUSPECTED
                    || stateBefore == WorkerMembershipState.DOWN) {
                workersRecovered.increment();
            }
        }

        LOGGER.info(() -> "[Membership] Worker " + workerUUID
                + " registered: incarnation=" + incarnationId);
        return membership;
    }

    // -------------------------------------------------------------------------
    // Heartbeat processing
    // -------------------------------------------------------------------------

    /**
     * Processes an incoming heartbeat from a worker.
     *
     * <p>Stale, duplicate, and wrong-incarnation heartbeats are silently ignored.
     * If the worker is unknown (UUID not found) the heartbeat is rejected.
     *
     * @param workerUUID    logical identity
     * @param incarnationId incarnation of the sending process
     * @param sequence      monotonically increasing per-incarnation sequence number
     * @return {@code true} if the heartbeat was accepted and updated state
     */
    public boolean processHeartbeat(String workerUUID, String incarnationId, long sequence) {
        if (workerUUID == null || workerUUID.isBlank()
                || incarnationId == null || incarnationId.isBlank() || sequence < 0) {
            heartbeatsIgnored.increment();
            return false;
        }
        WorkerMembership membership = memberships.get(workerUUID);
        if (membership == null) {
            System.err.println("[Membership] Heartbeat from unknown worker: " + workerUUID);
            heartbeatsIgnored.increment();
            return false;
        }

        long now = clock.currentTimeMillis();
        WorkerMembershipState stateBefore = membership.state();
        WorkerMembership.HeartbeatResult result =
                membership.processHeartbeat(incarnationId, sequence, now);
        if ((stateBefore == WorkerMembershipState.SUSPECTED
                || stateBefore == WorkerMembershipState.DOWN)
                && (result == WorkerMembership.HeartbeatResult.ACCEPTED
                || result == WorkerMembership.HeartbeatResult.NEW_INCARNATION)) {
            workersRecovered.increment();
        }

        switch (result) {
            case ACCEPTED -> {
                heartbeatsAccepted.increment();
                LOGGER.fine(() -> "[Membership] Worker " + workerUUID
                        + " heartbeat accepted: incarnation=" + incarnationId
                        + " sequence=" + sequence);
            }
            case DUPLICATE -> {
                heartbeatsDuplicate.increment();
                LOGGER.fine(() -> "[Membership] Worker " + workerUUID
                        + " duplicate heartbeat ignored: seq=" + sequence);
            }
            case STALE -> {
                heartbeatsStale.increment();
                System.err.printf(
                        "[Membership] Ignoring stale heartbeat: worker=%s incarnation=%s seq=%d%n",
                        workerUUID, incarnationId, sequence);
            }
            case STALE_INCARNATION -> {
                heartbeatsStaleIncarnation.increment();
                System.err.printf(
                        "[Membership] Ignoring stale heartbeat: worker=%s incarnation=%s seq=%d"
                                + " currentIncarnation=%s%n",
                        workerUUID, incarnationId, sequence,
                        membership.currentSession().incarnationId());
            }
            case NEW_INCARNATION -> {
                heartbeatsAccepted.increment();
                totalReconnections.increment();
                System.out.printf(
                        "[Membership] Worker %s new incarnation accepted: incarnation=%s%n",
                        workerUUID, incarnationId);
            }
        }

        return result == WorkerMembership.HeartbeatResult.ACCEPTED
                || result == WorkerMembership.HeartbeatResult.NEW_INCARNATION;
    }

    // -------------------------------------------------------------------------
    // Direct state manipulation (for FailureDetector)
    // -------------------------------------------------------------------------

    /**
     * Moves the worker to SUSPECTED if it is currently AVAILABLE.
     * Idempotent if already SUSPECTED or DOWN.
     */
    public void markSuspected(String workerUUID, long nowMillis) {
        WorkerMembership m = memberships.get(workerUUID);
        if (m != null) {
            boolean changed = m.markSuspected(nowMillis);
            if (changed) {
                long age = nowMillis - m.lastHeartbeatMillis();
                System.out.printf(
                        "[Membership] Worker %s suspected: lastHeartbeatAge=%dms%n",
                        workerUUID, age);
            }
        }
    }

    /**
     * Moves the worker to DOWN.
     * Idempotent if already DOWN.
     */
    public void markDown(String workerUUID, long nowMillis) {
        WorkerMembership m = memberships.get(workerUUID);
        if (m != null) {
            boolean changed = m.markDown(nowMillis);
            if (changed) {
                long age = nowMillis - m.lastHeartbeatMillis();
                System.out.printf(
                        "[Membership] Worker %s marked DOWN: lastHeartbeatAge=%dms%n",
                        workerUUID, age);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Queries
    // -------------------------------------------------------------------------

    /** Returns the membership record for the given UUID, or {@code null} if unknown. */
    public WorkerMembership getWorker(String workerUUID) {
        return memberships.get(workerUUID);
    }

    /** Returns an immutable snapshot of all membership records. */
    public Collection<WorkerMembership> getWorkers() {
        return List.copyOf(memberships.values());
    }

    /** Returns {@code true} if the worker is known and not DOWN or MAINTENANCE. */
    public boolean isAlive(String workerUUID) {
        WorkerMembership m = memberships.get(workerUUID);
        return m != null && m.isAlive();
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    public long totalRegistrations()         { return totalRegistrations.sum(); }
    public long totalReconnections()         { return totalReconnections.sum(); }
    public long heartbeatsAccepted()         { return heartbeatsAccepted.sum(); }
    public long heartbeatsIgnored()          { return heartbeatsIgnored.sum(); }
    public long heartbeatsStale()            { return heartbeatsStale.sum(); }
    public long heartbeatsDuplicate()        { return heartbeatsDuplicate.sum(); }
    public long heartbeatsStaleIncarnation() { return heartbeatsStaleIncarnation.sum(); }
    public long workersRecovered()           { return workersRecovered.sum(); }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void addGlobalListenersTo(WorkerMembership m) {
        for (WorkerFailureListener l : globalListeners) {
            m.addListener(l);
        }
    }
}
