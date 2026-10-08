package com.JobController.worker;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Optional;

/**
 * Health/membership record for one logical worker (identified by {@code workerUUID}).
 *
 * <p>A single {@code WorkerMembership} object is created when a worker first registers
 * and is then reused across reconnections. State transitions are protected by
 * {@code synchronized} blocks on {@code this}; listeners are invoked <em>after</em>
 * releasing the lock to prevent re-entrant deadlocks.
 *
 * <h3>State machine</h3>
 * <pre>
 *   JOINING → AVAILABLE ←── heartbeat ──← SUSPECTED
 *                                               │
 *                                         further timeout
 *                                               │
 *                                              DOWN → re-register → AVAILABLE
 * </pre>
 */
public final class WorkerMembership {

    // -------------------------------------------------------------------------
    // Identity
    // -------------------------------------------------------------------------

    /** Persistent logical identity. Does not change across restarts. */
    private final String workerUUID;

    // -------------------------------------------------------------------------
    // Mutable state (guarded by synchronized(this))
    // -------------------------------------------------------------------------

    private WorkerMembershipState state;

    /** Current process incarnation (changes on worker restart). */
    private volatile WorkerSession currentSession;

    /** Wall-clock time when this membership record was first created. */
    private final long registeredAtMillis;

    /** Wall-clock time of the most recent state change (ms). */
    private volatile long lastStateChangeMillis;

    /** Optional: wall-clock time the worker was first suspected (ms, 0 = never). */
    private volatile long suspectedAtMillis;

    /** Optional: wall-clock time the worker was declared DOWN (ms, 0 = never). */
    private volatile long downAtMillis;

    private volatile long lastSuspectLatencyMillis;
    private volatile long lastDownLatencyMillis;

    /** Count of missed heartbeats (informational / metrics only). */
    private volatile int missedHeartbeatCount;

    /** Listeners to notify on state changes (notified outside the lock). */
    private final CopyOnWriteArrayList<WorkerFailureListener> listeners =
            new CopyOnWriteArrayList<>();

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public WorkerMembership(String workerUUID, WorkerSession initialSession, long nowMillis) {
        if (workerUUID == null || workerUUID.isBlank()) {
            throw new IllegalArgumentException("workerUUID must not be blank");
        }
        this.workerUUID           = workerUUID;
        this.currentSession       = initialSession;
        this.state                = WorkerMembershipState.JOINING;
        this.registeredAtMillis   = nowMillis;
        this.lastStateChangeMillis = nowMillis;
    }

    // -------------------------------------------------------------------------
    // Listener management
    // -------------------------------------------------------------------------

    public void addListener(WorkerFailureListener listener) {
        listeners.addIfAbsent(listener);
    }

    public void removeListener(WorkerFailureListener listener) {
        listeners.remove(listener);
    }

    // -------------------------------------------------------------------------
    // State transitions (synchronized for atomicity; listeners called after unlock)
    // -------------------------------------------------------------------------

    /**
     * Transitions the worker to {@link WorkerMembershipState#AVAILABLE}.
     *
     * <p>Accepted from any state. Returns {@code true} if the state actually changed
     * (i.e. was not already AVAILABLE).
     */
    public boolean markAvailable(long nowMillis) {
        WorkerMembershipState previous;
        synchronized (this) {
            previous = this.state;
            if (previous == WorkerMembershipState.AVAILABLE) {
                return false;
            }
            this.state = WorkerMembershipState.AVAILABLE;
            this.lastStateChangeMillis = nowMillis;
            this.missedHeartbeatCount = 0;
        }
        // Notify outside the lock.
        if (previous == WorkerMembershipState.SUSPECTED
                || previous == WorkerMembershipState.DOWN) {
            fireRecovered();
        }
        return true;
    }

    /**
     * Transitions the worker to {@link WorkerMembershipState#SUSPECTED}.
     *
     * <p>Only accepted from {@link WorkerMembershipState#AVAILABLE}; ignored otherwise
     * to prevent moving backwards (e.g. DOWN → SUSPECTED).
     * Returns {@code true} if the transition actually happened.
     */
    public boolean markSuspected(long nowMillis) {
        synchronized (this) {
            if (this.state != WorkerMembershipState.AVAILABLE) {
                return false;
            }
            this.state = WorkerMembershipState.SUSPECTED;
            this.lastStateChangeMillis = nowMillis;
            this.suspectedAtMillis = nowMillis;
            this.lastSuspectLatencyMillis =
                    Math.max(0L, nowMillis - currentSession.lastHeartbeatMillis());
        }
        fireSuspected();
        return true;
    }

    /**
     * Transitions the worker to {@link WorkerMembershipState#DOWN}.
     *
     * <p>Accepted from {@link WorkerMembershipState#AVAILABLE} or
     * {@link WorkerMembershipState#SUSPECTED}.
     * Returns {@code true} if the transition actually happened.
     */
    public boolean markDown(long nowMillis) {
        synchronized (this) {
            if (this.state == WorkerMembershipState.DOWN
                    || this.state == WorkerMembershipState.MAINTENANCE) {
                return false;
            }
            this.state = WorkerMembershipState.DOWN;
            this.lastStateChangeMillis = nowMillis;
            this.downAtMillis = nowMillis;
            this.lastDownLatencyMillis =
                    Math.max(0L, nowMillis - currentSession.lastHeartbeatMillis());
        }
        fireDown();
        return true;
    }

    /**
     * Performs a time-based state transition atomically with the heartbeat timestamp
     * check, so a heartbeat cannot race a stale detector observation into SUSPECTED
     * or DOWN.
     */
    public Optional<WorkerMembershipState> evaluateHeartbeatAge(
            long nowMillis, long suspectTimeoutMillis, long downTimeoutMillis) {
        WorkerMembershipState previous;
        WorkerMembershipState next;
        synchronized (this) {
            previous = state;
            if (previous != WorkerMembershipState.AVAILABLE
                    && previous != WorkerMembershipState.SUSPECTED) {
                return Optional.empty();
            }
            long elapsedMillis = Math.max(0L, nowMillis - currentSession.lastHeartbeatMillis());
            if (previous == WorkerMembershipState.AVAILABLE
                    && elapsedMillis >= suspectTimeoutMillis) {
                next = WorkerMembershipState.SUSPECTED;
                suspectedAtMillis = nowMillis;
                lastSuspectLatencyMillis = elapsedMillis;
            } else if (previous == WorkerMembershipState.SUSPECTED
                    && elapsedMillis >= downTimeoutMillis) {
                next = WorkerMembershipState.DOWN;
                downAtMillis = nowMillis;
                lastDownLatencyMillis = elapsedMillis;
            } else {
                return Optional.empty();
            }
            state = next;
            lastStateChangeMillis = nowMillis;
            missedHeartbeatCount++;
        }
        if (next == WorkerMembershipState.SUSPECTED) {
            fireSuspected();
        } else {
            fireDown();
        }
        return Optional.of(next);
    }

    /**
     * Transitions the worker to {@link WorkerMembershipState#MAINTENANCE}.
     * Returns {@code true} if the state changed.
     */
    public boolean markMaintenance(long nowMillis) {
        synchronized (this) {
            if (this.state == WorkerMembershipState.MAINTENANCE) {
                return false;
            }
            this.state = WorkerMembershipState.MAINTENANCE;
            this.lastStateChangeMillis = nowMillis;
        }
        return true;
    }

    /**
     * Accepts an explicit registration for this logical worker. Same-incarnation
     * registration refreshes liveness without resetting its heartbeat sequence;
     * a newer incarnation replaces the session. Older incarnations are rejected.
     */
    public boolean registerSession(String incarnationId, long nowMillis) {
        boolean recovered;
        synchronized (this) {
            if (incarnationId == null || incarnationId.isBlank()) {
                return false;
            }
            WorkerSession session = currentSession;
            if (!session.incarnationId().equals(incarnationId)) {
                if (compareIncarnations(incarnationId, session.incarnationId()) <= 0) {
                    return false;
                }
                currentSession = new WorkerSession(workerUUID, incarnationId, nowMillis);
                currentSession.acceptHeartbeat(0L, nowMillis);
            } else {
                session.refreshRegistration(nowMillis);
            }
            recovered = state == WorkerMembershipState.SUSPECTED
                    || state == WorkerMembershipState.DOWN;
            if (state != WorkerMembershipState.AVAILABLE) {
                state = WorkerMembershipState.AVAILABLE;
                lastStateChangeMillis = nowMillis;
            }
            missedHeartbeatCount = 0;
        }
        if (recovered) {
            fireRecovered();
        }
        return true;
    }

    // -------------------------------------------------------------------------
    // Heartbeat processing
    // -------------------------------------------------------------------------

    /**
     * Heartbeat processing result.
     */
    public enum HeartbeatResult {
        /** Heartbeat accepted; membership updated. */
        ACCEPTED,
        /** Duplicate sequence within current incarnation; ignored safely. */
        DUPLICATE,
        /** Sequence is older than the latest accepted; stale. */
        STALE,
        /** Heartbeat belongs to an older incarnation; ignored. */
        STALE_INCARNATION,
        /** Heartbeat belongs to a new incarnation; session reset. */
        NEW_INCARNATION
    }

    /**
     * Processes an incoming heartbeat.
     *
     * <p>Rules (see spec §33):
     * <ul>
     *   <li>Newer incarnation → accept, reset session, mark AVAILABLE.</li>
     *   <li>Same incarnation, newer sequence → accept, update timestamp.</li>
     *   <li>Same incarnation, same sequence → duplicate, ignore.</li>
     *   <li>Same incarnation, older sequence → stale, ignore.</li>
     *   <li>Older incarnation → stale, ignore.</li>
     * </ul>
     *
     * <p>This method may change state from SUSPECTED → AVAILABLE; the associated
     * listener callback is fired outside the synchronized block.
     *
     * @param incarnationId  the incarnation ID from the worker's heartbeat
     * @param sequence       the sequence number from the worker's heartbeat
     * @param nowMillis      controller-observed receive time (not worker wall clock)
     * @return detailed result of the processing decision
     */
    public HeartbeatResult processHeartbeat(String incarnationId, long sequence, long nowMillis) {
        WorkerMembershipState stateBefore;
        boolean incarnationReset = false;
        boolean accepted = false;
        boolean recovered = false;

        synchronized (this) {
            WorkerSession session = currentSession;
            if (incarnationId == null || incarnationId.isBlank() || sequence < 0) {
                return HeartbeatResult.STALE;
            }

            // --- Incarnation comparison ---
            if (!session.incarnationId().equals(incarnationId)) {
                // Compare incarnations numerically when they look like longs; otherwise
                // fall back to string comparison for UUID-style incarnation IDs.
                int cmp = compareIncarnations(incarnationId, session.incarnationId());
                if (cmp < 0) {
                    // This heartbeat is from an older process; ignore.
                    missedHeartbeatCount++;
                    return HeartbeatResult.STALE_INCARNATION;
                }
                // cmp > 0 → new process incarnation.
                WorkerSession newSession = new WorkerSession(workerUUID, incarnationId, nowMillis);
                // Accept heartbeat #sequence into the new session.
                newSession.acceptHeartbeat(sequence, nowMillis);
                currentSession = newSession;
                incarnationReset = true;
                stateBefore = this.state;
                this.state = WorkerMembershipState.AVAILABLE;
                this.lastStateChangeMillis = nowMillis;
                this.missedHeartbeatCount = 0;
                recovered = stateBefore == WorkerMembershipState.SUSPECTED
                        || stateBefore == WorkerMembershipState.DOWN;
            } else {
                // --- Same incarnation: sequence comparison ---
                stateBefore = this.state;
                boolean sessionAccepted = session.acceptHeartbeat(sequence, nowMillis);
                if (!sessionAccepted) {
                    // Sequence ≤ latestSequence: duplicate or stale.
                    long latest = session.latestSequence();
                    if (sequence == latest) {
                        return HeartbeatResult.DUPLICATE;
                    }
                    return HeartbeatResult.STALE;
                }
                // Accepted: update health.
                missedHeartbeatCount = 0;
                accepted = true;
                if (this.state == WorkerMembershipState.SUSPECTED) {
                    this.state = WorkerMembershipState.AVAILABLE;
                    this.lastStateChangeMillis = nowMillis;
                    recovered = true;
                }
            }
        }

        // Notify listeners outside the lock.
        if (recovered) {
            fireRecovered();
        }

        return incarnationReset
                ? HeartbeatResult.NEW_INCARNATION
                : accepted ? HeartbeatResult.ACCEPTED : HeartbeatResult.STALE;
    }

    // -------------------------------------------------------------------------
    // Metrics helpers
    // -------------------------------------------------------------------------

    /** Increments missed-heartbeat counter. Thread-safe (volatile write is fine for metrics). */
    public void incrementMissedHeartbeat() {
        missedHeartbeatCount++;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public String workerUUID()                 { return workerUUID; }
    public WorkerMembershipState state()       { synchronized (this) { return state; } }
    public WorkerSession currentSession()      { return currentSession; }
    public long registeredAtMillis()           { return registeredAtMillis; }
    public long lastStateChangeMillis()        { return lastStateChangeMillis; }
    public long suspectedAtMillis()            { return suspectedAtMillis; }
    public long downAtMillis()                 { return downAtMillis; }
    public long lastSuspectLatencyMillis()      { return lastSuspectLatencyMillis; }
    public long lastDownLatencyMillis()         { return lastDownLatencyMillis; }
    public int missedHeartbeatCount()          { return missedHeartbeatCount; }

    /** Returns the controller-observed time of the most recent accepted heartbeat. */
    public long lastHeartbeatMillis() {
        return currentSession.lastHeartbeatMillis();
    }

    /** Returns {@code true} if the worker is not DOWN and not MAINTENANCE. */
    public boolean isAlive() {
        WorkerMembershipState s = state();
        return s != WorkerMembershipState.DOWN && s != WorkerMembershipState.MAINTENANCE;
    }

    /** Returns {@code true} if the worker is eligible for new job assignments. */
    public boolean isEligibleForScheduling() {
        return state() == WorkerMembershipState.AVAILABLE;
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Compares two incarnation IDs. Treats them as longs if parseable, otherwise
     * falls back to lexicographic comparison.
     *
     * @return negative if a < b, zero if equal, positive if a > b
     */
    private static int compareIncarnations(String a, String b) {
        try {
            long la = Long.parseLong(a);
            long lb = Long.parseLong(b);
            return Long.compare(la, lb);
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }

    private void fireSuspected() {
        for (WorkerFailureListener l : listeners) {
            try { l.onWorkerSuspected(workerUUID); }
            catch (RuntimeException ex) {
                System.err.println("[WorkerMembership] Listener threw on suspected: "
                        + ex.getMessage());
            }
        }
    }

    private void fireDown() {
        for (WorkerFailureListener l : listeners) {
            try { l.onWorkerDown(workerUUID); }
            catch (RuntimeException ex) {
                System.err.println("[WorkerMembership] Listener threw on down: "
                        + ex.getMessage());
            }
        }
    }

    private void fireRecovered() {
        for (WorkerFailureListener l : listeners) {
            try { l.onWorkerRecovered(workerUUID); }
            catch (RuntimeException ex) {
                System.err.println("[WorkerMembership] Listener threw on recovered: "
                        + ex.getMessage());
            }
        }
    }

    @Override
    public String toString() {
        WorkerSession s = currentSession;
        return "WorkerMembership{uuid=" + workerUUID
                + ", state=" + state()
                + ", incarnation=" + (s == null ? "null" : s.incarnationId())
                + ", lastHB=" + (s == null ? 0 : s.lastHeartbeatMillis())
                + '}';
    }
}
