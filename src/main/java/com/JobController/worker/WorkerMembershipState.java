package com.JobController.worker;

/**
 * Health/membership lifecycle states of a worker as seen by the controller.
 *
 * <p>This is deliberately separate from resource-allocation state (e.g. how many CPUs are
 * currently reserved). A worker can be {@code AVAILABLE} with 0 free CPUs, or
 * {@code SUSPECTED} while still running three jobs.
 *
 * <pre>
 *   JOINING → AVAILABLE ← heartbeat ← SUSPECTED
 *                                           ↓ (further timeout)
 *                                          DOWN → (re-registration) → AVAILABLE
 * </pre>
 */
public enum WorkerMembershipState {

    /** Worker sent its first registration but the controller has not yet confirmed it. */
    JOINING,

    /** Worker is registered, sending heartbeats, and eligible for new job assignments. */
    AVAILABLE,

    /**
     * No recent heartbeat; the worker might be temporarily unreachable.
     *
     * <p>The worker is <em>not</em> eligible for new jobs while suspected, but existing
     * jobs are not yet declared LOST. If a heartbeat arrives the worker returns to
     * {@code AVAILABLE}. If the timeout persists it transitions to {@code DOWN}.
     */
    SUSPECTED,

    /**
     * The controller has declared the worker definitively failed.
     *
     * <p>All active jobs on this worker will be marked LOST and resources released.
     * The worker can only leave this state via a fresh registration, which resets
     * the incarnation.
     */
    DOWN,

    /**
     * The operator has placed the worker into a maintenance window.
     *
     * <p>No new jobs are assigned; existing jobs continue running.
     */
    MAINTENANCE
}
