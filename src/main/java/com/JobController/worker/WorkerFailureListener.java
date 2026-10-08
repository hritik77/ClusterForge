package com.JobController.worker;

/**
 * Listener interface for significant worker membership state-change events.
 *
 * <p>Implementations must be non-blocking and must not call back into
 * {@link WorkerMembershipManager} while the callback is in progress (to avoid
 * deadlocks — the manager deliberately does <em>not</em> hold its lock when it
 * invokes listeners).
 */
public interface WorkerFailureListener {

    /**
     * Called when a worker transitions from {@link WorkerMembershipState#AVAILABLE}
     * to {@link WorkerMembershipState#SUSPECTED}.
     *
     * <p>No jobs should be assigned to the worker after this point, but existing
     * running jobs should <em>not</em> yet be declared lost.
     *
     * @param workerUUID logical identifier of the worker
     */
    default void onWorkerSuspected(String workerUUID) {}

    /**
     * Called when a worker transitions to {@link WorkerMembershipState#DOWN}.
     *
     * <p>The job-recovery path ({@code JobManager → DAGManager}) should be
     * triggered from this callback to mark jobs LOST and release reservations.
     *
     * @param workerUUID logical identifier of the worker
     */
    void onWorkerDown(String workerUUID);

    /**
     * Called when a worker returns to {@link WorkerMembershipState#AVAILABLE}
     * after having been {@code SUSPECTED} or {@code DOWN}.
     *
     * <p>The scheduler should be notified so that pending jobs can be reconsidered.
     *
     * @param workerUUID logical identifier of the worker
     */
    default void onWorkerRecovered(String workerUUID) {}
}
