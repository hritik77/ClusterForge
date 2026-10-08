package com.JobController.failure;

import com.JobController.worker.WorkerMembership;
import com.JobController.worker.WorkerMembershipManager;
import com.JobController.worker.WorkerMembershipState;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

/**
 * Periodic failure-detection sweep that evaluates each worker's heartbeat age and
 * drives {@link WorkerMembershipState} transitions.
 *
 * <h3>Algorithm</h3>
 * <pre>
 *   every checkInterval:
 *     now = clock.currentTimeMillis()
 *     for each worker:
 *       elapsed = now - lastHeartbeat
 *
 *       if state == AVAILABLE and elapsed >= suspectTimeout:
 *           markSuspected()
 *
 *       else if state == SUSPECTED and elapsed >= downTimeout:
 *           markDown()
 * </pre>
 *
 * <p>One single {@link ScheduledExecutorService} thread is used for all workers.
 * No per-worker threads.
 *
 * <p>The detector re-reads the latest state before committing any transition (via
 * {@link WorkerMembership#markSuspected} / {@link WorkerMembership#markDown}) so that
 * a heartbeat arriving concurrently with the sweep does not produce an incorrect result.
 */
public final class FailureDetector {

    private final WorkerMembershipManager membershipManager;
    private final FailureDetectorConfig config;
    private final TimeSource clock;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            runnable -> {
                Thread t = new Thread(runnable, "failure-detector");
                t.setDaemon(true);
                return t;
            });

    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledFuture<?> sweepFuture;

    // Metrics
    private volatile long workersSuspectedTotal;
    private volatile long workersDownTotal;
    private final LongAdder suspectLatencyTotalMillis = new LongAdder();
    private final LongAdder downLatencyTotalMillis = new LongAdder();

    public FailureDetector(WorkerMembershipManager membershipManager,
                           FailureDetectorConfig config,
                           TimeSource clock) {
        if (membershipManager == null) throw new IllegalArgumentException("membershipManager null");
        if (config == null) throw new IllegalArgumentException("config null");
        if (clock == null) throw new IllegalArgumentException("clock null");
        this.membershipManager = membershipManager;
        this.config = config;
        this.clock = clock;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Starts the periodic detection sweep. Idempotent.
     */
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            sweepFuture = scheduler.scheduleWithFixedDelay(
                    this::sweep,
                    config.checkIntervalMillis(),
                    config.checkIntervalMillis(),
                    TimeUnit.MILLISECONDS);
            System.out.println("[FailureDetector] Started: " + config);
        }
    }

    /**
     * Stops the detection sweep cleanly.
     *
     * <p>After this returns no further DOWN events will be generated. In-flight sweep
     * tasks are allowed to complete but no new ones are scheduled.
     * Does <em>not</em> generate spurious DOWN events for healthy workers.
     */
    public synchronized void shutdown() {
        running.set(false);
        if (sweepFuture != null) {
            sweepFuture.cancel(false); // Do not interrupt; let current sweep finish cleanly.
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("[FailureDetector] Shutdown complete.");
    }

    // -------------------------------------------------------------------------
    // Detection sweep (runs on detector thread)
    // -------------------------------------------------------------------------

    /**
     * Runs one sweep over all known workers and performs any necessary state
     * transitions. Called by the scheduled executor; exceptions are caught so that
     * the schedule is not silently cancelled.
     */
    void sweep() {
        try {
            long now = clock.currentTimeMillis();
            for (WorkerMembership membership : membershipManager.getWorkers()) {
                evaluateWorker(membership, now);
            }
        } catch (RuntimeException e) {
            // Must not propagate — an uncaught exception would silently cancel future sweeps.
            System.err.println("[FailureDetector] Unexpected exception in sweep: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void evaluateWorker(WorkerMembership membership, long now) {
        long elapsed = Math.max(0L, now - membership.lastHeartbeatMillis());
        Optional<WorkerMembershipState> transition = membership.evaluateHeartbeatAge(
                now, config.suspectTimeoutMillis(), config.downTimeoutMillis());
        if (transition.isPresent()) {
            if (transition.get() == WorkerMembershipState.SUSPECTED) {
                workersSuspectedTotal++;
                suspectLatencyTotalMillis.add(membership.lastSuspectLatencyMillis());
                System.out.printf(
                        "[FailureDetector] Worker %s suspected: lastHeartbeatAge=%dms%n",
                        membership.workerUUID(), elapsed);
            } else if (transition.get() == WorkerMembershipState.DOWN) {
                workersDownTotal++;
                downLatencyTotalMillis.add(membership.lastDownLatencyMillis());
                System.out.printf(
                        "[FailureDetector] Worker %s marked DOWN: lastHeartbeatAge=%dms%n",
                        membership.workerUUID(), elapsed);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    public long workersSuspectedTotal() { return workersSuspectedTotal; }
    public long workersDownTotal()      { return workersDownTotal; }
    public long suspectLatencyTotalMillis() { return suspectLatencyTotalMillis.sum(); }
    public long downLatencyTotalMillis() { return downLatencyTotalMillis.sum(); }
    public FailureDetectorConfig config() { return config; }
}
