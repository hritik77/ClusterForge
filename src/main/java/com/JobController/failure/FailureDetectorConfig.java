package com.JobController.failure;

/**
 * Immutable configuration for the {@link FailureDetector}.
 *
 * <p>Configure via system properties:
 * <ul>
 *   <li>{@code clusterforge.failure-detector.heartbeat-interval-ms}  (default 5000)</li>
 *   <li>{@code clusterforge.failure-detector.suspect-timeout-ms}      (default 15000)</li>
 *   <li>{@code clusterforge.failure-detector.down-timeout-ms}         (default 30000)</li>
 *   <li>{@code clusterforge.failure-detector.check-interval-ms}       (default 1000)</li>
 * </ul>
 */
public final class FailureDetectorConfig {

    /** Expected heartbeat interval sent by workers. */
    private final long heartbeatIntervalMillis;

    /**
     * How long after the last heartbeat a worker is moved to SUSPECTED.
     * Must be greater than {@link #heartbeatIntervalMillis}.
     */
    private final long suspectTimeoutMillis;

    /**
     * How long after the last heartbeat a SUSPECTED worker is moved to DOWN.
     * Must be greater than {@link #suspectTimeoutMillis}.
     */
    private final long downTimeoutMillis;

    /**
     * How often the failure-detector sweep runs.
     * Independent of the above timeouts; typically 1 000 ms.
     */
    private final long checkIntervalMillis;

    public FailureDetectorConfig(
            long heartbeatIntervalMillis,
            long suspectTimeoutMillis,
            long downTimeoutMillis,
            long checkIntervalMillis) {
        if (heartbeatIntervalMillis <= 0) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }
        if (suspectTimeoutMillis <= heartbeatIntervalMillis) {
            throw new IllegalArgumentException(
                    "suspectTimeout must be greater than heartbeatInterval");
        }
        if (downTimeoutMillis <= suspectTimeoutMillis) {
            throw new IllegalArgumentException(
                    "downTimeout must be greater than suspectTimeout");
        }
        if (checkIntervalMillis <= 0) {
            throw new IllegalArgumentException("checkInterval must be positive");
        }
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.suspectTimeoutMillis    = suspectTimeoutMillis;
        this.downTimeoutMillis       = downTimeoutMillis;
        this.checkIntervalMillis     = checkIntervalMillis;
    }

    /** Reads configuration from system properties with sensible defaults. */
    public static FailureDetectorConfig fromSystemProperties() {
        long heartbeat = readProperty(
                "clusterforge.failure-detector.heartbeat-interval-ms", 5_000L);
        long suspect = readProperty(
                "clusterforge.failure-detector.suspect-timeout-ms", 15_000L);
        long down = readProperty(
                "clusterforge.failure-detector.down-timeout-ms", 30_000L);
        long check = readProperty(
                "clusterforge.failure-detector.check-interval-ms", 1_000L);
        return new FailureDetectorConfig(heartbeat, suspect, down, check);
    }

    private static long readProperty(String name, long defaultValue) {
        String value = System.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer number of milliseconds", e);
        }
    }

    public long heartbeatIntervalMillis() { return heartbeatIntervalMillis; }
    public long suspectTimeoutMillis()    { return suspectTimeoutMillis; }
    public long downTimeoutMillis()       { return downTimeoutMillis; }
    public long checkIntervalMillis()     { return checkIntervalMillis; }

    @Override
    public String toString() {
        return "FailureDetectorConfig{"
                + "heartbeatInterval=" + heartbeatIntervalMillis + "ms"
                + ", suspectTimeout=" + suspectTimeoutMillis + "ms"
                + ", downTimeout=" + downTimeoutMillis + "ms"
                + ", checkInterval=" + checkIntervalMillis + "ms"
                + '}';
    }
}
