package com.JobController.dag;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TaskAttemptTest {
    @Test
    public void recordsWorkerAndLifecycleTimesWithoutOverwritingThem() {
        TaskAttempt attempt = new TaskAttempt("run-1", "preprocess", 2, 100L);
        assertEquals("run-1:preprocess:2", attempt.getAttemptId());
        assertTrue(attempt.markSubmitted(109));
        assertTrue(attempt.markAllocated("node-3", Map.of(
                "hostname", "worker-3", "architecture", "x86_64")));
        assertTrue(attempt.markRunning(200L));
        assertFalse(attempt.markRunning(250L));

        assertEquals(100L, attempt.getCreatedAtMillis());
        assertEquals(Long.valueOf(200), attempt.getStartedAtMillis());
        assertEquals("node-3", attempt.getWorkerId());
        assertEquals(Map.of("hostname", "worker-3", "architecture", "x86_64"),
                attempt.getWorkerLabels());
        assertTrue(attempt.markTerminal(
                AttemptState.LOST, RetryReason.WORKER_FAILURE, "worker stopped", 300L));
        assertFalse(attempt.markTerminal(
                AttemptState.COMPLETED, null, null, 400L));

        assertEquals(AttemptState.LOST, attempt.getState());
        assertEquals(RetryReason.WORKER_FAILURE, attempt.getFailureReason());
        assertEquals("worker stopped", attempt.getFailureMessage());
        assertEquals(Long.valueOf(300), attempt.getFinishedAtMillis());
        assertThrowsUnsupported(attempt.getWorkerLabels());
    }

    @Test
    public void preservesTimeoutAndCancellationTerminalInformation() {
        TaskAttempt timeout = new TaskAttempt("run", "task", 0, 1L);
        timeout.markSubmitted(1);
        assertTrue(timeout.markTerminal(
                AttemptState.FAILED, RetryReason.TIMEOUT, "timed out", 2L));
        assertEquals(RetryReason.TIMEOUT, timeout.getFailureReason());
        assertEquals(Long.valueOf(2), timeout.getFinishedAtMillis());

        TaskAttempt cancelled = new TaskAttempt("run", "cancelled", 0, 1L);
        cancelled.markSubmitted(2);
        assertTrue(cancelled.markTerminal(
                AttemptState.CANCELLED, null, "cancelled by user", 3L));
        assertNull(cancelled.getFailureReason());
        assertEquals(AttemptState.CANCELLED, cancelled.getState());
    }

    private static void assertThrowsUnsupported(Map<String, String> labels) {
        org.junit.Assert.assertThrows(
                UnsupportedOperationException.class, () -> labels.put("new", "value"));
    }
}
