package com.JobController.dag;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RetryPolicyTest {
    @Test
    public void allowsConfiguredRetriesAfterInitialAttempt() {
        RetryPolicy policy = new RetryPolicy(2);

        assertTrue(policy.canRetry(0));
        assertTrue(policy.canRetry(1));
        assertFalse(policy.canRetry(2));
    }

    @Test
    public void zeroRetriesDisablesRetries() {
        assertFalse(new RetryPolicy(0).canRetry(0));
    }

    @Test
    public void rejectsNegativeLimitsAndCounts() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(-1));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(1).canRetry(-1));
    }
}
