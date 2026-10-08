package com.JobController;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JobQTest {
    @Test
    public void removesOnlyTheMatchingQueuedJobId() {
        JobQ queue = new JobQ(3);
        assertTrue(queue.add(101));
        assertTrue(queue.add(102));

        assertTrue(queue.remove(101));
        assertFalse(queue.remove(101));
        assertEquals(java.util.List.of(102), queue.snapshot());
    }
}
