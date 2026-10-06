package com.JobController.dag;

import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

public class DAGTaskTest {
    @Test
    public void constructsTaskAndCopiesDependencies() {
        Set<String> dependencies = new java.util.HashSet<>(Set.of("preprocess"));
        DAGTask task = new DAGTask("train", 2, 4, dependencies);
        dependencies.add("augment");

        assertEquals("train", task.getId());
        assertEquals(2, task.getCpuRequested());
        assertEquals(4, task.getMemRequested());
        assertEquals(Set.of("preprocess"), task.getDependencies());
        assertThrows(UnsupportedOperationException.class,
                () -> task.getDependencies().add("another"));
    }

    @Test
    public void rejectsInvalidTaskFields() {
        assertThrows(IllegalArgumentException.class,
                () -> new DAGTask(null, 1, 1, Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DAGTask(" ", 1, 1, Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DAGTask("task", 0, 1, Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DAGTask("task", -1, 1, Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DAGTask("task", 1, 0, Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DAGTask("task", 1, -1, Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> new DAGTask("task", 1, 1, Set.of("task")));
    }

    @Test
    public void treatsNullDependenciesAsEmpty() {
        DAGTask task = new DAGTask("root", 1, 1, null);

        assertEquals(Set.of(), task.getDependencies());
    }

    @Test
    public void equalityAndHashCodeAreBasedOnId() {
        DAGTask first = new DAGTask("same", 1, 2, Set.of());
        DAGTask sameId = new DAGTask("same", 8, 9, Set.of("other"));
        DAGTask differentId = new DAGTask("different", 1, 2, Set.of());

        assertEquals(first, sameId);
        assertEquals(first.hashCode(), sameId.hashCode());
        assertNotEquals(first, differentId);
    }

    @Test
    public void storesImmutableRetryPolicy() {
        RetryPolicy policy = new RetryPolicy(3);
        DAGTask task = new DAGTask("retryable", 1, 1, Set.of(), policy);

        assertEquals(policy, task.getRetryPolicy());
        assertEquals(3, task.getRetryPolicy().getMaxRetries());
        assertThrows(IllegalArgumentException.class,
                () -> new DAGTask("invalid", 1, 1, Set.of(), (RetryPolicy) null));
    }
}
