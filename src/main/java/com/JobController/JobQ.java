package com.JobController;

import java.util.ArrayDeque;
import java.util.List;

final class JobQ {
    private final ArrayDeque<Integer> queuedJobIds=new ArrayDeque<>();
    private final int capacity;

    JobQ(int capacity) {
        if (capacity<=0) throw new IllegalArgumentException("Queue capacity must be positive");
        this.capacity=capacity;
    }

    synchronized boolean add(int jobId) {
        if (queuedJobIds.size()>=capacity) return false;
        queuedJobIds.addLast(jobId);
        return true;
    }

    synchronized boolean remove(long jobId) {
        return queuedJobIds.removeIf(queuedId -> queuedId.longValue() == jobId);
    }

    synchronized List<Integer> snapshot() {
        return List.copyOf(queuedJobIds);
    }
}
