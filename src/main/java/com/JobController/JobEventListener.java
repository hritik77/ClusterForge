package com.JobController;

public interface JobEventListener {
    default void onJobStateChanged(Job job) {}

    void onJobCompleted(Job job);

    void onJobFailed(Job job);

    void onJobCancelled(Job job);

    void onJobLost(Job job);
}
