package com.JobController.dag;

public enum AttemptState {
    CREATED,
    SUBMITTED,
    ALLOCATED,
    RUNNING,
    COMPLETED,
    FAILED,
    LOST,
    CANCELLED;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == LOST || this == CANCELLED;
    }
}
