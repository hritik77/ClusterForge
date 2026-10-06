package com.JobController.dag;

public enum TaskState {
    BLOCKED,
    READY,
    SUBMITTED,
    ALLOCATED,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
}
