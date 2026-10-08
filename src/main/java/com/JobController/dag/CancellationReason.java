package com.JobController.dag;

public enum CancellationReason {
    USER_REQUEST,
    DAG_TIMEOUT,
    DAG_FAILURE,
    SHUTDOWN
}
