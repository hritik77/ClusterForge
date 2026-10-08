package com.JobController;

import java.util.Optional;

public interface JobSubmissionService {
    long submit(Job job);

    default Optional<Job> find(long jobId) {
        return Optional.empty();
    }

    default boolean cancel(long jobId) {
        throw new UnsupportedOperationException(
                "Job cancellation is not supported by this submission service");
    }
}
