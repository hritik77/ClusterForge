package com.JobController;

import java.util.List;
import java.util.Optional;

final class FirstFitSchedular implements Scheduler {
    @Override
    public Optional<NodeEntry> selectNode(Job job, List<NodeEntry> candidates) {
        return SchedulerSupport.eligibleCandidates(job, candidates).stream().findFirst();
    }
}
