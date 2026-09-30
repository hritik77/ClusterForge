package com.JobController;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

final class BestFitSchedular implements Scheduler {
    @Override
    public Optional<NodeEntry> selectNode(Job job, List<NodeEntry> candidates) {
        return SchedulerSupport.eligibleCandidates(job, candidates).stream()
                .min(Comparator
                        .comparingDouble((NodeEntry candidate) -> remainingCapacity(candidate, job))
                        .thenComparingInt(NodeEntry::nodeId));
    }

    private static double remainingCapacity(NodeEntry candidate, Job job) {
        double remainingCpu=(double) (candidate.tally().availableCpu() - job.getCpuRequested())
                / candidate.tally().totalCpu();
        double remainingMem=(double) (candidate.tally().availableMem() - job.getMemRequested())
                / candidate.tally().totalMem();
        return remainingCpu + remainingMem;
    }
}
