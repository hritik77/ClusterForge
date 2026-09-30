package com.JobController;

import java.util.Comparator;
import java.util.List;

final class SchedulerSupport {
    private SchedulerSupport() {}

    static List<NodeEntry> eligibleCandidates(Job job, List<NodeEntry> candidates) {
        return candidates.stream()
                .filter(candidate -> candidate.node().getState() != NodeState.DOWN)
                .filter(candidate -> candidate.node().getState() != NodeState.MAINTENANCE)
                .filter(candidate -> candidate.tally().availableCpu() >= job.getCpuRequested())
                .filter(candidate -> candidate.tally().availableMem() >= job.getMemRequested())
                .sorted(Comparator.comparingInt(NodeEntry::nodeId))
                .toList();
    }
}
