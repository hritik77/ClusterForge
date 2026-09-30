package com.JobController;

import java.util.List;
import java.util.Optional;

final class RoundRobinSchedular implements Scheduler {
    private int lastSelectedNodeId=Integer.MIN_VALUE;

    @Override
    public synchronized Optional<NodeEntry> selectNode(Job job, List<NodeEntry> candidates) {
        List<NodeEntry> eligible=SchedulerSupport.eligibleCandidates(job, candidates);
        if (eligible.isEmpty()) {
            return Optional.empty();
        }

        NodeEntry selected=eligible.stream()
                .filter(candidate -> candidate.nodeId() > lastSelectedNodeId)
                .findFirst()
                .orElse(eligible.get(0));
        lastSelectedNodeId=selected.nodeId();
        return Optional.of(selected);
    }
}
