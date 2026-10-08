package com.JobController;

import com.JobController.scheduling.PlacementConstraint;
import com.JobController.scheduling.PlacementOperator;
import com.JobController.scheduling.PlacementRequirements;
import com.JobController.worker.WorkerMembership;
import com.JobController.worker.WorkerMembershipState;

import java.util.Comparator;
import java.util.List;

final class SchedulerSupport {
    private SchedulerSupport() {}

    /**
     * Returns the subset of {@code candidates} that are eligible for a new job assignment.
     *
     * <p>Candidate pipeline (Phase 9):
     * <ol>
     *   <li>Membership filter — only {@link WorkerMembershipState#AVAILABLE} workers.</li>
     *   <li>Maintenance filter — exclude workers in the {@link NodeState#MAINTENANCE} proto state.</li>
     *   <li>Placement constraints — label-based rules from the job specification.</li>
     *   <li>ResourceVector feasibility — {@link NodeResourceTally#canFit(Job)}.</li>
     * </ol>
     */
    static List<NodeEntry> eligibleCandidates(Job job, List<NodeEntry> candidates) {
        PlacementRequirements placementRequirements = new PlacementRequirements(
                job.getPlacementConstraintsList().stream()
                        .map(constraint -> new PlacementConstraint(
                                constraint.getKey(),
                                PlacementOperator.valueOf(constraint.getOperator().name()),
                                constraint.getValue()))
                        .toList());
        return candidates.stream()
                // 1. Membership filter: AVAILABLE only.
                .filter(candidate -> {
                    WorkerMembership membership = Discover.membershipManager.getWorker(
                            candidate.workerUUID());
                    if (membership == null) {
                        // No membership record yet (legacy/test node) — fall back to
                        // proto NodeState for backward-compatibility with tests that
                        // inject NodeEntry directly without going through registerNode.
                        return candidate.node().getState() != NodeState.DOWN
                                && candidate.node().getState() != NodeState.MAINTENANCE;
                    }
                    return membership.isEligibleForScheduling();
                })
                // 2. Maintenance filter (proto-level MAINTENANCE state).
                .filter(candidate -> candidate.node().getState() != NodeState.MAINTENANCE)
                // 3. Placement constraints.
                .filter(candidate -> placementRequirements.isSatisfiedBy(candidate.labels()))
                // 4. Resource feasibility.
                .filter(candidate -> candidate.tally().canFit(job))
                .sorted(Comparator.comparingInt(NodeEntry::nodeId))
                .toList();
    }
}
