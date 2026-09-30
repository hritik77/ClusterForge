package com.JobController;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

final class LeastLoadedSchedular implements Scheduler {
    @Override
    public Optional<NodeEntry> selectNode(Job job, List<NodeEntry> candidates) {
        return SchedulerSupport.eligibleCandidates(job, candidates).stream()
                .sorted(Comparator
                        .comparingDouble((NodeEntry candidate) -> loadRatio(candidate, job))
                        .thenComparingInt(NodeEntry::nodeId))
                .findFirst();
    }

    private static double loadRatio(NodeEntry candidate, Job job) {
        int totalCpu=candidate.tally().totalCpu();
        int totalMem=candidate.tally().totalMem();
        int usedCpu=totalCpu-candidate.tally().availableCpu();
        int usedMem=totalMem-candidate.tally().availableMem();
        double cpuRatio=totalCpu<=0 ? 0.0 : (double) usedCpu / totalCpu;
        double memRatio=totalMem<=0 ? 0.0 : (double) usedMem / totalMem;
        double demandCpuRatio=job.getCpuRequested()<=0 ? 0.0 : (double) job.getCpuRequested() / totalCpu;
        double demandMemRatio=job.getMemRequested()<=0 ? 0.0 : (double) job.getMemRequested() / totalMem;
        return cpuRatio + memRatio + demandCpuRatio + demandMemRatio;
    }
}
