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
        double remainingDisk=job.getDiskMbRequested() == 0 ? 0.0
                : ratioAfterAllocation(candidate.tally().availableDiskMb(),
                        job.getDiskMbRequested(), candidate.tally().totalDiskMb());
        double remainingGpu=job.getGpuCountRequested() == 0 ? 0.0
                : ratioAfterAllocation(candidate.tally().availableGpuCount(),
                        job.getGpuCountRequested(), candidate.tally().totalGpuCount());
        double remainingGpuMemory=job.getGpuCountRequested() == 0
                || job.getGpuMemoryMbPerGpu() == 0 ? 0.0
                : ratioAfterAllocation(candidate.tally().availableGpuMemoryMb(),
                        (long) Math.ceil((double) job.getGpuMemoryMbPerGpu()
                                * job.getGpuCountRequested()),
                        candidate.tally().totalGpuMemoryMb());
        return remainingCpu + remainingMem + remainingDisk + remainingGpu
                + remainingGpuMemory;
    }

    private static double ratioAfterAllocation(long available, long requested, long total) {
        return total <= 0 ? 0.0 : (double) (available - requested) / total;
    }
}
