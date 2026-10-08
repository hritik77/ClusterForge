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
        long totalDisk=candidate.tally().totalDiskMb();
        long totalGpuMemory=candidate.tally().totalGpuMemoryMb();
        double diskRatio=totalDisk<=0 ? 0.0
                : (double) (totalDisk-candidate.tally().availableDiskMb()) / totalDisk;
        double gpuRatio=candidate.tally().totalGpuCount()<=0 ? 0.0
                : (double) (candidate.tally().totalGpuCount()
                        - candidate.tally().availableGpuCount())
                        / candidate.tally().totalGpuCount();
        double gpuMemoryRatio=totalGpuMemory<=0 ? 0.0
                : (double) (totalGpuMemory-candidate.tally().availableGpuMemoryMb())
                        / totalGpuMemory;
        double demandDiskRatio=job.getDiskMbRequested()<=0 || totalDisk<=0 ? 0.0
                : (double) job.getDiskMbRequested() / totalDisk;
        double demandGpuRatio=job.getGpuCountRequested()<=0
                || candidate.tally().totalGpuCount()<=0 ? 0.0
                : (double) job.getGpuCountRequested() / candidate.tally().totalGpuCount();
        double demandGpuMemoryRatio=job.getGpuCountRequested()<=0 || totalGpuMemory<=0 ? 0.0
                : ((double) job.getGpuMemoryMbPerGpu() * job.getGpuCountRequested())
                        / totalGpuMemory;
        return cpuRatio + memRatio + diskRatio + gpuRatio + gpuMemoryRatio
                + demandCpuRatio + demandMemRatio + demandDiskRatio
                + demandGpuRatio + demandGpuMemoryRatio;
    }
}
