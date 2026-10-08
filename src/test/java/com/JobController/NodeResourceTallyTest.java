package com.JobController;

import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class NodeResourceTallyTest {
    @Test
    public void reservesAndReleasesDiskAndWholeGpuDevices() {
        NodeResourceTally tally = new NodeResourceTally(1, 8, 32, 1_000, List.of(
                gpu("gpu-0", 8_192),
                gpu("gpu-1", 16_384)));
        Job first = job(1, 500, 1, 8_000);
        Job second = job(2, 400, 1, 16_000);
        Job noCapacity = job(3, 200, 1, 1_000);

        assertTrue(tally.canFit(first));
        assertEquals(List.of("gpu-0"), tally.tryReserve(first));
        assertEquals(500, tally.availableDiskMb());
        assertEquals(1, tally.availableGpuCount());

        assertTrue(tally.canFit(second));
        assertEquals(List.of("gpu-1"), tally.tryReserve(second));
        assertEquals(100, tally.availableDiskMb());
        assertEquals(0, tally.availableGpuCount());
        assertFalse(tally.canFit(noCapacity));
        assertNull(tally.tryReserve(noCapacity));

        tally.release(first.getId());
        assertEquals(600, tally.availableDiskMb());
        assertEquals(1, tally.availableGpuCount());
        assertTrue(tally.canFit(noCapacity));

        tally.release(first.getId());
        assertEquals(600, tally.availableDiskMb());
        tally.release(second.getId());
        assertEquals(1_000, tally.availableDiskMb());
        assertEquals(2, tally.availableGpuCount());
    }

    @Test
    public void workerReconfigurationPreservesActiveReservations() {
        NodeResourceTally tally = new NodeResourceTally(1, 8, 32, 1_000, List.of(
                gpu("gpu-0", 8_192), gpu("gpu-1", 16_384)));
        Job active = job(1, 400, 1, 8_000).toBuilder()
                .setCpuRequested(2)
                .setMemRequested(4)
                .build();
        assertEquals(List.of("gpu-0"), tally.tryReserve(active));

        tally.reconfigure(12, 64, 2_000, List.of(
                gpu("gpu-0", 10_000), gpu("gpu-1", 16_384)));
        assertEquals(10, tally.availableCpu());
        assertEquals(60, tally.availableMem());
        assertEquals(1_600, tally.availableDiskMb());
        assertEquals(1, tally.availableGpuCount());

        assertThrows(IllegalArgumentException.class, () -> tally.reconfigure(
                1, 64, 2_000, List.of(gpu("gpu-1", 16_384))));
        assertEquals(10, tally.availableCpu());
        assertEquals(1, tally.availableGpuCount());

        tally.release(active.getId());
        assertEquals(12, tally.availableCpu());
        assertEquals(2, tally.availableGpuCount());
    }

    @Test
    public void gpuMemoryRequirementMustFitOnEachIndividualDevice() {
        NodeResourceTally tally = new NodeResourceTally(1, 8, 32, 0, List.of(
                gpu("gpu-0", 4_096),
                gpu("gpu-1", 4_096)));
        Job request = job(1, 0, 2, 4_500);

        assertFalse(tally.canFit(request));
    }

    @Test
    public void rejectsDuplicateOrInvalidGpuInventory() {
        assertThrows(IllegalArgumentException.class, () -> new NodeResourceTally(
                1, 8, 32, 0, List.of(gpu("gpu-0", 8_192), gpu("gpu-0", 8_192))));
        assertThrows(IllegalArgumentException.class, () -> new NodeResourceTally(
                1, 8, 32, -1, List.of()));
        assertThrows(IllegalArgumentException.class, () -> new NodeResourceTally(
                1, 8, 32, 0, List.of(gpu(" ", 8_192))));
    }

    @Test
    public void leastLoadedSchedulerRequiresDiskAndDeviceCapacity() {
        Node node = Node.newBuilder()
                .setId(1)
                .setState(NodeState.AVAILABLE)
                .setCpu(8)
                .setMem(32)
                .setDiskMb(512)
                .addGpuDevices(gpu("gpu-0", 8_192))
                .build();
        NodeEntry candidate = new NodeEntry(node, new NodeResourceTally(
                1, 8, 32, 512, node.getGpuDevicesList()));
        LeastLoadedSchedular scheduler = new LeastLoadedSchedular();

        assertEquals(Optional.empty(), scheduler.selectNode(job(1, 513, 1, 8_192),
                List.of(candidate)));
        assertEquals(Optional.empty(), scheduler.selectNode(job(2, 1, 1, 8_193),
                List.of(candidate)));
        assertEquals(Optional.of(candidate), scheduler.selectNode(job(3, 512, 1, 8_192),
                List.of(candidate)));
    }

    @Test
    public void schedulerFiltersPlacementBeforeResourceFeasibility() {
        Node matchingNode = Node.newBuilder()
                .setId(2)
                .setHostname("worker-2")
                .setState(NodeState.AVAILABLE)
                .setCpu(8)
                .setMem(32)
                .putAllLabels(Map.of("architecture", "x86_64", "disk", "ssd"))
                .build();
        Node wrongLabelNode = Node.newBuilder()
                .setId(1)
                .setHostname("worker-1")
                .setState(NodeState.AVAILABLE)
                .setCpu(16)
                .setMem(32)
                .putAllLabels(Map.of("architecture", "arm64", "disk", "ssd"))
                .build();
        NodeEntry matching = new NodeEntry(
                matchingNode, new NodeResourceTally(2, 8, 32));
        NodeEntry wrongLabel = new NodeEntry(
                wrongLabelNode, new NodeResourceTally(1, 16, 32));
        Job request = job(5, 0, 0, 0).toBuilder()
                .setCpuRequested(4)
                .addPlacementConstraints(com.JobController.PlacementConstraint.newBuilder()
                        .setKey("architecture")
                        .setOperator(com.JobController.PlacementOperator.EQUALS)
                        .setValue("x86_64"))
                .addPlacementConstraints(com.JobController.PlacementConstraint.newBuilder()
                        .setKey("disk")
                        .setOperator(com.JobController.PlacementOperator.EQUALS)
                        .setValue("ssd"))
                .build();

        assertEquals(Optional.of(matching), new LeastLoadedSchedular().selectNode(
                request, List.of(wrongLabel, matching)));
        assertEquals(Optional.empty(), new LeastLoadedSchedular().selectNode(
                request.toBuilder().setCpuRequested(9).build(), List.of(wrongLabel, matching)));
        assertEquals(Optional.empty(), new LeastLoadedSchedular().selectNode(
                request.toBuilder().setPlacementConstraints(0,
                        request.getPlacementConstraints(0).toBuilder().setValue("arm64"))
                        .build(),
                List.of(matching)));
    }

    private static GPUDevice gpu(String id, long memoryMb) {
        return GPUDevice.newBuilder().setId(id).setMemoryMb(memoryMb).build();
    }

    private static Job job(int id, long diskMb, int gpuCount, long gpuMemoryMb) {
        return Job.newBuilder()
                .setId(id)
                .setCpuRequested(1)
                .setMemRequested(1)
                .setDiskMbRequested(diskMb)
                .setGpuCountRequested(gpuCount)
                .setGpuMemoryMbPerGpu(gpuMemoryMb)
                .setState(JobState.PENDING)
                .build();
    }

}
