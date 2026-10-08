package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.job.JobType;
import com.JobController.job.spec.BatchJobSpecification;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BatchJobParserTest {
    private final BatchJobParser parser = new BatchJobParser();

    @Test
    public void reportsBatchJobType() {
        assertEquals(JobType.BATCH, parser.getJobType());
    }

    @Test
    public void countOneProducesOneTask() {
        DAG dag = parser.parse(specification("job-one", "simulation", 1, 2, 4));

        assertEquals(1, dag.getTasks().size());
        assertTrue(dag.hasTask("simulation-001"));
    }

    @Test
    public void countTenProducesTenIndependentTasks() {
        DAG dag = parser.parse(specification("job-ten", "simulation", 10, 2, 4));

        assertEquals(10, dag.getTasks().size());
        assertEquals(List.of(
                "simulation-001", "simulation-002", "simulation-003", "simulation-004",
                "simulation-005", "simulation-006", "simulation-007", "simulation-008",
                "simulation-009", "simulation-010"),
                dag.getTasks().keySet().stream().toList());
        assertTrue(dag.getTasks().keySet().stream()
                .allMatch(taskId -> dag.getDependencies(taskId).isEmpty()));
        assertTrue(dag.getTasks().keySet().stream()
                .allMatch(taskId -> dag.getDependents(taskId).isEmpty()));
    }

    @Test
    public void tasksPreserveRequestedResourcesAndCommand() {
        DAG dag = parser.parse(specification(
                "simulation-job", "simulation", 3, 6, 12));

        for (DAGTask task : dag.getTasks().values()) {
            assertEquals(6, task.getCpuRequested());
            assertEquals(12, task.getMemRequested());
            assertEquals("python simulation.py", task.getCommand());
            assertEquals(Set.of(), task.getDependencies());
        }
    }

    @Test
    public void copiesDiskAndGpuRequirementsToEveryGeneratedTask() {
        DAG dag = parser.parse(new BatchJobSpecification(
                "gpu-batch", "simulation", "python simulation.py", 2,
                2, 4, 1_024, 1, 8_192));

        for (DAGTask task : dag.getTasks().values()) {
            assertEquals(1_024, task.getDiskMbRequested());
            assertEquals(1, task.getGpuCountRequested());
            assertEquals(8_192, task.getGpuMemoryMbPerGpu());
        }
    }

    @Test
    public void taskIdsAreDeterministicAcrossParses() {
        BatchJobSpecification specification = specification(
                "stable-job", "batch", 4, 1, 1);

        List<String> first = parser.parse(specification).getTasks().keySet().stream().toList();
        List<String> second = parser.parse(specification).getTasks().keySet().stream().toList();

        assertEquals(List.of("batch-001", "batch-002", "batch-003", "batch-004"), first);
        assertEquals(first, second);
    }

    @Test
    public void registryIncludesBatchParser() {
        JobParserRegistry registry = new JobParserRegistry();

        DAG dag = registry.parse(specification("registered-job", "batch", 2, 2, 2));

        assertEquals(2, dag.getTasks().size());
    }

    @Test
    public void rejectsNullSpecification() {
        assertThrows(IllegalArgumentException.class, () -> parser.parse(null));
    }

    @Test
    public void rejectsZeroAndNegativeCount() {
        assertThrows(IllegalArgumentException.class,
                () -> specification("zero", "batch", 0, 1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> specification("negative", "batch", -1, 1, 1));
    }

    @Test
    public void rejectsBlankCommand() {
        assertThrows(IllegalArgumentException.class,
                () -> new BatchJobSpecification("blank-command", "batch", " ", 1, 1, 1));
    }

    @Test
    public void rejectsInvalidCpuAndMemory() {
        assertThrows(IllegalArgumentException.class,
                () -> specification("zero-cpu", "batch", 1, 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> specification("negative-cpu", "batch", 1, -1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> specification("zero-memory", "batch", 1, 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> specification("negative-memory", "batch", 1, 1, -1));
    }

    @Test
    public void rejectsBlankName() {
        assertThrows(IllegalArgumentException.class,
                () -> new BatchJobSpecification("blank-name", " ", "echo", 1, 1, 1));
    }

    private static BatchJobSpecification specification(
            String jobId, String name, int count, int cpu, int memory) {
        return new BatchJobSpecification(
                jobId, name, "python simulation.py", count, cpu, memory);
    }
}
