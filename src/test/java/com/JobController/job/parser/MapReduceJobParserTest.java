package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.job.JobType;
import com.JobController.job.spec.MapReduceJobSpecification;
import org.junit.Test;

import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class MapReduceJobParserTest {
    private final MapReduceJobParser parser = new MapReduceJobParser();

    @Test
    public void reportsMapReduceJobType() {
        assertEquals(JobType.MAP_REDUCE, parser.getJobType());
    }

    @Test
    public void onePartitionProducesOneMapAndOneReduceTask() {
        DAG dag = parser.parse(specification(1));

        assertEquals(2, dag.getTasks().size());
        assertEquals(Set.of("map-001"), dag.getDependencies("reduce"));
    }

    @Test
    public void eightPartitionsProduceEightMapsAndOneReduce() {
        DAG dag = parser.parse(specification(8));

        assertEquals(9, dag.getTasks().size());
        assertEquals(8, dag.getTasks().keySet().stream()
                .filter(taskId -> taskId.startsWith("map-")).count());
        assertEquals(1, dag.getTasks().keySet().stream()
                .filter("reduce"::equals).count());
    }

    @Test
    public void mapTasksHaveNoDependencies() {
        DAG dag = parser.parse(specification(4));

        for (int index = 1; index <= 4; index++) {
            assertEquals(Set.of(), dag.getDependencies("map-%03d".formatted(index)));
        }
    }

    @Test
    public void reduceDependsOnEveryMapTask() {
        DAG dag = parser.parse(specification(4));

        assertEquals(Set.of("map-001", "map-002", "map-003", "map-004"),
                dag.getDependencies("reduce"));
    }

    @Test
    public void assignsMapAndReduceResourcesAndCommands() {
        DAG dag = parser.parse(specification(3));

        for (int index = 1; index <= 3; index++) {
            DAGTask map = dag.getTask("map-%03d".formatted(index));
            assertEquals(2, map.getCpuRequested());
            assertEquals(4, map.getMemRequested());
            assertEquals("python mapper.py", map.getCommand());
        }

        DAGTask reduce = dag.getTask("reduce");
        assertEquals(4, reduce.getCpuRequested());
        assertEquals(8, reduce.getMemRequested());
        assertEquals("python reducer.py", reduce.getCommand());
    }

    @Test
    public void mapIdsAreDeterministic() {
        MapReduceJobSpecification specification = specification(4);

        assertEquals(
                IntStream.rangeClosed(1, 4)
                        .mapToObj(index -> "map-%03d".formatted(index))
                        .collect(java.util.stream.Collectors.toList()),
                parser.parse(specification).getTasks().keySet().stream()
                        .filter(taskId -> taskId.startsWith("map-"))
                        .toList());
    }

    @Test
    public void registryIncludesMapReduceParserAndPreservesInputOnSpecification() {
        MapReduceJobSpecification specification = specification(2);
        DAG dag = new JobParserRegistry().parse(specification);

        assertEquals("logs/", specification.getInput());
        assertEquals(3, dag.getTasks().size());
    }

    @Test
    public void rejectsInvalidPartitionsAndResources() {
        assertThrows(IllegalArgumentException.class, () -> specification(0));
        assertThrows(IllegalArgumentException.class, () -> specification(-1));
        assertThrows(IllegalArgumentException.class, () -> new MapReduceJobSpecification(
                "invalid-map-cpu", "logs", "logs/", "map", "reduce", 1, 0, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapReduceJobSpecification(
                "invalid-map-memory", "logs", "logs/", "map", "reduce", 1, 1, -1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapReduceJobSpecification(
                "invalid-reduce-cpu", "logs", "logs/", "map", "reduce", 1, 1, 1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapReduceJobSpecification(
                "invalid-reduce-memory", "logs", "logs/", "map", "reduce", 1, 1, 1, 1, 0));
    }

    @Test
    public void rejectsBlankCommandsAndInput() {
        assertThrows(IllegalArgumentException.class, () -> new MapReduceJobSpecification(
                "blank-mapper", "logs", "logs/", " ", "reduce", 1, 1, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapReduceJobSpecification(
                "blank-reducer", "logs", "logs/", "map", " ", 1, 1, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new MapReduceJobSpecification(
                "blank-input", "logs", " ", "map", "reduce", 1, 1, 1, 1, 1));
    }

    private static MapReduceJobSpecification specification(int partitions) {
        return new MapReduceJobSpecification(
                "log-analysis-job",
                "log-analysis",
                "logs/",
                "python mapper.py",
                "python reducer.py",
                partitions,
                2,
                4,
                4,
                8);
    }
}
