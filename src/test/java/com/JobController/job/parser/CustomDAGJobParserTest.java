package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.job.JobType;
import com.JobController.job.spec.CustomDAGJobSpecification;
import com.JobController.job.spec.CustomDAGTaskSpecification;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CustomDAGJobParserTest {
    private final CustomDAGJobParser parser = new CustomDAGJobParser();

    @Test
    public void parsesLinearDAG() {
        DAG dag = parser.parse(specification(
                task("A"),
                task("B", "A"),
                task("C", "B")));

        assertEquals(Set.of(), dag.getDependencies("A"));
        assertEquals(Set.of("A"), dag.getDependencies("B"));
        assertEquals(Set.of("B"), dag.getDependencies("C"));
    }

    @Test
    public void parsesDiamondDAG() {
        DAG dag = parser.parse(specification(
                task("A"),
                task("B", "A"),
                task("C", "A"),
                task("D", "B", "C")));

        assertEquals(Set.of(), dag.getDependencies("A"));
        assertEquals(Set.of("A"), dag.getDependencies("B"));
        assertEquals(Set.of("A"), dag.getDependencies("C"));
        assertEquals(Set.of("B", "C"), dag.getDependencies("D"));
    }

    @Test
    public void parsesMultipleRoots() {
        DAG dag = parser.parse(specification(
                task("A"),
                task("B"),
                task("C", "A", "B")));

        assertEquals(Set.of(), dag.getDependencies("A"));
        assertEquals(Set.of(), dag.getDependencies("B"));
        assertEquals(Set.of("A", "B"), dag.getDependencies("C"));
    }

    @Test
    public void parsesMultipleLeaves() {
        DAG dag = parser.parse(specification(
                task("A"),
                task("B", "A"),
                task("C", "A")));

        assertEquals(Set.of("B", "C"), dag.getDependents("A"));
        assertEquals(Set.of(), dag.getDependents("B"));
        assertEquals(Set.of(), dag.getDependents("C"));
    }

    @Test
    public void parsesSingleTaskDAG() {
        DAG dag = parser.parse(specification(task("only-task")));

        assertEquals(1, dag.getTasks().size());
        assertTrue(dag.hasTask("only-task"));
    }

    @Test
    public void rejectsDuplicateTaskIds() {
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse(specification(task("A"), task("A"))));
    }

    @Test
    public void rejectsMissingDependenciesThroughDagValidator() {
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse(specification(task("B", "missing"))));
    }

    @Test
    public void rejectsSelfDependency() {
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse(specification(task("A", "A"))));
    }

    @Test
    public void rejectsCyclesThroughDagValidator() {
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse(specification(
                        task("A", "B"),
                        task("B", "C"),
                        task("C", "A"))));
    }

    @Test
    public void propagatesResourcesAndCommands() {
        DAG dag = parser.parse(specification(
                task("download", "python download.py", 2, 4),
                task("train", "python train.py", 8, 16, "download")));

        DAGTask download = dag.getTask("download");
        assertEquals(2, download.getCpuRequested());
        assertEquals(4, download.getMemRequested());
        assertEquals("python download.py", download.getCommand());

        DAGTask train = dag.getTask("train");
        assertEquals(8, train.getCpuRequested());
        assertEquals(16, train.getMemRequested());
        assertEquals("python train.py", train.getCommand());
    }

    @Test
    public void preservesDependencyOrderAndMakesSpecificationCollectionsImmutable() {
        Set<String> dependencies = new LinkedHashSet<>(List.of("B", "A"));
        CustomDAGTaskSpecification dependent =
                task("C", "command", 1, 1, dependencies);
        dependencies.add("external");

        assertEquals(List.of("B", "A"), dependent.getDependencies().stream().toList());
        assertThrows(UnsupportedOperationException.class,
                () -> dependent.getDependencies().add("external"));

        List<CustomDAGTaskSpecification> tasks = new ArrayList<>();
        tasks.add(task("A"));
        CustomDAGJobSpecification specification =
                new CustomDAGJobSpecification("custom-job", "custom", tasks);
        tasks.add(task("B"));
        assertEquals(1, specification.getTasks().size());
        assertThrows(UnsupportedOperationException.class,
                () -> specification.getTasks().add(task("outside")));
    }

    @Test
    public void rejectsEmptyTaskListAndInvalidTaskFields() {
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse(specification(List.of())));
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse(specification(task(" ", "command", 1, 1))));
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse(specification(task("A", " ", 1, 1))));
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse(specification(task("A", "command", 0, 1))));
        assertThrows(IllegalArgumentException.class,
                () -> parser.parse(specification(task("A", "command", 1, 0))));
    }

    @Test
    public void registryIncludesCustomDagParser() {
        CustomDAGJobSpecification specification = specification(task("A"));

        DAG dag = new JobParserRegistry().parse(specification);

        assertEquals(JobType.CUSTOM_DAG, specification.getJobType());
        assertEquals(1, dag.getTasks().size());
    }

    private static CustomDAGJobSpecification specification(
            CustomDAGTaskSpecification... tasks) {
        return specification(List.of(tasks));
    }

    private static CustomDAGJobSpecification specification(
            List<CustomDAGTaskSpecification> tasks) {
        return new CustomDAGJobSpecification("custom-dag-job", "custom-workflow", tasks);
    }

    private static CustomDAGTaskSpecification task(String id, String... dependencies) {
        return task(id, "python " + id + ".py", 1, 1, dependencies);
    }

    private static CustomDAGTaskSpecification task(
            String id, String command, int cpu, int memory, String... dependencies) {
        return task(id, command, cpu, memory, Set.of(dependencies));
    }

    private static CustomDAGTaskSpecification task(
            String id, String command, int cpu, int memory, Set<String> dependencies) {
        return new CustomDAGTaskSpecification(id, command, cpu, memory, dependencies);
    }
}
