package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.dag.DAGValidator;
import com.JobController.job.JobType;
import com.JobController.job.spec.PipelineJobSpecification;
import com.JobController.job.spec.PipelineTaskSpecification;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PipelineJobParserTest {
    private final PipelineJobParser parser = new PipelineJobParser();

    @Test
    public void parsesLinearPipelineWithExactDependencies() {
        DAG dag = parser.parse(specification(
                task("A"),
                task("B", "A"),
                task("C", "B")));

        assertEquals(Set.of(), dag.getDependencies("A"));
        assertEquals(Set.of("A"), dag.getDependencies("B"));
        assertEquals(Set.of("B"), dag.getDependencies("C"));
        DAGValidator.validate(dag);
    }

    @Test
    public void parsesDiamondPipeline() {
        DAG dag = parser.parse(specification(
                task("A"),
                task("B", "A"),
                task("C", "A"),
                task("D", "B", "C")));

        assertEquals(Set.of(), dag.getDependencies("A"));
        assertEquals(Set.of("A"), dag.getDependencies("B"));
        assertEquals(Set.of("A"), dag.getDependencies("C"));
        assertEquals(Set.of("B", "C"), dag.getDependencies("D"));
        DAGValidator.validate(dag);
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
        DAGValidator.validate(dag);
    }

    @Test
    public void parsesSingleTaskPipeline() {
        DAG dag = parser.parse(specification(task("only-task")));

        assertEquals(1, dag.getTasks().size());
        assertTrue(dag.hasTask("only-task"));
        DAGValidator.validate(dag);
    }

    @Test
    public void rejectsDuplicateTaskIds() {
        assertThrows(IllegalArgumentException.class, () -> parser.parse(specification(
                task("A"),
                task("A"))));
    }

    @Test
    public void dagValidatorRejectsMissingDependenciesAndCycles() {
        DAG missingDependency = parser.parse(specification(task("B", "missing")));
        assertThrows(IllegalArgumentException.class,
                () -> DAGValidator.validate(missingDependency));

        DAG cycle = parser.parse(specification(task("A", "B"), task("B", "A")));
        assertThrows(IllegalArgumentException.class, () -> DAGValidator.validate(cycle));
    }

    @Test
    public void copiesTaskResourcesAndCommands() {
        DAG dag = parser.parse(specification(
                task("download", "python download.py", 2, 4),
                task("train", "python train.py", 8, 16, "download")));

        assertEquals(2, dag.getTask("download").getCpuRequested());
        assertEquals(4, dag.getTask("download").getMemRequested());
        assertEquals("python download.py", dag.getTask("download").getCommand());
        assertEquals(8, dag.getTask("train").getCpuRequested());
        assertEquals(16, dag.getTask("train").getMemRequested());
        assertEquals("python train.py", dag.getTask("train").getCommand());
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
    public void specificationAndTaskDependenciesAreImmutableCopies() {
        Set<String> dependencies = new LinkedHashSet<>();
        dependencies.add("A");
        PipelineTaskSpecification dependent =
                new PipelineTaskSpecification("B", "command", 1, 1, dependencies);
        dependencies.add("changed-after-construction");
        assertEquals(Set.of("A"), dependent.getDependencies());
        assertThrows(UnsupportedOperationException.class,
                () -> dependent.getDependencies().add("outside"));

        List<PipelineTaskSpecification> tasks = new ArrayList<>();
        tasks.add(task("A"));
        PipelineJobSpecification specification =
                new PipelineJobSpecification("pipeline", "pipeline", tasks);
        tasks.add(task("B"));
        assertEquals(1, specification.getTasks().size());
        assertThrows(UnsupportedOperationException.class,
                () -> specification.getTasks().add(task("outside")));
    }

    @Test
    public void registryIncludesPipelineParser() {
        PipelineJobSpecification specification = specification(task("A"));

        DAG dag = new JobParserRegistry().parse(specification);

        assertEquals(JobType.PIPELINE, specification.getJobType());
        assertEquals(1, dag.getTasks().size());
    }

    private static PipelineJobSpecification specification(PipelineTaskSpecification... tasks) {
        return specification(List.of(tasks));
    }

    private static PipelineJobSpecification specification(
            List<PipelineTaskSpecification> tasks) {
        return new PipelineJobSpecification("pipeline-job", "training-pipeline", tasks);
    }

    private static PipelineTaskSpecification task(String id, String... dependencies) {
        return task(id, "python " + id + ".py", 1, 1, dependencies);
    }

    private static PipelineTaskSpecification task(
            String id, String command, int cpu, int memory, String... dependencies) {
        return new PipelineTaskSpecification(id, command, cpu, memory, Set.of(dependencies));
    }
}
