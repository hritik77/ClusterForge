package com.JobController.job.parser;

import com.JobController.dag.DAG;
import com.JobController.dag.DAGTask;
import com.JobController.job.JobType;
import com.JobController.job.spec.ParameterSweepJobSpecification;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ParameterSweepJobParserTest {
    private final ParameterSweepJobParser parser = new ParameterSweepJobParser();

    @Test
    public void oneValueProducesOneTask() {
        DAG dag = parser.parse(specification(Map.of("A", List.of("1"))));

        assertEquals(1, dag.getTasks().size());
        assertEquals("python train.py --A 1", dag.getTask("experiment-001").getCommand());
    }

    @Test
    public void twoParametersProduceCartesianProduct() {
        DAG dag = parser.parse(specification(parameters(
                "B", List.of("X", "Y"),
                "A", List.of("1", "2"))));

        assertEquals(4, dag.getTasks().size());
        assertEquals(List.of(
                "python train.py --A 1 --B X",
                "python train.py --A 1 --B Y",
                "python train.py --A 2 --B X",
                "python train.py --A 2 --B Y"),
                commands(dag));
    }

    @Test
    public void threeParametersProduceTwelveTasks() {
        DAG dag = parser.parse(specification(parameters(
                "C", List.of("P", "Q"),
                "A", List.of("1", "2", "3"),
                "B", List.of("X", "Y"))));

        assertEquals(12, dag.getTasks().size());
    }

    @Test
    public void everyCombinationAppearsExactlyOnce() {
        DAG dag = parser.parse(specification(parameters(
                "learningRate", List.of("0.001", "0.0001"),
                "batchSize", List.of("32", "64"))));

        assertEquals(Set.of(
                "python train.py --batchSize 32 --learningRate 0.001",
                "python train.py --batchSize 64 --learningRate 0.001",
                "python train.py --batchSize 32 --learningRate 0.0001",
                "python train.py --batchSize 64 --learningRate 0.0001"),
                Set.copyOf(commands(dag)));
        assertEquals(4, commands(dag).stream().distinct().count());
    }

    @Test
    public void allGeneratedTasksAreIndependent() {
        DAG dag = parser.parse(specification(parameters(
                "A", List.of("1", "2"),
                "B", List.of("X", "Y"))));

        assertTrue(dag.getTasks().values().stream()
                .allMatch(task -> task.getDependencies().isEmpty()));
        assertTrue(dag.getTasks().keySet().stream()
                .allMatch(taskId -> dag.getDependents(taskId).isEmpty()));
    }

    @Test
    public void parameterOrderingAndTaskIdsAreDeterministic() {
        ParameterSweepJobSpecification specification = specification(parameters(
                "optimizer", List.of("adam"),
                "learningRate", List.of("0.1"),
                "batchSize", List.of("16", "32")));
        DAG first = parser.parse(specification);
        DAG second = parser.parse(specification);

        assertEquals(List.of("experiment-001", "experiment-002"),
                first.getTasks().keySet().stream().toList());
        assertEquals(first.getTasks().keySet(), second.getTasks().keySet());
        assertEquals(List.of(
                "python train.py --batchSize 16 --learningRate 0.1 --optimizer adam",
                "python train.py --batchSize 32 --learningRate 0.1 --optimizer adam"),
                commands(first));
        assertEquals(commands(first), commands(second));
    }

    @Test
    public void copiesResourcesToEveryTask() {
        DAG dag = parser.parse(new ParameterSweepJobSpecification(
                "sweep-job",
                "training-sweep",
                "python train.py",
                Map.of("A", List.of("1", "2", "3")),
                6,
                12));

        for (DAGTask task : dag.getTasks().values()) {
            assertEquals(6, task.getCpuRequested());
            assertEquals(12, task.getMemRequested());
        }
    }

    @Test
    public void copiesDiskAndGpuRequestsToEveryExperiment() {
        DAG dag = parser.parse(new ParameterSweepJobSpecification(
                "gpu-sweep", "sweep", "python train.py", Map.of("A", List.of("1", "2")),
                2, 4, 2_048, 1, 8_192));

        for (DAGTask task : dag.getTasks().values()) {
            assertEquals(2_048, task.getDiskMbRequested());
            assertEquals(1, task.getGpuCountRequested());
            assertEquals(8_192, task.getGpuMemoryMbPerGpu());
        }
    }

    @Test
    public void rejectsEmptyParameterMap() {
        assertThrows(IllegalArgumentException.class,
                () -> specification(Map.of()));
    }

    @Test
    public void rejectsEmptyParameterValues() {
        assertThrows(IllegalArgumentException.class,
                () -> specification(Map.of("A", List.of())));
    }

    @Test
    public void rejectsBlankParameterNames() {
        Map<String, List<String>> parameters = new LinkedHashMap<>();
        parameters.put(" ", List.of("value"));

        assertThrows(IllegalArgumentException.class, () -> specification(parameters));
    }

    @Test
    public void rejectsNullParameterValuesAndInvalidSpecificationFields() {
        Map<String, List<String>> nullValues = new LinkedHashMap<>();
        nullValues.put("A", null);
        assertThrows(IllegalArgumentException.class, () -> specification(nullValues));

        Map<String, List<String>> nullElement = new LinkedHashMap<>();
        nullElement.put("A", java.util.Arrays.asList("1", null));
        assertThrows(IllegalArgumentException.class, () -> specification(nullElement));

        assertThrows(IllegalArgumentException.class,
                () -> new ParameterSweepJobSpecification(
                        "blank-command", "sweep", " ", Map.of("A", List.of("1")), 1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new ParameterSweepJobSpecification(
                        "zero-cpu", "sweep", "run", Map.of("A", List.of("1")), 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new ParameterSweepJobSpecification(
                        "zero-memory", "sweep", "run", Map.of("A", List.of("1")), 1, 0));
    }

    @Test
    public void specificationCopiesParameterMapAndValueLists() {
        List<String> values = new ArrayList<>(List.of("1", "2"));
        Map<String, List<String>> parameters = new LinkedHashMap<>();
        parameters.put("A", values);
        ParameterSweepJobSpecification specification = specification(parameters);

        values.add("3");
        parameters.put("B", List.of("X"));

        assertEquals(List.of("1", "2"), specification.getParameters().get("A"));
        assertEquals(Set.of("A"), specification.getParameters().keySet());
        assertThrows(UnsupportedOperationException.class,
                () -> specification.getParameters().put("outside", List.of("value")));
        assertThrows(UnsupportedOperationException.class,
                () -> specification.getParameters().get("A").add("outside"));
    }

    @Test
    public void registryIncludesParameterSweepParser() {
        DAG dag = new JobParserRegistry().parse(specification(Map.of("A", List.of("1", "2"))));

        assertEquals(JobType.PARAMETER_SWEEP, specification(Map.of("A", List.of("1"))).getJobType());
        assertEquals(2, dag.getTasks().size());
    }

    private static List<String> commands(DAG dag) {
        return dag.getTasks().values().stream().map(DAGTask::getCommand).toList();
    }

    private static ParameterSweepJobSpecification specification(
            Map<String, List<String>> parameters) {
        return new ParameterSweepJobSpecification(
                "sweep-job", "training-sweep", "python train.py", parameters, 2, 4);
    }

    private static Map<String, List<String>> parameters(
            String firstName, List<String> firstValues,
            String secondName, List<String> secondValues) {
        Map<String, List<String>> parameters = new LinkedHashMap<>();
        parameters.put(firstName, firstValues);
        parameters.put(secondName, secondValues);
        return parameters;
    }

    private static Map<String, List<String>> parameters(
            String firstName, List<String> firstValues,
            String secondName, List<String> secondValues,
            String thirdName, List<String> thirdValues) {
        Map<String, List<String>> parameters =
                parameters(firstName, firstValues, secondName, secondValues);
        parameters.put(thirdName, thirdValues);
        return parameters;
    }
}
