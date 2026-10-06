package com.JobController.dag;

import com.JobController.Job;
import com.JobController.JobState;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertTrue;

public class DAGGraphPrinterTest {
    @Test
    public void rendersStandaloneJobAsOneNodeDag() {
        Job job = Job.newBuilder()
                .setId(42)
                .setDescription("report job")
                .setCpuRequested(2)
                .setMemRequested(4)
                .setState(JobState.PENDING)
                .build();

        String graph = DAGGraphPrinter.renderStandaloneJob(job);

        assertTrue(graph.contains("Job DAG (standalone submission)"));
        assertTrue(graph.contains("[PENDING] Job #42: report job (CPU=2, memory=4)"));
        assertTrue(graph.contains("(no dependencies or dependents)"));
    }

    @Test
    public void rendersDagEdgesTaskStatesAndJobIdsDeterministically() {
        DAG dag = new DAG("training", "training pipeline");
        dag.addTask(new DAGTask("preprocess", 1, 1, Set.of()));
        dag.addTask(new DAGTask("augment", 1, 1, Set.of("preprocess")));
        dag.addTask(new DAGTask("validate", 1, 1, Set.of("preprocess")));
        dag.addTask(new DAGTask("train", 1, 1, Set.of("augment", "validate")));
        DAGRun run = new DAGRun("run-1", dag);
        run.setTaskState("preprocess", TaskState.SUBMITTED);
        run.setJobId("preprocess", 17);

        String graph = DAGGraphPrinter.renderRun(dag, run);

        assertTrue(graph.contains("DAG \"training pipeline\" (training) / run run-1 [RUNNING]"));
        assertTrue(graph.contains("[SUBMITTED] preprocess (job #17)"));
        assertTrue(graph.contains("-> [BLOCKED] augment"));
        assertTrue(graph.contains("-> [BLOCKED] validate"));
        assertTrue(graph.contains("-> [BLOCKED] train"));
        assertTrue(graph.indexOf("\n  [SUBMITTED] preprocess")
                < graph.indexOf("\n  [BLOCKED] augment"));
        assertTrue(graph.indexOf("\n  [BLOCKED] augment")
                < graph.indexOf("\n  [BLOCKED] train"));
    }
}
