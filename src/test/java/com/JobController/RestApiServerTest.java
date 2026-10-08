package com.JobController;

import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.JobController.dag.DAG;
import com.JobController.dag.DAGManager;
import com.JobController.dag.DAGRun;
import com.JobController.job.parser.CustomDAGJobParser;
import com.JobController.job.spec.CustomDAGJobSpecification;
import com.JobController.job.spec.CustomDAGTaskSpecification;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RestApiServerTest {
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Test
    public void acceptsCustomDAGAndReturnsGraphRunAndTaskDetails() throws Exception {
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();
            HttpResponse<String> response = post(server.port(), """
                    {
                      "jobType":"CUSTOM_DAG",
                      "name":"training-workflow",
                      "tasks":[
                        {"id":"download","command":"python download.py","cpu":2,"memory":4,"dependsOn":[]},
                        {"id":"clean","command":"python clean.py","cpu":2,"memory":4,"dependsOn":["download"]},
                        {"id":"train","command":"python train.py","cpu":8,"memory":16,"dependsOn":["clean"]},
                        {"id":"test","command":"python test.py","cpu":4,"memory":8,"dependsOn":["clean"]},
                        {"id":"deploy","command":"python deploy.py","cpu":2,"memory":4,"dependsOn":["train","test"]}
                      ]
                    }
                    """);

            assertEquals(202, response.statusCode());
            assertTrue(response.body().contains("\"name\":\"training-workflow\""));
            assertTrue(response.body().contains("\"id\":\"deploy\""));
            assertTrue(response.body().contains("\"dependsOn\":[\"train\",\"test\"]"));
            assertTrue(response.body().contains("\"jobId\":"));
            Struct.Builder responseJson = Struct.newBuilder();
            JsonFormat.parser().merge(response.body(), responseJson);
            assertTrue(responseJson.build().getFieldsOrThrow("graph")
                    .getStringValue().contains("-> [BLOCKED] train"));

            String runId = extractRunId(response.body());
            HttpResponse<String> status = get(server.port(), "/api/dags/runs/" + runId);
            assertEquals(200, status.statusCode());
            assertTrue(status.body().contains("\"runId\":\"" + runId + "\""));
            assertTrue(status.body().contains("\"id\":\"download\""));
        }
    }

    @Test
    public void rejectsMalformedAndStructurallyInvalidDags() throws Exception {
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();

            HttpResponse<String> wrongType = post(server.port(), """
                    {"jobType":"BATCH","name":"batch","tasks":[]}
                    """);
            assertEquals(400, wrongType.statusCode());

            HttpResponse<String> missingDependency = post(server.port(), """
                    {"jobType":"CUSTOM_DAG","name":"broken","tasks":[
                      {"id":"B","command":"run","cpu":1,"memory":1,"dependsOn":["missing"]}
                    ]}
                    """);
            assertEquals(400, missingDependency.statusCode());
            assertTrue(missingDependency.body().contains("unknown task"));

            HttpResponse<String> cycle = post(server.port(), """
                    {"jobType":"CUSTOM_DAG","name":"cycle","tasks":[
                      {"id":"A","command":"run-a","cpu":1,"memory":1,"dependsOn":["B"]},
                      {"id":"B","command":"run-b","cpu":1,"memory":1,"dependsOn":["A"]}
                    ]}
                    """);
            assertEquals(400, cycle.statusCode());
            assertTrue(cycle.body().contains("cycle"));
        }
    }

    @Test
    public void existingSingleJobEndpointRemainsAvailable() throws Exception {
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();

            HttpResponse<String> response = post(server.port(), "/api/jobs", """
                    {"owner":"alice","description":"single job","cpuRequested":1,"memRequested":2}
                    """);

            assertEquals(202, response.statusCode());
            assertTrue(response.body().contains("\"description\":\"single job\""));
        }
    }

    @Test
    public void exposesWorkerMembershipThroughDebugEndpoint() throws Exception {
        String workerUuid = UUID.randomUUID().toString();
        Discover.membershipManager.registerWorker(workerUuid, "1");
        Discover.membershipManager.processHeartbeat(workerUuid, "1", 1);
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();
            HttpResponse<String> response = get(server.port(), "/api/workers");

            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"workerUUID\":\"" + workerUuid + "\""));
            assertTrue(response.body().contains("\"state\":\"AVAILABLE\""));
            assertTrue(response.body().contains("\"heartbeatSequence\":1"));
            assertTrue(response.body().contains("\"incarnationId\":\"1\""));
            assertTrue(response.body().contains("\"lastHeartbeatMillis\":"));
        }
    }

    @Test
    public void acceptsAndReturnsDiskAndGpuRequests() throws Exception {
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();
            HttpResponse<String> jobResponse = post(server.port(), "/api/jobs", """
                    {"owner":"alice","description":"gpu job","cpuRequested":2,"memRequested":4,
                     "diskMbRequested":2048,"gpuCountRequested":1,"gpuMemoryMbPerGpu":8192}
                    """);
            assertEquals(202, jobResponse.statusCode());
            assertTrue(jobResponse.body().contains("\"diskMbRequested\":\"2048\"")
                    || jobResponse.body().contains("\"diskMbRequested\":2048"));
            assertTrue(jobResponse.body().contains("\"gpuCountRequested\":1"));
            assertTrue(jobResponse.body().contains("\"gpuMemoryMbPerGpu\":\"8192\"")
                    || jobResponse.body().contains("\"gpuMemoryMbPerGpu\":8192"));

            HttpResponse<String> dagResponse = post(server.port(), """
                    {"jobType":"CUSTOM_DAG","name":"gpu-dag","tasks":[
                      {"id":"train","command":"python train.py","cpu":2,"memory":4,
                       "diskMb":1024,"gpuCount":1,"gpuMemoryMbPerGpu":8192,"dependsOn":[]}
                    ]}
                    """);
            assertEquals(202, dagResponse.statusCode());
            assertTrue(dagResponse.body().contains("\"diskMb\":1024"));
            assertTrue(dagResponse.body().contains("\"gpuCount\":1"));
            assertTrue(dagResponse.body().contains("\"gpuMemoryMbPerGpu\":8192"));
        }
    }

    @Test
    public void rejectsInvalidGpuAndDiskRequests() throws Exception {
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();

            HttpResponse<String> missingGpu = post(server.port(), """
                    {"jobType":"CUSTOM_DAG","name":"invalid-gpu","tasks":[
                      {"id":"train","command":"run","cpu":1,"memory":1,
                       "gpuMemoryMbPerGpu":1024}
                    ]}
                    """);
            assertEquals(400, missingGpu.statusCode());

            HttpResponse<String> negativeDisk = post(server.port(), """
                    {"owner":"alice","description":"invalid disk","cpuRequested":1,
                     "memRequested":1,"diskMbRequested":-1}
                    """);
            assertEquals(400, negativeDisk.statusCode());
        }
    }

    @Test
    public void acceptsPlacementConstraintsAndReturnsAttemptHistory() throws Exception {
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();
            HttpResponse<String> response = post(server.port(), """
                    {"jobType":"CUSTOM_DAG","name":"placement-dag","tasks":[
                      {"id":"train","command":"python train.py","cpu":2,"memory":4,
                       "placementConstraints":[
                         {"key":"architecture","operator":"EQUALS","value":"x86_64"},
                         {"key":"disk","operator":"NOT_EQUALS","value":"hdd"}
                       ]}
                    ]}
                    """);

            assertEquals(202, response.statusCode());
            assertTrue(response.body().contains("\"placementConstraints\""));
            assertTrue(response.body().contains("\"operator\":\"NOT_EQUALS\""));
            assertTrue(response.body().contains("\"currentAttempt\":0"));
            assertTrue(response.body().contains("\"attemptNumber\":0"));
            assertTrue(response.body().contains("\"attemptId\":"));
        }
    }

    @Test
    public void rejectsInvalidPlacementConstraint() throws Exception {
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();
            HttpResponse<String> response = post(server.port(), """
                    {"jobType":"CUSTOM_DAG","name":"invalid-placement","tasks":[
                      {"id":"task","command":"run","cpu":1,"memory":1,
                       "placementConstraints":[
                         {"key":"architecture","operator":"CONTAINS","value":"x86"}
                       ]}
                    ]}
                    """);

            assertEquals(400, response.statusCode());
            assertTrue(response.body().contains("EQUALS or NOT_EQUALS"));
        }
    }

    @Test
    public void cancelsDagRunAndReturnsCurrentState() throws Exception {
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();
            HttpResponse<String> created = post(server.port(), """
                    {"jobType":"CUSTOM_DAG","name":"cancel-me","timeoutMillis":5000,
                     "tasks":[{"id":"A","command":"run","cpu":1,"memory":1,
                               "timeoutMillis":1000,"dependsOn":[]}]}
                    """);
            assertEquals(202, created.statusCode());
            String runId = extractRunId(created.body());
            String dagId = extractJsonField(created.body(), "dagId");
            assertTrue(created.body().contains("\"timeoutMillis\":5000"));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + server.port()
                            + "/api/dags/" + dagId + "/runs/" + runId))
                    .DELETE()
                    .build();
            HttpResponse<String> cancelled =
                    HTTP.send(request, HttpResponse.BodyHandlers.ofString());

            assertEquals(200, cancelled.statusCode());
            assertTrue(cancelled.body().contains("\"state\":\"CANCELLED\""));
            assertTrue(cancelled.body().contains("\"cancellationReason\":\"USER_REQUEST\""));
            assertTrue(cancelled.body().contains("\"cancellationConfirmed\":true"));
        }
    }

    @Test
    public void rejectsInvalidTimeoutValues() throws Exception {
        FakeJobOperations operations = new FakeJobOperations();
        try (RestApiServer server = new RestApiServer("127.0.0.1", 0, operations)) {
            server.start();
            HttpResponse<String> response = post(server.port(), """
                    {"jobType":"CUSTOM_DAG","name":"invalid-timeout","timeoutMillis":0,
                     "tasks":[{"id":"A","command":"run","cpu":1,"memory":1,"dependsOn":[]}]}
                    """);
            assertEquals(400, response.statusCode());
            assertTrue(response.body().contains("timeoutMillis"));
        }
    }

    private static HttpResponse<String> post(int port, String body) throws Exception {
        return post(port, "/api/dags", body);
    }

    private static HttpResponse<String> post(int port, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + path))
                .GET()
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static String extractRunId(String body) {
        Matcher matcher = Pattern.compile("\"runId\":\"([^\"]+)\"").matcher(body);
        if (!matcher.find()) {
            throw new AssertionError("Response did not include runId: " + body);
        }
        return matcher.group(1);
    }

    private static String extractJsonField(String body, String field) {
        Matcher matcher = Pattern.compile("\"" + field + "\":\"([^\"]+)\"").matcher(body);
        if (!matcher.find()) {
            throw new AssertionError("Response did not include " + field + ": " + body);
        }
        return matcher.group(1);
    }

    private static final class FakeJobOperations implements RestApiServer.JobOperations {
        private final AtomicLong nextJobId = new AtomicLong();
        private final DAGManager manager = new DAGManager(new FakeSubmissionService());

        private final class FakeSubmissionService implements com.JobController.JobSubmissionService {
            @Override
            public long submit(Job job) {
                return nextJobId.incrementAndGet();
            }

            @Override
            public boolean cancel(long jobId) {
                return true;
            }
        }

        @Override
        public Optional<Job> submitJob(
                String owner, String description, int cpuRequested, int memRequested,
                long diskMbRequested, int gpuCountRequested, long gpuMemoryMbPerGpu,
                List<com.JobController.PlacementConstraint> constraints) {
            return Optional.of(Job.newBuilder()
                    .setId(Math.toIntExact(nextJobId.incrementAndGet()))
                    .setOwner(owner)
                    .setDescription(description)
                    .setCpuRequested(cpuRequested)
                    .setMemRequested(memRequested)
                    .setDiskMbRequested(diskMbRequested)
                    .setGpuCountRequested(gpuCountRequested)
                    .setGpuMemoryMbPerGpu(gpuMemoryMbPerGpu)
                    .addAllPlacementConstraints(constraints)
                    .setState(JobState.PENDING)
                    .build());
        }

        @Override
        public List<Job> listJobs() {
            return List.of();
        }

        @Override
        public Optional<Job> findJob(int id) {
            return Optional.empty();
        }

        @Override
        public RestApiServer.CancellationResult cancelJob(int id) {
            return new RestApiServer.CancellationResult(
                    RestApiServer.CancellationOutcome.NOT_FOUND, null);
        }

        @Override
        public RestApiServer.DAGSubmission submitDAG(
                String name, List<CustomDAGTaskSpecification> tasks, Long dagTimeoutMillis) {
            String dagId = "api-test-" + UUID.randomUUID();
            DAG dag = new CustomDAGJobParser().parse(
                    new CustomDAGJobSpecification(dagId, name, tasks, dagTimeoutMillis));
            manager.registerDAG(dag);
            DAGRun run = manager.startRun(dag.getId());
            return new RestApiServer.DAGSubmission(dag, run);
        }

        @Override
        public Optional<RestApiServer.DAGSubmission> findDAGRun(String runId) {
            return manager.findRun(runId).flatMap(run -> manager.findDAG(run.getDagId())
                    .map(dag -> new RestApiServer.DAGSubmission(dag, run)));
        }

        @Override
        public Optional<RestApiServer.DAGCancellation> cancelDAGRun(String dagId, String runId) {
            Optional<DAGRun> run = manager.findRun(runId);
            if (run.isEmpty() || !run.get().getDagId().equals(dagId)) {
                return Optional.empty();
            }
            boolean confirmed = run.get().getState() != com.JobController.dag.DAGRunState.RUNNING
                    || manager.cancelRun(runId);
            return manager.findDAG(dagId)
                    .map(dag -> new RestApiServer.DAGCancellation(
                            new RestApiServer.DAGSubmission(dag, run.get()), confirmed));
        }
    }
}
