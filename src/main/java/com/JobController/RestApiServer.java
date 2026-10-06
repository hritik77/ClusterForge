package com.JobController;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.JobController.dag.DAG;
import com.JobController.dag.DAGGraphPrinter;
import com.JobController.dag.DAGRun;
import com.JobController.job.spec.CustomDAGTaskSpecification;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class RestApiServer implements AutoCloseable {
    private static final int MAX_REQUEST_BYTES=1_048_576;
    private static final JsonFormat.Printer JSON_PRINTER =
            JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();

    enum CancellationOutcome {
        CANCELLED,
        ALREADY_CANCELLED,
        NOT_CANCELLABLE,
        UNAVAILABLE,
        NOT_FOUND
    }

    record CancellationResult(CancellationOutcome outcome, Job job) {}

    record DAGSubmission(DAG dag, DAGRun run) {}

    interface JobOperations {
        Optional<Job> submitJob(String owner, String description, int cpuRequested, int memRequested);
        List<Job> listJobs();
        Optional<Job> findJob(int id);
        CancellationResult cancelJob(int id);
        DAGSubmission submitDAG(String name, List<CustomDAGTaskSpecification> tasks);
        Optional<DAGSubmission> findDAGRun(String runId);
    }

    private final HttpServer server;
    private final ExecutorService executor=Executors.newFixedThreadPool(
            Math.min(8, Math.max(2, Runtime.getRuntime().availableProcessors())));

    RestApiServer(String host, int port, JobOperations jobs) throws IOException {
        server=HttpServer.create(
                new InetSocketAddress(InetAddress.getByName(host), port), 0);
        server.createContext("/api/jobs", exchange -> handleJobs(exchange, jobs));
        server.createContext("/api/dags", exchange -> handleDags(exchange, jobs));
        server.setExecutor(executor);
    }

    void start() {
        server.start();
    }

    int port() {
        return server.getAddress().getPort();
    }

    private static void handleJobs(HttpExchange exchange, JobOperations jobs) throws IOException {
        try {
            String path=exchange.getRequestURI().getPath();
            if (path.equals("/api/jobs") || path.equals("/api/jobs/")) {
                handleJobCollection(exchange, jobs);
                return;
            }
            if (!path.startsWith("/api/jobs/")) {
                sendError(exchange, 404, "Route not found");
                return;
            }

            String idPath=path.substring("/api/jobs/".length());
            if (idPath.isEmpty() || idPath.contains("/")) {
                sendError(exchange, 404, "Route not found");
                return;
            }

            int id;
            try {
                id=Integer.parseInt(idPath);
            } catch (NumberFormatException e) {
                sendError(exchange, 400, "Job ID must be a positive integer");
                return;
            }
            if (id<=0) {
                sendError(exchange, 400, "Job ID must be a positive integer");
                return;
            }

            switch (exchange.getRequestMethod()) {
                case "GET" -> getJob(exchange, jobs, id);
                case "DELETE" -> deleteJob(exchange, jobs, id);
                default -> methodNotAllowed(exchange, "GET, DELETE");
            }
        } finally {
            exchange.close();
        }
    }

    private static void handleDags(HttpExchange exchange, JobOperations jobs) throws IOException {
        try {
            String path=exchange.getRequestURI().getPath();
            if (path.equals("/api/dags") || path.equals("/api/dags/")) {
                if (!exchange.getRequestMethod().equals("POST")) {
                    methodNotAllowed(exchange, "POST");
                    return;
                }
                submitDAG(exchange, jobs);
                return;
            }

            String runPrefix="/api/dags/runs/";
            String runId=path.startsWith(runPrefix) ? path.substring(runPrefix.length()) : "";
            if (!runId.isEmpty() && !runId.contains("/")) {
                if (!exchange.getRequestMethod().equals("GET")) {
                    methodNotAllowed(exchange, "GET");
                    return;
                }
                getDAGRun(exchange, jobs, runId);
                return;
            }
            sendError(exchange, 404, "Route not found");
        } finally {
            exchange.close();
        }
    }

    private static void handleJobCollection(HttpExchange exchange, JobOperations jobs)
            throws IOException {
        switch (exchange.getRequestMethod()) {
            case "POST" -> submitJob(exchange, jobs);
            case "GET" -> sendJobs(exchange, jobs.listJobs());
            default -> methodNotAllowed(exchange, "GET, POST");
        }
    }

    private static void submitJob(HttpExchange exchange, JobOperations jobs) throws IOException {
        if (!isJsonRequest(exchange)) {
            sendError(exchange, 415, "Content-Type must be application/json");
            return;
        }
        byte[] body=readRequestBody(exchange);
        if (body==null) {
            return;
        }

        Job.Builder request=Job.newBuilder();
        try {
            JsonFormat.parser().merge(new String(body, StandardCharsets.UTF_8), request);
        } catch (InvalidProtocolBufferException e) {
            sendError(exchange, 400, "Malformed job JSON: " + e.getMessage());
            return;
        }

        Job input=request.build();
        if (input.getId()!=0 || input.getState()!=JobState.PENDING) {
            sendError(exchange, 400, "Job ID and state are assigned by the controller");
            return;
        }
        if (input.getOwner().isBlank() || input.getDescription().isBlank()) {
            sendError(exchange, 400, "owner and description are required");
            return;
        }
        if (input.getCpuRequested()<=0 || input.getMemRequested()<=0) {
            sendError(exchange, 400, "cpuRequested and memRequested must be positive");
            return;
        }

        Optional<Job> submitted=jobs.submitJob(input.getOwner(), input.getDescription(),
                input.getCpuRequested(), input.getMemRequested());
        if (submitted.isEmpty()) {
            sendError(exchange, 503, "Job queue is full; the job was not accepted");
            return;
        }
        sendJob(exchange, 202, submitted.get());
    }

    private static void submitDAG(HttpExchange exchange, JobOperations jobs) throws IOException {
        if (!isJsonRequest(exchange)) {
            sendError(exchange, 415, "Content-Type must be application/json");
            return;
        }
        byte[] body=readRequestBody(exchange);
        if (body==null) {
            return;
        }

        Struct.Builder request=Struct.newBuilder();
        try {
            JsonFormat.parser().merge(new String(body, StandardCharsets.UTF_8), request);
        } catch (InvalidProtocolBufferException e) {
            sendError(exchange, 400, "Malformed DAG JSON: " + e.getMessage());
            return;
        }

        try {
            Struct input=request.build();
            String jobType=stringField(input, "jobType");
            if (!jobType.equals("CUSTOM_DAG")) {
                throw new IllegalArgumentException("jobType must be CUSTOM_DAG");
            }
            String name=stringField(input, "name");
            DAGSubmission submission=jobs.submitDAG(name, parseDAGTasks(input));
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.getResponseHeaders().set(
                    "Location", "/api/dags/runs/" + submission.run().getRunId());
            send(exchange, 202, jsonDAGSubmission(submission));
        } catch (IllegalArgumentException e) {
            sendError(exchange, 400, e.getMessage());
        } catch (IllegalStateException e) {
            sendError(exchange, 503, "DAG could not be submitted: " + e.getMessage());
        }
    }

    private static List<CustomDAGTaskSpecification> parseDAGTasks(Struct input) {
        Value tasksValue=input.getFieldsMap().get("tasks");
        if (tasksValue == null || tasksValue.getKindCase() != Value.KindCase.LIST_VALUE) {
            throw new IllegalArgumentException("tasks must be a JSON array");
        }

        List<CustomDAGTaskSpecification> tasks=new ArrayList<>();
        for (Value taskValue : tasksValue.getListValue().getValuesList()) {
            if (taskValue.getKindCase() != Value.KindCase.STRUCT_VALUE) {
                throw new IllegalArgumentException("Each task must be a JSON object");
            }
            Struct task=taskValue.getStructValue();
            String id=stringField(task, "id");
            String command=stringField(task, "command");
            int cpu=intField(task, "cpu");
            int memory=intField(task, "memory");
            Set<String> dependencies=stringArrayField(task, "dependsOn");
            tasks.add(new CustomDAGTaskSpecification(id, command, cpu, memory, dependencies));
        }
        return List.copyOf(tasks);
    }

    private static String stringField(Struct object, String field) {
        Value value=object.getFieldsMap().get(field);
        if (value == null || value.getKindCase() != Value.KindCase.STRING_VALUE) {
            throw new IllegalArgumentException(field + " must be a string");
        }
        String result=value.getStringValue();
        if (result.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return result;
    }

    private static int intField(Struct object, String field) {
        Value value=object.getFieldsMap().get(field);
        if (value == null || value.getKindCase() != Value.KindCase.NUMBER_VALUE) {
            throw new IllegalArgumentException(field + " must be a positive integer");
        }
        double number=value.getNumberValue();
        if (!Double.isFinite(number) || number < 1 || number > Integer.MAX_VALUE
                || number != Math.rint(number)) {
            throw new IllegalArgumentException(field + " must be a positive integer");
        }
        return (int) number;
    }

    private static Set<String> stringArrayField(Struct object, String field) {
        Value value=object.getFieldsMap().get(field);
        if (value == null) {
            return Set.of();
        }
        if (value.getKindCase() != Value.KindCase.LIST_VALUE) {
            throw new IllegalArgumentException(field + " must be a string array");
        }
        Set<String> strings=new LinkedHashSet<>();
        for (Value item : value.getListValue().getValuesList()) {
            if (item.getKindCase() != Value.KindCase.STRING_VALUE) {
                throw new IllegalArgumentException(field + " must contain only strings");
            }
            strings.add(item.getStringValue());
        }
        return strings;
    }

    private static void getDAGRun(
            HttpExchange exchange, JobOperations jobs, String runId) throws IOException {
        Optional<DAGSubmission> submission=jobs.findDAGRun(runId);
        if (submission.isEmpty()) {
            sendError(exchange, 404, "DAG run not found");
            return;
        }
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        send(exchange, 200, jsonDAGSubmission(submission.get()));
    }

    private static String jsonDAGSubmission(DAGSubmission submission) {
        Struct.Builder response=Struct.newBuilder()
                .putFields("dagId", stringValue(submission.dag().getId()))
                .putFields("name", stringValue(submission.dag().getName()))
                .putFields("runId", stringValue(submission.run().getRunId()))
                .putFields("state", stringValue(submission.run().getState().name()))
                .putFields("graph", stringValue(
                        DAGGraphPrinter.renderRun(submission.dag(), submission.run())));
        ListValue.Builder tasks=ListValue.newBuilder();
        for (String taskId : submission.dag().getTasks().keySet()) {
            var dagTask=submission.dag().getTask(taskId);
            Struct.Builder task=Struct.newBuilder()
                    .putFields("id", stringValue(taskId))
                    .putFields("command", stringValue(dagTask.getCommand()))
                    .putFields("cpu", numberValue(dagTask.getCpuRequested()))
                    .putFields("memory", numberValue(dagTask.getMemRequested()))
                    .putFields("state", stringValue(
                            submission.run().getTaskState(taskId).name()));
            Long jobId=submission.run().getJobId(taskId);
            task.putFields("jobId", jobId == null
                    ? Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()
                    : numberValue(jobId));
            ListValue.Builder dependencies=ListValue.newBuilder();
            for (String dependency : submission.dag().getDependencies(taskId)) {
                dependencies.addValues(stringValue(dependency));
            }
            task.putFields("dependsOn", Value.newBuilder().setListValue(dependencies).build());
            tasks.addValues(Value.newBuilder().setStructValue(task).build());
        }
        response.putFields("tasks", Value.newBuilder().setListValue(tasks).build());
        try {
            return JSON_PRINTER.print(response.build());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Could not serialize DAG response", e);
        }
    }

    private static Value stringValue(String value) {
        return Value.newBuilder().setStringValue(value).build();
    }

    private static Value numberValue(long value) {
        return Value.newBuilder().setNumberValue(value).build();
    }

    private static byte[] readRequestBody(HttpExchange exchange) throws IOException {
        byte[] body=exchange.getRequestBody().readNBytes(MAX_REQUEST_BYTES + 1);
        if (body.length>MAX_REQUEST_BYTES) {
            sendError(exchange, 413, "Request body is too large");
            return null;
        }
        return body;
    }

    private static boolean isJsonRequest(HttpExchange exchange) {
        String contentType=exchange.getRequestHeaders().getFirst("Content-Type");
        return contentType != null
                && contentType.toLowerCase(Locale.ROOT).split(";", 2)[0].trim()
                        .equals("application/json");
    }

    private static void getJob(HttpExchange exchange, JobOperations jobs, int id)
            throws IOException {
        Optional<Job> job=jobs.findJob(id);
        if (job.isEmpty()) {
            sendError(exchange, 404, "Job not found");
            return;
        }
        sendJob(exchange, 200, job.get());
    }

    private static void deleteJob(HttpExchange exchange, JobOperations jobs, int id)
            throws IOException {
        CancellationResult result=jobs.cancelJob(id);
        switch (result.outcome()) {
            case CANCELLED, ALREADY_CANCELLED -> sendJob(exchange, 200, result.job());
            case NOT_CANCELLABLE -> {
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
                send(exchange, 409, jsonJob(result.job()));
            }
            case UNAVAILABLE -> sendError(exchange, 503, "Worker could not cancel the job");
            case NOT_FOUND -> sendError(exchange, 404, "Job not found");
        }
    }

    private static void sendJobs(HttpExchange exchange, List<Job> jobs) throws IOException {
        StringJoiner body=new StringJoiner(",", "[", "]");
        for (Job job : jobs) {
            body.add(jsonJob(job));
        }
        send(exchange, 200, body.toString());
    }

    private static void sendJob(HttpExchange exchange, int status, Job job) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        if (status==201 || status==202) {
            exchange.getResponseHeaders().set("Location", "/api/jobs/" + job.getId());
        }
        send(exchange, status, jsonJob(job));
    }

    private static String jsonJob(Job job) {
        try {
            return JSON_PRINTER.print(job);
        } catch (IOException e) {
            throw new IllegalStateException("Could not serialize job response", e);
        }
    }

    private static void sendError(HttpExchange exchange, int status, String message)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        String escaped;
        try {
            escaped=JSON_PRINTER.print(StringValue.of(message));
        } catch (IOException e) {
            throw new IllegalStateException("Could not serialize error response", e);
        }
        send(exchange, status, "{\"error\":" + escaped + "}");
    }

    private static void send(HttpExchange exchange, int status, String body) throws IOException {
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
        byte[] bytes=body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static void methodNotAllowed(HttpExchange exchange, String allowed) throws IOException {
        exchange.getResponseHeaders().set("Allow", allowed);
        sendError(exchange, 405, "Method not allowed");
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
