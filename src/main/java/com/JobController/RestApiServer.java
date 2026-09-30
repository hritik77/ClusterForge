package com.JobController;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.StringValue;
import com.google.protobuf.util.JsonFormat;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
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

    interface JobOperations {
        Optional<Job> submitJob(String owner, String description, int cpuRequested, int memRequested);
        List<Job> listJobs();
        Optional<Job> findJob(int id);
        CancellationResult cancelJob(int id);
    }

    private final HttpServer server;
    private final ExecutorService executor=Executors.newFixedThreadPool(
            Math.min(8, Math.max(2, Runtime.getRuntime().availableProcessors())));

    RestApiServer(String host, int port, JobOperations jobs) throws IOException {
        server=HttpServer.create(
                new InetSocketAddress(InetAddress.getByName(host), port), 0);
        server.createContext("/api/jobs", exchange -> handle(exchange, jobs));
        server.setExecutor(executor);
    }

    void start() {
        server.start();
    }

    int port() {
        return server.getAddress().getPort();
    }

    private static void handle(HttpExchange exchange, JobOperations jobs) throws IOException {
        try {
            String path=exchange.getRequestURI().getPath();
            if (path.equals("/api/jobs") || path.equals("/api/jobs/")) {
                handleCollection(exchange, jobs);
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

    private static void handleCollection(HttpExchange exchange, JobOperations jobs)
            throws IOException {
        switch (exchange.getRequestMethod()) {
            case "POST" -> submitJob(exchange, jobs);
            case "GET" -> sendJobs(exchange, jobs.listJobs());
            default -> methodNotAllowed(exchange, "GET, POST");
        }
    }

    private static void submitJob(HttpExchange exchange, JobOperations jobs) throws IOException {
        String contentType=exchange.getRequestHeaders().getFirst("Content-Type");
        if (contentType==null
                || !contentType.toLowerCase(Locale.ROOT).split(";", 2)[0].trim()
                        .equals("application/json")) {
            sendError(exchange, 415, "Content-Type must be application/json");
            return;
        }

        byte[] body=exchange.getRequestBody().readNBytes(MAX_REQUEST_BYTES + 1);
        if (body.length>MAX_REQUEST_BYTES) {
            sendError(exchange, 413, "Request body is too large");
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

    private static void getJob(HttpExchange exchange, JobOperations jobs, int id) throws IOException {
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
        for (Job job : jobs) body.add(jsonJob(job));
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
