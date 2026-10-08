package com.Worker;

import com.JobController.AllocationCommand;
import com.JobController.JobRef;
import com.JobController.Job;
import com.JobController.Cnf;
import com.JobController.JobState;
import com.JobController.Node;
import com.JobController.NodeState;
import com.JobController.GPUDevice;

import com.JobController.ControllerToWorkerGrpc;
import com.JobController.WorkerToControllerGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Worker {
    private static final Logger LOGGER=Logger.getLogger(Worker.class.getName());
    static int cpu=Integer.getInteger("clusterforge.worker.cpu", 8);
    static int mem=Integer.getInteger("clusterforge.worker.mem", 16);
    static long diskMb=Long.getLong("clusterforge.worker.disk-mb", 0L);
    private static final long heartbeatIntervalMillis = positiveLongProperty(
            "clusterforge.failure-detector.heartbeat-interval-ms", 5_000L);
    static List<GPUDevice> gpuDevices=parseGpuInventory(
            System.getProperty("clusterforge.worker.gpus", ""));
    static String Hostname=System.getProperty("clusterforge.worker.hostname", "localhost");
    static int nodeId;
    
    // Phase 9: the UUID is stable across restarts; the numeric incarnation advances
    // under a file lock so stale process heartbeats can be rejected deterministically.
    private static final WorkerIdentity.Identity identity = loadWorkerIdentity();
    static final String workerUuid = identity.workerUuid();
    static final String incarnationId = identity.incarnationId();

    private static WorkerIdentity.Identity loadWorkerIdentity() {
        String configuredPath = System.getProperty("clusterforge.worker.identity-file", ".worker_uuid");
        try {
            return WorkerIdentity.load(java.nio.file.Path.of(configuredPath));
        } catch (IOException e) {
            throw new ExceptionInInitializerError(
                    "Could not load persistent worker identity: " + e.getMessage());
        }
    }

    public static class Beating implements Runnable {
        private final WorkerToControllerGrpc.WorkerToControllerBlockingStub stub;
        private final Node n;
        private long sequence = 0;

        public Beating(WorkerToControllerGrpc.WorkerToControllerBlockingStub stub, Node n) {
            this.stub = stub;
            this.n = n;
        }
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    sequence++;
                    Node heartbeat = n.toBuilder()
                            .clearLabels()
                            .putAllLabels(workerLabels())
                            .setHeartbeatSequence(sequence)
                            .build();
                    stub.withDeadlineAfter(5, TimeUnit.SECONDS).heartBeat(heartbeat);
                    Thread.sleep(heartbeatIntervalMillis);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (StatusRuntimeException e) {
                    long retryDelayMillis = Math.max(1L, heartbeatIntervalMillis / 2);
                    LOGGER.log(Level.WARNING, "Heartbeat failed; retrying in "
                            + retryDelayMillis + " ms", e);
                    try {
                        Thread.sleep(retryDelayMillis);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    public static void reporter(WorkerToControllerGrpc.WorkerToControllerBlockingStub stub,Job j) {
        try {
            stub.withDeadlineAfter(5, TimeUnit.SECONDS).reportJobStatus(j);
            LOGGER.info(() -> "Reported job " + j.getId() + " as " + j.getState());
        } catch (RuntimeException e) {
            LOGGER.log(Level.SEVERE, "Could not report status for job " + j.getId(), e);
        }
    }

    public static class ControllerToWorkerService
            extends ControllerToWorkerGrpc.ControllerToWorkerImplBase {
        @Override
        public void allocate(AllocationCommand request,StreamObserver<Cnf> obs) {
            if (request.getJob().getId()<=0) {
                obs.onNext(Cnf.newBuilder()
                    .setSuccess(false)
                    .setMessage("A valid job ID is required")
                    .build());
                obs.onCompleted();
                return;
            }
            Job job=request.getJob();
            String allocationError=validateResourceAllocation(
                    job, request.getGpuDeviceIdsList());
            if (allocationError != null) {
                obs.onNext(Cnf.newBuilder()
                        .setSuccess(false)
                        .setJobId(job.getId())
                        .setMessage(allocationError)
                        .build());
                obs.onCompleted();
                return;
            }
            Job allocatedJob=job.toBuilder()
                            .setState(JobState.ALLOCATED)
                            .build();
            JobMap.ManagedJob previous=JobMap.jobs.putIfAbsent(
                    allocatedJob.getId(), new JobMap.ManagedJob(allocatedJob));
            LOGGER.info(() -> "Received allocation for job " + allocatedJob.getId());
            obs.onNext(Cnf.newBuilder()
                    .setSuccess(previous==null || previous.snapshot().equals(allocatedJob))
                    .setJobId(allocatedJob.getId())
                    .setMessage(previous==null ? "Job allocated" : "Job already known")
                    .build());
            obs.onCompleted();
        }

        @Override
        public void cancelAllocated(JobRef request,StreamObserver<Cnf> obs) {
            int jobId=request.getId();
            JobMap.ManagedJob managed=JobMap.jobs.get(jobId);

            if (managed==null) {
                LOGGER.warning(() -> "Cancellation requested for unknown job " + jobId);
                obs.onNext(Cnf.newBuilder()
                        .setSuccess(false)
                        .setJobId(jobId)
                        .setMessage("Job is not allocated on this worker")
                        .build());
                obs.onCompleted();
                return;
            }

            Process process;
            synchronized (managed) {
                JobState current=managed.snapshot().getState();

                // Cancellation is idempotent.
                if (current==JobState.CANCELLED) {
                    obs.onNext(Cnf.newBuilder()
                            .setSuccess(true)
                            .setJobId(jobId)
                            .setState(JobState.CANCELLED)
                            .setMessage("Job was already cancelled")
                            .build());
                    obs.onCompleted();
                    return;
                }

                // Do not turn a completed/failed job into CANCELLED.
                if (current==JobState.COMPLETED || current==JobState.FAILED) {
                    LOGGER.info(() -> "Cancellation rejected for terminal job " + jobId
                            + " in state " + current);
                    obs.onNext(Cnf.newBuilder()
                            .setSuccess(false)
                            .setJobId(jobId)
                            .setState(current)
                            .setMessage("Job has already finished")
                            .build());
                    obs.onCompleted();
                    return;
                }

                process=managed.process();
            }

            if (process!=null && process.isAlive()) {
                process.destroy(); // Sends a graceful SIGTERM on Linux.
                try {
                    if (!process.waitFor(10, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        if (!process.waitFor(2, TimeUnit.SECONDS)) {
                            throw new IllegalStateException(
                                    "Process for job " + jobId + " did not terminate");
                        }
                    }
                } catch (InterruptedException e) {
                    process.destroyForcibly();
                    Thread.currentThread().interrupt();
                    obs.onError(Status.UNAVAILABLE
                            .withDescription("Interrupted while cancelling job " + jobId)
                            .withCause(e)
                            .asRuntimeException());
                    return;
                } catch (IllegalStateException e) {
                    obs.onError(Status.UNAVAILABLE
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
                    return;
                }
            }

            synchronized (managed) {
                JobState current = managed.snapshot().getState();
                if (current==JobState.CANCELLED) {
                    obs.onNext(Cnf.newBuilder()
                            .setSuccess(true)
                            .setJobId(jobId)
                            .setState(JobState.CANCELLED)
                            .setMessage("Job was already cancelled")
                            .build());
                    obs.onCompleted();
                    return;
                }
                if (current==JobState.COMPLETED || current==JobState.FAILED) {
                    obs.onNext(Cnf.newBuilder()
                            .setSuccess(false)
                            .setJobId(jobId)
                            .setState(current)
                            .setMessage("Job has already finished")
                            .build());
                    obs.onCompleted();
                    return;
                }
                managed.setState(JobState.CANCELLED);
            }

            obs.onNext(Cnf.newBuilder()
                    .setSuccess(true)
                    .setJobId(jobId)
                    .setState(JobState.CANCELLED)
                    .setMessage("Cancellation accepted")
                    .build());
            LOGGER.info(() -> "Cancellation accepted for job " + jobId);
            obs.onCompleted();
        }

        @Override
        public void getJobStatus(Job request,StreamObserver<Job> obs) {
            int jobId=request.getId();
            JobMap.ManagedJob managed=JobMap.jobs.get(jobId);

            if (managed==null) {
                obs.onError(
                        Status.NOT_FOUND
                                .withDescription("Job " + jobId + " is not on this worker")
                                .asRuntimeException()
                );
                return;
            }

            Job snapshot;
            synchronized (managed) {
                snapshot=managed.snapshot();
                Process process=managed.process();

                // Reconcile a tracked process that ended before its final status was reported.
                if (snapshot.getState()==JobState.RUNNING
                        && process!=null
                        && !process.isAlive()) {
                    managed.setState(JobState.COMPLETED);
                    snapshot=managed.snapshot();
                }
            }

            obs.onNext(snapshot);
            obs.onCompleted();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int workerPort=Integer.getInteger("clusterforge.worker.port", 9000);
        String workerHost=System.getProperty("clusterforge.worker.host", "localhost");
        String controllerHost=System.getProperty(
                "clusterforge.controller.host", "35.200.151.242");
        int controllerPort=Integer.getInteger("clusterforge.controller.port", 9999);
        if (cpu<=0 || mem<=0 || diskMb<0) {
            throw new IllegalArgumentException(
                    "Worker CPU/memory must be positive and disk capacity non-negative");
        }
        ManagedChannel controllerChannel=ManagedChannelBuilder
                .forAddress(controllerHost, controllerPort)
                .usePlaintext()
                .build();
        WorkerToControllerGrpc.WorkerToControllerBlockingStub stub =
                WorkerToControllerGrpc.newBlockingStub(controllerChannel);
        Server server=ServerBuilder.forPort(workerPort)
                .addService(new ControllerToWorkerService())
                .build();
        Thread heartbeatThread=null;
        try {
            server.start();
            System.out.println("Worker server started on port " + workerPort);

            Node node=Node.newBuilder()
                    .setHostname(Hostname)
                    .setState(NodeState.AVAILABLE)
                    .setCpu(cpu)
                    .setMem(mem)
                    .setDiskMb(diskMb)
                    .addAllGpuDevices(gpuDevices)
                    .putAllLabels(workerLabels())
                    .setAgentEndpoint(workerHost + ":" + workerPort)
                    .setWorkerUuid(workerUuid)
                    .setIncarnationId(incarnationId)
                    .build();
            Cnf registration=stub.registerNode(node);
            if (!registration.getSuccess()) {
                throw new IllegalStateException(
                        "Node registration failed: " + registration.getMessage());
            }
            nodeId=registration.getNodeId();
            System.out.println("Worker registered as node " + nodeId);

            Node registeredNode=node.toBuilder().setId(nodeId).build();
            heartbeatThread=new Thread(new Beating(stub, registeredNode), "worker-heartbeat");
            heartbeatThread.setDaemon(true);
            heartbeatThread.start();

            Thread finalHeartbeatThread=heartbeatThread;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                finalHeartbeatThread.interrupt();
                server.shutdown();
                controllerChannel.shutdown();
            }, "worker-shutdown"));
            server.awaitTermination();
        } finally {
            if (heartbeatThread!=null) heartbeatThread.interrupt();
            server.shutdownNow();
            controllerChannel.shutdownNow();
        }
    }

    private static List<GPUDevice> parseGpuInventory(String inventory) {
        if (inventory.isBlank()) {
            return List.of();
        }
        List<GPUDevice> devices = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        for (String entry : inventory.split(",")) {
            String[] fields = entry.trim().split(":", -1);
            String gpuId = fields.length == 2 ? fields[0].trim() : "";
            if (gpuId.isBlank() || !ids.add(gpuId)) {
                throw new IllegalArgumentException(
                        "GPU inventory must be comma-separated unique id:memoryMb entries");
            }
            try {
                long memoryMb = Long.parseLong(fields[1].trim());
                if (memoryMb <= 0) {
                    throw new IllegalArgumentException("GPU memory must be positive");
                }
                devices.add(GPUDevice.newBuilder()
                        .setId(gpuId)
                        .setMemoryMb(memoryMb)
                        .build());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "GPU memory must be a positive integer MB value", e);
            }
        }
        return List.copyOf(devices);
    }

    private static String validateResourceAllocation(Job job, List<String> assignedGpuIds) {
        if (job.getCpuRequested() <= 0 || job.getCpuRequested() > cpu
                || job.getMemRequested() <= 0 || job.getMemRequested() > mem
                || job.getDiskMbRequested() < 0 || job.getDiskMbRequested() > diskMb
                || job.getGpuCountRequested() < 0
                || job.getGpuMemoryMbPerGpu() < 0
                || job.getGpuCountRequested() != assignedGpuIds.size()) {
            return "Allocation does not fit the worker's declared resources";
        }
        if (job.getGpuCountRequested() == 0 && job.getGpuMemoryMbPerGpu() != 0) {
            return "GPU memory request requires at least one GPU";
        }
        if (!job.getAssignedWorkerId().equals(workerUuid)
                || !job.getAssignedWorkerLabelsMap().equals(workerLabels())) {
            return "Allocation placement metadata does not match this worker";
        }
        Map<String, Long> memoryById = new HashMap<>();
        for (GPUDevice device : gpuDevices) {
            memoryById.put(device.getId(), device.getMemoryMb());
        }
        Set<String> uniqueIds = new HashSet<>();
        for (String gpuId : assignedGpuIds) {
            Long memoryMb = memoryById.get(gpuId);
            if (!uniqueIds.add(gpuId) || memoryMb == null
                    || memoryMb < job.getGpuMemoryMbPerGpu()) {
                return "Assigned GPU is unavailable or does not meet the memory request";
            }
        }
        return null;
    }

    static Map<String, String> workerLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put("hostname", Hostname);
        String configured = System.getProperty("clusterforge.worker.labels", "");
        if (!configured.isBlank()) {
            for (String entry : configured.split(",")) {
                String[] fields = entry.split("=", 2);
                if (fields.length != 2 || fields[0].isBlank() || fields[1].isBlank()) {
                    throw new IllegalArgumentException(
                            "Worker labels must be comma-separated key=value pairs");
                }
                String key = fields[0].trim();
                String value = fields[1].trim();
                if (key.equals("hostname") || labels.putIfAbsent(key, value) != null) {
                    throw new IllegalArgumentException(
                            "Worker label keys must be unique and hostname is reserved");
                }
            }
        }
        return Map.copyOf(labels);
    }

    private static long positiveLongProperty(String name, long defaultValue) {
        String configured = System.getProperty(name);
        if (configured == null) {
            return defaultValue;
        }
        try {
            long value = Long.parseLong(configured);
            if (value <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be an integer", e);
        }
    }
}