package com.Worker;

import com.JobController.AllocationCommand;
import com.JobController.JobRef;
import com.JobController.Job;
import com.JobController.Cnf;
import com.JobController.JobState;
import com.JobController.Node;
import com.JobController.NodeState;

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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Worker {
    private static final Logger LOGGER=Logger.getLogger(Worker.class.getName());
    static int cpu=Integer.getInteger("clusterforge.worker.cpu", 8);
    static int mem=Integer.getInteger("clusterforge.worker.mem", 16);
    static String Hostname=System.getProperty("clusterforge.worker.hostname", "localhost");
    static int nodeId;
    
    public static class Beating implements Runnable {
        private final WorkerToControllerGrpc.WorkerToControllerBlockingStub stub;
        private final Node n;

        public Beating(WorkerToControllerGrpc.WorkerToControllerBlockingStub stub, Node n) {
            this.stub = stub;
            this.n = n;
        }
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    stub.withDeadlineAfter(5, TimeUnit.SECONDS).heartBeat(n);
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (StatusRuntimeException e) {
                    LOGGER.log(Level.WARNING, "Heartbeat failed; retrying in 2.5 seconds", e);
                    try {
                        Thread.sleep(2500);
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
        private final WorkerToControllerGrpc.WorkerToControllerBlockingStub controllerStub;

        ControllerToWorkerService(
                WorkerToControllerGrpc.WorkerToControllerBlockingStub controllerStub) {
            this.controllerStub=controllerStub;
        }

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
            Job allocatedJob=job.toBuilder()
                            .setState(JobState.ALLOCATED)
                            .build();
            JobMap.ManagedJob previous=JobMap.jobs.putIfAbsent(
                    allocatedJob.getId(), new JobMap.ManagedJob(allocatedJob));
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
                            .setMessage("Job was already cancelled")
                            .build());
                    obs.onCompleted();
                    return;
                }

                // Do not turn a completed/failed job into CANCELLED.
                if (current==JobState.COMPLETED || current==JobState.FAILED) {
                    obs.onNext(Cnf.newBuilder()
                            .setSuccess(false)
                            .setJobId(jobId)
                            .setMessage("Job has already finished")
                            .build());
                    obs.onCompleted();
                    return;
                }

                managed.setState(JobState.CANCELLED);
                process=managed.process();
            }

            if (process!=null && process.isAlive()) {
                process.destroy(); // Sends a graceful SIGTERM on Linux.

                JobMap.cancellationExecutor.schedule(() -> {
                    if (process.isAlive()) {
                        process.destroyForcibly();
                    }
                }, 10, TimeUnit.SECONDS);

            }

            reporter(controllerStub, managed.snapshot());

            obs.onNext(Cnf.newBuilder()
                    .setSuccess(true)
                    .setJobId(jobId)
                    .setMessage("Cancellation accepted")
                    .build());
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
        String controllerHost=System.getProperty("clusterforge.controller.host", "localhost");
        int controllerPort=Integer.getInteger("clusterforge.controller.port", 9999);
        if (cpu<=0 || mem<=0) {
            throw new IllegalArgumentException("Worker CPU and memory capacity must be positive");
        }
        ManagedChannel controllerChannel=ManagedChannelBuilder
                .forAddress(controllerHost, controllerPort)
                .usePlaintext()
                .build();
        WorkerToControllerGrpc.WorkerToControllerBlockingStub stub =
                WorkerToControllerGrpc.newBlockingStub(controllerChannel);
        Server server=ServerBuilder.forPort(workerPort)
                .addService(new ControllerToWorkerService(stub))
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
                    .setAgentEndpoint(workerHost + ":" + workerPort)
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
                JobMap.cancellationExecutor.shutdownNow();
            }, "worker-shutdown"));
            server.awaitTermination();
        } finally {
            if (heartbeatThread!=null) heartbeatThread.interrupt();
            server.shutdownNow();
            controllerChannel.shutdownNow();
            JobMap.cancellationExecutor.shutdownNow();
        }
    }
}