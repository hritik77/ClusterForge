package com.java.Worker;

import com.java.JobController.AllocationCommand;
import com.java.JobController.JobRef;   
import com.java.JobController.Job;
import com.java.JobController.Cnf;
import com.java.JobController.JobState;
import com.java.JobController.Node;
import com.java.JobController.NodeState;

import com.java.JobController.ControllerToWorkerGrpc;
import com.java.JobController.WorkerToControllerGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.Status;

import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

class Worker {
    static int cpu=8;
    static int mem=16;
    static String Hostname="localhost";
    public static class ControllerToWorkerService extends ControllerToWorkerGrpc.ControllerToWorkerImplBase {
        @Override
        public void allocate(AllocationCommand request,StreamObserver<Cnf> obs) {
            if (request.getJob().getId() <= 0) {
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
            JobMap.ManagedJob previous=JobMap.jobs.putIfAbsent(allocatedJob.getId(),new JobMap.ManagedJob(allocatedJob));
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
                            .setSuccess(true)
                            .setJobId(jobId)
                            .setMessage("Job has already finished")
                            .build());
                    obs.onCompleted();
                    return;
                }request.getId();

                managed.setState(JobState.CANCELLED);
                process=managed.process();
            }

            if (process != null && process.isAlive()) {
                process.destroy(); // Sends a graceful SIGTERM on Linux.

                JobMap.cancellationExecutor.schedule(() -> {
                    if (process.isAlive()) {
                        process.destroyForcibly(); // SIGKILL fallback for MVP.
                    }
                }, 10, TimeUnit.SECONDS);

                /*
                * Beyond MVP:
                * - Stop the workload's systemd scope or cgroup, not merely its parent PID.
                * - Kill every child process in the job's process tree.
                * - Close job network/storage mounts and clean temporary directories.
                * - Capture cancellation reason and final resource usage.
                */
            }

            // Beyond MVP: call WorkerToController.ReportJobStatus here.
            // Discover should mark the allocation cancelled and release cluster capacity.
            // Local CPU/memory reservation should be released here as well.

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
                                .withDescription("Job "+jobId+" is not on this worker")
                                .asRuntimeException()
                );
                return;
            }

            Job snapshot;
            synchronized (managed) {
                snapshot=managed.snapshot();
                Process process=managed.process();

                // Optional MVP reconciliation if process execution is tracked.
                if (snapshot.getState()==JobState.RUNNING
                        && process != null
                        && !process.isAlive()) {
                    managed.setState(JobState.COMPLETED);
                    snapshot=managed.snapshot();
                }
            }

            obs.onNext(snapshot);
            obs.onCompleted();
        }
    }

    public static void main(String args[]) throws IOException,InterruptedException {
        Server server=ServerBuilder.forPort(9000)
                                .addService(new ControllerToWorkerService())
                                .build();
        server.start();
        System.out.println("Worker Server Started at port 9000");

        //Register Node
        ManagedChannel controllerChannel=ManagedChannelBuilder.forAddress("localhost",9999)
                                                .usePlaintext()
                                                .build();
        WorkerToControllerGrpc.WorkerToControllerBlockingStub stub=WorkerToControllerGrpc.newBlockingStub(controllerChannel);
        Node n=Node.newBuilder()
                    .setHostname(Hostname)
                    .setState(NodeState.AVAILABLE)
                    .setCpu(cpu)
                    .setMem(mem)
                    .setAgentEndpoint("localhost:9000")
                    .build();
        Cnf registration=stub.registerNode(n);
        if (!registration.getSuccess()) {
            throw new IllegalStateException("Node registration failed: "+registration.getMessage());
        }
        System.out.println("Worker registered as node "+registration.getNodeId());

        server.awaitTermination();
    }
}