/*For use at worker side 
  JobMap --> (JobId,ManagedJob) HashMap
  ManagedJob --> (Job,Process)
  */
package com.java.Worker;

import com.java.JobController.Job;
import com.java.JobController.JobState;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class JobMap {

    static final ConcurrentMap<Integer, ManagedJob> jobs =
            new ConcurrentHashMap<>();

    static final ScheduledExecutorService cancellationExecutor =
            Executors.newSingleThreadScheduledExecutor();

    static final class ManagedJob {
        private Job job;
        private Process process; // null while queued

        ManagedJob(Job job) {
            this.job=job;
        }

        synchronized Job snapshot() {
            return job;
        }

        synchronized void setState(JobState state) {
            job=job.toBuilder()
                    .setState(state)
                    .build();
        }

        synchronized Process process() {
            return process;
        }

        synchronized void setProcess(Process process) {
            this.process=process;
        }
    }
}