package com.Worker;

import com.JobController.Job;
import com.JobController.JobState;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class JobMap {

    static final ConcurrentMap<Integer, ManagedJob> jobs =
            new ConcurrentHashMap<>();

    static final class ManagedJob {
        private Job job;
        private Process process;

        ManagedJob(Job job) {
            this.job=job;
        }

        synchronized Job snapshot() {
            return job;
        }

        synchronized boolean setState(JobState state) {
            if (!isTerminal(job.getState())) {
                job=job.toBuilder().setState(state).build();
                return true;
            }
            return false;
        }

        synchronized Process process() {
            return process;
        }

        synchronized void setProcess(Process process) {
            this.process=process;
        }

        private static boolean isTerminal(JobState state) {
            return state==JobState.COMPLETED || state==JobState.FAILED
                    || state==JobState.CANCELLED;
        }
    }
}