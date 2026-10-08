package com.JobController.dag;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class TaskTimerManager implements AutoCloseable {
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            runnable -> {
                Thread thread = new Thread(runnable, "dag-timeout-manager");
                thread.setDaemon(true);
                return thread;
            });
    private final Map<TaskTimerKey, ScheduledFuture<?>> taskTimers = new HashMap<>();
    private final Map<String, ScheduledFuture<?>> dagTimers = new HashMap<>();

    public synchronized void scheduleTaskTimeout(
            String runId, String taskId, long delayMillis, Runnable timeoutHandler) {
        validate(runId, delayMillis, timeoutHandler);
        if (taskId == null || taskId.isBlank()) {
            throw new IllegalArgumentException("taskId must not be blank");
        }
        TaskTimerKey key = new TaskTimerKey(runId, taskId);
        cancel(taskTimers.remove(key));
        AtomicReference<ScheduledFuture<?>> scheduled = new AtomicReference<>();
        ScheduledFuture<?> future = executor.schedule(() -> {
            synchronized (TaskTimerManager.this) {
                taskTimers.remove(key, scheduled.get());
            }
            timeoutHandler.run();
        }, delayMillis, TimeUnit.MILLISECONDS);
        scheduled.set(future);
        taskTimers.put(key, future);
    }

    public synchronized void scheduleDagTimeout(
            String runId, long delayMillis, Runnable timeoutHandler) {
        validate(runId, delayMillis, timeoutHandler);
        cancel(dagTimers.remove(runId));
        AtomicReference<ScheduledFuture<?>> scheduled = new AtomicReference<>();
        ScheduledFuture<?> future = executor.schedule(() -> {
            synchronized (TaskTimerManager.this) {
                dagTimers.remove(runId, scheduled.get());
            }
            timeoutHandler.run();
        }, delayMillis, TimeUnit.MILLISECONDS);
        scheduled.set(future);
        dagTimers.put(runId, future);
    }

    public synchronized void cancelTaskTimeout(String runId, String taskId) {
        cancel(taskTimers.remove(new TaskTimerKey(runId, taskId)));
    }

    public synchronized void cancelDagTimeout(String runId) {
        cancel(dagTimers.remove(runId));
    }

    public synchronized void cancelRunTimers(String runId) {
        taskTimers.entrySet().removeIf(entry -> {
            if (entry.getKey().runId().equals(runId)) {
                cancel(entry.getValue());
                return true;
            }
            return false;
        });
        cancel(dagTimers.remove(runId));
    }

    @Override
    public void close() {
        synchronized (this) {
            taskTimers.values().forEach(TaskTimerManager::cancel);
            taskTimers.clear();
            dagTimers.values().forEach(TaskTimerManager::cancel);
            dagTimers.clear();
            executor.shutdownNow();
        }
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("DAG timeout scheduler did not terminate");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while stopping DAG timeout scheduler", e);
        }
    }

    public void shutdown() {
        close();
    }

    private static void validate(String runId, long delayMillis, Runnable handler) {
        if (runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
        if (delayMillis <= 0) {
            throw new IllegalArgumentException("delayMillis must be positive");
        }
        Objects.requireNonNull(handler, "timeoutHandler must not be null");
    }

    private static void cancel(ScheduledFuture<?> future) {
        if (future != null) {
            future.cancel(false);
        }
    }

    private record TaskTimerKey(String runId, String taskId) {}
}
