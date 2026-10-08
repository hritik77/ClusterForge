package com.JobController.dag;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;

public class TaskTimerManagerTest {
    @Test
    public void runsTaskAndDagTimeoutCallbacks() throws Exception {
        try (TaskTimerManager timers = new TaskTimerManager()) {
            CountDownLatch callbacks = new CountDownLatch(2);
            timers.scheduleTaskTimeout("run", "task", 40, callbacks::countDown);
            timers.scheduleDagTimeout("run", 60, callbacks::countDown);

            assertTrue(callbacks.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void cancellationPreventsTaskAndDagCallbacks() throws Exception {
        try (TaskTimerManager timers = new TaskTimerManager()) {
            AtomicInteger callbackCount = new AtomicInteger();
            timers.scheduleTaskTimeout("run", "task", 100, callbackCount::incrementAndGet);
            timers.scheduleDagTimeout("run", 100, callbackCount::incrementAndGet);
            timers.cancelTaskTimeout("run", "task");
            timers.cancelDagTimeout("run");

            Thread.sleep(160);
            assertEquals(0, callbackCount.get());
        }
    }

    @Test
    public void cancelsEveryTimerForOneRunWithoutTouchingAnotherRun() throws Exception {
        try (TaskTimerManager timers = new TaskTimerManager()) {
            CountDownLatch otherRunCallback = new CountDownLatch(1);
            AtomicInteger cancelledCallbacks = new AtomicInteger();
            timers.scheduleTaskTimeout(
                    "cancelled", "task", 120, cancelledCallbacks::incrementAndGet);
            timers.scheduleDagTimeout(
                    "cancelled", 120, cancelledCallbacks::incrementAndGet);
            timers.scheduleTaskTimeout("other", "task", 60, otherRunCallback::countDown);
            timers.cancelRunTimers("cancelled");

            assertTrue(otherRunCallback.await(2, TimeUnit.SECONDS));
            Thread.sleep(100);
            assertEquals(0, cancelledCallbacks.get());
        }
    }

    @Test
    public void rejectsInvalidTimerArguments() {
        try (TaskTimerManager timers = new TaskTimerManager()) {
            assertThrows(IllegalArgumentException.class,
                    () -> timers.scheduleTaskTimeout("", "task", 1, () -> {}));
            assertThrows(IllegalArgumentException.class,
                    () -> timers.scheduleTaskTimeout("run", " ", 1, () -> {}));
            assertThrows(IllegalArgumentException.class,
                    () -> timers.scheduleDagTimeout("run", 0, () -> {}));
            assertThrows(NullPointerException.class,
                    () -> timers.scheduleDagTimeout("run", 1, null));
        }
    }

    @Test
    public void shutdownCancelsTimersAndRejectsNewSchedules() {
        TaskTimerManager timers = new TaskTimerManager();
        timers.scheduleDagTimeout("run", 1_000, () -> {});
        timers.shutdown();

        assertThrows(java.util.concurrent.RejectedExecutionException.class,
                () -> timers.scheduleDagTimeout("run", 1, () -> {}));
        timers.close();
    }
}
