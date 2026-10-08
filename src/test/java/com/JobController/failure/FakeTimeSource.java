package com.JobController.failure;

public class FakeTimeSource implements TimeSource {
    private long currentTimeMillis = 0;

    @Override
    public long currentTimeMillis() {
        return currentTimeMillis;
    }

    public void advance(long millis) {
        currentTimeMillis += millis;
    }

    public void set(long millis) {
        currentTimeMillis = millis;
    }
}
