package com.koshevoi.threadpool.demo;

import com.koshevoi.threadpool.PoolLogger;

public final class DemoTask implements Runnable {
    private final String name;
    private final long durationMillis;

    public DemoTask(String name, long durationMillis) {
        this.name = name;
        this.durationMillis = durationMillis;
    }

    @Override
    public void run() {
        PoolLogger.info("Task", name + " started on " + Thread.currentThread().getName());
        try {
            Thread.sleep(durationMillis);
        } catch (InterruptedException interrupted) {
            PoolLogger.warn("Task", name + " interrupted on " + Thread.currentThread().getName());
            Thread.currentThread().interrupt();
            return;
        }
        PoolLogger.info("Task", name + " finished on " + Thread.currentThread().getName());
    }

    @Override
    public String toString() {
        return name + "(" + durationMillis + "ms)";
    }
}
