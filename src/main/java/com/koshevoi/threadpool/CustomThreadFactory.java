package com.koshevoi.threadpool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class CustomThreadFactory implements ThreadFactory {
    private final String poolName;
    private final AtomicInteger sequence = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        if (poolName == null || poolName.isBlank()) {
            throw new IllegalArgumentException("poolName must not be blank");
        }
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String threadName = poolName + "-worker-" + sequence.getAndIncrement();
        PoolLogger.info("ThreadFactory", "Creating new thread: " + threadName);

        Thread thread = new Thread(() -> {
            try {
                runnable.run();
            } finally {
                PoolLogger.info("ThreadFactory", threadName + " finished execution wrapper.");
            }
        }, threadName);

        thread.setUncaughtExceptionHandler(
                (t, error) -> PoolLogger.error("ThreadFactory", "Uncaught exception in " + t.getName(), error));
        return thread;
    }
}
