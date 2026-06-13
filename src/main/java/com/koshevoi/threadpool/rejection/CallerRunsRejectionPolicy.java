package com.koshevoi.threadpool.rejection;

import com.koshevoi.threadpool.CustomThreadPool;
import com.koshevoi.threadpool.PoolLogger;

import java.util.concurrent.RejectedExecutionException;

public final class CallerRunsRejectionPolicy implements RejectionPolicy {
    @Override
    public void reject(Runnable task, CustomThreadPool pool) {
        if (pool.isShutdown()) {
            PoolLogger.warn("Rejected", "Task " + task + " was rejected because pool is shutting down.");
            throw new RejectedExecutionException("Pool is shutting down: " + task);
        }

        PoolLogger.warn("Rejected", "Task " + task + " will run in caller thread " + Thread.currentThread().getName());
        task.run();
    }
}
