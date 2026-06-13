package com.koshevoi.threadpool.rejection;

import com.koshevoi.threadpool.CustomThreadPool;
import com.koshevoi.threadpool.PoolLogger;

import java.util.concurrent.RejectedExecutionException;

public final class AbortRejectionPolicy implements RejectionPolicy {
    @Override
    public void reject(Runnable task, CustomThreadPool pool) {
        PoolLogger.warn("Rejected", "Task " + task + " was rejected due to overload! metrics=" + pool.metrics());
        throw new RejectedExecutionException("Task rejected due to pool overload: " + task);
    }
}
