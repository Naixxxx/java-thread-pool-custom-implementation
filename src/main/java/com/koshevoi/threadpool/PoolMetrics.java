package com.koshevoi.threadpool;

public record PoolMetrics(
        int workerCount,
        int idleWorkerCount,
        int queuedTaskCount,
        int activeWorkerCount,
        boolean shutdown,
        boolean shutdownNow) {
}
