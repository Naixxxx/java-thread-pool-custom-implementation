package com.koshevoi.threadpool;

public record WorkerSnapshot(int index, String name, int queueSize, int remainingCapacity, boolean idle) {
}
