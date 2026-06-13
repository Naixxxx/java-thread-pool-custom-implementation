package com.koshevoi.threadpool.balancing;

import com.koshevoi.threadpool.WorkerSnapshot;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class RoundRobinBalancer implements TaskBalancer {
    private final AtomicInteger counter = new AtomicInteger();

    @Override
    public int selectWorkerIndex(List<WorkerSnapshot> workers) {
        if (workers == null || workers.isEmpty()) {
            throw new IllegalArgumentException("workers must not be empty");
        }
        int position = Math.floorMod(counter.getAndIncrement(), workers.size());
        return workers.get(position).index();
    }
}
