package com.koshevoi.threadpool.balancing;

import com.koshevoi.threadpool.WorkerSnapshot;

import java.util.List;

public final class LeastLoadedBalancer implements TaskBalancer {
    @Override
    public int selectWorkerIndex(List<WorkerSnapshot> workers) {
        if (workers == null || workers.isEmpty()) {
            throw new IllegalArgumentException("workers must not be empty");
        }

        WorkerSnapshot best = workers.get(0);
        for (int i = 1; i < workers.size(); i++) {
            WorkerSnapshot current = workers.get(i);
            if (isBetter(current, best)) {
                best = current;
            }
        }
        return best.index();
    }

    private boolean isBetter(WorkerSnapshot current, WorkerSnapshot best) {
        if (current.queueSize() != best.queueSize()) {
            return current.queueSize() < best.queueSize();
        }
        if (current.idle() != best.idle()) {
            return current.idle();
        }
        return current.index() < best.index();
    }
}
