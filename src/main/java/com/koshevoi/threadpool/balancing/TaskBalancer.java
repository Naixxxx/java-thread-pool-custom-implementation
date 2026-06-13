package com.koshevoi.threadpool.balancing;

import com.koshevoi.threadpool.WorkerSnapshot;

import java.util.List;

public interface TaskBalancer {
    int selectWorkerIndex(List<WorkerSnapshot> workers);
}
