package com.koshevoi.threadpool.rejection;

import com.koshevoi.threadpool.CustomThreadPool;

public interface RejectionPolicy {
    void reject(Runnable task, CustomThreadPool pool);
}
