package com.koshevoi.threadpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class Worker implements Runnable {
    private final CustomThreadPool pool;
    private final BlockingQueue<Runnable> queue;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final AtomicBoolean idle = new AtomicBoolean(false);

    private volatile Thread thread;

    Worker(CustomThreadPool pool, int queueSize, long keepAliveTime, TimeUnit timeUnit) {
        this.pool = pool;
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
    }

    void attachThread(Thread thread) {
        this.thread = thread;
    }

    String name() {
        Thread currentThread = thread;
        return currentThread == null ? "not-started-worker" : currentThread.getName();
    }

    boolean offer(Runnable task) {
        return queue.offer(task);
    }

    int queueSize() {
        return queue.size();
    }

    int remainingCapacity() {
        return queue.remainingCapacity();
    }

    boolean isIdle() {
        return idle.get();
    }

    boolean isSpare() {
        return idle.get() && queue.isEmpty();
    }

    void interruptIfIdle() {
        Thread currentThread = thread;
        if (currentThread != null && idle.get()) {
            currentThread.interrupt();
        }
    }

    void interruptNow() {
        Thread currentThread = thread;
        if (currentThread != null) {
            currentThread.interrupt();
        }
    }

    List<Runnable> drainQueue() {
        List<Runnable> drained = new ArrayList<>();
        queue.drainTo(drained);
        return drained;
    }

    @Override
    public void run() {
        PoolLogger.info("Worker", name() + " started.");
        try {
            while (true) {
                if (pool.isShutdownNow()) {
                    break;
                }

                if (pool.isShutdown() && queue.isEmpty()) {
                    break;
                }

                Runnable task;
                idle.set(true);
                try {
                    task = queue.poll(keepAliveTime, timeUnit);
                } catch (InterruptedException interrupted) {
                    idle.set(false);
                    if (pool.isShutdownNow() || (pool.isShutdown() && queue.isEmpty())) {
                        break;
                    }

                    continue;
                } finally {
                    idle.set(false);
                }

                if (task == null) {
                    if (pool.canWorkerStopAfterIdle(this)) {
                        PoolLogger.info("Worker", name() + " idle timeout, stopping.");
                        break;
                    }
                    continue;
                }

                if (pool.isShutdownNow()) {
                    PoolLogger.info("Worker", name() + " stopNow detected, dropping task: " + task);
                    break;
                }

                PoolLogger.info("Worker", name() + " executes " + task);
                try {
                    task.run();
                } catch (Throwable taskError) {
                    PoolLogger.error("Worker", name() + " caught task error in " + task, taskError);
                }
            }
        } finally {
            idle.set(false);
            PoolLogger.info("Worker", name() + " terminated.");
            pool.onWorkerExit(this);
        }
    }
}
