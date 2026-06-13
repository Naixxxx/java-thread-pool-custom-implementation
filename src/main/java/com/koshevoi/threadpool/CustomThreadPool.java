package com.koshevoi.threadpool;

import com.koshevoi.threadpool.balancing.TaskBalancer;
import com.koshevoi.threadpool.rejection.RejectionPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public final class CustomThreadPool implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;
    private final ThreadFactory threadFactory;
    private final TaskBalancer balancer;
    private final RejectionPolicy rejectionPolicy;

    private final ReentrantLock lock = new ReentrantLock();
    private final List<Worker> workers = new ArrayList<>();
    private final Object terminationMonitor = new Object();
    private final AtomicLong acceptedTasks = new AtomicLong();

    private volatile boolean shutdown;
    private volatile boolean shutdownNow;

    public CustomThreadPool(
            String poolName,
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            TimeUnit timeUnit,
            int queueSize,
            int minSpareThreads,
            TaskBalancer balancer,
            RejectionPolicy rejectionPolicy) {
        this(
                corePoolSize,
                maxPoolSize,
                keepAliveTime,
                timeUnit,
                queueSize,
                minSpareThreads,
                new CustomThreadFactory(poolName),
                balancer,
                rejectionPolicy);
    }

    public CustomThreadPool(
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            TimeUnit timeUnit,
            int queueSize,
            int minSpareThreads,
            ThreadFactory threadFactory,
            TaskBalancer balancer,
            RejectionPolicy rejectionPolicy) {
        validateParameters(corePoolSize, maxPoolSize, keepAliveTime, timeUnit, queueSize, minSpareThreads);
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = Objects.requireNonNull(timeUnit, "timeUnit");
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory");
        this.balancer = Objects.requireNonNull(balancer, "balancer");
        this.rejectionPolicy = Objects.requireNonNull(rejectionPolicy, "rejectionPolicy");

        lock.lock();
        try {
            ensureCoreWorkersLocked();
            ensureSpareWorkersLocked();
        } finally {
            lock.unlock();
        }
    }

    private static void validateParameters(
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            TimeUnit timeUnit,
            int queueSize,
            int minSpareThreads) {
        if (corePoolSize <= 0) {
            throw new IllegalArgumentException("corePoolSize must be positive");
        }
        if (maxPoolSize < corePoolSize) {
            throw new IllegalArgumentException("maxPoolSize must be greater than or equal to corePoolSize");
        }
        if (keepAliveTime <= 0) {
            throw new IllegalArgumentException("keepAliveTime must be positive");
        }
        Objects.requireNonNull(timeUnit, "timeUnit");
        if (queueSize <= 0) {
            throw new IllegalArgumentException("queueSize must be positive");
        }
        if (minSpareThreads < 0 || minSpareThreads > maxPoolSize) {
            throw new IllegalArgumentException("minSpareThreads must be between 0 and maxPoolSize");
        }
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command, "command");

        if (shutdown) {
            throw new RejectedExecutionException("Pool is shutting down: " + command);
        }

        boolean accepted = false;
        lock.lock();
        try {
            if (shutdown) {
                throw new RejectedExecutionException("Pool is shutting down: " + command);
            }

            ensureCoreWorkersLocked();
            accepted = tryPutIntoExistingQueueLocked(command);

            if (!accepted && workers.size() < maxPoolSize) {
                Worker newWorker = addWorkerLocked();
                accepted = newWorker.offer(command);
                if (accepted) {
                    logAccepted(newWorker, command);
                }
            }

            if (accepted) {
                acceptedTasks.incrementAndGet();
                ensureSpareWorkersLocked();
                return;
            }
        } finally {
            lock.unlock();
        }

        rejectionPolicy.reject(command, this);
    }

    private boolean tryPutIntoExistingQueueLocked(Runnable command) {
        if (workers.isEmpty()) {
            return false;
        }

        List<WorkerSnapshot> snapshots = snapshotsLocked();
        int selectedIndex = balancer.selectWorkerIndex(snapshots);
        Worker selected = workers.get(selectedIndex);
        if (selected.offer(command)) {
            logAccepted(selected, command);
            return true;
        }

        Worker fallback = null;
        for (Worker worker : workers) {
            if (worker == selected || worker.remainingCapacity() == 0) {
                continue;
            }
            if (fallback == null || worker.queueSize() < fallback.queueSize()) {
                fallback = worker;
            }
        }

        if (fallback == null) {
            return false;
        }

        boolean offered = fallback.offer(command);
        if (offered) {
            logAccepted(fallback, command);
        }
        return offered;
    }

    private List<WorkerSnapshot> snapshotsLocked() {
        List<WorkerSnapshot> snapshots = new ArrayList<>(workers.size());
        for (int i = 0; i < workers.size(); i++) {
            Worker worker = workers.get(i);
            snapshots.add(new WorkerSnapshot(
                    i,
                    worker.name(),
                    worker.queueSize(),
                    worker.remainingCapacity(),
                    worker.isIdle()));
        }
        return snapshots;
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        Objects.requireNonNull(callable, "callable");
        FutureTask<T> futureTask = new FutureTask<>(callable) {
            @Override
            public String toString() {
                return "FutureTask{" + callable + '}';
            }
        };
        execute(futureTask);
        return futureTask;
    }

    @Override
    public void shutdown() {
        lock.lock();
        try {
            if (shutdown) {
                return;
            }
            shutdown = true;
            PoolLogger.info("Pool", "shutdown requested. Already queued tasks will be completed.");
            workers.forEach(Worker::interruptIfIdle);
        } finally {
            lock.unlock();
        }
        signalIfTerminated();
    }

    @Override
    public void shutdownNow() {
        lock.lock();
        try {
            if (shutdownNow) {
                return;
            }
            shutdown = true;
            shutdownNow = true;
            int dropped = 0;
            for (Worker worker : workers) {
                dropped += worker.drainQueue().size();
                worker.interruptNow();
            }
            PoolLogger.warn("Pool", "shutdownNow requested. Dropped queued tasks: " + dropped);
        } finally {
            lock.unlock();
        }
        signalIfTerminated();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit");
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (terminationMonitor) {
            while (!isTerminated()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(terminationMonitor, remaining);
            }
            return true;
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public boolean isShutdownNow() {
        return shutdownNow;
    }

    public boolean isTerminated() {
        lock.lock();
        try {
            return shutdown && workers.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    public long acceptedTaskCount() {
        return acceptedTasks.get();
    }

    public PoolMetrics metrics() {
        lock.lock();
        try {
            int queued = 0;
            for (Worker worker : workers) {
                queued += worker.queueSize();
            }
            int idle = countSpareWorkersLocked();
            return new PoolMetrics(
                    workers.size(),
                    idle,
                    queued,
                    Math.max(0, workers.size() - idle),
                    shutdown,
                    shutdownNow);
        } finally {
            lock.unlock();
        }
    }

    boolean canWorkerStopAfterIdle(Worker worker) {
        lock.lock();
        try {
            if (shutdown || shutdownNow) {
                return true;
            }
            int currentWorkers = workers.size();
            int spareWorkers = countSpareWorkersLocked();
            return workers.contains(worker)
                    && currentWorkers - 1 >= corePoolSize
                    && spareWorkers - 1 >= minSpareThreads;
        } finally {
            lock.unlock();
        }
    }

    void onWorkerExit(Worker worker) {
        lock.lock();
        try {
            workers.remove(worker);
            if (!shutdown) {
                ensureCoreWorkersLocked();
                ensureSpareWorkersLocked();
            }
        } finally {
            lock.unlock();
        }
        signalIfTerminated();
    }

    private void ensureCoreWorkersLocked() {
        while (!shutdown && workers.size() < corePoolSize) {
            addWorkerLocked();
        }
    }

    private void ensureSpareWorkersLocked() {
        int spare = countSpareWorkersLocked();
        while (!shutdown && spare < minSpareThreads && workers.size() < maxPoolSize) {
            addWorkerLocked();
            spare++;
            PoolLogger.info("Pool",
                    "Spare worker created to keep reserve: spare=" + spare + ", minSpareThreads=" + minSpareThreads);
        }
    }

    private int countSpareWorkersLocked() {
        int count = 0;
        for (Worker worker : workers) {
            if (worker.isSpare()) {
                count++;
            }
        }
        return count;
    }

    private Worker addWorkerLocked() {
        Worker worker = new Worker(this, queueSize, keepAliveTime, timeUnit);
        Thread thread = threadFactory.newThread(worker);
        worker.attachThread(thread);
        workers.add(worker);
        thread.start();
        return worker;
    }

    private void logAccepted(Worker worker, Runnable command) {
        PoolLogger.info("Pool", "Task accepted into queue of " + worker.name() + ": " + command
                + " | queued=" + worker.queueSize() + ", remainingCapacity=" + worker.remainingCapacity());
    }

    private void signalIfTerminated() {
        if (shutdown && metrics().workerCount() == 0) {
            synchronized (terminationMonitor) {
                terminationMonitor.notifyAll();
            }
        }
    }
}
