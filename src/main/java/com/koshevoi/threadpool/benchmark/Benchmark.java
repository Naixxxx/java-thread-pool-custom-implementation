package com.koshevoi.threadpool.benchmark;

import com.koshevoi.threadpool.CustomThreadPool;
import com.koshevoi.threadpool.PoolLogger;
import com.koshevoi.threadpool.balancing.LeastLoadedBalancer;
import com.koshevoi.threadpool.rejection.CallerRunsRejectionPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class Benchmark {
    private static final int TASKS = 20_000;
    private static final int CORE = 4;
    private static final int MAX = 8;
    private static final int QUEUE_PER_WORKER = 256;

    private Benchmark() {
    }

    public static void main(String[] args) throws Exception {
        PoolLogger.setEnabled(false);
        System.out.println("Benchmark: tasks=" + TASKS + ", core=" + CORE + ", max=" + MAX);
        System.out.println("Workload: short CPU-bound task with small deterministic loop");

        runCustomPoolOnce(3_000);
        runJdkBoundedOnce(3_000);
        runJdkFixedOnce(3_000);

        List<Result> results = new ArrayList<>();
        results.add(runCustomPoolOnce(TASKS));
        results.add(runJdkBoundedOnce(TASKS));
        results.add(runJdkFixedOnce(TASKS));

        System.out.println();
        System.out.printf("%-28s %12s %14s%n", "Executor", "Time, ms", "Tasks/sec");
        for (Result result : results) {
            System.out.printf("%-28s %12.2f %14.2f%n", result.name(), result.millis(), result.tasksPerSecond());
        }
    }

    private static Result runCustomPoolOnce(int tasks) throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                "BenchCustom",
                CORE,
                MAX,
                1,
                TimeUnit.SECONDS,
                QUEUE_PER_WORKER,
                1,
                new LeastLoadedBalancer(),
                new CallerRunsRejectionPolicy());
        Result result = measure("CustomThreadPool", pool, tasks);
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        return result;
    }

    private static Result runJdkBoundedOnce(int tasks) throws InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                CORE,
                MAX,
                1,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(QUEUE_PER_WORKER * MAX),
                new ThreadPoolExecutor.CallerRunsPolicy());
        Result result = measure("JDK ThreadPoolExecutor bounded", executor, tasks);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        return result;
    }

    private static Result runJdkFixedOnce(int tasks) throws InterruptedException {
        var executor = Executors.newFixedThreadPool(CORE);
        Result result = measure("JDK fixedThreadPool", executor, tasks);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        return result;
    }

    private static Result measure(String name, Executor executor, int tasks) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(tasks);
        long started = System.nanoTime();
        for (int i = 0; i < tasks; i++) {
            executor.execute(() -> {
                consumeCpu();
                latch.countDown();
            });
        }
        latch.await();
        long finished = System.nanoTime();
        double millis = (finished - started) / 1_000_000.0;
        return new Result(name, millis, tasks / (millis / 1_000.0));
    }

    private static void consumeCpu() {
        long value = 0;
        for (int i = 0; i < 2_000; i++) {
            value += (long) i * 31L;
        }
        if (value == 42) {
            System.out.println("Impossible branch to prevent full elimination");
        }
    }

    private record Result(String name, double millis, double tasksPerSecond) {
    }
}
