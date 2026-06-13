package com.koshevoi.threadpool.benchmark;

import com.koshevoi.threadpool.CustomThreadPool;
import com.koshevoi.threadpool.PoolLogger;
import com.koshevoi.threadpool.balancing.LeastLoadedBalancer;
import com.koshevoi.threadpool.balancing.RoundRobinBalancer;
import com.koshevoi.threadpool.balancing.TaskBalancer;
import com.koshevoi.threadpool.rejection.CallerRunsRejectionPolicy;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public final class TuningStudy {
    private static final int TASKS = 10_000;

    private TuningStudy() {
    }

    public static void main(String[] args) throws Exception {
        PoolLogger.setEnabled(false);

        List<Config> configs = List.of(
                new Config("small-queue", 2, 4, 32, 0, new LeastLoadedBalancer()),
                new Config("balanced", 4, 8, 256, 1, new LeastLoadedBalancer()),
                new Config("large-queue", 4, 8, 1024, 1, new LeastLoadedBalancer()),
                new Config("fixed-8", 8, 8, 256, 0, new LeastLoadedBalancer()),
                new Config("round-robin", 4, 8, 256, 1, new RoundRobinBalancer())
        );

        System.out.println("Tuning study: tasks=" + TASKS);
        System.out.printf("%-14s %4s %4s %8s %8s %-14s %12s %14s%n",
                "Name", "core", "max", "queue", "spare", "balancer", "Time, ms", "Tasks/sec");
        for (Config config : configs) {
            Result result = run(config);
            System.out.printf("%-14s %4d %4d %8d %8d %-14s %12.2f %14.2f%n",
                    config.name(), config.core(), config.max(), config.queue(), config.minSpare(),
                    config.balancer().getClass().getSimpleName(), result.millis(), result.tasksPerSecond());
        }
    }

    private static Result run(Config config) throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                "Tuning",
                config.core(),
                config.max(),
                1,
                TimeUnit.SECONDS,
                config.queue(),
                config.minSpare(),
                config.balancer(),
                new CallerRunsRejectionPolicy()
        );
        Result result = measure(pool, TASKS);
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        return result;
    }

    private static Result measure(Executor executor, int tasks) throws InterruptedException {
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
        return new Result(millis, tasks / (millis / 1_000.0));
    }

    private static void consumeCpu() {
        long value = 0;
        for (int i = 0; i < 2_000; i++) {
            value += (long) i * 31L;
        }
        if (value == 42) {
            System.out.println("Impossible branch");
        }
    }

    private record Config(String name, int core, int max, int queue, int minSpare, TaskBalancer balancer) {
    }

    private record Result(double millis, double tasksPerSecond) {
    }
}
