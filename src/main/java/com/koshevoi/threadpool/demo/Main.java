package com.koshevoi.threadpool.demo;

import com.koshevoi.threadpool.CustomThreadPool;
import com.koshevoi.threadpool.PoolLogger;
import com.koshevoi.threadpool.balancing.LeastLoadedBalancer;
import com.koshevoi.threadpool.rejection.AbortRejectionPolicy;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        PoolLogger.setEnabled(true);
        normalWorkAndShutdownDemo();
        idleTimeoutDemo();
        overloadDemo();
        submitDemo();
    }

    private static void normalWorkAndShutdownDemo() throws InterruptedException {
        PoolLogger.info("Demo", "=== Scenario 1: normal work + graceful shutdown ===");
        CustomThreadPool pool = new CustomThreadPool(
                "MainPool",
                2,
                4,
                5,
                TimeUnit.SECONDS,
                5,
                1,
                new LeastLoadedBalancer(),
                new AbortRejectionPolicy()
        );

        for (int i = 1; i <= 8; i++) {
            pool.execute(new DemoTask("normal-task-" + i, 700));
        }

        Thread.sleep(1_500);
        PoolLogger.info("Demo", "Metrics before shutdown: " + pool.metrics());
        pool.shutdown();
        boolean terminated = pool.awaitTermination(10, TimeUnit.SECONDS);
        PoolLogger.info("Demo", "Scenario 1 terminated=" + terminated + ", acceptedTasks=" + pool.acceptedTaskCount());
    }

    private static void idleTimeoutDemo() throws InterruptedException {
        PoolLogger.info("Demo", "=== Scenario 2: pool grows and extra workers stop by idle timeout ===");
        CustomThreadPool pool = new CustomThreadPool(
                "IdlePool",
                1,
                3,
                1,
                TimeUnit.SECONDS,
                1,
                0,
                new LeastLoadedBalancer(),
                new AbortRejectionPolicy()
        );

        for (int i = 1; i <= 5; i++) {
            try {
                pool.execute(new DemoTask("idle-demo-task-" + i, 450));
            } catch (RejectedExecutionException rejected) {
                PoolLogger.warn("Demo", "Unexpected rejection in idle scenario: " + rejected.getMessage());
            }
        }

        Thread.sleep(3_000);
        PoolLogger.info("Demo", "Metrics after idle timeout: " + pool.metrics());
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
    }

    private static void overloadDemo() throws InterruptedException {
        PoolLogger.info("Demo", "=== Scenario 3: overload and rejection ===");
        CustomThreadPool pool = new CustomThreadPool(
                "OverloadPool",
                1,
                2,
                3,
                TimeUnit.SECONDS,
                1,
                0,
                new LeastLoadedBalancer(),
                new AbortRejectionPolicy()
        );

        for (int i = 1; i <= 8; i++) {
            try {
                pool.execute(new DemoTask("overload-task-" + i, 1_200));
            } catch (RejectedExecutionException rejected) {
                PoolLogger.warn("Demo", "Client observed rejection for overload-task-" + i + ": " + rejected.getMessage());
            }
        }

        Thread.sleep(2_800);
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
    }

    private static void submitDemo() throws Exception {
        PoolLogger.info("Demo", "=== Scenario 4: submit(Callable) returns Future ===");
        CustomThreadPool pool = new CustomThreadPool(
                "SubmitPool",
                2,
                2,
                2,
                TimeUnit.SECONDS,
                2,
                0,
                new LeastLoadedBalancer(),
                new AbortRejectionPolicy()
        );

        Future<String> future = pool.submit(() -> {
            Thread.sleep(300);
            return "Callable result from " + Thread.currentThread().getName();
        });

        PoolLogger.info("Demo", "Future result: " + future.get());
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
    }
}
