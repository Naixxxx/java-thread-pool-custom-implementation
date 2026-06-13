package com.koshevoi.threadpool;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public final class PoolLogger {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static volatile boolean enabled = true;

    private PoolLogger() {
    }

    public static void setEnabled(boolean enabled) {
        PoolLogger.enabled = enabled;
    }

    public static void info(String component, String message) {
        log(component, message);
    }

    public static void warn(String component, String message) {
        log(component, message);
    }

    public static void error(String component, String message, Throwable throwable) {
        log(component, message + " | " + throwable.getClass().getSimpleName() + ": " + throwable.getMessage());
    }

    private static void log(String component, String message) {
        if (!enabled) {
            return;
        }
        String time = LocalTime.now().format(FORMATTER);
        synchronized (System.out) {
            System.out.printf("[%s] [%s] %s%n", time, component, message);
        }
    }
}
