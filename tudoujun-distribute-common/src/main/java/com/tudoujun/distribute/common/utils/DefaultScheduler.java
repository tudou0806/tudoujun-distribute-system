package com.tudoujun.distribute.common.utils;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description 调度器
 * @create: 2025/02/25 18:29
 */
@Slf4j
public class DefaultScheduler {

    private ScheduledThreadPoolExecutor executor;
    private AtomicInteger schedulerThreadId = new AtomicInteger(0);
    private AtomicBoolean shutdown = new AtomicBoolean(true);

    public DefaultScheduler(String threadNamePrefix) {
        this(threadNamePrefix, Runtime.getRuntime().availableProcessors() * 2, true);
    }

    public DefaultScheduler(String threadNamePrefix, int threads) {
        this(threadNamePrefix, threads, true);
    }

    public DefaultScheduler(String threadNamePrefix, int threads, boolean daemon) {
        if (shutdown.compareAndSet(true, false)) {
            this.executor = new ScheduledThreadPoolExecutor(threads, r -> new DefaultThread(threadNamePrefix + schedulerThreadId.getAndIncrement(), r, daemon));
            this.executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        }
    }

    /**
     * 调度任务
     */
    public void scheduleOnce(String name, Runnable r) {
        scheduleOnce(name, r, 0);
    }

    /**
     * 调度任务
     */
    public void scheduleOnce(String name, Runnable r, long delay) {
        schedule(name, r, delay, 0, TimeUnit.MILLISECONDS);
    }

    /**
     * 调度任务
     */
    public void schedule(String name, Runnable r, long delay, long period, TimeUnit unit) {
        if (log.isDebugEnabled()) {
            log.debug("Scheduling task {} with initial delay {} ms and period {} ms", name, delay, period);
        }
        Runnable delegate = () -> {
            try {
                if (log.isTraceEnabled()) {
                    log.trace("Beginning execution of scheduled task {}", name);
                }
                r.run();
            } catch (Throwable e) {
                log.trace("Uncaught exception in scheduled task {}", name, e);
            } finally {
                if (log.isTraceEnabled()) {
                    log.trace("Completed execution of scheduled task {}", name);
                }
            }
        };

        if (shutdown.get()) {
            return;
        }

        if (period > 0) {
            executor.scheduleWithFixedDelay(delegate, delay, period, unit);
        } else {
            executor.schedule(delegate, delay, unit);
        }
    }

    /**
     * 优雅停止
     */
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            log.info("DefaultScheduler shutdown");
            executor.shutdown();;
        }
    }

}
