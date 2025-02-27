package com.tudoujun.distribute.common.utils;

import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/25 18:33
 */
@Slf4j
public class DefaultThread extends Thread {

    public DefaultThread(String name, boolean daemon) {
        super(name);
        configureThread(name, daemon);
    }

    public DefaultThread(String name, Runnable r, boolean daemon) {
        super(r, name);
        configureThread(name, daemon);
    }

    private void configureThread(String name, boolean daemon) {
        setDaemon(daemon);
        setUncaughtExceptionHandler((t, e) -> log.error("uncaught exception in thread {} :", name, e));
    }
}
