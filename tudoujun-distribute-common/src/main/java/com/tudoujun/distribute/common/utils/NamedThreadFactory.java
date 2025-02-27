package com.tudoujun.distribute.common.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/26 13:38
 */
public class NamedThreadFactory implements ThreadFactory {

    private boolean daemon;
    private String prefix;
    private AtomicInteger threadId = new AtomicInteger(0);

    public NamedThreadFactory(String prefix) {
        this(prefix, true);
    }

    public NamedThreadFactory(String prefix, boolean daemon) {
        this.daemon = daemon;
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        return new DefaultThread(prefix + "-" + threadId.getAndIncrement(), r, daemon);
    }
}
