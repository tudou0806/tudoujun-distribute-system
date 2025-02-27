package com.tudoujun.distribute.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/19 16:57
 */
public class Constants {

    /**
     * 网络传输最大字节数
     */
    public static final int MAX_BYTES = 10 * 1024 * 1024;
    /**
     * 分块传输，每一块的大小
     */
    public static final int CHUNKED_SIZE = (int) (MAX_BYTES * 0.5F);

    /**
     * 应用请求计数器
     */
    public static AtomicLong REQUEST_COUNTER = new AtomicLong(1);
}
