package com.tudoujun.distribute.common.exception;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/25 14:32
 */
public class RequestTimeoutException extends RuntimeException {

    public RequestTimeoutException(String msg) {
        super(msg);
    }
}
