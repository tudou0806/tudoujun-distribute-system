package com.tudoujun.distribute.common.network;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/21 16:54
 */
public interface NettyPacketListener {

    void onMessage(RequestWrapper requestWrapper) throws Exception;
}
