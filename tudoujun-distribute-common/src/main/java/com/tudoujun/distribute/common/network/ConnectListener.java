package com.tudoujun.distribute.common.network;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/21 16:52
 */
public interface ConnectListener {

    void onConnectStatusChanged(boolean connected) throws Exception;
}
