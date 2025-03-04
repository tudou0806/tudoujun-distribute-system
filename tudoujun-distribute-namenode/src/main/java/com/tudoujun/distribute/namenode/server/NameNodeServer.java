package com.tudoujun.distribute.namenode.server;

import java.util.Collections;

import com.tudoujun.distribute.common.network.NetServer;
import com.tudoujun.distribute.common.utils.DefaultScheduler;
import com.tudoujun.distribute.namenode.config.NameNodeConfig;

import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/28 11:06
 */
@Slf4j
public class NameNodeServer {

    private NameNodeApis nameNodeApis;
    private NetServer netServer;
    private NameNodeConfig nameNodeConfig;

    public NameNodeServer(NameNodeConfig nameNodeConfig, DefaultScheduler defaultScheduler, NameNodeApis nameNodeApis) {
        this.nameNodeApis = nameNodeApis;
        this.netServer = new NetServer("NameNode-Sever-", defaultScheduler);
        this.nameNodeConfig = nameNodeConfig;
    }

    public void start() throws InterruptedException {
        this.netServer.addHandlers(Collections.singletonList(nameNodeApis));
        this.netServer.bind(nameNodeConfig.getPort());
    }

    public void shutdown() {
        log.info("Shutdown NameNodeServer");
        this.netServer.shutdown();
    }
}
