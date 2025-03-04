package com.tudoujun.distribute.namenode;

import java.util.concurrent.atomic.AtomicBoolean;

import com.tudoujun.distribute.common.utils.DefaultScheduler;
import com.tudoujun.distribute.namenode.config.NameNodeConfig;
import com.tudoujun.distribute.namenode.server.NameNodeApis;
import com.tudoujun.distribute.namenode.server.NameNodeServer;
import com.tudoujun.distribute.namenode.shard.ShardingManager;
import com.tudoujun.distribute.namenode.shard.controller.ControllerManager;
import com.tudoujun.distribute.namenode.shard.peer.PeerNameNodes;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/03/03 15:18
 */
public class NameNode {

    private final DefaultScheduler defaultScheduler;
    private final ControllerManager controllerManager;
    private final NameNodeApis nameNodeApis;
    private final NameNodeServer nameNodeServer;
    private final PeerNameNodes peerNameNodes;
    private final ShardingManager shardingManager;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public NameNode(NameNodeConfig nameNodeConfig) {
        this.defaultScheduler = new DefaultScheduler("NameNode-Scheduler-");
        this.peerNameNodes = new PeerNameNodes(defaultScheduler, nameNodeConfig);
        this.controllerManager = new ControllerManager(nameNodeConfig, peerNameNodes);
        this.nameNodeApis = new NameNodeApis(peerNameNodes, nameNodeConfig, controllerManager, defaultScheduler);
        this.nameNodeServer = new NameNodeServer(nameNodeConfig, defaultScheduler, nameNodeApis);
        this.shardingManager = new ShardingManager(nameNodeConfig, peerNameNodes, controllerManager);
    }

    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            shardingManager.start();
            nameNodeServer.start();
        }
    }

    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            nameNodeServer.shutdown();
        }
    }
}
