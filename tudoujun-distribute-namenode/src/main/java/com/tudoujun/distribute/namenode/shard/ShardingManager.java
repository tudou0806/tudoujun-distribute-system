package com.tudoujun.distribute.namenode.shard;

import com.tudoujun.distribute.namenode.config.NameNodeConfig;
import com.tudoujun.distribute.namenode.shard.controller.ControllerManager;
import com.tudoujun.distribute.namenode.shard.peer.PeerNameNodes;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/03/04 17:39
 */
public class ShardingManager {

    private PeerNameNodes peerNameNodes;
    private NameNodeConfig nameNodeConfig;
    private ControllerManager controllerManager;

    public ShardingManager(NameNodeConfig nameNodeConfig, PeerNameNodes peerNameNodes, ControllerManager controllerManager) {
        this.nameNodeConfig = nameNodeConfig;
        this.peerNameNodes = peerNameNodes;
        this.controllerManager = controllerManager;
    }

    public void start() {
        String nameNodePeerServers = nameNodeConfig.getNameNodePeerServers();
        String[] servers = nameNodePeerServers.split(",");
        for (String server : servers) {
            peerNameNodes.connect(server);
        }
    }
}
