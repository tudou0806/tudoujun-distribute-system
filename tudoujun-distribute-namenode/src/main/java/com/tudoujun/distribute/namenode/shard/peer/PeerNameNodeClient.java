package com.tudoujun.distribute.namenode.shard.peer;

import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.exception.RequestTimeoutException;
import com.tudoujun.distribute.common.network.NetClient;

/**
 * @author xiaowenjun
 * @description 和PeerNameNode连接，作为客户端
 * @create: 2025/02/28 11:23
 */
public class PeerNameNodeClient extends AbstractPeerNameNode {

    private NetClient netClient;

    public PeerNameNodeClient(NetClient netClient, int currentNodeId, int targetNodeId, String server) {
        super(currentNodeId, targetNodeId, server);
        this.netClient = netClient;
    }

    @Override
    public void send(NettyPacket nettyPacket) throws InterruptedException {
        netClient.send(nettyPacket);
    }

    @Override
    public NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException {
        return netClient.sendSync(nettyPacket);
    }

    @Override
    public void close() {
        netClient.shutdown();
    }

    @Override
    public boolean isConnected() {
        return netClient.isConnected();
    }
}
