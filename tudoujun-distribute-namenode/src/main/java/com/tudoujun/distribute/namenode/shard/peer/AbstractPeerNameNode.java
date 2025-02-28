package com.tudoujun.distribute.namenode.shard.peer;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/28 11:20
 */
public abstract class AbstractPeerNameNode implements PeerNameNode {

    private final String server;
    protected int currentNodeId;
    private final int targetNodeId;

    public AbstractPeerNameNode(int currentNodeId, int targetNodeId, String server) {
        this.currentNodeId = currentNodeId;
        this.targetNodeId = targetNodeId;
        this.server = server;
    }

    @Override
    public int getTargetNodeId() {
        return targetNodeId;
    }

    @Override
    public String getServer() {
        return server;
    }
}
