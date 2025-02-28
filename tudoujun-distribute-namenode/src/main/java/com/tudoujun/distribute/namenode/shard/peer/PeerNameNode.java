package com.tudoujun.distribute.namenode.shard.peer;

import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.exception.RequestTimeoutException;

/**
 * @author xiaowenjun
 * @description NameNode节点的连接
 * @create: 2025/02/28 11:10
 */
public interface PeerNameNode {

    /**
     * 往PeerNodeName节点发送网络包，如果连接断开，会等待连接重新建立
     */
    void send(NettyPacket nettyPacket) throws InterruptedException;

    /**
     * 往PeerNodeName节点发送网络包，同步发送
     */
    NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException;

    /**
     * 关闭连接
     */
    void close();

    /**
     * 获取NameNode节点ID
     */
    int getTargetNodeId();

    /**
     * 获取服务连接的IP和端口
     */
    String getServer();

    /**
     * 是否连接上
     */
    boolean isConnected();
}
