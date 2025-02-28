package com.tudoujun.distribute.namenode.shard.peer;

import com.tudoujun.distribute.common.Constants;
import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.exception.RequestTimeoutException;
import com.tudoujun.distribute.common.network.SyncRequestSupport;
import com.tudoujun.distribute.common.utils.DefaultScheduler;

import io.netty.channel.socket.SocketChannel;

import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description 和PeerNameNode连接，作为服务端
 * @create: 2025/02/28 11:26
 */
@Slf4j
public class PeerNameNodeServer extends AbstractPeerNameNode {

    private final String name;
    private volatile SocketChannel socketChannel;
    private final SyncRequestSupport syncRequestSupport;

    public PeerNameNodeServer(SocketChannel socketChannel, int currentNodeId, int targetNodeId, String server, DefaultScheduler defaultScheduler) {
        super(currentNodeId, targetNodeId, server);
        this.name = "NameNode-PeerNode-" + currentNodeId + "-" + targetNodeId;
        this.syncRequestSupport = new SyncRequestSupport(this.name, defaultScheduler, 5000);
        this.setSocketChannel(socketChannel);
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        synchronized (this) {
            this.socketChannel = socketChannel;
            this.syncRequestSupport.setSocketChannel(socketChannel);
            notifyAll();
        }
    }

    @Override
    public void send(NettyPacket nettyPacket) throws InterruptedException {
        synchronized (this) {
            while(!isConnected()) {
                try {
                    wait(10);
                } catch (InterruptedException e) {
                    log.error("PeerNodeNameServer#send has been Interrupted!", e);
                }
            }
        }
        nettyPacket.setSequence(name + "-" + Constants.REQUEST_COUNTER.getAndIncrement());
        socketChannel.writeAndFlush(nettyPacket);
    }

    @Override
    public NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException {
        return syncRequestSupport.sendRequest(nettyPacket);
    }

    /**
     * 收到消息响应
     */
    public boolean onMessage(NettyPacket nettyPacket) {
        return syncRequestSupport.onResponse(nettyPacket);
    }

    @Override
    public void close() {
        socketChannel.close();
    }

    @Override
    public boolean isConnected() {
        return socketChannel != null && socketChannel.isActive();
    }
}
