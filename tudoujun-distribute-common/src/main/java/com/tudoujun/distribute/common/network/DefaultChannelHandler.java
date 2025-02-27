package com.tudoujun.distribute.common.network;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.exception.RequestTimeoutException;
import com.tudoujun.distribute.common.utils.DefaultScheduler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description 默认消息处理器
 * @create: 2025/02/26 13:04
 */
@Slf4j
public class DefaultChannelHandler extends AbstractChannelHandler {

    private String name;
    private volatile SocketChannel socketChannel;
    private volatile boolean hasOtherHandlers = false;
    private List<NettyPacketListener> nettyPacketListeners = new ArrayList<>();
    private List<ConnectListener> connectListeners = new ArrayList<>();
    private SyncRequestSupport syncRequestSupport;

    public DefaultChannelHandler(String name, DefaultScheduler defaultScheduler, long requestTimeout) {
        this.name = name;
        this.syncRequestSupport = new SyncRequestSupport(name, defaultScheduler, requestTimeout);
    }

    public void setHasOtherHandlers(boolean hasOtherHandlers) {
        this.hasOtherHandlers = hasOtherHandlers;
    }

    /**
     * 是否建立连接
     */
    public boolean isConnected() {
        return socketChannel != null && socketChannel.isActive();
    }

    public SocketChannel socketChannel() {
        return socketChannel;
    }

    /**
     * 发送消息，异步转同步响应
     */
    public NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException {
        return syncRequestSupport.sendRequest(nettyPacket);
    }

    /**
     * 发送消息，不需要同步响应
     */
    public void send(NettyPacket nettyPacket) {
        syncRequestSupport.setSequence(nettyPacket);
        socketChannel.writeAndFlush(nettyPacket);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        socketChannel = (SocketChannel)ctx.channel();
        syncRequestSupport.setSocketChannel(socketChannel);
        invokeConnectListener(true);
        log.debug("Socket channel is connected. {}", socketChannel.id().asLongText());
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        socketChannel = null;
        syncRequestSupport.setSocketChannel(null);
        invokeConnectListener(false);
        log.debug("Socket channel is disconnected. {}", socketChannel.id().asLongText());
        ctx.fireChannelInactive();
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) throws Exception {
        synchronized (this) {
            boolean ret = syncRequestSupport.onResponse(nettyPacket);
            RequestWrapper requestWrapper = new RequestWrapper(ctx, nettyPacket);
            invokeListeners(requestWrapper);
            return !hasOtherHandlers || ret;
        }
    }

    /**
     * 回调消息监听器
     */
    private void invokeListeners(RequestWrapper requestWrapper) {
        for (NettyPacketListener listener : nettyPacketListeners) {
            try {
                listener.onMessage(requestWrapper);
            } catch (Exception e) {
                log.error("Exception occur on invoke listener: ", e);
            }
        }
    }

    /**
     * 回调连接监听器
     */
    private void invokeConnectListener(boolean isConnected) {
        for (ConnectListener listener : connectListeners) {
            try {
                listener.onConnectStatusChanged(isConnected);
            } catch (Exception e) {
                log.error("Exception occur on invoke listener: ", e);
            }
        }
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return Collections.emptySet();
    }

    /**
     * 添加消息监听器
     */
    public void addNettyPacketListener(NettyPacketListener listener) {
        nettyPacketListeners.add(listener);
    }

    public void addConnectListener(ConnectListener listener) {
        connectListeners.add(listener);
    }

    public void clearNettyPacketListener() {
        nettyPacketListeners.clear();
    }

    public void clearConnectListener() {
        connectListeners.clear();
    }
}
