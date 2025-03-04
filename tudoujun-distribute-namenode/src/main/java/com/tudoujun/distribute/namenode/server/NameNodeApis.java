package com.tudoujun.distribute.namenode.server;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.enums.PacketType;
import com.tudoujun.distribute.common.network.AbstractChannelHandler;
import com.tudoujun.distribute.common.network.RequestWrapper;
import com.tudoujun.distribute.common.utils.DefaultScheduler;
import com.tudoujun.distribute.model.namenode.NameNodeAwareRequest;
import com.tudoujun.distribute.namenode.config.NameNodeConfig;
import com.tudoujun.distribute.namenode.shard.controller.ControllerManager;
import com.tudoujun.distribute.namenode.shard.peer.PeerNameNode;
import com.tudoujun.distribute.namenode.shard.peer.PeerNameNodes;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/28 11:07
 */
@Slf4j
public class NameNodeApis extends AbstractChannelHandler {

    private final PeerNameNodes peerNameNodes;
    private final NameNodeConfig nameNodeConfig;
    private final ControllerManager controllerManager;
    private final DefaultScheduler defaultScheduler;
    private final ThreadPoolExecutor executor;
    protected int nodeId;

    public NameNodeApis(PeerNameNodes peerNameNodes,
                        NameNodeConfig nameNodeConfig,
                        ControllerManager controllerManager,
                        DefaultScheduler defaultScheduler) {
        this.peerNameNodes = peerNameNodes;
        this.nameNodeConfig = nameNodeConfig;
        this.controllerManager = controllerManager;
        this.defaultScheduler = defaultScheduler;
        this.executor = new ThreadPoolExecutor(nameNodeConfig.getNameNodeApiCoreSize(), nameNodeConfig.getNameNodeApiMaximumPoolSize(),
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(nameNodeConfig.getNameNodeApiQueueSize()));
        this.nodeId = nameNodeConfig.getNameNodeId();
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket request) throws Exception {
        boolean consumedMsg = peerNameNodes.onMessage(request);
        if (consumedMsg) {
            return true;
        }
        if (request.isError()) {
            log.info("收到一个异常请求或响应，丢弃不进行处理 [request={}]", request.getHeader());
            return true;
        }
        RequestWrapper requestWrapper = new RequestWrapper(ctx, request, nodeId, bodyLength -> {});
        PacketType packetType = PacketType.of(request.getPacketType());
        switch (packetType) {
            case NAME_NODE_PEER_AWARE -> handleNameNodePeerAwareRequest(requestWrapper);
            case NAME_NODE_CONTROLLER_VOTE -> controllerManager.onReceiveControllerVote(requestWrapper);
        }
        return false;
    }

    /**
     * 处理节点发起连接后立即发送NameNode节点请求
     */
    private void handleNameNodePeerAwareRequest(RequestWrapper requestWrapper) throws Exception {
        NettyPacket request = requestWrapper.getRequest();
        ChannelHandlerContext ctx = requestWrapper.getCtx();
        NameNodeAwareRequest nameNodeAwareRequest = NameNodeAwareRequest.parseFrom(request.getBody());
        if (nameNodeAwareRequest.getIsClient()) {
            // 只有作为服务端，才会保存新增的连接
            PeerNameNode peer = peerNameNodes.addPeerNode(nameNodeAwareRequest.getNameNodeId(), (SocketChannel) ctx.channel(),
                    nameNodeAwareRequest.getServer(), nameNodeAwareRequest.getNameNodeId(), defaultScheduler);
            if (peer != null) {
                // 作为服务端收到连接请求同时也发送自身信息给别的节点
                controllerManager.reportSelfInfoToPeer(peer, false);
            }
        }
        controllerManager.onAwarePeerNameNode(nameNodeAwareRequest);
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return new HashSet<>();
    }

    @Override
    public ThreadPoolExecutor getExecutor() {
        return executor;
    }
}
