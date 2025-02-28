package com.tudoujun.distribute.namenode.shard.peer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.exception.RequestTimeoutException;
import com.tudoujun.distribute.common.network.NetClient;
import com.tudoujun.distribute.common.utils.DefaultScheduler;
import com.tudoujun.distribute.namenode.config.NameNodeConfig;
import com.tudoujun.distribute.namenode.server.NameNodeApis;
import com.tudoujun.distribute.namenode.shard.controller.ControllerManager;

import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description 负责维护所有NameNode节点之间的组件
 * @create: 2025/02/28 13:38
 */
@Slf4j
public class PeerNameNodes {

    private ControllerManager controllerManager;
    private NameNodeConfig nameNodeConfig;
    private DefaultScheduler defaultScheduler;
    private Map<Integer, PeerNameNode> peerNameNodeMap = new ConcurrentHashMap<>();
    private NameNodeApis nameNodeApis;

    public PeerNameNodes(DefaultScheduler defaultScheduler, NameNodeConfig nameNodeConfig) {
        this.defaultScheduler = defaultScheduler;
        this.nameNodeConfig = nameNodeConfig;
    }

    public void setControllerManager(ControllerManager controllerManager) {
        this.controllerManager = controllerManager;
    }

    public void setNameNodeApis(NameNodeApis nameNodeApis) {
        this.nameNodeApis = nameNodeApis;
    }

    public void connect(String server) {
        connect(server, false);
    }

    /**
     * 作为客户端添加PeerNode,主动连接PeerNameNode
     */
    public void connect(String server, boolean force) {
        String[] info = server.split(":");
        String hostname = info[0];
        int port = Integer.parseInt(info[1]);
        int targetNodeId = Integer.parseInt(info[2]);
        if (targetNodeId == nameNodeConfig.getNameNodeId() && port == nameNodeConfig.getPort()) {
            return;
        }

        synchronized (this) {
            PeerNameNode peer = peerNameNodeMap.get(targetNodeId);
            if (force || peer == null) {
                if (peer != null) {
                    peer.close();
                }
                NetClient netClient = new NetClient("NameNode-PeerNode-" + targetNodeId, defaultScheduler);
                netClient.addHandlers(Collections.singletonList(nameNodeApis));
                PeerNameNode newPeer = new PeerNameNodeClient(netClient, nameNodeConfig.getNameNodeId(), targetNodeId, server);
                peerNameNodeMap.put(targetNodeId, newPeer);
                netClient.addConnectListener((connected) -> {
                    controllerManager.reportSelfInfoToPeer(newPeer, true);
                });
                netClient.connect(hostname, port);
                log.info("新建PeerNameNode连接: [hostname={} port={} nameNodeId={}]", hostname, port, targetNodeId);
            }
        }
    }

    /**
     * 作为服务端添加PeerNode, 收到其它NameNode节点的请求
     */
    public PeerNameNode addPeerNode(int nameNodeId, SocketChannel socketChannel, String server,
                                    int selfNameNodeId, DefaultScheduler defaultScheduler) {
        synchronized (this) {
            PeerNameNode oldPeer = peerNameNodeMap.get(nameNodeId);
            PeerNameNode newPeer = new PeerNameNodeServer(socketChannel, nameNodeConfig.getNameNodeId(), nameNodeId, server, defaultScheduler);
            if (oldPeer == null) {
                log.info("收到新的PeerNameNode通知包，保存连接以便下次使用 [nodeId={}]", nameNodeId);
                peerNameNodeMap.put(nameNodeId, newPeer);
                return newPeer;
            }

            if (oldPeer instanceof PeerNameNodeServer peerNameNodeServer && newPeer.getTargetNodeId() == oldPeer.getTargetNodeId()) {
                // 此种情况为断线连接，需要重置连接
                peerNameNodeServer.setSocketChannel(socketChannel);
                log.info("PeerNameNode 断线重连，需要更新Channel [nodeId={}]", nameNodeId);
                return oldPeer;
            }

            if (selfNameNodeId > nameNodeId) {
                newPeer.close();
                connect(server, true);
                log.info("新的连接NameNodeId比较小，关闭新的连接，并主动往小id的节点发起连接 [nodeId={}]", newPeer.getTargetNodeId());
                return null;
            } else {
                peerNameNodeMap.put(nameNodeId, newPeer);
                oldPeer.close();
                log.info("新的连接NameNodeId比较大，关闭旧的连接，并替换连接,[nodeId={}]", oldPeer.getTargetNodeId());
                return newPeer;
            }
        }
    }

    public List<Integer> broadcast(NettyPacket nettyPacket) {
        return broadcast(nettyPacket, -1);
    }

    public List<Integer> broadcast(NettyPacket nettyPacket, Integer excludeNodeId) {
        return broadcast(nettyPacket, new HashSet<>(excludeNodeId));
    }

    /**
     * 广播给所有的NameNode
     */
    public List<Integer> broadcast(NettyPacket nettyPacket, Set<Integer> excludeNodeIds) {
        try {
            List<Integer> result = new ArrayList<>();
            for (PeerNameNode peer : peerNameNodeMap.values()) {
                if (excludeNodeIds.contains(peer.getTargetNodeId())) {
                    continue;
                }
                peer.send(nettyPacket);
                result.add(peer.getTargetNodeId());
            }
            return result;
        } catch (Exception e) {
            log.error("PeerNameNodes#broadcast has been interrupted!", e);
            return Collections.emptyList();
        }
    }

    /**
     * 广播给所有NameNode节点，同步获取结果
     */
    public List<NettyPacket> broadcastSync(NettyPacket request) {
        try {
            if (peerNameNodeMap.size() == 0) {
                return Collections.emptyList();
            }
            List<NettyPacket> result = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(peerNameNodeMap.size());
            for (PeerNameNode peer : peerNameNodeMap.values()) {
                defaultScheduler.scheduleOnce("同步请求PeerNameNode", () -> {
                    NettyPacket response;
                    NettyPacket requestCopy = NettyPacket.copy(request);
                    try {
                        response = peer.sendSync(requestCopy);
                        result.add(response);
                    } catch (Exception e) {
                        log.error("同步请求PeerNode失败 [nodeId={} sequenceId={}]", peer.getTargetNodeId(), requestCopy.getSequence(), e);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.wait();
            return result;
        } catch (Exception e) {
            log.error("PeerNameNodes#broadcastSync has been interrupted!", e);
            return Collections.emptyList();
        }
    }

    /**
     * 停止
     */
    public void shutdown() {
        log.info("Shutdown PeerNameNodes");
        for (PeerNameNode peer : peerNameNodeMap.values()) {
            peer.close();
        }
    }

    public List<String> getAllServers() {
        return peerNameNodeMap.values()
                .stream()
                .map(PeerNameNode::getServer)
                .collect(Collectors.toList());
    }

    public List<Integer> getAllNodeId() {
        return new ArrayList<>(peerNameNodeMap.keySet());
    }

    /**
     * 获取所有已经建立连接的节点数量
     */
    public int getConnectedCount() {
        synchronized (this) {
            return (int)peerNameNodeMap.values().stream()
                    .filter(PeerNameNode::isConnected)
                    .count();
        }
    }

    /**
     * 收到消息，确认消息是否先被PeerNameNodeServer消费
     */
    public boolean onMessage(NettyPacket request) {
        PeerNameNode peer = peerNameNodeMap.get(request.getNodeId());
        if (peer == null) {
            return false;
        }

        if (!(peer instanceof PeerNameNodeServer server)) {
            return false;
        }

        return server.onMessage(request);
    }

    public void send(int nameNodeId, NettyPacket nettyPacket) throws InterruptedException {
        PeerNameNode peer = peerNameNodeMap.get(nameNodeId);
        if (peer != null) {
            peer.send(nettyPacket);
        } else {
            log.warn("找不到peer节点 [nodeId={}]", nameNodeId);
        }
    }

    public NettyPacket sendSync(int nameNodeId, NettyPacket request) throws InterruptedException, RequestTimeoutException {
        PeerNameNode peer = peerNameNodeMap.get(nameNodeId);
        if (peer != null) {
            return peer.sendSync(request);
        } else {
            log.warn("找不到peer节点 [nodeId={}]", nameNodeId);
        }

        throw new IllegalArgumentException("Invalid nodeId: " + nameNodeId);
    }
}
