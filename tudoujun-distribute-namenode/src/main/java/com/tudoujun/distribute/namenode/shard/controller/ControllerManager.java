package com.tudoujun.distribute.namenode.shard.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.enums.PacketType;
import com.tudoujun.distribute.common.network.RequestWrapper;
import com.tudoujun.distribute.common.utils.DefaultScheduler;
import com.tudoujun.distribute.common.utils.NetUtils;
import com.tudoujun.distribute.model.namenode.ControllerVote;
import com.tudoujun.distribute.model.namenode.NameNodeAwareRequest;
import com.tudoujun.distribute.namenode.config.NameNodeConfig;
import com.tudoujun.distribute.namenode.shard.peer.PeerNameNode;
import com.tudoujun.distribute.namenode.shard.peer.PeerNameNodeClient;
import com.tudoujun.distribute.namenode.shard.peer.PeerNameNodes;

import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/28 13:38
 */
@Slf4j
public class ControllerManager {

    private NameNodeConfig nameNodeConfig;
    private AtomicInteger numOfNode;
    private PeerNameNodes peerNameNodes;
    private AtomicInteger newVoteCount = new AtomicInteger(0);
    private boolean hasSelectController = false;
    private ControllerVote currentVote;
    private List<ControllerVote> voteList = new ArrayList<>();
    private AtomicInteger newNodeCount = new AtomicInteger(0);
    private AtomicBoolean isForeController = new AtomicBoolean(false);
    private AtomicInteger voteRound = new AtomicInteger(0);
    private AtomicBoolean startControllerElection = new AtomicBoolean(false);

    public ControllerManager(NameNodeConfig nameNodeConfig,
                             PeerNameNodes peerNameNodes) {
        this.nameNodeConfig = nameNodeConfig;
        this.peerNameNodes = peerNameNodes;
        this.peerNameNodes.setControllerManager(this);
        this.numOfNode = new AtomicInteger(nameNodeConfig.numOfNode());
        this.currentVote = ControllerVote.newBuilder()
                .setVoterNodeId(nameNodeConfig.getNameNodeId())
                .setControllerNodeId(nameNodeConfig.getNameNodeId())
                .setVoteRound(voteRound.get())
                .setForce(false)
                .build();
    }

    /**
     * 上报自身节点信息给其它PeerNameNode
     */
    public void reportSelfInfoToPeer(PeerNameNode nameNode, boolean isClient) throws InterruptedException {
        String hostName = NetUtils.getHostName();
        NameNodeAwareRequest nameNodeInfo = NameNodeAwareRequest.newBuilder()
                .setNameNodeId(nameNodeConfig.getNameNodeId())
                .setServer(hostName + ":" + nameNodeConfig.getPort() + ":" + nameNodeConfig.getNameNodeId())
                .setNumOfNode(numOfNode.get())
                .addAllServers(peerNameNodes.getAllServers())
                .setIsClient(isClient)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(nameNodeInfo.toByteArray(), PacketType.NAME_NODE_PEER_AWARE);
        nameNode.send(nettyPacket);
        log.info("建立了PeerNameNode的连接，发送自身信息[currentNodeId={} targetNodeId={}]", nameNodeConfig.getNameNodeId(), nameNode.getTargetNodeId());
    }

    /**
     * 收到Controller的选举归票
     */
    public void onReceiveControllerVote(RequestWrapper requestWrapper) throws Exception {
        synchronized (this) {
            NettyPacket request = requestWrapper.getRequest();
            ControllerVote controllerVote = ControllerVote.parseFrom(request.getBody());
            log.info("收到Controller选举投票 [voter={} voteNodeId]={} voteRound={}]", controllerVote.getVoterNodeId(),
                    controllerVote.getControllerNodeId(), controllerVote.getVoteRound());
            if (hasSelectController && newVoteCount.get() > 0) {
                log.info("集群已经有Controller节点了，让他强制承认当前集群的Controller [voter={} voteNodeId={} voteRound={}]",
                        currentVote.getVoterNodeId(), currentVote.getControllerNodeId(), currentVote.getVoteRound());
                ControllerVote vote = ControllerVote.newBuilder()
                        .setVoterNodeId(currentVote.getVoterNodeId())
                        .setControllerNodeId(currentVote.getControllerNodeId())
                        .setVoteRound(currentVote.getVoteRound())
                        .build();
                NettyPacket votePackage = NettyPacket.buildPacket(vote.toByteArray(), PacketType.NAME_NODE_CONTROLLER_VOTE);
                requestWrapper.sendResponse(votePackage, null);
                newVoteCount.decrementAndGet();
            } else {
                voteList.add(controllerVote);
                int quorum = numOfNode.get() / 2 + 1;
                if (voteList.size() >= quorum) {
                    notifyAll();
                }
            }
        }
    }

    /**
     * 收到PeerNameNode发过来的信息
     */
    public void onAwarePeerNameNode(NameNodeAwareRequest request) throws Exception {
        newNodeCount.incrementAndGet();
        numOfNode.set(Math.max(numOfNode.get(), request.getNumOfNode()));
        request.getServersList().forEach(peerNameNodes::connect);
        log.info("收到PeerNameNode发送过来的信息: [nodeId={} curNumOfNum={} peerNodeNum={}]",
                request.getNameNodeId(), numOfNode.get(), peerNameNodes.getConnectedCount());
        // 自身节点无需与自身节点连接
        if (peerNameNodes.getConnectedCount() == numOfNode.get() - 1) {
            startControllerElection();
        }
    }

    /**
     * 启动Controller选举
     */
    public void startControllerElection() throws Exception {
        if (startControllerElection.compareAndSet(false, true)) {
            voteList.add(currentVote);
            // 发送信息给所有PeerNameNode,开始选举
            NettyPacket nettyPacket = NettyPacket.buildPacket(currentVote.toByteArray(), PacketType.NAME_NODE_CONTROLLER_VOTE);
            List<Integer> nodeIdList = peerNameNodes.broadcast(nettyPacket);
            log.info("开始尝试选举Controller, 发送当前选票给所有的节点: [nodeIdList={}]", nodeIdList);

            // 归票选出Controller
            int quorum = numOfNode.get() / 2 + 1;
            Integer controllerId;
            while (true) {
                if (voteList.size() != numOfNode.get()) {
                    synchronized (this) {
                        wait(10);
                    }

                    controllerId = getControllerFromVote(voteList, quorum);
                    if (controllerId != null) {
                        log.info("选举出了Controller [controllerNodeId={}]", controllerId);
                        hasSelectController = true;
                        return;
                    }

                    if (voteList.size() == numOfNode.get()) {
                        log.info("当前选举无法选出Controller, 进行下一轮选举");
                        break;
                    }
                }
            }

            int betterControllerNodeId = getBetterControllerNodeId(voteList);
            this.currentVote = ControllerVote.newBuilder()
                    .setVoterNodeId(nameNodeConfig.getNameNodeId())
                    .setControllerNodeId(betterControllerNodeId)
                    .setVoteRound(voteRound.get())
                    .setForce(false)
                    .build();
            voteList.clear();
            startControllerElection.set(false);
            startControllerElection();
        }
    }

    /**
     * 从选票中获取最大的controllerId
     */
    private int getBetterControllerNodeId(List<ControllerVote> votes) {
        return votes.stream().map(ControllerVote::getControllerNodeId).reduce(Integer::max).orElse(0);
    }

    /**
     * 从选票中归票出Controller节点
     */
    private Integer getControllerFromVote(List<ControllerVote> votes, int quorum) {
        synchronized (this) {
            Map<Integer, Integer> voteCountMap = new HashMap<>();
            for (ControllerVote vote : votes) {
                // 如果票据是强制有效的，直接认定是Controller
                if (vote.getForce()) {
                    isForeController.set(true);
                    voteRound.set(vote.getVoteRound());
                    this.currentVote = ControllerVote.newBuilder()
                            .setVoterNodeId(nameNodeConfig.getNameNodeId())
                            .setControllerNodeId(vote.getControllerNodeId())
                            .setVoteRound(voteRound.get())
                            .setForce(true)
                            .build();
                    log.info("收到强制性选票，更新Controller信息 [currentNodeId={} controllerId={} voteRound={}]",
                            nameNodeConfig.getNameNodeId(), vote.getControllerNodeId(), voteRound.get());
                    return vote.getControllerNodeId();
                }

                Integer controllerNodeId = vote.getControllerNodeId();
                voteCountMap.put(controllerNodeId, voteCountMap.getOrDefault(controllerNodeId, 0) + 1);
                for (Map.Entry<Integer, Integer> entry : voteCountMap.entrySet()) {
                    if (entry.getValue() > quorum) {
                        return entry.getKey();
                    }
                }
            }
            return null;
        }
    }
}
