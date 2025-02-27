package com.tudoujun.distribute.common.network;

import java.util.List;

import com.google.protobuf.MessageLite;
import com.tudoujun.distribute.common.Constants;
import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.enums.PacketType;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description 网络请求
 * @create: 2025/02/19 17:51
 */
@Slf4j
public class RequestWrapper {

    private OnResponseListener listener;
    private ChannelHandlerContext ctx;
    private NettyPacket request;
    private String requestSequence;
    private int nodeId;

    public RequestWrapper(ChannelHandlerContext ctx, NettyPacket request) {
        this(ctx, request, -1, null);
    }

    public RequestWrapper(ChannelHandlerContext ctx, NettyPacket request, int nodeId, OnResponseListener listener) {
        this.ctx = ctx;
        this.request = request;
        this.requestSequence = request.getSequence();
        this.nodeId = nodeId;
        this.listener = listener;
    }

    public NettyPacket getRequest() {
        return request;
    }

    public String getRequestSequence() {
        return requestSequence;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public void sendResponse() {
        sendResponse(null);
    }

    public void sendResponse(MessageLite response) {
        byte[] body = response == null ? new byte[0] : response.toByteArray();
        NettyPacket responsePacket = NettyPacket.buildPacket(body, PacketType.of(request.getPacketType()));
        List<NettyPacket> responses = responsePacket.partitionChunk(request.isSupportChunked(), Constants.CHUNKED_SIZE);
        if (responses.size() > 0) {
            log.info("返回响应通过chunked方式发送，共{}块", responses.size());
        }
        for (NettyPacket packet : responses) {
            sendResponse(packet, requestSequence);
        }
    }

    public void sendResponse(NettyPacket response, String sequence) {
        response.setSequence(sequence);
        response.setNodeId(nodeId);
        ctx.writeAndFlush(response);
        if (listener != null) {
            this.listener.onResponse(response.getBody().length);
        }
    }

    public interface OnResponseListener {
        void onResponse(int bodyLength);
    }
}
