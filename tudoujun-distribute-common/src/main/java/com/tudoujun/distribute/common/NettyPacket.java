package com.tudoujun.distribute.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import com.tudoujun.distribute.common.enums.PacketType;
import com.tudoujun.distribute.model.common.NettyPacketHeader;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description
 * | -------------------- | -------------------- | -------------- | --------------------- |
 * | HeaderLength         | Actual Header Length | ContentLength  | Actual Content Length |
 * | -------------------- | -------------------- | -------------- | --------------------- |
 * @create: 2025/02/19 14:49
 */
@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NettyPacket {

    /**
     * 消息体
     */
    private byte[] body;

    /**
     * 请求头
     */
    private Map<String, String> header;

    public static NettyPacket copy(NettyPacket nettyPacket) {
        return new NettyPacket(nettyPacket.getBody(), new HashMap<>(nettyPacket.getHeader()));
    }

    /**
     * 是否需要广播给其它节点
     */
    public void setBroadcast(boolean broadcast) {
        header.put("broadcast", String.valueOf(broadcast));
    }

    public boolean getBroadcast() {
        return Boolean.parseBoolean(header.getOrDefault("broadcast", String.valueOf("false")));
    }

    /**
     * 请求序列号
     */
    public void setSequence(String sequence) {
        if (sequence != null) {
            header.put("sequence", sequence);
        }
    }

    public String getSequence() {
        return header.get("sequence");
    }

    /**
     * 请求包类型
     */
    public void setPacketType(int packetType) {
        header.put("packetType", String.valueOf(packetType));
    }

    public int getPacketType() {
        return Integer.parseInt(header.getOrDefault("packetType", "0"));
    }

    public void setUserToken(String token) {
        header.put("userToken", token);
    }

    public String getUserToken() {
        return header.getOrDefault("userToken", "");
    }

    public void setUserName(String userName) {
        header.put("username", userName);
    }

    public String getUserName() {
        return header.getOrDefault("username", "");
    }

    public void setError(String error) {
        header.put("error", error);
    }

    public String getError() {
        return header.getOrDefault("error", null);
    }

    public boolean isSuccess() {
        return getError() == null;
    }

    public boolean isError() {
        return !isSuccess();
    }

    public void setNodeId(int nodeId) {
        header.put("nodeId", String.valueOf(nodeId));
    }

    public int getNodeId() {
        return Integer.parseInt(header.getOrDefault("nodeId", "-1"));
    }

    public void setAck(int ack) {
        header.put("ack", String.valueOf(ack));
    }

    public int getAck() {
        return Integer.parseInt(header.getOrDefault("ack", "0"));
    }

    public void setTimeoutInMs(int timeoutInMs) {
        header.put("timeoutInMs", String.valueOf(timeoutInMs));
    }

    public int getTimeoutInMs() {
        return Integer.parseInt(header.getOrDefault("timeoutInMs", "0"));
    }

    public void setSupportChunked(boolean chunkedFinish) {
        header.put("supportChunked", String.valueOf(chunkedFinish));
    }

    public boolean isSupportChunked() {
        return Boolean.parseBoolean(header.getOrDefault("supportChunked", "false"));
    }

    /**
     * 创建通用网络请求
     */
    public static NettyPacket buildPacket(byte[] body, PacketType packetType) {
        NettyPacket nettyPacket = NettyPacket.builder()
                .body(body)
                .header(new HashMap<>())
                .build();
        nettyPacket.setPacketType(packetType.getValue());
        return nettyPacket;
    }

    /**
     * 合并消息体
     */
    public void mergeChunkedBody(NettyPacket otherPacket) {
        int newBodyLength = body.length + otherPacket.getBody().length;
        byte[] newBody = new byte[newBodyLength];
        System.arraycopy(body, 0, newBody, 0, body.length);
        System.arraycopy(otherPacket.getBody(), 0, newBody, body.length, otherPacket.getBody().length);
        this.body = newBody;
    }

    /**
     * 拆分消息体
     */
    public List<NettyPacket> partitionChunk(boolean supportChunked, int maxPackageSize) {
        if (!supportChunked) {
            return Collections.singletonList(this);
        }

        int bodyLength = this.body.length;
        if (bodyLength <= maxPackageSize) {
            return Collections.singletonList(this);
        }

        int packageCount = bodyLength / maxPackageSize;
        if (bodyLength % maxPackageSize > 0) {
            packageCount++;
        }

        int remainLength = bodyLength;
        List<NettyPacket> results = new ArrayList<>();
        for (int i = 0; i < packageCount; i++) {
            int partitionBodyLength = Math.min(remainLength, maxPackageSize);
            byte[] partitionBody = new byte[partitionBodyLength];
            System.arraycopy(body, bodyLength - remainLength, partitionBody, 0, partitionBodyLength);
            remainLength -= partitionBodyLength;
            NettyPacket partitionPacket = NettyPacket.builder()
                    .header(header)
                    .body(partitionBody)
                    .build();
            partitionPacket.setSupportChunked(true);
            results.add(partitionPacket);
        }
        // 结束标识包
        NettyPacket tailPacket = NettyPacket.builder()
                .header(header)
                .body(new byte[0])
                .build();
        tailPacket.setSupportChunked(true);
        results.add(tailPacket);

        return results;
    }

    /**
     * 将数据写入ByteBuf
     */
    public void write(ByteBuf out) {
        NettyPacketHeader nettyPacketHeader = NettyPacketHeader.newBuilder().putAllHeaders(header).build();
        byte[] headerBytes = nettyPacketHeader.toByteArray();
        out.writeInt(headerBytes.length);
        out.writeBytes(headerBytes);
        out.writeInt(body.length);
        out.writeBytes(body);
    }

    /**
     * 解包
     */
    public static NettyPacket parsePacket(ByteBuf byteBuf) throws InvalidProtocolBufferException {
        int headerLength = byteBuf.readInt();
        byte[] headerBytes = new byte[headerLength];
        byteBuf.readBytes(headerBytes);
        NettyPacketHeader nettyPacketHeader = NettyPacketHeader.parseFrom(headerBytes);
        int bodyLength = byteBuf.readInt();
        byte[] bodyBytes = new byte[bodyLength];
        byteBuf.readBytes(bodyBytes);
        return NettyPacket.builder()
                .header(nettyPacketHeader.getHeadersMap())
                .body(bodyBytes)
                .build();
    }
}
