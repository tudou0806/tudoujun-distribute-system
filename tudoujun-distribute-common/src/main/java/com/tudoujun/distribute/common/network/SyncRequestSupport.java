package com.tudoujun.distribute.common.network;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.tudoujun.distribute.common.Constants;
import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.enums.PacketType;
import com.tudoujun.distribute.common.exception.RequestTimeoutException;
import com.tudoujun.distribute.common.utils.DefaultScheduler;

import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description 同步请求支持
 * @create: 2025/02/25 14:48
 */
@Slf4j
public class SyncRequestSupport {

    private Map<String, RequestPromise> promiseMap = new ConcurrentHashMap<>();
    private SocketChannel socketChannel;
    private String name;

    public SyncRequestSupport(String name, DefaultScheduler defaultScheduler, long requestTimeout) {
        this.name = name;
        defaultScheduler.schedule("定时检测超时", () -> checkRequestTimeout(requestTimeout), 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void setSocketChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    /**
     * 同步发送请求
     */
    public NettyPacket sendRequest(NettyPacket request) throws RequestTimeoutException {
        setSequence(request);
        RequestPromise promise = new RequestPromise(request);
        promiseMap.put(request.getSequence(), promise);
        socketChannel.writeAndFlush(request);
        if (log.isDebugEnabled()) {
            log.debug("发送请求并同步等待结果: [request={} sequence={}]",
                    PacketType.of(request.getPacketType()).getDescription(), request.getSequence());
        }
        return promise.getResult();
    }

    /**
     * 收到响应
     */
    public boolean onResponse(NettyPacket response) {
        String sequence = response.getSequence();
        if (sequence != null) {
            boolean isChunkFinish = !response.isSupportChunked() || response.getBody().length == 0;
            RequestPromise wrapper = isChunkFinish ? promiseMap.remove(sequence) : promiseMap.get(sequence);

            if (wrapper != null) {
                wrapper.setResult(response);
                return true;
            }
        }

        return false;
    }

    /**
     * 设置请求的序号
     */
    public void setSequence(NettyPacket nettyPacket) {
        if (socketChannel == null || !socketChannel.isActive()) {
            throw new IllegalStateException("Socket channel is disconnect");
        }
        nettyPacket.setSequence(name + "-" + Constants.REQUEST_COUNTER.getAndIncrement());
    }

    /**
     * 定时检查请求是否超时，避免请求hang死
     */
    private void checkRequestTimeout(long requestTimeout) {
        synchronized (this) {
            promiseMap.forEach((__, value) -> {
                if (value.isTimeout(requestTimeout)) {
                    value.markTimeout();
                }
            });
        }
    }

}
