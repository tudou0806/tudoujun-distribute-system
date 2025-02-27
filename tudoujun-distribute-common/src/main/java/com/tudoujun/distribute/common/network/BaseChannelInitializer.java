package com.tudoujun.distribute.common.network;

import java.util.LinkedList;
import java.util.List;

import com.tudoujun.distribute.common.Constants;
import com.tudoujun.distribute.common.NettyPacketDecoder;
import com.tudoujun.distribute.common.NettyPacketEncoder;

import org.apache.commons.collections4.CollectionUtils;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/19 16:59
 */
public class BaseChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final List<AbstractChannelHandler> handlers = new LinkedList<>();

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(
                new NettyPacketDecoder(Constants.MAX_BYTES),
                new LengthFieldPrepender(3),
                new NettyPacketEncoder()
        );
        for (AbstractChannelHandler handler : handlers) {
            ch.pipeline().addLast(handler);
        }
    }

    /**
     * 添加自定义Handler
     */
    public void addHandler(AbstractChannelHandler handler) {
        handlers.add(handler);
    }

    /**
     * 添加自定义Handler
     */
    public void addHandlers(List<AbstractChannelHandler> handles) {
        if (CollectionUtils.isEmpty(handles)) {
            return;
        }

        this.handlers.addAll(handles);
    }
}
