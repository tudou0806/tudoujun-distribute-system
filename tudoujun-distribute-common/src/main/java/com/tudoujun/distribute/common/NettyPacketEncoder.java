package com.tudoujun.distribute.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/19 14:49
 */
public class NettyPacketEncoder extends MessageToByteEncoder<NettyPacket> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NettyPacket msg, ByteBuf out) throws Exception {
        msg.write(out);
    }
}
