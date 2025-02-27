package com.tudoujun.distribute.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/19 14:49
 */
public class NettyPacketDecoder extends LengthFieldBasedFrameDecoder {

    public NettyPacketDecoder(int maxFrameLength) {
        super(maxFrameLength, 0, 3, 0, 3);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf byteBuf = (ByteBuf) super.decode(ctx, in);
        if (byteBuf != null) {
            try {
                return NettyPacket.parsePacket(byteBuf);
            } finally {
                ReferenceCountUtil.release(byteBuf);
            }
        }

        return null;
    }
}
