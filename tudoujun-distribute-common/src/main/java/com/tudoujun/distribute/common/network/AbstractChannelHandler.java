package com.tudoujun.distribute.common.network;

import java.util.Set;
import java.util.concurrent.Executor;

import com.tudoujun.distribute.common.NettyPacket;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description 默认消息处理器
 * @create: 2025/02/19 17:00
 */
@Slf4j
@ChannelHandler.Sharable
public abstract class AbstractChannelHandler extends ChannelInboundHandlerAdapter {

    private Set<Integer> interestPackageTypes;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("AbstractChannelHandler#exceptionCaught: ", cause);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Executor executor = getExecutor();
        if (executor != null) {
            executor.execute(() -> channelReadInternal(ctx, msg));
        } else {
            channelReadInternal(ctx, msg);
        }
    }

    private void channelReadInternal(ChannelHandlerContext ctx, Object msg) {
        NettyPacket nettyPacket = (NettyPacket) msg;
        boolean consumedMsg = false;
        if (getPackageTypes().isEmpty() || getPackageTypes().contains(nettyPacket.getPacketType())) {
            try {
                consumedMsg = handlePackage(ctx, nettyPacket);
            } catch (Exception e) {
                log.error("处理请求发生异常: ", e);
            }
        }

        if (!consumedMsg) {
            ctx.fireChannelRead(msg);
        }
    }

    private Set<Integer> getPackageTypes() {
        if (interestPackageTypes == null) {
            return interestPackageTypes();
        }
        return interestPackageTypes;
    }

    /**
     * 获取执行器
     */
    protected Executor getExecutor() {
        return null;
    }

    /**
     * 处理网络包
     * @param ctx 上下文
     * @param nettyPacket 网络包
     * @exception Exception 序列化异常
     * @return 是否消费了该消息
     */
    protected abstract boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) throws Exception;

    /**
     * 感兴趣的消息类型
     * @return 返回空集合表示对所有的消息类型感兴趣
     */
    protected abstract Set<Integer> interestPackageTypes();
}
