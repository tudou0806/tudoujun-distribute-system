package com.tudoujun.distribute.namenode.server;

import java.util.Set;

import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.network.AbstractChannelHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author xiaowenjun
 * @description
 * @create: 2025/02/28 11:07
 */
public class NameNodeApis extends AbstractChannelHandler {

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) throws Exception {
        return false;
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return null;
    }
}
