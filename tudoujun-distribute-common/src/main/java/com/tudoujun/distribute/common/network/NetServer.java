package com.tudoujun.distribute.common.network;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.tudoujun.distribute.common.utils.DefaultScheduler;
import com.tudoujun.distribute.common.utils.NamedThreadFactory;

import org.apache.commons.collections4.CollectionUtils;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ResourceLeakDetector;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description 网络服务端
 * @create: 2025/02/26 13:33
 */
@Slf4j
public class NetServer {

    private String name;
    private DefaultScheduler defaultScheduler;
    private EventLoopGroup boss;
    private EventLoopGroup worker;
    private BaseChannelInitializer baseChannelInitializer;
    private boolean supportEpoll;

    public NetServer(String name, DefaultScheduler defaultScheduler) {
        this(name, defaultScheduler, 0, false);
    }

    public NetServer(String name, DefaultScheduler defaultScheduler, int workThreads) {
        this(name, defaultScheduler, workThreads, false);
    }

    public NetServer(String name, DefaultScheduler defaultScheduler, int workerThreads, boolean supportEpoll) {
        this.name = name;
        this.defaultScheduler = defaultScheduler;
        this.boss = new NioEventLoopGroup(0, new NamedThreadFactory("NettyServer-Boss-", false));
        this.worker = new NioEventLoopGroup(workerThreads, new NamedThreadFactory("NettyServer-Worker-", false));
        this.supportEpoll = supportEpoll;
        this.baseChannelInitializer = new BaseChannelInitializer();
    }

    public void setChannelInitializer(BaseChannelInitializer baseChannelInitializer) {
        this.baseChannelInitializer = baseChannelInitializer;
    }

    /**
     * 添加自定义的handlers
     */
    public void addHandlers(List<AbstractChannelHandler> handlers) {
        if (CollectionUtils.isEmpty(handlers)) {
            return;
        }

        baseChannelInitializer.addHandlers(handlers);
    }

    /**
     * 绑定端口，等待同步关闭
     */
    public void bind(int port) throws InterruptedException {
        internalBind(Collections.singletonList(port));
    }

    public void bind(List<Integer> ports) throws InterruptedException {
        internalBind(ports);
    }

    /**
     * 异步绑定接口
     */
    private void bindAsync(int port) {
        defaultScheduler.scheduleOnce("绑定服务端口", () -> {
            try {
                internalBind(Collections.singletonList(port));
            } catch (InterruptedException e) {
                log.error("NetServer internalBind is Interrupted!!");
            }
        }, 0);
    }

    /**
     * 绑定端口
     */
    private void internalBind(List<Integer> ports) throws InterruptedException {
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(boss, worker)
                    .channel(supportEpoll ? EpollServerDomainSocketChannel.class : NioServerSocketChannel.class)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childHandler(baseChannelInitializer);
            ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
            List<ChannelFuture> channelFuture = new ArrayList<>();
            for (int port : ports) {
                ChannelFuture future = bootstrap.bind(port);
                log.info("Netty Server started on port : {}",port);
                channelFuture.add(future);
            }
            for (ChannelFuture future : channelFuture) {
                future.channel().closeFuture().addListener((ChannelFutureListener) f -> f.channel().close());
            }
            for (ChannelFuture future : channelFuture) {
                future.channel().closeFuture().sync();
            }
        } finally {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }

    /**
     * 停止
     */
    public void shutdown() {
        log.info("Shutdown NetServer: [name={}]", name);
        if (boss != null && worker != null) {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }
}
