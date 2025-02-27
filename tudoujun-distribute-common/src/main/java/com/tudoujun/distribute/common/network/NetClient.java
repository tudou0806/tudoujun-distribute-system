package com.tudoujun.distribute.common.network;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tudoujun.distribute.common.NettyPacket;
import com.tudoujun.distribute.common.exception.RequestTimeoutException;
import com.tudoujun.distribute.common.utils.DefaultScheduler;
import com.tudoujun.distribute.common.utils.NamedThreadFactory;

import org.apache.commons.collections4.CollectionUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ResourceLeakDetector;
import lombok.extern.slf4j.Slf4j;

/**
 * @author xiaowenjun
 * @description 网络客户端
 * @create: 2025/02/26 14:18
 */
@Slf4j
public class NetClient {

    private final String name;
    private BaseChannelInitializer baseChannelInitializer;
    private DefaultScheduler defaultScheduler;
    private EventLoopGroup connectThreadGroup;
    private DefaultChannelHandler defaultChannelHandler;
    private int retryTime;
    private List<NetClientFailListener> netClientFailListeners = new ArrayList<>();
    private AtomicBoolean started = new AtomicBoolean(true);

    public NetClient(String name, DefaultScheduler defaultScheduler) {
        this(name, defaultScheduler, -1, 3000);
    }

    public NetClient(String name, DefaultScheduler defaultScheduler, int retryTime) {
        this(name, defaultScheduler, retryTime, 3000);
    }

    public NetClient(String name, DefaultScheduler defaultScheduler, int retryTime, long requestTimeout) {
        this.name = name;
        this.retryTime = retryTime;
        this.defaultScheduler = defaultScheduler;
        this.connectThreadGroup = new NioEventLoopGroup(1,
                new NamedThreadFactory("NetClient-Event-", false));
        this.defaultChannelHandler = new DefaultChannelHandler(name, defaultScheduler, requestTimeout);
        this.defaultChannelHandler.addConnectListener(connected -> {
            if (connected) {
                synchronized (NetClient.this) {
                    NetClient.this.notifyAll();
                }
            }
        });
        this.baseChannelInitializer = new BaseChannelInitializer();
        this.baseChannelInitializer.addHandler(defaultChannelHandler);
    }

    public SocketChannel socketChannel() {
        return defaultChannelHandler.socketChannel();
    }

    public void ensureConnected() throws InterruptedException {
        ensureConnected(-1);
    }

    /**
     * 同步等待确保连接已经建立
     * 如果连接断开了，会阻塞等到连接建立
     */
    public void ensureConnected(int timeout) throws InterruptedException {
        int remainTimeout = timeout;
        synchronized (this) {
            while(!isConnected()) {
                if (!started.get()) {
                    throw new InterruptedException("无法连接上服务器:" + name);
                }
                if (timeout > 0) {
                    if (remainTimeout <= 0) {
                        throw new InterruptedException("无法连接上服务器:" + name);
                    }
                    wait(10);
                    remainTimeout -= 10;
                } else {
                    wait(10);
                }
            }
        }
    }

    /**
     * 添加自定义Handler
     */
    public void addHandlers(List<AbstractChannelHandler> handlers) {
        if (CollectionUtils.isEmpty(handlers)) {
            return;
        }
        defaultChannelHandler.setHasOtherHandlers(true);
        baseChannelInitializer.addHandlers(handlers);
    }

    public void connect(String hostname, int port) {
        connect(hostname, port, 1, 0);
    }

    /**
     * 启动连接
     */
    private void connect(String hostname, int port, final int connectTimes, int delay) {
        defaultScheduler.scheduleOnce("连接服务器", () -> {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(connectThreadGroup)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .channel(NioSocketChannel.class)
                    .handler(baseChannelInitializer);
            ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
            try {
                ChannelFuture channelFuture = bootstrap.connect(hostname, port).sync();
                channelFuture.channel().closeFuture().addListener((ChannelFutureListener)f -> f.channel().close());
                channelFuture.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                log.info("发起连接后同步等待连接被打断");
            } catch (Exception e) {
                log.error("发起连接过程中出现异常: [ex={} started={} name={}]", e.getMessage(), started, name);
            } finally {
                int curConnectTimes = connectTimes + 1;
                maybeRetry(hostname, port, curConnectTimes);
            }
        }, delay);
    }

    /**
     * 尝试重新发起连接
     */
    private void maybeRetry(String hostname, int port, int connectTimes) {
        if (started.get()) {
            boolean retry = retryTime < 0 || connectTimes <= retryTime;
            if (retry) {
                log.error("重新发起连接: [started={} name]{}]", started.get(), name);
                connect(hostname, port, connectTimes, 3000);
            } else {
                shutdown();
                log.info("重试次数超过阈值，不再进行重试: [retryTime={}]", retryTime);
                for (NetClientFailListener listener : netClientFailListeners) {
                    try {
                        listener.onConnectFail();
                    } catch (Exception e) {
                        log.error("Uncaught Exception occur on invoke listener: ", e);
                    }
                }
            }
        }
    }

    /**
     * 发送请求，同步获取结果
     */
    public NettyPacket sendSync(NettyPacket nettyPacket) throws InterruptedException, RequestTimeoutException {
        ensureConnected();
        return defaultChannelHandler.sendSync(nettyPacket);
    }

    /**
     * 发送结果，异步通过listener获取结果
     */
    public void send(NettyPacket nettyPacket) throws InterruptedException {
        ensureConnected();
        defaultChannelHandler.send(nettyPacket);
    }

    /**
     * 是否连接上
     */
    public boolean isConnected() {
        return defaultChannelHandler.isConnected();
    }

    /**
     * 关闭服务，关闭连接，释放资源
     */
    public void shutdown() {
        if (log.isDebugEnabled()) {
            log.debug("Shutdown NetClient : [name={}]", name);
        }
        started.set(false);
        if (connectThreadGroup != null) {
            connectThreadGroup.shutdownGracefully();
        }
        defaultChannelHandler.clearConnectListener();
        defaultChannelHandler.clearNettyPacketListener();
    }

    /**
     * 添加连接状态监听器
     */
    public void addConnectListener(ConnectListener connectListener) {
        defaultChannelHandler.addConnectListener(connectListener);
    }

    public void addNettyPacketListener(NettyPacketListener nettyPacketListener) {
        defaultChannelHandler.addNettyPacketListener(nettyPacketListener);
    }

    public void addNetClientFailListener(NetClientFailListener netClientFailListener) {
        netClientFailListeners.add(netClientFailListener);
    }

    public void setRetryTime(int retryTime) {
        this.retryTime = retryTime;
    }
}
