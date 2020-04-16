package com.xiaohongshu.infra.transport.server;

import com.google.common.annotations.VisibleForTesting;
import com.xiaohongshu.infra.transport.ssl.SslContextFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;
import org.apache.thrift.TProcessor;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.xiaohongshu.infra.transport.ssl.SslContextFactory.createSslContextFactory;
import static io.netty.channel.ChannelOption.*;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NettyServerTransport
        implements ServerTransport
{
    private final ServerBootstrap bootstrap;
    private final int port;

    private final EventLoopGroup ioGroup;
    private final EventLoopGroup workerGroup;

    private Channel channel;

    private final AtomicBoolean running = new AtomicBoolean();

    public NettyServerTransport(TProcessor processor, NettyServerConfig config)
    {
        this(processor, config, ByteBufAllocator.DEFAULT);
    }

    @VisibleForTesting
    public NettyServerTransport(TProcessor processor, NettyServerConfig config, ByteBufAllocator allocator)
    {
        requireNonNull(config, "config is null");
        this.port = config.getPort();

        ioGroup = new NioEventLoopGroup(config.getIoThreadCount());

        workerGroup = new NioEventLoopGroup(config.getWorkerThreadCount());

        Optional<Supplier<SslContext>> sslContext = Optional.empty();
        if (config.isSslEnabled()) {
            SslContextFactory sslContextFactory = createSslContextFactory(false, config.getSslContextRefreshTime(), workerGroup);
            sslContext = Optional.of(sslContextFactory.get(
                    config.getTrustCertificate(),
                    Optional.ofNullable(config.getKey()),
                    Optional.ofNullable(config.getKey()),
                    Optional.ofNullable(config.getKeyPassword()),
                    config.getSessionCacheSize(),
                    config.getSessionTimeout(),
                    config.getCiphers()));

            // validate ssl context configuration is valid
            sslContext.get().get();
        }

        ThriftServerInitializer serverInitializer = new ThriftServerInitializer(
                processor,
                config.getMaxFrameSize(),
                config.getRequestTimeout(),
                sslContext,
                config.isAllowPlaintext(),
                config.isAssumeClientsSupportOutOfOrderResponses(),
                workerGroup);

        bootstrap = new ServerBootstrap()
                .group(ioGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(serverInitializer)
                .option(SO_BACKLOG, config.getAcceptBacklog())
                .option(ALLOCATOR, allocator)
                .childOption(SO_KEEPALIVE, true)
                .validate();
    }

    @Override
    public void start()
    {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        try {
            channel = bootstrap.bind(port).sync().channel();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while starting", e);
        }
    }

    public int getPort()
    {
        return ((InetSocketAddress) channel.localAddress()).getPort();
    }

    @Override
    public void shutdown()
    {
        try {
            if (channel != null) {
                await(channel.close());
            }
        }
        finally {
            Future<?> ioShutdown;
            try {
                ioShutdown = ioGroup.shutdownGracefully(0, 0, SECONDS);
            }
            finally {
                await(workerGroup.shutdownGracefully(0, 0, SECONDS));
            }
            await(ioShutdown);
        }
    }

    private static void await(Future<?> future)
    {
        try {
            future.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
