package com.xiaohongshu.infra.transport.client;

import com.google.common.net.HostAndPort;
import com.xiaohongshu.infra.transport.codec.Protocol;
import com.xiaohongshu.infra.transport.codec.Transport;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.proxy.Socks4ProxyHandler;
import io.netty.handler.ssl.SslContext;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

class ThriftClientInitializer
        extends ChannelInitializer<SocketChannel>
{
    private final Transport transport;
    private final Protocol protocol;
    private final Integer maxFrameSize;
    private final Duration requestTimeout;
    private final Optional<HostAndPort> socksProxyAddress;
    private final Optional<Supplier<SslContext>> sslContextSupplier;

    public ThriftClientInitializer(
            Transport transport,
            Protocol protocol,
            Integer maxFrameSize,
            Duration requestTimeout,
            Optional<HostAndPort> socksProxyAddress,
            Optional<Supplier<SslContext>> sslContextSupplier)
    {
        this.transport = transport;
        this.protocol = protocol;
        this.maxFrameSize = maxFrameSize;
        this.requestTimeout = requestTimeout;
        this.socksProxyAddress = socksProxyAddress;
        this.sslContextSupplier = sslContextSupplier;
    }

    @Override
    protected void initChannel(SocketChannel channel)
    {
        ChannelPipeline pipeline = channel.pipeline();

        //socksProxyAddress.ifPresent(socks -> pipeline.addLast(new Socks4ProxyHandler(new InetSocketAddress(socks.getHost(), socks.getPort()))));

        //sslContextSupplier.ifPresent(sslContext -> pipeline.addLast(sslContext.get().newHandler(channel.alloc())));

        transport.addFrameHandlers(pipeline, Optional.of(protocol), maxFrameSize, true);

        pipeline.addLast(new ThriftClientHandler(requestTimeout, transport, protocol))
                .addLast(new LoggingHandler(LogLevel.INFO));
    }
}
