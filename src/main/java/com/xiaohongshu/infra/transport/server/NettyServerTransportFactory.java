package com.xiaohongshu.infra.transport.server;

import io.netty.buffer.ByteBufAllocator;
import org.apache.thrift.TProcessor;
import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class NettyServerTransportFactory
        implements ServerTransportFactory
{
    private final NettyServerConfig config;
    private final ByteBufAllocator allocator;

    public NettyServerTransportFactory(NettyServerConfig config)
    {
        this(config, ByteBufAllocator.DEFAULT);
    }

    @Inject
    public NettyServerTransportFactory(NettyServerConfig config, ByteBufAllocator allocator)
    {
        this.config = requireNonNull(config, "config is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
    }

    @Override
    public ServerTransport createServerTransport(TProcessor processor)
    {
        return new NettyServerTransport(processor, config, allocator);
    }

}
