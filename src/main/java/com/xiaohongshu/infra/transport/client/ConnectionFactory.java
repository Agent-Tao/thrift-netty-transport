package com.xiaohongshu.infra.transport.client;

import com.google.common.net.HostAndPort;
import com.xiaohongshu.infra.transport.ssl.SslContextFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.apache.thrift.transport.TTransportException;

import java.net.InetSocketAddress;
import java.util.Optional;

import static com.google.common.primitives.Ints.saturatedCast;
import static io.netty.channel.ChannelOption.ALLOCATOR;
import static io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;
import static java.util.Objects.requireNonNull;

public class ConnectionFactory
        implements ConnectionManager
{
    private final EventLoopGroup group;
    //private final SslContextFactory sslContextFactory;
    private final ByteBufAllocator allocator;

    public ConnectionFactory(EventLoopGroup group, SslContextFactory sslContextFactory, ByteBufAllocator allocator)
    {

        this.group = requireNonNull(group, "group is null");
        //this.sslContextFactory = requireNonNull(sslContextFactory, "sslContextFactory is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
    }

    @Override
    public Future<Channel> getConnection(ConnectionParameters connectionParameters, HostAndPort address)
    {
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .option(ALLOCATOR, allocator)
                    .option(CONNECT_TIMEOUT_MILLIS, saturatedCast(connectionParameters.getConnectTimeout().toMillis()))
                    .handler(new ThriftClientInitializer(
                            connectionParameters.getTransport(),
                            connectionParameters.getProtocol(),
                            connectionParameters.getMaxFrameSize(),
                            connectionParameters.getRequestTimeout(),
                            connectionParameters.getSocksProxy(),
                            Optional.empty()));

            Promise<Channel> promise = group.next().newPromise();
            promise.setUncancellable();
            bootstrap.connect(new InetSocketAddress(address.getHost(), address.getPort()))
                    .addListener((ChannelFutureListener) future -> notifyConnect(future, promise));
            return promise;
        }
        catch (Throwable e) {
            return group.next().newFailedFuture(new TTransportException(e));
        }
    }

    private static void notifyConnect(ChannelFuture future, Promise<Channel> promise)
    {
        if (future.isSuccess()) {
            Channel channel = future.channel();
            if (!promise.trySuccess(channel)) {
                // Promise was completed in the meantime (likely cancelled), just release the channel again
                channel.close();
            }
        }
        else {
            promise.tryFailure(future.cause());
        }
    }

    @Override
    public void returnConnection(Channel connection)
    {
        connection.close();
    }

    @Override
    public void close() {}
}
