package com.xiaohongshu.infra.transport.client;

import com.google.common.net.HostAndPort;
import com.xiaohongshu.infra.transport.codec.Protocol;
import com.xiaohongshu.infra.transport.codec.Transport;
import com.xiaohongshu.infra.transport.ssl.SslContextFactory;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Future;

import java.io.Closeable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface ConnectionManager
        extends Closeable
{
    Future<Channel> getConnection(ConnectionParameters connectionParameters, HostAndPort address);

    void returnConnection(Channel connection);

    @Override
    void close();

    public class ConnectionParameters
    {
        private final Transport transport;
        private final Protocol protocol;
        private final Integer maxFrameSize;

        private final Duration connectTimeout;
        private final Duration requestTimeout;

        private final Optional<HostAndPort> socksProxy;
        private final Optional<SslContextFactory.SslContextParameters> sslContextParameters;

        public ConnectionParameters(
                Transport transport,
                Protocol protocol,
                Integer maxFrameSize,
                Duration connectTimeout,
                Duration requestTimeout,
                Optional<HostAndPort> socksProxy,
                Optional<SslContextFactory.SslContextParameters> sslContextParameters)
        {
            this.transport = requireNonNull(transport, "transport is null");
            this.protocol = requireNonNull(protocol, "protocol is null");
            this.maxFrameSize = requireNonNull(maxFrameSize, "maxFrameSize is null");
            this.connectTimeout = requireNonNull(connectTimeout, "connectTimeout is null");
            this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
            this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
            this.sslContextParameters = requireNonNull(sslContextParameters, "sslContextParameters is null");
        }

        public Transport getTransport()
        {
            return transport;
        }

        public Protocol getProtocol()
        {
            return protocol;
        }

        public Integer getMaxFrameSize()
        {
            return maxFrameSize;
        }

        public Duration getConnectTimeout()
        {
            return connectTimeout;
        }

        public Duration getRequestTimeout()
        {
            return requestTimeout;
        }

        public Optional<HostAndPort> getSocksProxy()
        {
            return socksProxy;
        }

        public Optional<SslContextFactory.SslContextParameters> getSslContextParameters()
        {
            return sslContextParameters;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConnectionParameters that = (ConnectionParameters) o;
            return transport == that.transport &&
                    protocol == that.protocol &&
                    Objects.equals(maxFrameSize, that.maxFrameSize) &&
                    Objects.equals(connectTimeout, that.connectTimeout) &&
                    Objects.equals(requestTimeout, that.requestTimeout) &&
                    Objects.equals(socksProxy, that.socksProxy) &&
                    Objects.equals(sslContextParameters, that.sslContextParameters);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(transport, protocol, maxFrameSize, connectTimeout, requestTimeout, socksProxy, sslContextParameters);
        }
    }
}
