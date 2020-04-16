package com.xiaohongshu.infra.transport.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import org.apache.thrift.TProcessor;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ThriftServerInitializer
        extends ChannelInitializer<SocketChannel>
{
    TProcessor processor;
    private final Integer maxFrameSize;
    private final Duration requestTimeout;
    private final Optional<Supplier<SslContext>> sslContextSupplier;
    private final boolean allowPlainText;
    private final boolean assumeClientsSupportOutOfOrderResponses;
    private final ScheduledExecutorService timeoutExecutor;

    public ThriftServerInitializer(
            TProcessor processor,
            Integer maxFrameSize,
            Duration requestTimeout,
            Optional<Supplier<SslContext>> sslContextSupplier,
            boolean allowPlainText,
            boolean assumeClientsSupportOutOfOrderResponses,
            ScheduledExecutorService timeoutExecutor)
    {
        requireNonNull(processor, "methodInvoker is null");
        requireNonNull(maxFrameSize, "maxFrameSize is null");
        requireNonNull(requestTimeout, "requestTimeout is null");
        requireNonNull(sslContextSupplier, "sslContextSupplier is null");
        checkArgument(allowPlainText || sslContextSupplier.isPresent(), "Plain text is not allowed, but SSL is not configured");
        requireNonNull(timeoutExecutor, "timeoutExecutor is null");

        this.processor = processor;
        this.maxFrameSize = maxFrameSize;
        this.requestTimeout = requestTimeout;
        this.sslContextSupplier = sslContextSupplier;
        this.allowPlainText = allowPlainText;
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
        this.timeoutExecutor = timeoutExecutor;
    }

    @Override
    protected void initChannel(SocketChannel channel)
    {
        ChannelPipeline pipeline = channel.pipeline();

        if (sslContextSupplier.isPresent()) {
            if (allowPlainText) {
                pipeline.addLast(new OptionalSslHandler(sslContextSupplier.get().get()));
            }
            else {
                pipeline.addLast(sslContextSupplier.get().get().newHandler(channel.alloc()));
            }
        }

        pipeline.addLast(new ThriftProtocolDetection(
                //new ThriftServerHandler(methodInvoker, requestTimeout, timeoutExecutor),
                new ThriftServerProcessor(processor,requestTimeout,timeoutExecutor),
                maxFrameSize,
                assumeClientsSupportOutOfOrderResponses));
    }
}
