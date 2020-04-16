package com.xiaohongshu.infra.transport.codec;

import com.google.common.collect.ImmutableMap;
import com.xiaohongshu.infra.transport.ssl.TChannelBufferInputTransport;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.transport.TTransportException;

import static java.util.Objects.requireNonNull;

public class SimpleFrameCodec
        extends ChannelDuplexHandler
{
    private final Transport transport;
    private final Protocol protocol;
    private final boolean assumeClientsSupportOutOfOrderResponses;

    public SimpleFrameCodec(Transport transport, Protocol protocol, boolean assumeClientsSupportOutOfOrderResponses)
    {
        this.transport = requireNonNull(transport, "transport is null");
        this.protocol = requireNonNull(protocol, "protocol is null");
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message)
            throws Exception
    {
        if (message instanceof ByteBuf) {
            ByteBuf buffer = (ByteBuf) message;
            if (buffer.isReadable()) {
                context.fireChannelRead(new ThriftMessage(
                        extractResponseSequenceId(buffer.retain()),
                        buffer,
                        ImmutableMap.of(),
                        transport,
                        protocol,
                        assumeClientsSupportOutOfOrderResponses));
                return;
            }
        }
        context.fireChannelRead(message);
    }

    private int extractResponseSequenceId(ByteBuf buffer)
            throws TTransportException
    {
        TChannelBufferInputTransport inputTransport = new TChannelBufferInputTransport(buffer.duplicate());
        try {
            TMessage message = protocol.createProtocol(inputTransport).readMessageBegin();
            return message.seqid;
        }
        catch (Throwable e) {
            throw new TTransportException("Could not find sequenceId in Thrift message", e);
        }
        finally {
            inputTransport.release();
        }
    }

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise)
    {
        if (message instanceof ThriftMessage) {
            // strip the underlying message from the frame
            ThriftMessage thriftMessage = (ThriftMessage) message;
            try {
                // Note: simple transports do not support headers. This is acceptable since headers should be inconsequential to the request
                message = thriftMessage.getMessage();
            }
            finally {
                thriftMessage.release();
            }
        }
        context.write(message, promise);
    }
}
