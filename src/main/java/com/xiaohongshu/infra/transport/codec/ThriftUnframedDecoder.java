package com.xiaohongshu.infra.transport.codec;

import com.xiaohongshu.infra.transport.ssl.TChannelBufferInputTransport;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

import java.util.List;
import java.util.Optional;


import static java.util.Objects.requireNonNull;

class ThriftUnframedDecoder
        extends ByteToMessageDecoder
{
    private final Protocol protocol;
    private final int maxFrameSize;
    private final boolean assumeClientsSupportOutOfOrderResponses;

    public ThriftUnframedDecoder(Protocol protocol, Integer maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
    {
        this.protocol = requireNonNull(protocol, "protocol is null");
        this.maxFrameSize = maxFrameSize;
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
    }

    // This method is an exception to the normal reference counted rules and buffer should not be released
    @Override
    protected final void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out)
    {
        int frameOffset = buffer.readerIndex();
        TChannelBufferInputTransport transport = new TChannelBufferInputTransport(buffer.retain());
        try {
            TProtocol protocolReader = protocol.createProtocol(transport);

            TMessage message = protocolReader.readMessageBegin();
            TProtocolUtil.skip(protocolReader, TType.STRUCT);
            protocolReader.readMessageEnd();

            int frameLength = buffer.readerIndex() - frameOffset;
            if (frameLength > maxFrameSize) {
                FrameInfo frameInfo = new FrameInfo(message.name, message.type, message.seqid, Transport.UNFRAMED, protocol, assumeClientsSupportOutOfOrderResponses);
                ctx.fireExceptionCaught(new FrameTooLargeException(
                        Optional.of(frameInfo),
                        frameLength,
                        maxFrameSize));
            }

            out.add(buffer.slice(frameOffset, frameLength).retain());
        }
        catch (Throwable th) {
            buffer.readerIndex(frameOffset);
        }
        finally {
            transport.release();
        }
    }
}
