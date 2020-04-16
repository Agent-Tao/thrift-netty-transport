package com.xiaohongshu.infra.transport.codec;

import com.xiaohongshu.infra.transport.ssl.TChannelBufferInputTransport;

import io.netty.buffer.ByteBuf;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SimpleFrameInfoDecoder
        implements FrameInfoDecoder
{
    private final Transport transportType;
    private final Protocol protocolType;
    private final boolean assumeClientsSupportOutOfOrderResponses;

    public SimpleFrameInfoDecoder(Transport transportType, Protocol protocolType, boolean assumeClientsSupportOutOfOrderResponses)
    {
        this.transportType = requireNonNull(transportType, "transportType is null");
        this.protocolType = requireNonNull(protocolType, "protocolType is null");
        this.assumeClientsSupportOutOfOrderResponses = assumeClientsSupportOutOfOrderResponses;
    }

    @Override
    public Optional<FrameInfo> tryDecodeFrameInfo(ByteBuf buffer)
    {

        TChannelBufferInputTransport transport = new TChannelBufferInputTransport(buffer.retainedDuplicate());
        try {
            TProtocol protocol = protocolType.createProtocol(transport);
            TMessage message;
            try {
                message = protocol.readMessageBegin();
            }
            catch (TException | RuntimeException e) {
                // not enough bytes in the input to decode sequence id
                return Optional.empty();
            }
            return Optional.of(new FrameInfo(
                    message.name,
                    message.type,
                    message.seqid,
                    transportType,
                    protocolType,
                    assumeClientsSupportOutOfOrderResponses));
        }
        finally {
            transport.release();
        }
    }
}
