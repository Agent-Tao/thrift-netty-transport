package com.xiaohongshu.infra.transport.codec;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

import javax.annotation.CheckReturnValue;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static javax.annotation.meta.When.UNKNOWN;

public class ThriftMessage
        implements ReferenceCounted
{
    private final int sequenceId;
    private final ByteBuf message;
    private final Map<String, String> headers;
    private final Transport transport;
    private final Protocol protocol;
    private final boolean supportOutOfOrderResponse;

    public ThriftMessage(int sequenceId, ByteBuf message, Map<String, String> headers, Transport transport, Protocol protocol, boolean supportOutOfOrderResponse)
    {
        this.sequenceId = sequenceId;
        this.message = requireNonNull(message, "message is null");
        this.headers = requireNonNull(headers, "headers is null");
        this.transport = requireNonNull(transport, "transport is null");
        this.protocol = requireNonNull(protocol, "protocol is null");
        this.supportOutOfOrderResponse = supportOutOfOrderResponse;
    }

    public int getSequenceId()
    {
        return sequenceId;
    }

    /**
     * @return a retained message; caller must release this buffer
     */
    public ByteBuf getMessage()
    {
        return message.retainedDuplicate();
    }

    public Map<String, String> getHeaders()
    {
        return headers;
    }

    public Transport getTransport()
    {
        return transport;
    }

    public Protocol getProtocol()
    {
        return protocol;
    }

    public boolean isSupportOutOfOrderResponse()
    {
        return supportOutOfOrderResponse;
    }

    @Override
    public int refCnt()
    {
        return message.refCnt();
    }

    @Override
    public ThriftMessage retain()
    {
        message.retain();
        return this;
    }

    @Override
    public ThriftMessage retain(int increment)
    {
        message.retain(increment);
        return this;
    }

    @Override
    public ThriftMessage touch()
    {
        message.touch();
        return this;
    }

    @Override
    public ThriftMessage touch(Object hint)
    {
        message.touch(hint);
        return this;
    }

    @CheckReturnValue(when = UNKNOWN)
    @Override
    public boolean release()
    {
        return message.release();
    }

    @Override
    public boolean release(int decrement)
    {
        return message.release(decrement);
    }
}
