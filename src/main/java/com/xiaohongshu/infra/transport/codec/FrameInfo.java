package com.xiaohongshu.infra.transport.codec;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FrameInfo
{
    private final String methodName;
    private final byte messageType;
    private final int sequenceId;
    private final Transport transport;
    private final Protocol protocol;
    private final boolean supportOutOfOrderResponse;

    public FrameInfo(String methodName, byte messageType, int sequenceId, Transport transport, Protocol protocol, boolean supportOutOfOrderResponse)
    {
        this.methodName = requireNonNull(methodName, "methodName is null");
        this.messageType = messageType;
        this.sequenceId = sequenceId;
        this.transport = requireNonNull(transport, "transport is null");
        this.protocol = requireNonNull(protocol, "protocol is null");
        this.supportOutOfOrderResponse = supportOutOfOrderResponse;
    }

    public String getMethodName()
    {
        return methodName;
    }

    public byte getMessageType()
    {
        return messageType;
    }

    public int getSequenceId()
    {
        return sequenceId;
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
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FrameInfo that = (FrameInfo) o;
        return messageType == that.messageType &&
                sequenceId == that.sequenceId &&
                supportOutOfOrderResponse == that.supportOutOfOrderResponse &&
                Objects.equals(methodName, that.methodName) &&
                transport == that.transport &&
                protocol == that.protocol;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(methodName, messageType, sequenceId, transport, protocol, supportOutOfOrderResponse);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("methodName", methodName)
                .add("messageType", messageType)
                .add("sequenceId", sequenceId)
                .add("transport", transport)
                .add("protocol", protocol)
                .add("supportOutOfOrderResponse", supportOutOfOrderResponse)
                .toString();
    }
}
