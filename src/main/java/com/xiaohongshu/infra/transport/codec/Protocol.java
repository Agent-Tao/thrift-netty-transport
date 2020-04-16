package com.xiaohongshu.infra.transport.codec;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

public enum Protocol
{
    BINARY {
        @Override
        public TProtocol createProtocol(TTransport transport)
        {
            return new TBinaryProtocol(transport);
        }

        @Override
        public int getHeaderTransportId()
        {
            return 0;
        }
    },
    COMPACT {
        @Override
        public TProtocol createProtocol(TTransport transport)
        {
            return new TCompactProtocol(transport);
        }

        @Override
        public int getHeaderTransportId()
        {
            throw new IllegalStateException("COMPACT can not be used with HEADER transport; use FB_COMPACT instead");
        }
    },
    FB_COMPACT {
        @Override
        public TProtocol createProtocol(TTransport transport)
        {
            return new TFacebookCompactProtocol(transport);
        }

        @Override
        public int getHeaderTransportId()
        {
            return 2;
        }
    };

    public abstract TProtocol createProtocol(TTransport transport);

    public abstract int getHeaderTransportId();

    public static Protocol getProtocolByHeaderTransportId(int headerTransportId)
    {
        if (headerTransportId == BINARY.getHeaderTransportId()) {
            return BINARY;
        }
        if (headerTransportId == FB_COMPACT.getHeaderTransportId()) {
            return FB_COMPACT;
        }
        throw new IllegalArgumentException("Unsupported header transport protocol ID: " + headerTransportId);
    }
}
