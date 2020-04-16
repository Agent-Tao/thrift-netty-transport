package com.xiaohongshu.infra.transport.server;

import org.apache.thrift.TProcessor;

public interface ServerTransportFactory
{
    ServerTransport createServerTransport(TProcessor processor);
}
