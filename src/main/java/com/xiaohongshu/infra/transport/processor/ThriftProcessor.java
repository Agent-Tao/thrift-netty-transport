
package com.xiaohongshu.infra.transport.processor;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

public interface ThriftProcessor
{
    public ListenableFuture<Boolean> process(TProtocol in, TProtocol out) throws TException;
}
