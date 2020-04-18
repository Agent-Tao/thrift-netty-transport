package com.xiaohongshu.infra.transport.processor;

import io.netty.buffer.ByteBuf;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;

/**
 * @Author: JiangTao
 * @Date: Created on 下午1:07 2018/3/27.
 * @Description:
 */

public class SeqNumReplacer {

    public static byte[] replace(byte[] in, int num) throws TException {
        BufferMockTransport transport = new BufferMockTransport(in,0);
        TProtocol p = new TBinaryProtocol(transport);
        TMessage msg = p.readMessageBegin();
        //int pos = transport.getBufferPosition();

        TMessage msg2 = new TMessage(msg.name,msg.type,num);
        transport.resetPos();
        p.writeMessageBegin(msg2);
        return transport.getBuffer();
    }
}
