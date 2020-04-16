package com.xiaohongshu.infra.test.client;

import com.xiaohongshu.infra.test.api.HelloService;
import com.xiaohongshu.infra.transport.server.TMemoryTransport;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: jiangtao
 * Date: 2020-04-15
 * Time: 下午7:27
 */
public class TestMemory {

    public static void main(String[] args) throws TException {

        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        TMemoryTransport transport = new TMemoryTransport(null,byteBuf);

        TProtocol protocol = new TBinaryProtocol(transport);

        HelloService.Client client = new HelloService.Client(protocol);
        //transport.open();
        client.send_hello("hoho");

        System.out.println(transport.getOutputBuffer().readableBytes());



    }

}
