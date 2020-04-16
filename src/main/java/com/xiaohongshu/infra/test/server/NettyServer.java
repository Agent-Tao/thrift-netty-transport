package com.xiaohongshu.infra.test.server;

import com.xiaohongshu.infra.test.api.HelloService;
import com.xiaohongshu.infra.test.api.HelloServiceImpl;
import com.xiaohongshu.infra.transport.server.NettyServerConfig;
import com.xiaohongshu.infra.transport.server.NettyServerTransportFactory;
import com.xiaohongshu.infra.transport.server.ServerTransport;
import io.netty.buffer.ByteBufAllocator;
import org.apache.thrift.TProcessor;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: jiangtao
 * Date: 2020-04-13
 * Time: 下午2:16
 */
public class NettyServer {

    public static void main(String[] args) {

        TProcessor processor = new HelloService.Processor<HelloService.Iface>(new HelloServiceImpl());

        NettyServerConfig config = new NettyServerConfig()
                .setPort(7777)
                .setSslEnabled(false);

        NettyServerTransportFactory factory = new NettyServerTransportFactory(config, ByteBufAllocator.DEFAULT);

        ServerTransport transport = factory.createServerTransport(processor);

        transport.start();

        System.out.println("gaga");

    }
}
