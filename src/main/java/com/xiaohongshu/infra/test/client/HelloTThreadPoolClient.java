package com.xiaohongshu.infra.test.client;

import com.xiaohongshu.infra.test.api.HelloService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * 客户端调用HelloTSimpleServer,HelloTThreadPoolServer
 * 阻塞
 * Created by chenshunyang on 2016/10/31.
 */
public class HelloTThreadPoolClient {
    public static final String SERVER_IP = "127.0.0.1";
    public static final int SERVER_PORT = 7777;
    public static final int TIMEOUT = 300000;

    public static void main(String[] args) throws Exception{
        // 设置传输通道
        TTransport transport = new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT);

        //TTransport transport = new TFramedTransport(new TSocket(SERVER_IP,SERVER_PORT,TIMEOUT));

        // 协议要和服务端一致
        //使用二进制协议
        TProtocol protocol = new TBinaryProtocol(transport);
        //创建Client
        HelloService.Client client = new HelloService.Client(protocol);
        transport.open();
        String result = client.hello("jack1");
        System.out.println("result : " + result);
        transport.close();

        Thread.sleep(1000);


        transport.open();
        result = client.hello("jack2");
        System.out.println("result : " + result);
        result = client.hello("jack3");
        System.out.println("result : " + result);
        //关闭资源
        transport.close();
    }
}
