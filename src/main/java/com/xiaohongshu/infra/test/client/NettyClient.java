package com.xiaohongshu.infra.test.client;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: jiangtao
 * Date: 2019-03-18
 * Time: 下午8:21
 */
import com.google.common.net.HostAndPort;
import com.xiaohongshu.infra.test.api.HelloService;
import com.xiaohongshu.infra.transport.client.ConnectionFactory;
import com.xiaohongshu.infra.transport.client.ConnectionManager;
import com.xiaohongshu.infra.transport.client.ThriftClientHandler;
import com.xiaohongshu.infra.transport.codec.Protocol;
import com.xiaohongshu.infra.transport.codec.ThriftMessage;
import com.xiaohongshu.infra.transport.codec.Transport;
import com.xiaohongshu.infra.transport.processor.SeqNumReplacer;
import com.xiaohongshu.infra.transport.server.TMemoryTransport;
import com.xiaohongshu.infra.transport.ssl.TChannelBufferInputTransport;
import com.xiaohongshu.infra.transport.ssl.TChannelBufferOutputTransport;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;

import java.time.Duration;
import java.util.Optional;


public final class NettyClient {

    static final String HOST = "127.0.0.1";
    static final int PORT = 7777;

    public static void main(String[] args) throws Exception {

        // Configure the client.
        EventLoopGroup group = new NioEventLoopGroup();
        ConnectionFactory factory = new ConnectionFactory(group,null, ByteBufAllocator.DEFAULT);

        HostAndPort addr = HostAndPort.fromParts(HOST,PORT);

        ConnectionManager.ConnectionParameters parameters = new ConnectionManager.ConnectionParameters(Transport.UNFRAMED, Protocol.BINARY,9999999, Duration.ofSeconds(10), Duration.ofSeconds(10),
                Optional.of(addr), Optional.empty());

        Future<Channel> f= factory.getConnection(parameters,addr);


        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        TMemoryTransport transport = new TMemoryTransport(null,byteBuf);
        TProtocol protocol = new TBinaryProtocol(transport);

        HelloService.Client client = new HelloService.Client(protocol);
        client.send_hello("jack1");

        while (transport.getOutputBuffer().isReadable()){
            System.out.print(transport.getOutputBuffer().readByte()+" ");
        }

        transport.getOutputBuffer().resetReaderIndex();

        ThriftClientHandler.ThriftRequest request = new ThriftClientHandler.ThriftRequest(false,transport.getOutputBuffer());

        Channel c= f.get();

        c.writeAndFlush(request);

        ThriftMessage ret = request.get();

        byte[] out = new byte[ret.getMessage().readableBytes()];
        ret.getMessage().readBytes(out);

        SeqNumReplacer.replace(out,0);

        ByteBuf buf = Unpooled.wrappedBuffer(out);
        TChannelBufferOutputTransport outputTransport = new TChannelBufferOutputTransport(buf);

        TBinaryProtocol binaryProtocol = new TBinaryProtocol(outputTransport);

        HelloService.Client clientOut = new HelloService.Client(binaryProtocol);

        String r = clientOut.recv_hello();
        System.out.println(r);

        c.closeFuture().sync();
    }
}
