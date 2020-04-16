package com.xiaohongshu.infra.transport.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class HeaderCodec
        extends ChannelDuplexHandler
{
    @Override
    public void channelRead(ChannelHandlerContext context, Object message)
    {
        if (message instanceof ByteBuf) {
            ByteBuf request = (ByteBuf) message;
            if (request.isReadable()) {
                context.fireChannelRead(HeaderTransport.decodeFrame(request));
                return;
            }
        }
        context.fireChannelRead(message);
    }

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise)
    {
        if (message instanceof ThriftMessage) {
            context.write(HeaderTransport.encodeFrame((ThriftMessage) message), promise);
        }
        else {
            context.write(message, promise);
        }
    }
}
