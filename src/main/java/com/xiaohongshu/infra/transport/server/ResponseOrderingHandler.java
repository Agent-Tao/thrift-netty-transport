package com.xiaohongshu.infra.transport.server;

import com.xiaohongshu.infra.transport.codec.ThriftMessage;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class ResponseOrderingHandler
        extends ChannelDuplexHandler
{
    @Override
    public void channelRead(ChannelHandlerContext context, Object message)
    {
        if (message instanceof ThriftMessage) {
            ThriftMessage thriftMessage = (ThriftMessage) message;
            if (!thriftMessage.isSupportOutOfOrderResponse()) {
                context.channel().config().setAutoRead(false);
            }
        }
        context.fireChannelRead(message);
    }

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise)
    {
        if (message instanceof ThriftMessage) {
            // always re-enable auto read
            context.channel().config().setAutoRead(true);
        }
        context.write(message, promise);
    }
}
