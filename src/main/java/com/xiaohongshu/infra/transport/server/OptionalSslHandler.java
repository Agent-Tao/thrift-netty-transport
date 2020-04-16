package com.xiaohongshu.infra.transport.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;

import java.util.List;

public class OptionalSslHandler
        extends ByteToMessageDecoder
{
    // see https://www.iana.org/assignments/tls-parameters/tls-parameters.xhtml#tls-parameters-5
    private static final int SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC = 20;
    private static final int SSL_CONTENT_TYPE_ALERT = 21;
    private static final int SSL_CONTENT_TYPE_HANDSHAKE = 22;
    private static final int SSL_CONTENT_TYPE_APPLICATION_DATA = 23;

    private static final int SSL_RECORD_HEADER_LENGTH =
            Byte.BYTES +               // content type
                    Byte.BYTES +       // major version
                    Byte.BYTES +       // minor version
                    Short.BYTES;       // length

    private final SslContext sslContext;

    public OptionalSslHandler(SslContext sslContext)
    {
        this.sslContext = sslContext;
    }

    @Override
    protected void decode(ChannelHandlerContext context, ByteBuf in, List<Object> out)
    {
        // minimum bytes required to detect ssl
        if (in.readableBytes() < SSL_RECORD_HEADER_LENGTH) {
            return;
        }

        ChannelPipeline pipeline = context.pipeline();
        if (isTls(in, in.readerIndex())) {
            pipeline.replace(this, "ssl", sslContext.newHandler(context.alloc()));
        }
        else {
            pipeline.remove(this);
        }
    }

    private static boolean isTls(ByteBuf buffer, int offset)
    {
        // SSLv3 or TLS - Check ContentType
        int contentType = buffer.getUnsignedByte(offset);
        if (contentType != SSL_CONTENT_TYPE_CHANGE_CIPHER_SPEC &&
                contentType != SSL_CONTENT_TYPE_ALERT &&
                contentType != SSL_CONTENT_TYPE_HANDSHAKE &&
                contentType != SSL_CONTENT_TYPE_APPLICATION_DATA) {
            return false;
        }

        // SSLv3 or TLS - Check ProtocolVersion
        int majorVersion = buffer.getUnsignedByte(offset + 1);
        if (majorVersion != 3) {
            return false;
        }

        // SSLv3 or TLS  - Check packet length is positive
        if (buffer.getUnsignedShort(offset + 3) <= 0) {
            return false;
        }

        return true;
    }
}
