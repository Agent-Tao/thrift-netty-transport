package com.xiaohongshu.infra.transport.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.CheckReturnValue;
import javax.annotation.concurrent.NotThreadSafe;

import static java.util.Objects.requireNonNull;
import static javax.annotation.meta.When.UNKNOWN;

@NotThreadSafe
public class TChannelBufferInputTransport
        extends TTransport implements ReferenceCounted
{
    private final ByteBuf buffer;

    public TChannelBufferInputTransport(ByteBuf buffer)
    {
        this.buffer = requireNonNull(buffer, "buffer is null");
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void open() throws TTransportException {

    }

    @Override
    public void close() {

    }

    @Override
    public int read(byte[] buf, int off, int len)
    {
        buffer.readBytes(buf, off, len);
        return len-off;
    }

    @Override
    public void write(byte[] buf, int off, int len)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int refCnt()
    {
        return buffer.refCnt();
    }

    @Override
    public ReferenceCounted retain()
    {
        buffer.retain();
        return this;
    }

    @Override
    public ReferenceCounted retain(int increment)
    {
        buffer.retain(increment);
        return this;
    }

    @Override
    public ReferenceCounted touch()
    {
        buffer.touch();
        return this;
    }

    @Override
    public ReferenceCounted touch(Object hint)
    {
        buffer.touch(hint);
        return this;
    }

    @CheckReturnValue(when = UNKNOWN)
    @Override
    public boolean release()
    {
        return buffer.release();
    }

    @Override
    public boolean release(int decrement)
    {
        return buffer.release(decrement);
    }
}
