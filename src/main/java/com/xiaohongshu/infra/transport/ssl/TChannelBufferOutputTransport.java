package com.xiaohongshu.infra.transport.ssl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCounted;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.CheckReturnValue;
import javax.annotation.concurrent.NotThreadSafe;

import static javax.annotation.meta.When.UNKNOWN;

@NotThreadSafe
public class TChannelBufferOutputTransport
        extends TTransport implements ReferenceCounted
{
    private final ByteBuf buffer;

    public TChannelBufferOutputTransport(ByteBufAllocator byteBufAllocator)
    {
        this(byteBufAllocator.buffer(1024));
    }

    public TChannelBufferOutputTransport(ByteBuf buffer)
    {
        this.buffer = buffer;
    }

    public byte[] getBuffer()
    {
        return buffer.retainedDuplicate().array();
    }

    public ByteBuf getByteBuffer()
    {
        return buffer.retainedDuplicate();
    }

    @Override
    public void write(byte[] buf, int off, int len)
    {
        buffer.writeBytes(buf, off, len);
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
