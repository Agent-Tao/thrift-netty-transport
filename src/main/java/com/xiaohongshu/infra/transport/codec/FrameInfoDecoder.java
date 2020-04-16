package com.xiaohongshu.infra.transport.codec;

import io.netty.buffer.ByteBuf;

import java.util.Optional;

interface FrameInfoDecoder
{
    /**
     * Attempts to decode basic frame info without moving the reader index
     */
    Optional<FrameInfo> tryDecodeFrameInfo(ByteBuf buffer);
}
