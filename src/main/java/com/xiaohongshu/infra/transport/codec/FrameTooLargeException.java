package com.xiaohongshu.infra.transport.codec;

import io.netty.handler.codec.DecoderException;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

public class FrameTooLargeException
        extends DecoderException
{
    private final Optional<FrameInfo> frameInfo;
    private final long frameSizeInBytes;
    private final int maxFrameSizeInBytes;

    public FrameTooLargeException(Optional<FrameInfo> frameInfo, long frameSizeInBytes, int maxFrameSizeInBytes)
    {
        this.frameInfo = requireNonNull(frameInfo, "sequenceId is null");
        checkArgument(frameSizeInBytes >= 0, "frameSizeInBytes cannot be negative");
        this.frameSizeInBytes = frameSizeInBytes;
        checkArgument(maxFrameSizeInBytes >= 0, "maxFrameSizeInBytes cannot be negative");
        this.maxFrameSizeInBytes = maxFrameSizeInBytes;
    }

    public Optional<FrameInfo> getFrameInfo()
    {
        return frameInfo;
    }

    @Override
    public String getMessage()
    {
        //return format("Frame size %s exceeded max size %s: %s", succinctBytes(frameSizeInBytes), succinctBytes(maxFrameSizeInBytes), frameInfo);
        return "";
    }
}
