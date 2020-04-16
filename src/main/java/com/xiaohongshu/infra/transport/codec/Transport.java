package com.xiaohongshu.infra.transport.codec;


import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.Optional;

public enum Transport
{
    UNFRAMED {
        @Override
        public void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, Integer maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
        {
            Protocol protocolType = protocol.orElseThrow(() -> new IllegalArgumentException("UNFRAMED transport requires a protocol"));
            pipeline.addLast("thriftUnframedDecoder", new ThriftUnframedDecoder(protocolType, maxFrameSize, assumeClientsSupportOutOfOrderResponses));
            pipeline.addLast(new SimpleFrameCodec(this, protocolType, assumeClientsSupportOutOfOrderResponses));
        }
    },
    FRAMED {
        @Override
        public void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, Integer maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
        {
            Protocol protocolType = protocol.orElseThrow(() -> new IllegalArgumentException("FRAMED transport requires a protocol"));
            pipeline.addLast("frameEncoder", new LengthFieldPrepender(Integer.BYTES));
            FrameInfoDecoder frameInfoDecoder = new SimpleFrameInfoDecoder(FRAMED, protocolType, assumeClientsSupportOutOfOrderResponses);
            pipeline.addLast("thriftFramedDecoder", new ThriftFramedDecoder(frameInfoDecoder, maxFrameSize));
            pipeline.addLast(new SimpleFrameCodec(this, protocolType, assumeClientsSupportOutOfOrderResponses));
        }
    },
    HEADER {
        @Override
        public void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, Integer maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses)
        {
            pipeline.addLast("frameEncoder", new LengthFieldPrepender(Integer.BYTES));
            pipeline.addLast("thriftFramedDecoder", new ThriftFramedDecoder(HeaderTransport::tryDecodeFrameInfo, maxFrameSize));
            pipeline.addLast(new HeaderCodec());
        }
    };

    public abstract void addFrameHandlers(ChannelPipeline pipeline, Optional<Protocol> protocol, Integer maxFrameSize, boolean assumeClientsSupportOutOfOrderResponses);
}
