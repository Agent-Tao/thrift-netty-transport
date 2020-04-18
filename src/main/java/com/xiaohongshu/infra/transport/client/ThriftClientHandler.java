package com.xiaohongshu.infra.transport.client;

import com.google.common.util.concurrent.AbstractFuture;
import com.xiaohongshu.infra.transport.codec.FrameTooLargeException;
import com.xiaohongshu.infra.transport.codec.Protocol;
import com.xiaohongshu.infra.transport.codec.ThriftMessage;
import com.xiaohongshu.infra.transport.codec.Transport;
import com.xiaohongshu.infra.transport.ssl.TChannelBufferOutputTransport;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;

import javax.annotation.concurrent.ThreadSafe;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class ThriftClientHandler
        extends ChannelDuplexHandler
{
    private static final int ONEWAY_SEQUENCE_ID = 0xFFFF_FFFF;

    private final Duration requestTimeout;
    private final Transport transport;
    private final Protocol protocol;

    private final ConcurrentHashMap<Integer, RequestHandler> pendingRequests = new ConcurrentHashMap<>();
    private final AtomicReference<TException> channelError = new AtomicReference<>();
    //private final AtomicInteger sequenceId = new AtomicInteger(0);

    ThriftClientHandler(Duration requestTimeout, Transport transport, Protocol protocol)
    {
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
        this.transport = requireNonNull(transport, "transport is null");
        this.protocol = requireNonNull(protocol, "protocol is null");
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise)
            throws Exception
    {
        if (message instanceof ThriftRequest) {
            ThriftRequest thriftRequest = (ThriftRequest) message;
            sendMessage(ctx, thriftRequest, promise);
        }
        else {
            ctx.write(message, promise);
        }
    }

    private void sendMessage(ChannelHandlerContext ctx, ThriftRequest thriftRequest, ChannelPromise promise)
            throws Exception
    {
        // todo ONEWAY_SEQUENCE_ID is a header protocol thing... make sure this works with framed and unframed
        int sequenceId;
        if(thriftRequest.isOneway()) {
            sequenceId = ONEWAY_SEQUENCE_ID;
        } else {
            sequenceId = 1;
        }


        RequestHandler requestHandler = new RequestHandler(thriftRequest, sequenceId);

        // register timeout
        requestHandler.registerRequestTimeout(ctx.executor());

        ByteBuf requestBuffer = thriftRequest.getRequestBuf();

        // register request if we are expecting a response
        if (!thriftRequest.isOneway()) {
            if (pendingRequests.putIfAbsent(sequenceId, requestHandler) != null) {
                requestHandler.onChannelError(new TTransportException("Another request with the same sequenceId is already in progress"));
                requestBuffer.release();
                return;
            }
        }

        // if this connection is failed, immediately fail the request
        TException channelError = this.channelError.get();
        if (channelError != null) {
            thriftRequest.failed(channelError);
            requestBuffer.release();
            return;
        }

        try {
            ThriftMessage thriftFrame = new ThriftMessage(
                    sequenceId,
                    requestBuffer,
                    null,
                    transport,
                    protocol,
                    true);

            //ChannelFuture sendFuture = context.write(thriftFrame, promise);

            ChannelFuture sendFuture = ctx.write(thriftFrame, promise);
            sendFuture.addListener(future -> messageSent(ctx, sendFuture, requestHandler));
        }
        catch (Throwable t) {
            onError(ctx, t, Optional.of(requestHandler));
            requestBuffer.release();
        }
    }

    private void messageSent(ChannelHandlerContext context, ChannelFuture future, RequestHandler requestHandler)
    {
        try {
            if (!future.isSuccess()) {
                onError(context, new TTransportException("Sending request failed", future.cause()), Optional.of(requestHandler));
                return;
            }

            requestHandler.onRequestSent();
        }
        catch (Throwable t) {
            onError(context, t, Optional.of(requestHandler));
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message)
    {
        if (message instanceof ThriftMessage) {
            messageReceived(context, (ThriftMessage) message);
            return;
        }
        context.fireChannelRead(message);
    }

    private void messageReceived(ChannelHandlerContext context, ThriftMessage thriftMessage)
    {
        RequestHandler requestHandler = null;
        try {
            requestHandler = pendingRequests.remove(thriftMessage.getSequenceId());
            if (requestHandler == null) {
                throw new TTransportException("Unknown sequence id in response: " + thriftMessage.getSequenceId());
            }

            requestHandler.onResponseReceived(thriftMessage.retain());
        }
        catch (Throwable t) {
            onError(context, t, Optional.ofNullable(requestHandler));
        }
        finally {
            thriftMessage.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause)
    {
        onError(context, cause, Optional.empty());
    }

    @Override
    public void channelInactive(ChannelHandlerContext context)
    {
        onError(context, new TTransportException("Client was disconnected by server"), Optional.empty());
    }

    private void onError(ChannelHandlerContext context, Throwable throwable, Optional<RequestHandler> currentRequest)
    {
        if (throwable instanceof FrameTooLargeException) {
            checkArgument(!currentRequest.isPresent(), "current request should not be set for FrameTooLargeException");
            onFrameTooLargeException(context, (FrameTooLargeException) throwable);
            return;
        }

        TException thriftException;
        if (throwable instanceof TException) {
            thriftException = (TException) throwable;
        }
        else {
            thriftException = new TTransportException(throwable);
        }

        // set channel error
        if (!channelError.compareAndSet(null, thriftException)) {
            // another thread is already tearing down this channel
            return;
        }

        // current request may have already been removed from pendingRequests, so notify it directly
        currentRequest.ifPresent(request -> {
            pendingRequests.remove(request.getSequenceId());
            request.onChannelError(thriftException);
        });

        // notify all pending requests of the error
        // Note while loop should not be necessary since this class should be single
        // threaded, but it is better to be safe in cleanup code
        while (!pendingRequests.isEmpty()) {
            pendingRequests.values().removeIf(request -> {
                request.onChannelError(thriftException);
                return true;
            });
        }

        context.close();
    }

    private void onFrameTooLargeException(ChannelHandlerContext context, FrameTooLargeException frameTooLargeException)
    {
        /*
        TException thriftException = new MessageTooLargeException(frameTooLargeException.getMessage(), frameTooLargeException);
        Optional<FrameInfo> frameInfo = frameTooLargeException.getFrameInfo();
        if (frameInfo.isPresent()) {
            RequestHandler request = pendingRequests.remove(frameInfo.get().getSequenceId());
            if (request != null) {
                request.onChannelError(thriftException);
                return;
            }
        }
        // if sequence id is missing - fail all requests on a give channel
        onError(context, new MessageTooLargeException("unexpected too large response happened on communication channel", frameTooLargeException), Optional.empty());*/
    }

    public static class ThriftRequest
            extends AbstractFuture<ThriftMessage>
    {

        private final boolean isOneway;

        private final ByteBuf requestBuf;

        public ThriftRequest(boolean isOneway,ByteBuf requestBuf)
        {
            this.isOneway = isOneway;
            this.requestBuf = requestBuf;
        }

        boolean isOneway()
        {
            return isOneway;
        }

        ByteBuf getRequestBuf() {
            return requestBuf;
        }

        void setResponse(ThriftMessage response)
        {
            set(response);
        }

        void failed(Throwable throwable)
        {
            setException(throwable);
        }
    }

    private final class RequestHandler
    {
        private final ThriftRequest thriftRequest;
        private final int sequenceId;

        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicReference<ScheduledFuture<?>> timeout = new AtomicReference<>();

        public RequestHandler(ThriftRequest thriftRequest, int sequenceId)
        {
            this.thriftRequest = thriftRequest;
            this.sequenceId = sequenceId;
        }

        public int getSequenceId()
        {
            return sequenceId;
        }

        void registerRequestTimeout(EventExecutor executor)
        {
            /*
            try {
                timeout.set(executor.schedule(
                        () -> onChannelError(new RequestTimeoutException("Timed out waiting " + requestTimeout + " to receive response")),
                        requestTimeout.toMillis(),
                        MILLISECONDS));
            }
            catch (Throwable throwable) {
                onChannelError(new TTransportException("Unable to schedule request timeout", throwable));
                throw throwable;
            }*/
        }


        ByteBuf encodeRequest(ByteBufAllocator allocator)
                throws Exception
        {
            return allocator.buffer(100);
            /*
            TChannelBufferOutputTransport transport = new TChannelBufferOutputTransport(allocator);
            try {
                TProtocol protocolWriter = protocol.createProtocol(transport);

                // Note that though setting message type to ONEWAY can be helpful when looking at packet
                // captures, some clients always send CALL and so servers are forced to rely on the "oneway"
                // attribute on thrift method in the interface definition, rather than checking the message
                // type.
                MethodMetadata method = thriftRequest.getMethod();
                protocolWriter.writeMessageBegin(new TMessage(method.getName(), method.isOneway() ? ONEWAY : CALL, sequenceId));

                // write the parameters
                ProtocolWriter writer = new ProtocolWriter(protocolWriter);
                writer.writeStructBegin(method.getName() + "_args");
                List<Object> parameters = thriftRequest.getParameters();
                for (int i = 0; i < parameters.size(); i++) {
                    Object value = parameters.get(i);
                    ParameterMetadata parameter = method.getParameters().get(i);
                    writer.writeField(parameter.getName(), parameter.getFieldId(), parameter.getCodec(), value);
                }
                writer.writeStructEnd();

                protocolWriter.writeMessageEnd();
                return transport.getBuffer();
            }
            catch (Throwable throwable) {
                onChannelError(throwable);
                throw throwable;
            }
            finally {
                transport.release();
            }

             */
        }

        void onRequestSent()
        {
            if (!thriftRequest.isOneway()) {
                return;
            }

            if (!finished.compareAndSet(false, true)) {
                return;
            }

            try {
                cancelRequestTimeout();
                thriftRequest.setResponse(null);
            }
            catch (Throwable throwable) {
                onChannelError(throwable);
            }
        }

        void onResponseReceived(ThriftMessage thriftFrame)
        {
            try {
                if (!finished.compareAndSet(false, true)) {
                    return;
                }

                cancelRequestTimeout();
                //Object response = decodeResponse(thriftFrame.getMessage());
                thriftRequest.setResponse(thriftFrame.retain());
            }
            catch (Throwable throwable) {
                thriftRequest.failed(throwable);
            }
            finally {
                thriftFrame.release();
            }
        }

        Object decodeResponse(ByteBuf responseMessage)
                throws Exception
        {

            /*
            TChannelBufferInputTransport transport = new TChannelBufferInputTransport(responseMessage);
            try {
                TProtocolReader protocolReader = protocol.createProtocol(transport);
                MethodMetadata method = thriftRequest.getMethod();

                // validate response header
                TMessage message = protocolReader.readMessageBegin();
                if (message.getType() == EXCEPTION) {
                    TApplicationException exception = ExceptionReader.readTApplicationException(protocolReader);
                    protocolReader.readMessageEnd();
                    throw exception;
                }
                if (message.getType() != REPLY) {
                    throw new TApplicationException(INVALID_MESSAGE_TYPE, format("Received invalid message type %s from server", message.getType()));
                }
                if (!message.getName().equals(method.getName())) {
                    throw new TApplicationException(WRONG_METHOD_NAME, format("Wrong method name in reply: expected %s but received %s", method.getName(), message.getName()));
                }
                if (message.getSequenceId() != sequenceId) {
                    throw new TApplicationException(BAD_SEQUENCE_ID, format("%s failed: out of sequence response", method.getName()));
                }

                // read response struct
                ProtocolReader reader = new ProtocolReader(protocolReader);
                reader.readStructBegin();

                Object results = null;
                Exception exception = null;
                while (reader.nextField()) {
                    if (reader.getFieldId() == 0) {
                        results = reader.readField(method.getResultCodec());
                    }
                    else {
                        ThriftCodec<Object> exceptionCodec = method.getExceptionCodecs().get(reader.getFieldId());
                        if (exceptionCodec != null) {
                            exception = (Exception) reader.readField(exceptionCodec);
                        }
                        else {
                            reader.skipFieldData();
                        }
                    }
                }
                reader.readStructEnd();
                protocolReader.readMessageEnd();

                if (exception != null) {
                    throw new DriftApplicationException(exception);
                }

                if (method.getResultCodec().getType() == ThriftType.VOID) {
                    return null;
                }

                if (results == null) {
                    throw new TApplicationException(MISSING_RESULT, format("%s failed: unknown result", method.getName()));
                }
                return results;
            }
            finally {
                transport.release();
            }
             */
            return responseMessage;
        }

        void onChannelError(Throwable requestException)
        {
            if (!finished.compareAndSet(false, true)) {
                return;
            }

            try {
                cancelRequestTimeout();
            }
            finally {
                thriftRequest.failed(requestException);
            }
        }

        private void cancelRequestTimeout()
        {
            ScheduledFuture<?> timeout = this.timeout.get();
            if (timeout != null) {
                timeout.cancel(false);
            }
        }
    }
}
