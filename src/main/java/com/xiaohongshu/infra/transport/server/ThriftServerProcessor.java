/*
 * Copyright (C) 2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xiaohongshu.infra.transport.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.xiaohongshu.infra.transport.codec.*;
import com.xiaohongshu.infra.transport.duplex.TDuplexProtocolFactory;
import com.xiaohongshu.infra.transport.duplex.TProtocolPair;
import com.xiaohongshu.infra.transport.duplex.TTransportPair;
import com.xiaohongshu.infra.transport.processor.ThriftProcessorAdapters;
import com.xiaohongshu.infra.transport.processor.ThriftProcessorFactory;
import com.xiaohongshu.infra.transport.ssl.TChannelBufferInputTransport;
import com.xiaohongshu.infra.transport.ssl.TChannelBufferOutputTransport;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;


import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.apache.thrift.protocol.TMessageType.EXCEPTION;

public class ThriftServerProcessor
        extends ChannelDuplexHandler
{
    private static final Logger log = LoggerFactory.getLogger(ThriftServerProcessor.class);

    private static final Pattern CONNECTION_CLOSED_MESSAGE = Pattern.compile(
            "^.*(?:connection.*(?:reset|closed|abort|broken)|broken.*pipe).*$", CASE_INSENSITIVE);

    private final ScheduledExecutorService timeoutExecutor;
    private final Duration requestTimeout;
    private final Executor exe = Executors.newFixedThreadPool(100);
    private final AtomicInteger dispatcherSequenceId = new AtomicInteger(0);
    private final AtomicInteger lastResponseWrittenId = new AtomicInteger(0);

    private final TProcessor processor;

    private final ThriftProcessorFactory processorFactory;

    public ThriftServerProcessor(TProcessor processor, Duration requestTimeout, ScheduledExecutorService timeoutExecutor)
    {
        this.processor = processor;
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.processorFactory = ThriftProcessorAdapters.factoryFromTProcessor(processor);
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

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause)
    {
        // if possible, try to reply with an exception in case of a too large request
        if (cause instanceof FrameTooLargeException) {
            FrameTooLargeException e = (FrameTooLargeException) cause;
            // frame info may be missing in case of a large, but invalid request
            if (e.getFrameInfo().isPresent()) {
                FrameInfo frameInfo = e.getFrameInfo().get();
                try {
                    context.writeAndFlush(writeApplicationException(
                            context,
                            frameInfo.getMethodName(),
                            frameInfo.getTransport(),
                            frameInfo.getProtocol(),
                            frameInfo.getSequenceId(),
                            frameInfo.isSupportOutOfOrderResponse(),
                            TApplicationException.PROTOCOL_ERROR,
                            e.getMessage()));
                }
                catch (Throwable t) {
                    context.close();
                    log.error("Failed to write frame info", t);
                }
                return;
            }
        }

        context.close();

        // Don't log connection closed exceptions
        if (!isConnectionClosed(cause)) {
            log.error("",cause);
        }
    }

    private void messageReceived(ChannelHandlerContext context, ThriftMessage frame)
    {

        TMemoryTransport messageTransport = new TMemoryTransport(context.channel(), frame.getMessage());

        TTransportPair transportPair = TTransportPair.fromSingleTransport(messageTransport);


        TDuplexProtocolFactory duplexProtocolFactory = TDuplexProtocolFactory.fromSingleFactory(new TBinaryProtocol.Factory(true,true));

        TProtocolPair protocolPair = duplexProtocolFactory.getProtocolPair(transportPair);

        TProtocol inProtocol = protocolPair.getInputProtocol();
        TProtocol outProtocol = protocolPair.getOutputProtocol();

        TChannelBufferInputTransport inputTransport = new TChannelBufferInputTransport(frame.getMessage());


        try {

            processRequest(context,frame,messageTransport,inProtocol,outProtocol);

        }
        catch (Exception e) {
            log.error("Exception processing request", e);
            context.disconnect();
        }
        catch (Throwable e) {
            log.error("Error processing request", e);
            context.disconnect();
            throw e;
        }
        finally {
            inputTransport.release();
            frame.release();
        }
    }

    private void processRequest(
            final ChannelHandlerContext ctx,
            final ThriftMessage message,
            final TMemoryTransport messageTransport,
            final TProtocol inProtocol,
            final TProtocol outProtocol) {
        // Remember the ordering of requests as they arrive, used to enforce an order on the
        // responses.
        final int requestSequenceId = dispatcherSequenceId.incrementAndGet();

        /*
        if (DispatcherContext.isResponseOrderingRequired(ctx)) {
            synchronized (responseMap) {
                // Limit the number of pending responses (responses which finished out of order, and are
                // waiting for previous requests to be finished so they can be written in order), by
                // blocking further channel reads. Due to the way Netty frame decoders work, this is more
                // of an estimate than a hard limit. Netty may continue to decode and process several
                // more requests that were in the latest read, even while further reads on the channel
                // have been blocked.
                if (requestSequenceId > lastResponseWrittenId.get() + queuedResponseLimit &&
                        !DispatcherContext.isChannelReadBlocked(ctx)) {
                    DispatcherContext.blockChannelReads(ctx);
                }
            }
        }*/

        try {
            exe.execute(new Runnable() {
                @Override
                public void run() {
                    ListenableFuture<Boolean> processFuture;
                    final AtomicBoolean responseSent = new AtomicBoolean(false);
                    // Use AtomicReference as a generic holder class to be able to mark it final
                    // and pass into inner classes. Since we only use .get() and .set(), we don't
                    // actually do any atomic operations.
                    //final AtomicReference<Timeout> expireTimeout = new AtomicReference<>(null);

                    try {
                        try {
                            /*
                            long timeRemaining = 0;
                            long timeElapsed = System.currentTimeMillis() - message.getProcessStartTimeMillis();
                            if (queueTimeoutMillis > 0) {
                                if (timeElapsed >= queueTimeoutMillis) {
                                    org.apache.thrift.TApplicationException taskTimeoutException = new org.apache.thrift.TApplicationException(
                                            org.apache.thrift.TApplicationException.INTERNAL_ERROR,
                                            "Task stayed on the queue for " + timeElapsed +
                                                    " milliseconds, exceeding configured queue timeout of " + queueTimeoutMillis +
                                                    " milliseconds."
                                    );
                                    sendTApplicationException(taskTimeoutException, ctx, message, requestSequenceId, messageTransport,
                                            inProtocol, outProtocol);
                                    return;
                                }
                            } else if (taskTimeoutMillis > 0) {
                                if (timeElapsed >= taskTimeoutMillis) {
                                    org.apache.thrift.TApplicationException taskTimeoutException = new org.apache.thrift.TApplicationException(
                                            org.apache.thrift.TApplicationException.INTERNAL_ERROR,
                                            "Task stayed on the queue for " + timeElapsed +
                                                    " milliseconds, exceeding configured task timeout of " + taskTimeoutMillis +
                                                    " milliseconds."
                                    );
                                    sendTApplicationException(taskTimeoutException, ctx, message, requestSequenceId, messageTransport,
                                            inProtocol, outProtocol);
                                    return;
                                } else {
                                    timeRemaining = taskTimeoutMillis - timeElapsed;
                                }
                            }

                            if (timeRemaining > 0) {
                                expireTimeout.set(taskTimeoutTimer.newTimeout(new TimerTask() {
                                    @Override
                                    public void run(Timeout timeout) throws Exception {
                                        // The immediateFuture returned by processors isn't cancellable, cancel() and
                                        // isCanceled() always return false. Use a flag to detect task expiration.
                                        if (responseSent.compareAndSet(false, true)) {
                                            org.apache.thrift.TApplicationException ex = new org.apache.thrift.TApplicationException(
                                                    org.apache.thrift.TApplicationException.INTERNAL_ERROR,
                                                    "Task timed out while executing."
                                            );
                                            // Create a temporary transport to send the exception
                                            ChannelBuffer duplicateBuffer = message.getBuffer().duplicate();
                                            duplicateBuffer.resetReaderIndex();
                                            TNiftyTransport temporaryTransport = new TNiftyTransport(
                                                    ctx.getChannel(),
                                                    duplicateBuffer,
                                                    message.getTransportType());
                                            TProtocolPair protocolPair = duplexProtocolFactory.getProtocolPair(
                                                    TTransportPair.fromSingleTransport(temporaryTransport));
                                            sendTApplicationException(ex, ctx, message,
                                                    requestSequenceId,
                                                    temporaryTransport,
                                                    protocolPair.getInputProtocol(),
                                                    protocolPair.getOutputProtocol());
                                        }
                                    }
                                }, timeRemaining, TimeUnit.MILLISECONDS));
                            }

                            ConnectionContext connectionContext = ConnectionContexts.getContext(ctx.getChannel());
                            RequestContext requestContext = new NiftyRequestContext(connectionContext, inProtocol, outProtocol, messageTransport);
                            RequestContexts.setCurrentContext(requestContext);
                             */

                            processFuture = processorFactory.getProcessor(messageTransport).process(inProtocol, outProtocol);
                        } finally {
                            // RequestContext does NOT stay set while we are waiting for the process
                            // future to complete. This is by design because we'll might move on to the
                            // next request using this thread before this one is completed. If you need
                            // the context throughout an asynchronous handler, you need to read and store
                            // it before returning a future.
                            //RequestContexts.clearCurrentContext();
                        }

                        Futures.addCallback(
                                processFuture,
                                new FutureCallback<Boolean>() {
                                    @Override
                                    public void onSuccess(Boolean result) {
                                        /*deleteExpirationTimer(expireTimeout.get());*/
                                        try {
                                            // Only write response if the client is still there and the task timeout
                                            // hasn't expired.
                                            if (ctx.channel().isOpen() && responseSent.compareAndSet(false, true)) {
                                                //ThriftMessage response = message.getMessageFactory().create(
                                                //        messageTransport.getOutputBuffer());
                                                writeResponse(ctx, messageTransport.getOutputBuffer(), requestSequenceId,
                                                        false);
                                            }
                                        } catch (Throwable t) {
                                            //onDispatchException(ctx, t);
                                        }
                                    }

                                    @Override
                                    public void onFailure(Throwable t) {

                                        //deleteExpirationTimer(expireTimeout.get());
                                        //onDispatchException(ctx, t);
                                    }
                                },directExecutor()
                        );
                    } catch (TException e) {
                        //onDispatchException(ctx, e);
                    }
                }
            });
        }
        catch (RejectedExecutionException ex) {
            org.apache.thrift.TApplicationException x = new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.INTERNAL_ERROR,
                    "Server overloaded");
            //sendTApplicationException(x, ctx, message, requestSequenceId, messageTransport, inProtocol, outProtocol);
        }
    }


    private void writeResponse(ChannelHandlerContext ctx,
                               ByteBuf response,
                               int responseSequenceId,
                               boolean isOrderedResponsesRequired)
    {
        if (isOrderedResponsesRequired) {
            //writeResponseInOrder(ctx, response, responseSequenceId);
        }
        else {
            // No ordering required, just write the response immediately
            ctx.channel().writeAndFlush(response);
            lastResponseWrittenId.incrementAndGet();
        }
    }

/*
    private static ThriftFrame writeSuccessResponse(
            ChannelHandlerContext context,
            MethodMetadata methodMetadata,
            Transport transport,
            Protocol protocol,
            int sequenceId,
            boolean supportOutOfOrderResponse,
            Object result)
            throws Exception
    {
        TChannelBufferOutputTransport outputTransport = new TChannelBufferOutputTransport(context.alloc());
        try {
            writeResponse(
                    methodMetadata.getName(),
                    protocol.createProtocol(outputTransport),
                    sequenceId,
                    "success",
                    (short) 0,
                    methodMetadata.getResultCodec(),
                    result);

            return new ThriftFrame(
                    sequenceId,
                    outputTransport.getBuffer(),
                    ImmutableMap.of(),
                    transport,
                    protocol,
                    supportOutOfOrderResponse);
        }
        finally {
            outputTransport.release();
        }
    }

    private static ThriftFrame writeExceptionResponse(ChannelHandlerContext context,
            MethodMetadata methodMetadata,
            Transport transport,
            Protocol protocol,
            int sequenceId,
            boolean supportOutOfOrderResponse,
            Throwable exception)
            throws Exception
    {
        Optional<Short> exceptionId = methodMetadata.getExceptionId(exception.getClass());
        if (exceptionId.isPresent()) {
            TChannelBufferOutputTransport outputTransport = new TChannelBufferOutputTransport(context.alloc());
            try {
                TProtocolWriter protocolWriter = protocol.createProtocol(outputTransport);

                writeResponse(
                        methodMetadata.getName(),
                        protocolWriter,
                        sequenceId,
                        "exception",
                        exceptionId.get(),
                        methodMetadata.getExceptionCodecs().get(exceptionId.get()),
                        exception);

                return new ThriftFrame(
                        sequenceId,
                        outputTransport.getBuffer(),
                        ImmutableMap.of(),
                        transport,
                        protocol,
                        supportOutOfOrderResponse);
            }
            finally {
                outputTransport.release();
            }
        }

        String message = format("Internal error processing method [%s]", methodMetadata.getName());

        TApplicationException.Type type = INTERNAL_ERROR;
        if (exception instanceof TApplicationException) {
            type = ((TApplicationException) exception).getType().orElse(INTERNAL_ERROR);
        }
        else {
            log.warn(exception, message);
        }

        return writeApplicationException(
                context,
                methodMetadata.getName(),
                transport,
                protocol,
                sequenceId,
                supportOutOfOrderResponse,
                type,
                message + ": " + exception.getMessage());
    }
*/
    private static ThriftMessage writeApplicationException(
            ChannelHandlerContext context,
            String methodName,
            Transport transport,
            Protocol protocol,
            int sequenceId,
            boolean supportOutOfOrderResponse,
            int errorCode,
            String errorMessage)
            throws Exception
    {

        //TApplicationException applicationException = new TApplicationException(errorCode.getType(), errorMessage);

        TChannelBufferOutputTransport outputTransport = new TChannelBufferOutputTransport(context.alloc());
        try {
            TProtocol protocolWriter = protocol.createProtocol(outputTransport);

            protocolWriter.writeMessageBegin(new TMessage(methodName, EXCEPTION, sequenceId));

            //ExceptionWriter.writeTApplicationException(applicationException, protocolWriter);

            protocolWriter.writeMessageEnd();
            return new ThriftMessage(
                    sequenceId,
                    outputTransport.getByteBuffer(),
                    ImmutableMap.of(),
                    transport,
                    protocol,
                    supportOutOfOrderResponse);
        }
        finally {
            outputTransport.release();
        }
    }

    private static void writeResponse(
            String methodName,
            TProtocol protocolWriter,
            int sequenceId,
            String responseFieldName,
            short responseFieldId,
            //ThriftCodec<Object> responseCodec,
            Object result)
            throws Exception
    {
        protocolWriter.writeMessageBegin(new TMessage(methodName, TMessageType.REPLY, sequenceId));

        /*
        ProtocolWriter writer = new ProtocolWriter(protocolWriter);
        writer.writeStructBegin(methodName + "_result");
        writer.writeField(responseFieldName, responseFieldId, responseCodec, result);
        writer.writeStructEnd();
        */
        protocolWriter.writeMessageEnd();
    }

    /*
     * There is no good way of detecting connection closed exception
     *
     * This implementation is a simplified version of the implementation proposed
     * in Netty: io.netty.handler.ssl.SslHandler#exceptionCaught
     *
     * This implementation ony checks a message with the regex, and doesn't do any
     * more sophisticated matching, as the regex works in most of the cases.
     */
    private boolean isConnectionClosed(Throwable t)
    {
        if (t instanceof IOException) {
            return CONNECTION_CLOSED_MESSAGE.matcher(nullToEmpty(t.getMessage())).matches();
        }
        return false;
    }
}
