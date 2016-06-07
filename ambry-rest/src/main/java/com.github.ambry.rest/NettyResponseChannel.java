/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.rest;

import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.GenericProgressiveFutureListener;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link RestResponseChannel} used to return responses via Netty. It is supported by
 * an underlying Netty channel whose handle this class has in the form of a {@link ChannelHandlerContext}.
 * <p/>
 * Data is sent in the order that threads call {@link #write(ByteBuffer, Callback)}.
 * <p/>
 * If a write through this class fails at any time, the underlying channel will be closed immediately and no more writes
 * will be accepted and all scheduled writes will be notified of the failure.
 */
class NettyResponseChannel implements RestResponseChannel {
  // add to this list if the connection needs to be closed on certain errors on GET, DELETE and HEAD.
  // for a POST, we always close the connection on error because we expect the channel to be in a bad state.
  static final List<HttpResponseStatus> CLOSE_CONNECTION_ERROR_STATUSES = new ArrayList<>();

  private final ChannelHandlerContext ctx;
  private final NettyMetrics nettyMetrics;
  private final ChannelProgressivePromise writeFuture;
  private final ChunkedWriteHandler chunkedWriteHandler;

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final HttpResponse responseMetadata = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
  // tracks whether onResponseComplete() has been called. Helps make it idempotent and also treats response channel as
  // closed if this is true.
  private final AtomicBoolean responseCompleteCalled = new AtomicBoolean(false);
  // tracks whether response metadata write has been initated. Rejects any more attempts at writing metadata after this
  // has been set to true.
  private final AtomicBoolean responseMetadataWriteInitiated = new AtomicBoolean(false);
  private final ResponseMetadataWriteListener responseMetadataWriteListener = new ResponseMetadataWriteListener();
  private final CleanupCallback cleanupCallback = new CleanupCallback();
  private final AtomicLong totalBytesWritten = new AtomicLong(0);
  private final Queue<Chunk> chunksToWrite = new ConcurrentLinkedQueue<Chunk>();
  private final Queue<Chunk> chunksAwaitingCallback = new ConcurrentLinkedQueue<Chunk>();
  private final AtomicLong chunksToWriteCount = new AtomicLong(0);

  private NettyRequest request = null;
  // marked as true if force close is required because close() was called.
  private volatile boolean forceClose = false;
  // the ResponseStatus that will be sent (or has been sent) as a part of the response.
  private volatile ResponseStatus responseStatus = ResponseStatus.Ok;
  // the response metadata that was actually sent.
  private volatile HttpResponse finalResponseMetadata = null;
  // temp variable to hold the error response status which will be overwritten on responseStatus if the error response
  // was successfully sent
  private ResponseStatus errorResponseStatus = null;

  /**
   * Create an instance of NettyResponseChannel that will use {@code ctx} to return responses.
   * @param ctx the {@link ChannelHandlerContext} to use.
   * @param nettyMetrics the {@link NettyMetrics} instance to use.
   */
  public NettyResponseChannel(ChannelHandlerContext ctx, NettyMetrics nettyMetrics) {
    this.ctx = ctx;
    this.nettyMetrics = nettyMetrics;
    chunkedWriteHandler = ctx.pipeline().get(ChunkedWriteHandler.class);
    writeFuture = ctx.newProgressivePromise();
    logger.trace("Instantiated NettyResponseChannel");
  }

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    long writeProcessingStartTime = System.currentTimeMillis();
    if (!responseMetadataWriteInitiated.get()) {
      maybeWriteResponseMetadata(responseMetadata, responseMetadataWriteListener);
    }
    Chunk chunk = new Chunk(src, callback);
    chunksToWrite.add(chunk);
    if (!isOpen()) {
      // the isOpen() check is not before addition to the queue because chunks need to be acknowledged in the order
      // they were received. If we don't add it to the queue and clean up, chunks may be acknowledged out of order.
      logger.debug("Scheduling a chunk cleanup on channel {}", ctx.channel());
      writeFuture.addListener(cleanupCallback);
    } else {
      chunkedWriteHandler.resumeTransfer();
    }

    long writeProcessingTime = System.currentTimeMillis() - writeProcessingStartTime;
    nettyMetrics.writeProcessingTimeInMs.update(writeProcessingTime);
    if (request != null) {
      request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(writeProcessingTime);
    }
    return chunk.future;
  }

  @Override
  public boolean isOpen() {
    return !responseCompleteCalled.get() && ctx.channel().isActive();
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Marks the channel as closed. No further communication will be possible. Any pending writes (that are not already
   * flushed) might be discarded. The process of closing the network channel is also initiated.
   * <p/>
   * The underlying network channel might not be closed immediately but no more writes will be accepted and any calls to
   * {@link #isOpen()} after a call to this function will return {@code false}.
   */
  @Override
  public void close() {
    forceClose = true;
    onResponseComplete(new ClosedChannelException());
  }

  @Override
  public void onResponseComplete(Exception exception) {
    long responseCompleteStartTime = System.currentTimeMillis();
    try {
      if (responseCompleteCalled.compareAndSet(false, true)) {
        logger.trace("Finished responding to current request on channel {}", ctx.channel());
        nettyMetrics.requestCompletionRate.mark();
        if (exception == null) {
          if (!maybeWriteResponseMetadata(responseMetadata, responseMetadataWriteListener)) {
            // There were other writes. Let ChunkedWriteHandler finish if it has been kicked off.
            chunkedWriteHandler.resumeTransfer();
          }
        } else {
          log(exception);
          if (request != null) {
            request.getMetricsTracker().markFailure();
          }
          // need to set writeFuture as failed in case writes have started or chunks have been queued.
          if (!writeFuture.isDone()) {
            writeFuture.setFailure(exception);
          }
          if (!maybeSendErrorResponse(exception)) {
            completeRequest(true);
          }
        }
      } else if (exception != null) {
        // this is probably an attempt to force close the channel *after* the response is already complete.
        log(exception);
        if (!writeFuture.isDone()) {
          writeFuture.setFailure(exception);
        }
        completeRequest(true);
      }
      long responseFinishProcessingTime = System.currentTimeMillis() - responseCompleteStartTime;
      nettyMetrics.responseFinishProcessingTimeInMs.update(responseFinishProcessingTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(responseFinishProcessingTime);
      }
    } catch (Exception e) {
      logger.error("Swallowing exception encountered during onResponseComplete tasks", e);
      nettyMetrics.responseCompleteTasksError.inc();
      if (!writeFuture.isDone()) {
        writeFuture.setFailure(exception);
      }
      completeRequest(true);
    }
  }

  @Override
  public void setStatus(ResponseStatus status)
      throws RestServiceException {
    responseMetadata.setStatus(getHttpResponseStatus(status));
    responseStatus = status;
    logger.trace("Set status to {} for response on channel {}", responseMetadata.getStatus(), ctx.channel());
  }

  @Override
  public ResponseStatus getStatus() {
    return responseStatus;
  }

  @Override
  public void setHeader(String headerName, Object headerValue)
      throws RestServiceException {
    setResponseHeader(headerName, headerValue);
  }

  @Override
  public Object getHeader(String headerName) {
    HttpResponse response = finalResponseMetadata;
    if (response == null) {
      response = responseMetadata;
    }
    return HttpHeaders.getHeader(response, headerName);
  }

  /**
   * Sets the request whose response is being served through this instance of NettyResponseChannel.
   * @param request the {@link NettyRequest} whose response is being served through this instance of
   *                NettyResponseChannel.
   */
  protected void setRequest(NettyRequest request) {
    if (request != null) {
      if (this.request == null) {
        this.request = request;
        HttpHeaders.setKeepAlive(responseMetadata, request.isKeepAlive());
      } else {
        throw new IllegalStateException(
            "Request has already been set inside NettyResponseChannel for channel {} " + ctx.channel());
      }
    } else {
      throw new IllegalArgumentException("RestRequest provided is null");
    }
  }

  /**
   * Closes the request associated with this NettyResponseChannel.
   */
  private void closeRequest() {
    if (request != null && request.isOpen()) {
      request.close();
    }
  }

  /**
   * Writes response metadata to the channel if not already written previously and channel is active.
   * @param responseMetadata the {@link HttpResponse} that needs to be written.
   * @param listener the {@link GenericFutureListener} that needs to be attached to the write.
   * @return {@code true} if response metadata was written to the channel in this call. {@code false} otherwise.
   */
  private boolean maybeWriteResponseMetadata(HttpResponse responseMetadata,
      GenericFutureListener<ChannelFuture> listener) {
    long writeProcessingStartTime = System.currentTimeMillis();
    boolean writtenThisTime = false;
    if (responseMetadataWriteInitiated.compareAndSet(false, true)) {
      // we do some manipulation here for chunking. According to the HTTP spec, we can have either a Content-Length
      // or Transfer-Encoding:chunked, never both. So we check for Content-Length - if it is not there, we add
      // Transfer-Encoding:chunked. Note that sending HttpContent chunks data anyway - we are just explicitly specifying
      // this in the header.
      if (!HttpHeaders.isContentLengthSet(responseMetadata)) {
        // This makes sure that we don't stomp on any existing transfer-encoding.
        HttpHeaders.setTransferEncodingChunked(responseMetadata);
      }
      logger.trace("Sending response with status {} on channel {}", responseMetadata.getStatus(), ctx.channel());
      ChannelPromise writePromise = ctx.newPromise().addListener(listener);
      ctx.writeAndFlush(responseMetadata, writePromise);
      writtenThisTime = true;
      finalResponseMetadata = responseMetadata;
      long writeProcessingTime = System.currentTimeMillis() - writeProcessingStartTime;
      nettyMetrics.responseMetadataProcessingTimeInMs.update(writeProcessingTime);
    }
    return writtenThisTime;
  }

  /**
   * Sets the value of response headers after making sure that the response metadata is not already sent.
   * @param headerName The name of the header.
   * @param headerValue The intended value of the header.
   * @throws IllegalArgumentException if any of {@code headerName} or {@code headerValue} is null.
   * @throws IllegalStateException if response metadata has already been written to the channel.
   */
  private void setResponseHeader(String headerName, Object headerValue) {
    if (headerName != null && headerValue != null) {
      long startTime = System.currentTimeMillis();
      if (headerValue instanceof Date) {
        HttpHeaders.setDateHeader(responseMetadata, headerName, (Date) headerValue);
      } else {
        HttpHeaders.setHeader(responseMetadata, headerName, headerValue);
      }
      if (responseMetadataWriteInitiated.get()) {
        nettyMetrics.deadResponseAccessError.inc();
        throw new IllegalStateException("Response metadata changed after it has already been written to the channel");
      } else {
        logger.trace("Header {} set to {} for channel {}", headerName, responseMetadata.headers().get(headerName),
            ctx.channel());
        nettyMetrics.headerSetTimeInMs.update(System.currentTimeMillis() - startTime);
      }
    } else {
      throw new IllegalArgumentException("Header name [" + headerName + "] or header value [" + headerValue + "] null");
    }
  }

  /**
   * Builds and sends an error response to the client based on {@code cause}.
   * @param exception the cause of the request handling failure.
   * @return {@code true} if error response was scheduled to be sent. {@code false} otherwise.
   */
  private boolean maybeSendErrorResponse(Exception exception) {
    long processingStartTime = System.currentTimeMillis();
    boolean responseSent = false;
    logger.trace("Sending error response to client on channel {}", ctx.channel());
    FullHttpResponse errorResponse = getErrorResponse(exception);
    if (maybeWriteResponseMetadata(errorResponse,
        new ErrorResponseWriteListener(HttpHeaders.isKeepAlive(errorResponse)))) {
      logger.trace("Successfully sent error response on channel {}", ctx.channel());
      responseStatus = errorResponseStatus;
      responseSent = true;
      long processingTime = System.currentTimeMillis() - processingStartTime;
      nettyMetrics.errorResponseProcessingTimeInMs.update(processingTime);
    } else {
      logger
          .debug("Could not send error response on channel {} because a response has already been sent", ctx.channel());
    }
    return responseSent;
  }

  /**
   * Provided a cause, returns an error response with the right status and error message.
   * @param cause the cause of the error.
   * @return a {@link FullHttpResponse} with the error message that can be sent to the client.
   */
  private FullHttpResponse getErrorResponse(Throwable cause) {
    HttpResponseStatus status;
    StringBuilder errReason = new StringBuilder();
    if (cause instanceof RestServiceException) {
      RestServiceErrorCode restServiceErrorCode = ((RestServiceException) cause).getErrorCode();
      errorResponseStatus = ResponseStatus.getResponseStatus(restServiceErrorCode);
      status = getHttpResponseStatus(errorResponseStatus);
      if (status == HttpResponseStatus.BAD_REQUEST) {
        errReason.append(" [").append(Utils.getRootCause(cause).getMessage()).append("]");
      }
    } else {
      nettyMetrics.internalServerErrorCount.inc();
      status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
      errorResponseStatus = ResponseStatus.InternalServerError;
    }
    String fullMsg = "Failure: " + status + errReason;
    logger.trace("Constructed error response for the client - [{}]", fullMsg);
    FullHttpResponse response;
    if (request != null && !request.getRestMethod().equals(RestMethod.HEAD)) {
      response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(fullMsg.getBytes()));
    } else {
      // for HEAD, we cannot send the actual body but we need to return what the length would have been if this was GET.
      // https://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html (Section 9.4)
      response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status);
    }
    HttpHeaders.setDate(response, new GregorianCalendar().getTime());
    HttpHeaders.setContentLength(response, fullMsg.length());
    HttpHeaders.setHeader(response, HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
    boolean keepAlive = HttpHeaders.isKeepAlive(responseMetadata) &&
        request != null && !request.getRestMethod().equals(RestMethod.POST) && !CLOSE_CONNECTION_ERROR_STATUSES
        .contains(status);
    HttpHeaders.setKeepAlive(response, keepAlive);
    return response;
  }

  /**
   * Converts a {@link ResponseStatus} into a {@link HttpResponseStatus}.
   * @param responseStatus {@link ResponseStatus} that needs to be mapped to a {@link HttpResponseStatus}.
   * @return the {@link HttpResponseStatus} that maps to the {@link ResponseStatus}.
   */
  private HttpResponseStatus getHttpResponseStatus(ResponseStatus responseStatus) {
    HttpResponseStatus status;
    switch (responseStatus) {
      case Ok:
        status = HttpResponseStatus.OK;
        break;
      case Created:
        status = HttpResponseStatus.CREATED;
        break;
      case Accepted:
        status = HttpResponseStatus.ACCEPTED;
        break;
      case NotModified:
        status = HttpResponseStatus.NOT_MODIFIED;
        break;
      case BadRequest:
        nettyMetrics.badRequestCount.inc();
        status = HttpResponseStatus.BAD_REQUEST;
        break;
      case Unauthorized:
        nettyMetrics.unauthorizedCount.inc();
        status = HttpResponseStatus.UNAUTHORIZED;
        break;
      case NotFound:
        nettyMetrics.notFoundCount.inc();
        status = HttpResponseStatus.NOT_FOUND;
        break;
      case Gone:
        nettyMetrics.goneCount.inc();
        status = HttpResponseStatus.GONE;
        break;
      case Forbidden:
        nettyMetrics.forbiddenCount.inc();
        status = HttpResponseStatus.FORBIDDEN;
        break;
      case ProxyAuthenticationRequired:
        nettyMetrics.proxyAuthRequiredCount.inc();
        status = HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED;
        break;
      case InternalServerError:
        nettyMetrics.internalServerErrorCount.inc();
        status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        break;
      default:
        nettyMetrics.unknownResponseStatusCount.inc();
        status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
        break;
    }
    return status;
  }

  /**
   * Completes the request by closing the request and network channel (if {@code closeNetworkChannel} is {@code true}).
   * </p>
   * May also close the channel if the class internally is forcing a close (i.e. if {@link #close()} is called.
   * @param closeNetworkChannel network channel is closed if {@code true}.
   */
  private void completeRequest(boolean closeNetworkChannel) {
    if ((closeNetworkChannel || forceClose) && ctx.channel().isOpen()) {
      writeFuture.addListener(ChannelFutureListener.CLOSE);
      logger.trace("Requested closing of channel {}", ctx.channel());
    }
    closeRequest();
  }

  /**
   * Handles post-mortem of writes that have failed.
   * @param cause the cause of the failure.
   * @param propagateErrorIfRequired if {@code true} and {@code cause} is not an instance of {@link Exception}, the
   *                                 error is propagated through the netty pipeline.
   */
  private void handleChannelWriteFailure(Throwable cause, boolean propagateErrorIfRequired) {
    long writeFailureProcessingStartTime = System.currentTimeMillis();
    try {
      nettyMetrics.channelWriteError.inc();
      Exception exception;
      if (!(cause instanceof Exception)) {
        logger.warn("Encountered a throwable on channel write failure", cause);
        exception = new IllegalStateException("Encountered a Throwable - " + cause.getMessage());
        if (propagateErrorIfRequired) {
          // we can't ignore throwables - so we let Netty deal with it.
          ctx.fireExceptionCaught(cause);
          nettyMetrics.throwableCount.inc();
        }
      } else {
        exception = (Exception) cause;
      }
      onResponseComplete(exception);

      logger.trace("Cleaning up remaining chunks on write failure");
      Chunk chunk = chunksAwaitingCallback.poll();
      while (chunk != null) {
        chunk.resolveChunk(exception);
        chunk = chunksAwaitingCallback.poll();
      }
      chunk = chunksToWrite.poll();
      while (chunk != null) {
        chunk.onDequeue();
        chunk.resolveChunk(exception);
        chunk = chunksToWrite.poll();
      }
    } finally {
      nettyMetrics.channelWriteFailureProcessingTimeInMs
          .update(System.currentTimeMillis() - writeFailureProcessingStartTime);
    }
  }

  /**
   * Logs the exception at the appropriate level.
   * @param exception the {@link Exception} that has to be logged.
   */
  private void log(Exception exception) {
    String uri = "unknown";
    RestMethod restMethod = RestMethod.UNKNOWN;
    if (request != null) {
      uri = request.getUri();
      restMethod = request.getRestMethod();
    }
    if (exception instanceof RestServiceException) {
      RestServiceErrorCode errorCode = ((RestServiceException) exception).getErrorCode();
      ResponseStatus responseStatus = ResponseStatus.getResponseStatus(errorCode);
      if (responseStatus == ResponseStatus.InternalServerError) {
        logger.error("Internal error handling request {} with method {}.", uri, restMethod, exception);
      } else {
        logger.trace("Error handling request {} with method {}.", uri, restMethod, exception);
      }
    } else {
      logger.error("Unexpected error handling request {} with method {}.", uri, restMethod, exception);
    }
  }

  // helper classes

  /**
   * Class that represents a chunk of data.
   */
  private class Chunk {
    /**
     * The future that will be set on chunk resolution.
     */
    public final FutureResult<Long> future = new FutureResult<Long>();
    /**
     * The bytes associated with this chunk.
     */
    public final ByteBuffer buffer;
    /**
     * The number of bytes that will need to be written.
     */
    public final long bytesToBeWritten;
    /**
     * If progress in {@link #writeFuture} becomes greater than this number, the future/callback will be triggered.
     */
    public long writeCompleteThreshold;
    private final Callback<Long> callback;
    private final long chunkQueueStartTime = System.currentTimeMillis();
    private long chunkWriteStartTime;

    /**
     * Creates a chunk.
     * @param buffer the {@link ByteBuffer} that forms the data of this chunk.
     * @param callback the {@link Callback} to invoke when {@link #writeCompleteThreshold} is reached.
     */
    public Chunk(ByteBuffer buffer, Callback<Long> callback) {
      this.buffer = buffer;
      bytesToBeWritten = buffer.remaining();
      this.callback = callback;
      chunksToWriteCount.incrementAndGet();
    }

    /**
     * Does tasks (like tracking) that need to be done when a chunk is dequeued for processing.
     */
    public void onDequeue() {
      chunksToWriteCount.decrementAndGet();
      chunkWriteStartTime = System.currentTimeMillis();
      long chunkQueueTime = chunkWriteStartTime - chunkQueueStartTime;
      nettyMetrics.chunkQueueTimeInMs.update(chunkQueueTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(chunkQueueTime);
      }
    }

    /**
     * Marks a chunk as handled and invokes the callback and future that accompanied this chunk of data. Once a chunk is
     * resolved, the data inside it is considered void.
     * @param exception the reason for chunk handling failure.
     */
    public void resolveChunk(Exception exception) {
      long chunkWriteFinishTime = System.currentTimeMillis();
      long bytesWritten = 0;
      if (exception == null) {
        bytesWritten = bytesToBeWritten;
        buffer.position(buffer.limit());
      }
      nettyMetrics.bytesWriteRate.mark(bytesWritten);
      future.done(bytesWritten, exception);
      if (callback != null) {
        callback.onCompletion(bytesWritten, exception);
      }
      long chunkResolutionProcessingTime = System.currentTimeMillis() - chunkWriteFinishTime;
      long chunkWriteTime = chunkWriteFinishTime - chunkWriteStartTime;
      nettyMetrics.channelWriteTimeInMs.update(chunkWriteTime);
      nettyMetrics.chunkResolutionProcessingTimeInMs.update(chunkResolutionProcessingTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(chunkWriteTime);
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(chunkResolutionProcessingTime);
      }
    }
  }

  /**
   * Dispenses chunks when asked to by the {@link ChunkedWriteHandler}.
   */
  private class ChunkDispenser implements ChunkedInput<HttpContent> {
    // NOTE: Due to a bug in HttpChunkedInput, some code there has been reproduced here instead of using it directly.
    private boolean sentLastChunk = false;

    /**
     * Determines if input has ended by examining response state and number of chunks pending for write.
     * @return {@code true} if there are no more chunks to write and the end marker has been sent. {@code false}
     *         otherwise.
     */
    @Override
    public boolean isEndOfInput() {
      return allChunksWritten() && sentLastChunk;
    }

    @Override
    public void close() {
      // nothing to do.
    }

    /**
     * Dispenses the next chunk from {@link #chunksToWrite} if one is available and adds the chunk dispensed to
     * {@link #chunksAwaitingCallback}.
     * @param ctx the {@link ChannelHandlerContext} for the channel being written to.
     * @return a chunk of data if one is available. {@code null} otherwise.
     */
    @Override
    public HttpContent readChunk(ChannelHandlerContext ctx) {
      long chunkDispenseStartTime = System.currentTimeMillis();
      logger.trace("Servicing request for next chunk on channel {}", ctx.channel());
      HttpContent content = null;
      Chunk chunk = chunksToWrite.poll();
      if (chunk != null) {
        chunk.onDequeue();
        ByteBuf buf = Unpooled.wrappedBuffer(chunk.buffer);
        chunk.writeCompleteThreshold = totalBytesWritten.addAndGet(chunk.bytesToBeWritten);
        chunksAwaitingCallback.add(chunk);
        content = new DefaultHttpContent(buf);
      } else if (allChunksWritten() && !sentLastChunk) {
        // Send last chunk for this input
        sentLastChunk = true;
        content = LastHttpContent.EMPTY_LAST_CONTENT;
        logger.trace("Last chunk was sent on channel {}", ctx.channel());
      }
      nettyMetrics.chunkDispenseTimeInMs.update(System.currentTimeMillis() - chunkDispenseStartTime);
      return content;
    }

    /**
     * Determines if input has ended by examining response state and number of chunks pending for write.
     * @return {@code true} if there are no more chunks to write. {@code false} otherwise.
     */
    private boolean allChunksWritten() {
      return responseCompleteCalled.get() && !writeFuture.isDone() && chunksToWriteCount.get() == 0;
    }
  }

  // callback classes

  /**
   * Invokes callback on any chunks that are eligible for callback.
   */
  private class CallbackInvoker implements GenericProgressiveFutureListener<ChannelProgressiveFuture> {

    /**
     * Uses {@code progress} to determine chunks whose callbacks need to be invoked.
     * @param future the {@link ChannelProgressiveFuture} that is being listened on.
     * @param progress the total number of bytes that have been written starting from the time writes were invoked via
     *                 {@link ChunkedWriteHandler}.
     * @param total the total number of bytes that need to be written i.e. the target number. This is not relevant to
     *              {@link ChunkedWriteHandler} because there is no target number. All calls to this function except
     *              the very last one will have a negative {@code total}.
     */
    @Override
    public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
      logger.trace("{} bytes of response written on channel {}", progress, ctx.channel());
      while (chunksAwaitingCallback.peek() != null && progress >= chunksAwaitingCallback
          .peek().writeCompleteThreshold) {
        chunksAwaitingCallback.poll().resolveChunk(null);
      }
    }

    /**
     * Called once the write is complete i.e. either all chunks that were needed to be written were written or there
     * was an error  writing the chunks.
     * @param future the {@link ChannelProgressiveFuture} that is being listened on.
     */
    @Override
    public void operationComplete(ChannelProgressiveFuture future) {
      if (future.isSuccess()) {
        logger.trace("Response sending complete on channel {}", ctx.channel());
        completeRequest(request == null || !request.isKeepAlive());
      } else {
        handleChannelWriteFailure(future.cause(), true);
      }
    }
  }

  /**
   * Callback for writes of response metadata.
   */
  private class ResponseMetadataWriteListener implements GenericFutureListener<ChannelFuture> {

    /**
     * If the operation completed successfully, a write via the {@link ChunkedWriteHandler} is initiated. Otherwise,
     * failure is handled.
     * @param future the {@link ChannelFuture} that is being listened on.
     */
    @Override
    public void operationComplete(ChannelFuture future) {
      if (future.isSuccess()) {
        logger.trace("Starting ChunkedWriteHandler on channel {}", ctx.channel());
        writeFuture.addListener(new CallbackInvoker());
        ctx.writeAndFlush(new ChunkDispenser(), writeFuture);
      } else {
        handleChannelWriteFailure(future.cause(), true);
      }
    }
  }

  /**
   * Listener that will be attached to the write of an error response.
   */
  private class ErrorResponseWriteListener implements GenericFutureListener<ChannelFuture> {
    private final long responseWriteStartTime = System.currentTimeMillis();
    private final boolean keepAlive;

    /**
     * Constructs a channel write listener for error responses.
     * @param keepAlive {@code true} if the channel needs to be kept open. {@code false} otherwise.
     */
    ErrorResponseWriteListener(boolean keepAlive) {
      this.keepAlive = keepAlive;
    }

    @Override
    public void operationComplete(ChannelFuture future)
        throws Exception {
      long channelWriteTime = System.currentTimeMillis() - responseWriteStartTime;
      if (!future.isSuccess()) {
        logger.error("Swallowing write exception encountered while sending error response to client on channel {}",
            ctx.channel(), future.cause());
        nettyMetrics.channelWriteError.inc();
      }
      nettyMetrics.channelWriteTimeInMs.update(channelWriteTime);
      if (request != null) {
        request.getMetricsTracker().nioMetricsTracker.addToResponseProcessingTime(channelWriteTime);
      }
      completeRequest(!keepAlive || !future.isSuccess());
    }
  }

  /**
   * Cleans up any chunks that were added after an error occurred. This is to guard against the case where
   * {@link CallbackInvoker#operationComplete(ChannelProgressiveFuture)} might have finished already.
   */
  private class CleanupCallback implements GenericFutureListener<ChannelFuture> {

    @Override
    public void operationComplete(ChannelFuture future)
        throws Exception {
      Throwable cause = future.cause();
      if (cause == null) {
        cause = new ClosedChannelException();
      }
      handleChannelWriteFailure(cause, false);
      logger.debug("Chunk cleanup complete on channel {}", ctx.channel());
    }
  }
}
