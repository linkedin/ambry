package com.github.ambry.rest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.concurrent.GenericFutureListener;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link RestResponseChannel}. It is supported by an underlying Netty channel whose
 * handle this class has in the form of a {@link ChannelHandlerContext}.
 * <p/>
 * Used by implementations of {@link BlobStorageService} to return their response via Netty.
 * <p/>
 * The implementation is thread safe and data is sent in the order that threads call {@link #write(ByteBuffer)}. Any
 * semantic ordering that is required must be enforced by the callers.
 * <p/>
 * Although it is guaranteed that no writes will be accepted through this class once {@link #close()} is called, data
 * might or might not be written to the underlying channel even if this class accepted a write. This is because others
 * may have a handle on the underlying channel and can close it independently or the underlying channel can experience
 * an error in the future.
 * <p/>
 * If a write through this class fails at any time, the underlying channel will be closed immediately and no more writes
 * will be accepted.
 */
class NettyResponseChannel implements RestResponseChannel {
  private final ChannelHandlerContext ctx;
  private final NettyMetrics nettyMetrics;

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final HttpResponse responseMetadata = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
  // tracks whether onResponseComplete() has been called. Helps make it idempotent.
  private final AtomicBoolean responseComplete = new AtomicBoolean(false);
  // tracks whether responseMetadata has been written to the channel. Rejects changes to metadata once this is true.
  private final AtomicBoolean responseMetadataWritten = new AtomicBoolean(false);
  // tracks whether this response channel is open to operations. Even if this is false, the underlying network channel
  // may still be open.
  private final AtomicBoolean responseChannelOpen = new AtomicBoolean(true);
  // signifies that a flush() is required if the write buffer fills up.
  private final AtomicBoolean emptyingFlushRequired = new AtomicBoolean(true);
  private final ReentrantLock responseMetadataChangeLock = new ReentrantLock();
  private final ReentrantLock channelWriteLock = new ReentrantLock();

  private volatile ChannelFuture lastWriteFuture;
  private NettyRequest request = null;

  enum ChannelWriteType {
    /**
     * Checks if the underlying Netty channel is writable before writing data. Use this when writing data more than a
     * few bytes (to avoid OOM).
     */
    Safe,
    /**
     * Does not check if the underlying Netty channel is writable before writing data. Use this only if you are writing
     * very few bytes of data (content end markers) or if you know the channel's write buffer cannot be full (response
     * metadata).
     */
    Unsafe
  }

  public NettyResponseChannel(ChannelHandlerContext ctx, NettyMetrics nettyMetrics) {
    this.ctx = ctx;
    this.nettyMetrics = nettyMetrics;
    lastWriteFuture = ctx.newSucceededFuture();
    logger.trace("Instantiated NettyResponseChannel");
  }

  @Override
  public boolean isOpen() {
    return responseChannelOpen.get() && ctx.channel().isOpen();
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Writes some bytes, starting from {@code src.position()} possibly until {@code src.remaining()}, from {@code src} to
   * the channel. The number of bytes written depends on the current space remaining in the underlying channel's write
   * buffer.
   * <p/>
   * This function works <b><i>only</i></b> if {@code src} is backed by a byte array.
   * @param src the {@link ByteBuffer} containing the bytes that need to be written.
   * @return the number of bytes written to the channel.
   * @throws IllegalArgumentException if {@code src.hasArray()} is {@code false}.
   * @throws ClosedChannelException if the channel is not active.
   */
  @Override
  public int write(ByteBuffer src)
      throws ClosedChannelException {
    long writeProcessingStartTime = System.currentTimeMillis();
    // needed to avoid double counting.
    long responseMetadataWriteTime = 0;
    long channelWriteTime = 0;
    try {
      if (!src.hasArray()) {
        throw new IllegalArgumentException(
            "NettyResponseChannel does not work with ByteBuffers that are not backed by byte arrays");
      }

      if (!responseMetadataWritten.get()) {
        long responseMetadataWriteStartTime = System.currentTimeMillis();
        maybeWriteResponseMetadata();
        responseMetadataWriteTime = System.currentTimeMillis() - responseMetadataWriteStartTime;
      }
      verifyChannelActive();
      int bytesWritten = 0;
      if (ctx.channel().isWritable()) {
        emptyingFlushRequired.set(true);
        int bytesToWrite = Math.min(src.remaining(), ctx.channel().config().getWriteBufferLowWaterMark());
        ByteBuf buf =
            Unpooled.wrappedBuffer(src.array(), src.arrayOffset() + src.position(), bytesToWrite).order(src.order());
        logger.trace("Writing {} bytes to channel {}", bytesToWrite, ctx.channel());
        long channelWriteStartTime = System.currentTimeMillis();
        ChannelFuture writeFuture = writeToChannel(new DefaultHttpContent(buf), ChannelWriteType.Safe);
        channelWriteTime = System.currentTimeMillis() - channelWriteStartTime;
        if (!writeFuture.isDone() || writeFuture.isSuccess()) {
          bytesWritten = bytesToWrite;
          src.position(src.position() + bytesToWrite);
        }
      } else if (emptyingFlushRequired.compareAndSet(true, false)) {
        nettyMetrics.emptyingFlushCount.inc();
        flush();
      }
      nettyMetrics.bytesWriteRate.mark(bytesWritten);
      return bytesWritten;
    } finally {
      long writeProcessingTime =
          System.currentTimeMillis() - writeProcessingStartTime - responseMetadataWriteTime - channelWriteTime;
      nettyMetrics.writeProcessingTimeInMs.update(writeProcessingTime);
      if (request != null) {
        request.getMetrics().nioLayerMetrics.addToResponseProcessingTime(writeProcessingTime);
      }
    }
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
    closeResponseChannel();
    maybeCloseNetworkChannel(true);
  }

  @Override
  public void flush() {
    logger.trace("Flushing response data to channel {}", ctx.channel());
    // CAVEAT: It is possible that this flush might fail because the channel has been closed by an external thread with
    // a direct reference to the ChannelHandlerContext.
    ctx.flush();
  }

  @Override
  public void onResponseComplete(Throwable cause) {
    try {
      if (responseComplete.compareAndSet(false, true)) {
        logger.trace("Finished responding to current request on channel {}", ctx.channel());
        nettyMetrics.requestCompletionRate.mark();
        if (cause == null) {
          if (!responseMetadataWritten.get()) {
            maybeWriteResponseMetadata();
          }
          writeToChannel(new DefaultLastHttpContent(), ChannelWriteType.Unsafe);
        } else {
          sendErrorResponse(cause);
        }
        flush();
        closeResponseChannel();
        maybeCloseNetworkChannel(cause != null);
      }
    } catch (Exception e) {
      logger.error("Swallowing exception encountered during onResponseComplete tasks", e);
      nettyMetrics.responseCompleteTasksError.inc();
    }
  }

  @Override
  public void setStatus(ResponseStatus status)
      throws RestServiceException {
    responseMetadata.setStatus(getHttpResponseStatus(status));
    logger.trace("Set status to {} for response on channel {}", responseMetadata.getStatus(), ctx.channel());
  }

  @Override
  public void setContentType(String type)
      throws RestServiceException {
    setResponseHeader(HttpHeaders.Names.CONTENT_TYPE, type);
  }

  @Override
  public void setContentLength(long length)
      throws RestServiceException {
    setResponseHeader(HttpHeaders.Names.CONTENT_LENGTH, length);
  }

  @Override
  public void setLocation(String location)
      throws RestServiceException {
    setResponseHeader(HttpHeaders.Names.LOCATION, location);
  }

  @Override
  public void setLastModified(Date lastModified)
      throws RestServiceException {
    setResponseHeader(HttpHeaders.Names.LAST_MODIFIED, lastModified);
  }

  @Override
  public void setExpires(Date expireTime)
      throws RestServiceException {
    setResponseHeader(HttpHeaders.Names.EXPIRES, expireTime);
  }

  @Override
  public void setCacheControl(String cacheControl)
      throws RestServiceException {
    setResponseHeader(HttpHeaders.Names.CACHE_CONTROL, cacheControl);
  }

  @Override
  public void setPragma(String pragma)
      throws RestServiceException {
    setResponseHeader(HttpHeaders.Names.PRAGMA, pragma);
  }

  @Override
  public void setDate(Date date)
      throws RestServiceException {
    setResponseHeader(HttpHeaders.Names.DATE, date);
  }

  @Override
  public void setHeader(String headerName, Object headerValue)
      throws RestServiceException {
    setResponseHeader(headerName, headerValue);
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
      } else {
        throw new IllegalStateException(
            "Request has already been set inside NettyResponseChannel for channel {} " + ctx.channel());
      }
    } else {
      throw new IllegalArgumentException("RestRequest provided is null");
    }
  }

  /**
   * Writes response metadata to the channel if not already written previously and channel is active.
   * <p/>
   * Other than Netty write failures, this operation can fail for two reasons: -
   * 1. Response metadata has already been written - results in a {@link RestServiceException}.
   * 2. Channel is inactive - results in a {@link ClosedChannelException}.
   * In both cases, a failed {@link ChannelFuture} wrapping the exact exception is returned.
   * @return A {@link ChannelFuture} that tracks the write operation if sanity checks succeeded. Else, a failed
   * {@link ChannelFuture} wrapping the exact exception.
   */
  private ChannelFuture maybeWriteResponseMetadata() {
    long writeProcessingStartTime = System.currentTimeMillis();
    // needed to avoid double counting.
    Long channelWriteStartTime = null;
    responseMetadataChangeLock.lock();
    try {
      verifyResponseMetadataAlive();
      // we do some manipulation here for chunking. According to the HTTP spec, we can have either a Content-Length
      // or Transfer-Encoding:chunked, never both. So we check for Content-Length - if it is not there, we add
      // Transfer-Encoding:chunked. Note that sending HttpContent chunks data anyway - we are just explicitly specifying
      // this in the header.
      if (!HttpHeaders.isContentLengthSet(responseMetadata)) {
        // This makes sure that we don't stomp on any existing transfer-encoding.
        HttpHeaders.setTransferEncodingChunked(responseMetadata);
      }
      logger
          .trace("Sending response metadata with status {} on channel {}", responseMetadata.getStatus(), ctx.channel());
      responseMetadataWritten.set(true);
      channelWriteStartTime = System.currentTimeMillis();
      return writeToChannel(responseMetadata, ChannelWriteType.Unsafe);
    } catch (Exception e) {
      // specifically don't want this to throw Exceptions because the semantic "maybe" hints that it is possible that
      // the caller does not care whether this happens or not. If he does care, he will check the future returned.
      return ctx.newFailedFuture(e);
    } finally {
      responseMetadataChangeLock.unlock();
      long currentTime = System.currentTimeMillis();
      long channelWriteTime = 0;
      if (channelWriteStartTime != null) {
        channelWriteTime = currentTime - channelWriteStartTime;
      }
      long writeProcessingTime = currentTime - writeProcessingStartTime - channelWriteTime;
      nettyMetrics.responseMetadataProcessingTimeInMs.update(writeProcessingTime);
      if (request != null) {
        request.getMetrics().nioLayerMetrics.addToResponseProcessingTime(writeProcessingTime);
      }
    }
  }

  /**
   * Writes the provided {@link HttpObject} to the channel. This function is thread safe and writes occur in the order
   * that they were received (if there is a write in progress, others are blocked until the first write completes).
   * Any semantic ordering has to be enforced by the callers.
   * @param httpObject the {@link HttpObject} to be written.
   * @return A {@link ChannelFuture} that tracks the write operation.
   * @throws ClosedChannelException if the channel is not active.
   */
  private ChannelFuture writeToChannel(HttpObject httpObject, ChannelWriteType channelWriteType)
      throws ClosedChannelException {
    long channelWriteProcessingTime = System.currentTimeMillis();
    ChannelWriteResultListener writeResultListener = null;
    channelWriteLock.lock();
    try {
      verifyChannelActive();
      if (ChannelWriteType.Safe.equals(channelWriteType) && !ctx.channel().isWritable()) {
        nettyMetrics.channelWriteAbortCount.inc();
        logger.debug("writeToChannel discovered that the channel is not writable. Not an error but unexpected");
        emptyingFlushRequired.set(true);
        return ctx.newFailedFuture(new BufferOverflowException());
      }
      // CAVEAT: This write may or may not succeed depending on whether the channel is open at actual write time.
      // While this class makes sure that close happens only after all writes of this class are complete, any external
      // thread that has a direct reference to the ChannelHandlerContext can close the channel at any time and we
      // might not have got in our write when the channel was requested to be closed.
      ChannelPromise writePromise = ctx.newPromise();
      writeResultListener = new ChannelWriteResultListener(request, nettyMetrics);
      writePromise.addListener(writeResultListener);
      lastWriteFuture = ctx.write(httpObject, writePromise);
      return lastWriteFuture;
    } finally {
      channelWriteLock.unlock();
      long currentTime = System.currentTimeMillis();
      long channelWriteTime = 0;
      if (writeResultListener != null) {
        channelWriteTime = currentTime - writeResultListener.writeStartTime;
      }
      long writeProcessingTime = currentTime - channelWriteProcessingTime - channelWriteTime;
      nettyMetrics.channelWriteProcessingTimeInMs.update(writeProcessingTime);
      if (request != null) {
        request.getMetrics().nioLayerMetrics.addToResponseProcessingTime(writeProcessingTime);
      }
    }
  }

  /**
   * Sets the value of response headers after making sure that the response metadata is not already sent or is being
   * sent.
   * @param headerName The name of the header.
   * @param headerValue The intended value of the header.
   * @throws IllegalArgumentException if any of {@code headerName} or {@code headerValue} is null.
   * @throws RestServiceException if channel is closed or the response metadata is already sent or is being sent.
   */
  private void setResponseHeader(String headerName, Object headerValue)
      throws RestServiceException {
    if (headerName != null && headerValue != null) {
      long startTime = System.currentTimeMillis();
      responseMetadataChangeLock.lock();
      try {
        verifyResponseMetadataAlive();
        if (headerValue instanceof Date) {
          HttpHeaders.setDateHeader(responseMetadata, headerName, (Date) headerValue);
        } else {
          HttpHeaders.setHeader(responseMetadata, headerName, headerValue);
        }
        logger.trace("Header {} set to {} for channel {}", headerName, responseMetadata.headers().get(headerName),
            ctx.channel());
      } catch (RestServiceException e) {
        nettyMetrics.deadResponseAccessError.inc();
        throw e;
      } finally {
        responseMetadataChangeLock.unlock();
        nettyMetrics.headerSetTimeInMs.update(System.currentTimeMillis() - startTime);
      }
    } else {
      throw new IllegalArgumentException("Header name [" + headerName + "] or header value [" + headerValue + "] null");
    }
  }

  /**
   * Clears all the headers in the response.
   */
  private void clearHeaders() {
    try {
      responseMetadataChangeLock.lock();
      responseMetadata.headers().clear();
      logger.trace("Headers cleared for response in channel {}", ctx.channel());
    } finally {
      responseMetadataChangeLock.unlock();
    }
  }

  /**
   * Verify state of response metadata so that we do not try to modify response metadata after it has been written to
   * the channel.
   * <p/>
   * Simply checks for invalid state transitions. No atomicity guarantees. If the caller requires atomicity, it is
   * their responsibility to ensure it.
   * @throws RestServiceException if response metadata has already been sent.
   */
  private void verifyResponseMetadataAlive()
      throws RestServiceException {
    if (responseMetadataWritten.get() || !isOpen() || !(ctx.channel().isActive())) {
      throw new RestServiceException("No more changes to response metadata possible",
          RestServiceErrorCode.IllegalResponseMetadataStateTransition);
    }
  }

  /**
   * Verify that the channel is not closed and is active. There are no atomicity guarantees. If the caller requires
   * atomicity, it is their responsibility to ensure it.
   * @throws ClosedChannelException if the channel is not active.
   */
  private void verifyChannelActive()
      throws ClosedChannelException {
    if (!isOpen() || !(ctx.channel().isActive())) {
      throw new ClosedChannelException();
    }
  }

  /**
   * Builds and sends an error response to the client based on {@code cause}.
   * @param cause the cause of the request handling failure.
   */
  private void sendErrorResponse(Throwable cause) {
    long errorResponseProcessingStartTime = System.currentTimeMillis();
    // needed to avoid double counting.
    long channelWriteTime = 0;
    try {
      logger.trace("Sending error response to client on channel {}", ctx.channel());
      LastHttpContent errorMessageContent = prepareErrorResponse(cause);
      long responseMetadataWriteStartTime = System.currentTimeMillis();
      ChannelFuture errorResponseWrite = maybeWriteResponseMetadata();
      long responseMetadataWriteFinishTime = System.currentTimeMillis();
      channelWriteTime = responseMetadataWriteFinishTime - responseMetadataWriteStartTime;
      if (errorResponseWrite.isDone() && !errorResponseWrite.isSuccess()) {
        logger.error("Swallowing write exception encountered while sending error response to client on channel {}",
            ctx.channel(), errorResponseWrite.cause());
        nettyMetrics.errorResponseSendingError.inc();
      } else {
        writeToChannel(errorMessageContent, ChannelWriteType.Unsafe);
        channelWriteTime += (System.currentTimeMillis() - responseMetadataWriteFinishTime);
      }
    } catch (Exception e) {
      nettyMetrics.errorResponseSendingError.inc();
      logger.debug("Could not send error response", e);
    } finally {
      long errorResponseProcessingTime =
          System.currentTimeMillis() - errorResponseProcessingStartTime - channelWriteTime;
      nettyMetrics.errorResponseProcessingTimeInMs.update(errorResponseProcessingTime);
      if (request != null) {
        request.getMetrics().nioLayerMetrics.addToResponseProcessingTime(errorResponseProcessingTime);
      }
    }
  }

  /**
   * Provided a cause, returns an error response with the right status and error message.
   * @param cause the cause of the error.
   * @return a {@link LastHttpContent} with the error message that can be sent to the client.
   * @throws RestServiceException if there was any error while constructing the response.
   */
  private LastHttpContent prepareErrorResponse(Throwable cause)
      throws RestServiceException {
    ResponseStatus status;
    StringBuilder errReason = new StringBuilder();
    if (cause instanceof RestServiceException) {
      RestServiceErrorCode restServiceErrorCode = ((RestServiceException) cause).getErrorCode();
      status = ResponseStatus.getResponseStatus(restServiceErrorCode);
      if (status == ResponseStatus.BadRequest) {
        errReason.append(" [Reason - ").append(cause.getMessage()).append("]");
      }
    } else {
      nettyMetrics.internalServerErrorCount.inc();
      status = ResponseStatus.InternalServerError;
    }
    String fullMsg = "Failure: " + getHttpResponseStatus(status) + errReason;
    logger.trace("Constructed error response for the client - [{}]", fullMsg);
    clearHeaders();
    setStatus(status);
    setContentType("text/plain; charset=UTF-8");
    setContentLength(fullMsg.length());
    return new DefaultLastHttpContent(Unpooled.wrappedBuffer(fullMsg.getBytes()));
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
      case BadRequest:
        nettyMetrics.badRequestCount.inc();
        status = HttpResponseStatus.BAD_REQUEST;
        break;
      case NotFound:
        nettyMetrics.notFoundCount.inc();
        status = HttpResponseStatus.NOT_FOUND;
        break;
      case Gone:
        nettyMetrics.goneCount.inc();
        status = HttpResponseStatus.GONE;
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
   * Closes this NettyResponseChannel to further operations. The underlying network channel is not closed.
   */
  private void closeResponseChannel() {
    if (isOpen()) {
      channelWriteLock.lock();
      try {
        responseChannelOpen.set(false);
        logger.trace("NettyResponseChannel for network channel {} closed", ctx.channel());
      } finally {
        channelWriteLock.unlock();
      }
    }
  }

  /**
   * May close the underlying network channel depending on whether it has been forced or depending on the value of
   * keep-alive.
   * @param forceClose if {@code true}, closes channel despite keep-alive or any other concerns.
   * @return {@code true} if a close was initiated on the channel. Otherwise {@code false}.
   */
  private boolean maybeCloseNetworkChannel(boolean forceClose) {
    lastWriteFuture.addListener(ChannelFutureListener.CLOSE);
    logger.trace("Requested closing of channel {}", ctx.channel());
    return true;
  }
}

/**
 * Class that tracks a write and tracks metrics on completion of the write.
 * <p/>
 * Currently closes the connection on write failure.
 */
class ChannelWriteResultListener implements GenericFutureListener<ChannelFuture> {
  protected final long writeStartTime = System.currentTimeMillis();
  private final NettyRequest nettyRequest;
  private final NettyMetrics nettyMetrics;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public ChannelWriteResultListener(NettyRequest nettyRequest, NettyMetrics nettyMetrics) {
    this.nettyRequest = nettyRequest;
    this.nettyMetrics = nettyMetrics;
    logger.trace("ChannelWriteResultListener instantiated");
  }

  /**
   * Callback for when the operation represented by the {@code future} is done.
   * @param future the {@link ChannelFuture} whose operation finished.
   */
  @Override
  public void operationComplete(ChannelFuture future) {
    if (!future.isSuccess()) {
      future.channel().close();
      logger.error("Write on channel {} failed due to exception. Closed channel", future.channel(), future.cause());
      nettyMetrics.channelWriteError.inc();
    } else {
      if (nettyRequest != null) {
        nettyRequest.getMetrics().nioLayerMetrics
            .addToResponseProcessingTime(System.currentTimeMillis() - writeStartTime);
      } else {
        nettyMetrics.metricsTrackingError.inc();
        logger.warn("Request not set in response channel for {}", future.channel());
      }
    }
  }
}