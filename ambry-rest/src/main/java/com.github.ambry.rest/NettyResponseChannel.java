package com.github.ambry.rest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GenericFutureListener;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


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
  private final ChannelWriteResultListener channelWriteResultListener;
  private final HttpResponse responseMetadata = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean requestComplete = new AtomicBoolean(false);
  private final AtomicBoolean responseMetadataWritten = new AtomicBoolean(false);
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final AtomicBoolean emptyingFlushRequired = new AtomicBoolean(true);
  private final ReentrantLock responseMetadataChangeLock = new ReentrantLock();
  private final ReentrantLock channelWriteLock = new ReentrantLock();
  private ChannelFuture lastWriteFuture;

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
    channelWriteResultListener = new ChannelWriteResultListener(nettyMetrics);
    lastWriteFuture = ctx.newSucceededFuture();
    logger.trace("Instantiated NettyResponseChannel");
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get() && ctx.channel().isOpen();
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
    if (!src.hasArray()) {
      throw new IllegalArgumentException(
          "NettyResponseChannel does not work with ByteBuffers that are not backed by byte arrays");
    }

    if (!responseMetadataWritten.get()) {
      maybeWriteResponseMetadata(responseMetadata);
    }
    verifyChannelActive();
    int bytesWritten = 0;
    if (ctx.channel().isWritable()) {
      emptyingFlushRequired.set(true);
      int bytesToWrite = Math.min(src.remaining(), ctx.channel().config().getWriteBufferLowWaterMark());
      logger.trace("Adding {} bytes of data to response on channel {}", bytesToWrite, ctx.channel());
      ByteBuf buf =
          Unpooled.wrappedBuffer(src.array(), src.arrayOffset() + src.position(), bytesToWrite).order(src.order());
      ChannelFuture writeFuture = writeToChannel(new DefaultHttpContent(buf), ChannelWriteType.Safe);
      if (!writeFuture.isDone() || writeFuture.isSuccess()) {
        bytesWritten = bytesToWrite;
        src.position(src.position() + bytesToWrite);
      }
    } else if (emptyingFlushRequired.compareAndSet(true, false)) {
      flush();
    }
    return bytesWritten;
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Marks the channel as closed. No further communication will be possible. Any pending writes (that are not already
   * flushed) might be discarded.
   * <p/>
   * The underlying channel might not be closed immediately but no more writes will be accepted and any calls to
   * {@link #isOpen()} after a call to this function will return {@code false}.
   */
  @Override
  public void close() {
    if (isOpen()) {
      try {
        channelWriteLock.lock();
        channelOpen.set(false);
        // Waits for the last write operation performed by this class to succeed before closing.
        // This is NOT blocking.
        lastWriteFuture.addListener(ChannelFutureListener.CLOSE);
        logger.trace("Requested closing of channel {}", ctx.channel());
      } finally {
        channelWriteLock.unlock();
      }
    }
  }

  @Override
  public void flush() {
    logger.trace("Flushing response data to channel {}", ctx.channel());
    // CAVEAT: It is possible that this flush might fail because the channel has been closed by an external thread with
    // a direct reference to the ChannelHandlerContext.
    ctx.flush();
  }

  @Override
  public void onRequestComplete(Throwable cause, boolean forceClose) {
    try {
      if (requestComplete.compareAndSet(false, true)) {
        logger.trace("Finished responding to current request on channel {}", ctx.channel());
        nettyMetrics.requestCompletionRate.mark();
        if (cause == null) {
          if (!responseMetadataWritten.get()) {
            maybeWriteResponseMetadata(responseMetadata);
          }
          writeToChannel(new DefaultLastHttpContent(), ChannelWriteType.Unsafe);
        } else {
          nettyMetrics.requestHandlingError.inc();
          logger.trace("Sending error response to client on channel {}", ctx.channel());
          ChannelFuture errorResponseWrite = maybeWriteResponseMetadata(generateErrorResponse(cause));
          if (errorResponseWrite.isDone() && !errorResponseWrite.isSuccess()) {
            logger.error("Swallowing write exception encountered while sending error response to client on channel {}",
                ctx.channel(), errorResponseWrite.cause());
            nettyMetrics.responseSendingError.inc();
            // close the connection anyway so that the client knows something went wrong.
          }
        }
        flush();
        close();
      }
    } catch (Exception e) {
      logger.error("Swallowing exception encountered during onRequestComplete tasks", e);
      nettyMetrics.responseChannelRequestCompleteTasksError.inc();
    }
  }

  @Override
  public boolean isRequestComplete() {
    return requestComplete.get();
  }

  @Override
  public void setContentType(String type)
      throws RestServiceException {
    HttpHeaders headers = setResponseHeader(HttpHeaders.Names.CONTENT_TYPE, type);
    if (!type.equals(headers.get(HttpHeaders.Names.CONTENT_TYPE))) {
      nettyMetrics.responseMetadataBuildingFailure.inc();
      throw new RestServiceException("Unable to set content-type to " + type,
          RestServiceErrorCode.ResponseMetadataBuildingFailure);
    }
    logger.trace("Set content type to {} for response on channel {}", type, ctx.channel());
  }

  /**
   * Writes response metadata to the channel if not already written previously and channel is active.
   * <p/>
   * Other than Netty write failures, this operation can fail for two reasons: -
   * 1. Response metadata has already been written - results in a {@link RestServiceException}.
   * 2. Channel is inactive - results in a {@link ClosedChannelException}.
   * In both cases, a failed {@link ChannelFuture} wrapping the exact exception is returned.
   * @param responseMetadata The response metadata to be written.
   * @return A {@link ChannelFuture} that tracks the write operation if sanity checks succeeded. Else, a failed
   * {@link ChannelFuture} wrapping the exact exception.
   */
  private ChannelFuture maybeWriteResponseMetadata(HttpResponse responseMetadata) {
    try {
      responseMetadataChangeLock.lock();
      verifyResponseAlive();
      logger
          .trace("Sending response metadata with status {} on channel {}", responseMetadata.getStatus(), ctx.channel());
      responseMetadataWritten.set(true);
      return writeToChannel(responseMetadata, ChannelWriteType.Unsafe);
    } catch (Exception e) {
      // specifically don't want this to throw Exceptions because the semantic "maybe" hints that it is possible that
      // the caller does not care whether this happens or not. If he does care, he will check the future returned.
      return ctx.newFailedFuture(e);
    } finally {
      responseMetadataChangeLock.unlock();
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
    try {
      channelWriteLock.lock();
      verifyChannelActive();
      if (ChannelWriteType.Safe.equals(channelWriteType) && !ctx.channel().isWritable()) {
        emptyingFlushRequired.set(true);
        return ctx.newFailedFuture(new BufferOverflowException());
      }
      // CAVEAT: This write may or may not succeed depending on whether the channel is open at actual write time.
      // While this class makes sure that close happens only after all writes of this class are complete, any external
      // thread that has a direct reference to the ChannelHandlerContext can close the channel at any time and we
      // might not have got in our write when the channel was requested to be closed.
      logger.trace("Writing to channel {}", ctx.channel());
      lastWriteFuture = channelWriteResultListener.trackWrite(ctx.write(httpObject));
      return lastWriteFuture;
    } finally {
      channelWriteLock.unlock();
    }
  }

  /**
   * Sets the value of response headers after making sure that the response metadata is not already sent or is being
   * sent.
   * @param headerName The name of the header.
   * @param headerValue The intended value of the header.
   * @return The updated headers.
   * @throws RestServiceException if the response metadata is already sent or is being sent.
   */
  private HttpHeaders setResponseHeader(String headerName, Object headerValue)
      throws RestServiceException {
    try {
      responseMetadataChangeLock.lock();
      verifyResponseAlive();
      logger.trace("Changing header {} to {} for channel {}", headerName, headerValue, ctx.channel());
      return responseMetadata.headers().set(headerName, headerValue);
    } catch (RestServiceException e) {
      nettyMetrics.deadResponseAccessError.inc();
      throw e;
    } finally {
      responseMetadataChangeLock.unlock();
    }
  }

  /**
   * Verify state of responseMetadata so that we do not try to modify responseMetadata after it has been written to the
   * channel.
   * <p/>
   * Simply checks for invalid state transitions. No atomicity guarantees. If the caller requires atomicity, it is
   * their responsibility to ensure it.
   * @throws RestServiceException if response metadata has already been sent.
   */
  private void verifyResponseAlive()
      throws RestServiceException {
    if (responseMetadataWritten.get()) {
      throw new RestServiceException("Response metadata has already been written to channel. No more changes possible",
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
   * Provided a cause, returns an error response with the right status and error message.
   * @param cause the cause of the error.
   * @return a {@link HttpResponse} that includes the right status and message.
   */
  private HttpResponse generateErrorResponse(Throwable cause) {
    HttpResponseStatus status;
    StringBuilder errReason = new StringBuilder();
    if (cause != null && cause instanceof RestServiceException) {
      status = getHttpResponseStatus(((RestServiceException) cause).getErrorCode());
      if (status == HttpResponseStatus.BAD_REQUEST) {
        errReason.append(" (Reason - ").append(cause.getMessage()).append(")");
      }
    } else {
      status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
      nettyMetrics.unknownExceptionError.inc();
    }
    String fullMsg = "Failure: " + status + errReason;
    logger.trace("Constructed error response for the client - [{}]", fullMsg);
    FullHttpResponse response =
        new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(fullMsg, CharsetUtil.UTF_8));
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
    return response;
  }

  /**
   * Converts a {@link RestServiceErrorCode} into a {@link HttpResponseStatus}.
   * @param restServiceErrorCode {@link RestServiceErrorCode} that needs to be mapped to a {@link HttpResponseStatus}.
   * @return the {@link HttpResponseStatus} that maps to the {@link RestServiceErrorCode}.
   */
  private HttpResponseStatus getHttpResponseStatus(RestServiceErrorCode restServiceErrorCode) {
    RestServiceErrorCode errorCodeGroup = RestServiceErrorCode.getErrorCodeGroup(restServiceErrorCode);
    switch (errorCodeGroup) {
      case BadRequest:
        nettyMetrics.badRequestError.inc();
        return HttpResponseStatus.BAD_REQUEST;
      case InternalServerError:
        nettyMetrics.internalServerError.inc();
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
      default:
        nettyMetrics.unknownRestServiceExceptionError.inc();
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }
  }
}

/**
 * Class that tracks multiple writes and takes actions on completion of those writes.
 * <p/>
 * Currently closes the connection on write failure.
 */
class ChannelWriteResultListener implements GenericFutureListener<ChannelFuture> {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ConcurrentHashMap<ChannelFuture, Long> writeFutures = new ConcurrentHashMap<ChannelFuture, Long>();
  private final NettyMetrics nettyMetrics;

  public ChannelWriteResultListener(NettyMetrics nettyMetrics) {
    this.nettyMetrics = nettyMetrics;
    logger.trace("ChannelWriteResultListener instantiated");
  }

  /**
   * Adds the received write future to the list of futures being tracked and requests a callback after the future
   * finishes.
   * @param writeFuture the write {@link ChannelFuture} that needs to be tracked.
   * @return the write {@link ChannelFuture} that was submitted to be tracked.
   */
  public ChannelFuture trackWrite(ChannelFuture writeFuture) {
    Long writeStartTime = System.currentTimeMillis();
    Long prevStartTime = writeFutures.putIfAbsent(writeFuture, writeStartTime);
    if (prevStartTime == null) {
      writeFuture.addListener(this);
    } else {
      logger.warn("Discarding duplicate write tracking request for ChannelFuture. Prev write time {}. Current time {}",
          prevStartTime, writeStartTime);
      nettyMetrics.channelWriteFutureAlreadyExistsError.inc();
    }
    return writeFuture;
  }

  /**
   * Callback for when the operation represented by the {@link ChannelFuture} is done.
   * @param future the {@link ChannelFuture} whose operation finished.
   */
  @Override
  public void operationComplete(ChannelFuture future) {
    Long writeStartTime = writeFutures.remove(future);
    if (writeStartTime != null) {
      if (!future.isSuccess()) {
        future.channel().close();
        logger.error("Write on channel {} failed due to exception. Closed channel", future.channel(), future.cause());
        nettyMetrics.channelWriteError.inc();
      } else {
        // TODO: track small, medium, large and huge writes.
        nettyMetrics.channelWriteLatencyInMs.update(System.currentTimeMillis() - writeStartTime);
      }
    } else {
      logger.warn("Received operationComplete callback for ChannelFuture not found in tracking map for channel {}",
          future.channel());
      nettyMetrics.channelWriteFutureNotFoundError.inc();
    }
  }
}
