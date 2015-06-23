package com.github.ambry.rest;

import com.github.ambry.restservice.RestRequestMetadata;
import com.github.ambry.restservice.RestResponseHandler;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
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
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


/**
 * Netty specific implementation of {@link RestResponseHandler}.
 * <p/>
 * Used by implementations of {@link com.github.ambry.restservice.BlobStorageService} to return their response via Netty
 * <p/>
 * The implementation is thread safe but provides no ordering guarantees. This means that data sent in might or might
 * not be written to the channel (in case other threads close the channel).
 */
class NettyResponseHandler implements RestResponseHandler {
  // TODO: Should upgrade this implementation to throw exceptions or provide callbacks to inform the caller of
  // TODO: success or failure of writes.

  private final ChannelHandlerContext ctx;
  private final HttpResponse responseMetadata;
  private final NettyMetrics nettyMetrics;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  // This guarantees that errors are never sent twice.
  private final AtomicBoolean errorSent = new AtomicBoolean(false);
  // This guarantees that duplicate response status/headers are never sent.
  private boolean responseMetadataWritten = false;
  // This can be used to do a sanity check on the channel. Mostly used to ensure the state transitions are correct i.e.
  // no thread calls addToResponseBody() after close() etc.
  private boolean channelClosed = false;

  // synchronization for changes to response metadata.
  private final ReentrantLock responseMetadataChangeLock = new ReentrantLock();
  // synchronization for writes to channel.
  private final ReentrantLock channelWriteLock = new ReentrantLock();
  // Maintains the future of the last write operation so that close can use it.
  private ChannelFuture lastWriteFuture;

  public NettyResponseHandler(ChannelHandlerContext ctx, NettyMetrics nettyMetrics) {
    this.ctx = ctx;
    this.nettyMetrics = nettyMetrics;
    this.responseMetadata = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    lastWriteFuture = ctx.newSucceededFuture();
  }

  @Override
  public void addToResponseBody(byte[] data, boolean isLast)
      throws RestServiceException {
    if (!responseMetadataWritten) {
      // Just made a sanity check to avoid locking + exceptions in maybeWriteResponseMetadata().
      maybeWriteResponseMetadata(responseMetadata);
    }
    // TODO: When we return data via gets, we need to be careful not to modify data while ctx.write() is in flight.
    // TODO: Working on getting a future implementation that can wait for the write to finish. Or at least indicate
    // TODO: completion of write. Will do this with the handleGet() API.
    ByteBuf buf = Unpooled.wrappedBuffer(data);
    HttpContent content;
    if (isLast) {
      content = new DefaultLastHttpContent(buf);
    } else {
      content = new DefaultHttpContent(buf);
    }
    writeToChannel(content);
  }

  @Override
  public void flush() {
    if (!responseMetadataWritten) {
      // Just made a sanity check to avoid locking + exceptions in maybeWriteResponseMetadata().
      maybeWriteResponseMetadata(responseMetadata);
    }
    // CAVEAT: It is possible that this flush might fail because the channel has been closed by an external thread with
    // a direct reference to the ChannelHandlerContext.
    ctx.flush();
  }

  @Override
  public void close() {
    if (!channelClosed && ctx.channel().isOpen()) {
      // made a quick sanity check to avoid locking.
      try {
        channelWriteLock.lockInterruptibly();
        channelClosed = true;
        // Waits for the last write operation performed by this object to succeed before closing.
        // This is NOT blocking.
        lastWriteFuture.addListener(ChannelFutureListener.CLOSE);
      } catch (InterruptedException e) {
        logger.error("Interrupted while trying to acquire a lock for channel close. Aborting.. ");
      } finally {
        if (channelWriteLock.isHeldByCurrentThread()) {
          channelWriteLock.unlock();
        }
      }
    }
  }

  @Override
  public void onError(RestRequestMetadata requestMetadata, Throwable cause) {
    if (errorSent.compareAndSet(false, true)) {
      ChannelFuture errorResponseWrite = maybeWriteResponseMetadata(generateErrorResponse(cause));
      if (errorResponseWrite.isDone() && !errorResponseWrite.isSuccess()) {
        // TODO: lookup what the general directives on how to handle the case where we realize there is problem in the
        // TODO: middle of sending a (huge) response and we have already sent 200 OK to the client.
        logger.error("Could not send error response to client. Reason for failure and original error follow: ",
            errorResponseWrite.cause(), cause);
        // close the connection anyway so that the client knows something went wrong.
      } else {
        flush();
      }
      close();
    }
  }

  @Override
  public void onRequestComplete(RestRequestMetadata restRequestMetadata) {
    //nothing to do for now
  }

  @Override
  public void setContentType(String type)
      throws RestServiceException {
    changeResponseHeader(HttpHeaders.Names.CONTENT_TYPE, type);
  }

  /**
   * Writes response metadata to the channel if not already written previously and channel is active.
   * <p/>
   * Other than Netty write failures, this operation can fail for three reasons: -
   * 1. Response metadata has already been written - results in a {@link RestServiceException}.
   * 2. Channel is inactive - results in a {@link RestServiceException}.
   * 3. Synchronize for response metadata write was interrupted- results in a {@link InterruptedException}.
   * In all three cases, a failed {@link ChannelFuture} wrapping the exact exception is returned.
   * @param responseMetadata - The response metadata to be written.
   * @return - A {@link ChannelFuture} that tracks the write operation if sanity checks succeeded. Else, a failed
   * {@link ChannelFuture} wrapping the exact exception.
   */
  private ChannelFuture maybeWriteResponseMetadata(HttpResponse responseMetadata) {
    try {
      responseMetadataChangeLock.lockInterruptibly();
      verifyResponseAlive();
      responseMetadataWritten = true;
      return writeToChannel(responseMetadata);
    } catch (Exception e) {
      // specifically don't want this to throw Exceptions because the semantic "maybe" hints that it is possible that
      // the caller does not care whether this happens or not. If he does care, he will check the future returned.
      return ctx.newFailedFuture(e);
    } finally {
      if (channelWriteLock.isHeldByCurrentThread()) {
        channelWriteLock.unlock();
      }
    }
  }

  /**
   * Writes the provided {@link HttpObject} to the channel.
   * </p>
   * This function is thread safe but offers no ordering guarantees. The write can fail if synchronization to write to
   * channel is interrupted.
   * @param httpObject - the {@link HttpObject} to be written.
   * @return - A {@link ChannelFuture} that tracks the write operation.
   * @throws RestServiceException - If the channel is not active.
   */
  private ChannelFuture writeToChannel(HttpObject httpObject)
      throws RestServiceException {
    try {
      channelWriteLock.lockInterruptibly();
      // CAVEAT: This is just a sanity check. There is no guarantee that the channel is open at write/flush time.
      // Netty generally puts all writes on the channel before close but there is no guarantee that we will get the
      // write in before some external thread closes the connection. This merely checks for bad state transitions
      // through this object.
      verifyChannelActive();
      // CAVEAT: This write may or may not succeed depending on whether the channel is open at actual write time.
      // While this object makes sure that close happens only after all writes of this object are complete, any external
      // thread that has a direct reference to the ChannelHandlerContext can close the channel at any time.
      // CAVEAT: This write is thread-safe but there are no ordering guarantees (there cannot be).
      lastWriteFuture = ctx.write(httpObject);
      return lastWriteFuture;
    } catch (InterruptedException e) {
      throw new RestServiceException("Channel write synchronization was interrupted", e,
          RestServiceErrorCode.OperationInterrupted);
    } finally {
      if (channelWriteLock.isHeldByCurrentThread()) {
        channelWriteLock.unlock();
      }
    }
  }

  /**
   * Changes the value of response headers after making sure that the response metadata is not already sent or is being
   * sent.
   * <p/>
   * The update can fail for two reasons: -
   * 1. Synchronization for response metadata write was interrupted - results in a {@link InterruptedException}. This is
   * wrapped in a {@link RestServiceException}.
   * 2. The response metadata was already sent or is being sent - results in a {@link RestServiceException} that is
   * thrown as is.
   * @param headerName - The name of the header.
   * @param headerValue - The intended value of the header.
   * @return - The updated headers.
   * @throws RestServiceException - if the response metadata is already sent or is being sent.
   */
  private HttpHeaders changeResponseHeader(String headerName, Object headerValue)
      throws RestServiceException {
    try {
      responseMetadataChangeLock.lockInterruptibly();
      verifyResponseAlive();
      return responseMetadata.headers().set(headerName, headerValue);
    } catch (InterruptedException e) {
      throw new RestServiceException("Response metadata change synchronization was interrupted", e,
          RestServiceErrorCode.OperationInterrupted);
    } finally {
      if (channelWriteLock.isHeldByCurrentThread()) {
        channelWriteLock.unlock();
      }
    }
  }

  /**
   * Verify state of responseMetadata so that we do not try to modify responseMetadata after it has been written to the
   * channel.
   * <p/>
   * Simply checks for invalid state transitions. No atomicity guarantees. If the caller requires atomicity, it is
   * their responsibility to ensure it.
   */
  private void verifyResponseAlive()
      throws RestServiceException {
    if (responseMetadataWritten) {
      nettyMetrics.deadResponseAccess.inc();
      throw new RestServiceException("Response metadata has already been written to channel",
          RestServiceErrorCode.IllegalResponseMetadataStateTransition);
    }
  }

  /**
   * Verify that the channel is still active.
   * <p/>
   * Simply checks for invalid state transitions. No atomicity guarantees. If the caller requires atomicity, it is
   * their responsibility to ensure it.
   */
  private void verifyChannelActive()
      throws RestServiceException {
    // TODO: once we support keepalive, we might want to verify if request is alive rather than channel.
    if (channelClosed || !(ctx.channel().isActive())) {
      nettyMetrics.channelOperationAfterCloseErrorCount.inc();
      throw new RestServiceException("Channel " + ctx.channel() + " has already been closed before write",
          RestServiceErrorCode.ChannelPreviouslyClosed);
    }
  }

  /**
   * Provided a cause, returns an error response with the right status and error message.
   * @param cause - the cause of the error.
   */
  private FullHttpResponse generateErrorResponse(Throwable cause) {
    nettyMetrics.errorStateCount.inc();
    HttpResponseStatus status;
    StringBuilder errReason = new StringBuilder();
    if (cause != null && cause instanceof RestServiceException) {
      status = getHttpResponseStatus(((RestServiceException) cause).getErrorCode());
      if (status == HttpResponseStatus.BAD_REQUEST) {
        errReason.append(" (Reason - ").append(cause.getMessage()).append(")");
      }
    } else {
      status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
      nettyMetrics.unknownExceptionCount.inc();
      logger.error("While building error response: Unknown cause received", cause);
      // TODO: handle special case of cause = null with metrics. That's a bad state transition.
    }

    String fullMsg = "Failure: " + status + errReason;
    FullHttpResponse response =
        new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(fullMsg, CharsetUtil.UTF_8));
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
    return response;
  }

  /**
   * Converts a {@link RestServiceErrorCode} into a {@link HttpResponseStatus}.
   * @param restServiceErrorCode
   * @return
   */
  private HttpResponseStatus getHttpResponseStatus(RestServiceErrorCode restServiceErrorCode) {
    switch (restServiceErrorCode) {
      case BadRequest:
      case InvalidArgs:
      case MalformedRequest:
      case MissingArgs:
      case NoRequest:
      case UnknownHttpObject:
      case UnsupportedHttpMethod:
        nettyMetrics.badRequestErrorCount.inc();
        return HttpResponseStatus.BAD_REQUEST;
      case ChannelActiveTasksFailure:
      case OperationInterrupted:
      case RequestHandlerSelectionError:
      case InternalServerError:
      case RequestHandleFailure:
      case RequestHandlerUnavailable:
      case RestRequestInfoQueueingFailure:
      case RestRequestInfoNull:
      case ResponseBuildingFailure:
      case ReponseHandlerNull:
      case RequestMetadataNull:
      case UnsupportedRestMethod:
        nettyMetrics.internalServerErrorCount.inc();
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
      default:
        nettyMetrics.unknownRestExceptionCount.inc();
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }
  }
}
