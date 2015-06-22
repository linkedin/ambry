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
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import java.util.concurrent.atomic.AtomicBoolean;
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
  //
  // TODO: Find out if netty channel close waits for data written to be flushed.
  //
  // TODO: Review the thread safety of this class.

  private final ChannelHandlerContext ctx;
  private final HttpResponse responseMetadata;
  private final NettyMetrics nettyMetrics;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  // This can be used to do a sanity check on the channel. Mostly used to ensure the state transitions are correct i.e.
  // no thread calls addToResponseBody() after close() etc.
  private final AtomicBoolean channelClosed = new AtomicBoolean(false);
  // This guarantees that errors are never sent twice.
  private final AtomicBoolean errorSent = new AtomicBoolean(false);
  // This guarantees that duplicate response status/headers are never sent.
  private final AtomicBoolean responseMetadataWritten = new AtomicBoolean(false);

  public NettyResponseHandler(ChannelHandlerContext ctx, NettyMetrics nettyMetrics) {
    this.ctx = ctx;
    this.nettyMetrics = nettyMetrics;
    this.responseMetadata = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
  }

  @Override
  public void addToResponseBody(byte[] data, boolean isLast)
      throws RestServiceException {
    // This is just a sanity check. There is no guarantee that the channel is open at write time.
    verifyChannelActive();

    if (responseMetadataWritten.compareAndSet(false, true)) {
      writeResponseMetadata();
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
    // This write may or may not succeed depending on whether the channel is open. This is thread-safe but there
    // are no ordering guarantees.
    ctx.write(content);
  }

  @Override
  public void flush()
      throws RestServiceException {
    // This is just a sanity check. There is no guarantee that the channel is open at flush time.
    verifyChannelActive();
    if (responseMetadataWritten.compareAndSet(false, true)) {
      writeResponseMetadata();
    }
    // This flush may or may not succeed depending on whether the channel is open. This is thread-safe but there
    // are no ordering guarantees.
    ctx.flush();
  }

  @Override
  public void close() {
    close(ctx.newSucceededFuture());
  }

  @Override
  public void onError(RestRequestMetadata requestMetadata, Throwable cause) {
    if (errorSent.compareAndSet(false, true)) {
      if (!responseMetadataWritten.getAndSet(true)) {
        FullHttpResponse errorResponse = getErrorResponse(cause);
        try {
          verifyChannelActive();
          // no guarantee that the channel is not already closed at write time. Thread safe, but no ordering guarantees.
          close(ctx.writeAndFlush(errorResponse));
        } catch (RestServiceException e) {
          logger.error("Channel has been closed before error response could be sent to client. Original error follows",
              cause);
        }
      } else {
        logger.error("Discovered that response metadata had already been sent to the client when attempting to send " +
            " an error. This indicates a bad state transition. The request metadata is " + requestMetadata +
            ". The original cause" + " of error follows: ", cause);
      }
    }
  }

  @Override
  public void onRequestComplete(RestRequestMetadata restRequestMetadata) {
    //nothing to do for now
  }

  @Override
  public void setContentType(String type)
      throws RestServiceException {
    verifyResponseAlive();
    responseMetadata.headers().set(HttpHeaders.Names.CONTENT_TYPE, type);
  }

  /**
   * Writes the response data to the channel.
   */
  private void writeResponseMetadata() {
    ctx.write(responseMetadata);
  }

  /**
   * Closes the channel once the operation represented by the future succeeds.
   * @param future
   */
  private void close(ChannelFuture future) {
    if (channelClosed.compareAndSet(false, true) && ctx.channel().isOpen()) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Verify state of responseMetadata so that we do not try to modify responseMetadata after it has been written to the
   * channel.
   * <p/>
   * Simply checks for invalid state transitions. No atomicity guarantees.
   */
  private void verifyResponseAlive()
      throws RestServiceException {
    if (responseMetadataWritten.get()) {
      nettyMetrics.deadResponseAccess.inc();
      throw new RestServiceException("Response metadata has already been written to channel",
          RestServiceErrorCode.IllegalResponseMetadataStateTransition);
    }
  }

  /**
   * Verify that channel is still open.
   * <p/>
   * Simply checks for invalid state transitions. No atomicity guarantees.
   */
  private void verifyChannelActive()
      throws RestServiceException {
    // TODO: once we support keepalive, we might want to verify if request is alive rather than channel
    if (channelClosed.get() || !(ctx.channel().isActive())) {
      nettyMetrics.channelOperationAfterCloseErrorCount.inc();
      throw new IllegalStateException("Channel " + ctx.channel() + " has already been closed before write");
    }
  }

  /**
   * Provided a cause, returns an error response with the right status and error message.
   * @param cause - the cause of the error.
   */
  private FullHttpResponse getErrorResponse(Throwable cause) {
    nettyMetrics.errorStateCount.inc();
    HttpResponseStatus status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
    StringBuilder errReason = new StringBuilder();
    if (cause instanceof RestServiceException) {
      status = getHttpResponseStatus(((RestServiceException) cause).getErrorCode());
      if (status == HttpResponseStatus.BAD_REQUEST) {
        errReason.append(" (Reason - ").append(cause.getCause()).append(")");
      }
    } else {
      nettyMetrics.unknownExceptionCount.inc();
      logger.error("While building error response: Unknown cause received", cause);
    }

    String fullMsg = "Failure: " + status + errReason;
    FullHttpResponse response =
        new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(fullMsg, CharsetUtil.UTF_8));
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
    return response;
  }

  /**
   * Converts a RestServiceErrorCode into a HTTP response status.
   * @param restServiceErrorCode
   * @return
   */
  private HttpResponseStatus getHttpResponseStatus(RestServiceErrorCode restServiceErrorCode) {
    switch (restServiceErrorCode) {
      case BadRequest:
      case DuplicateRequest:
      case InvalidArgs:
      case MalformedRequest:
      case MissingArgs:
      case NoRequest:
      case UnsupportedHttpMethod:
        nettyMetrics.badRequestErrorCount.inc();
        return HttpResponseStatus.BAD_REQUEST;
      case ChannelActiveTasksFailure:
      case RequestHandlerSelectionError:
      case InternalServerError:
      case RequestHandleFailure:
      case RequestHandlerUnavailable:
      case RestRequestInfoQueueingFailure:
      case RestRequestInfoNull:
      case ResponseBuildingFailure:
      case ReponseHandlerNull:
      case RequestMetadataNull:
      case UnknownHttpObject:
      case UnsupportedRestMethod:
        nettyMetrics.internalServerErrorCount.inc();
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
      default:
        nettyMetrics.unknownRestExceptionCount.inc();
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }
  }
}
