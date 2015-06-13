package com.github.ambry.rest;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;


/**
 * Netty specific implementation of RestResponseHandler. Used by ambry to return its response via Http
 */
public class NettyResponseHandler implements RestResponseHandler {
  private final ChannelHandlerContext ctx;
  private final HttpResponse response;
  private final NettyMetrics nettyMetrics;

  private boolean channelClosed = false;
  private boolean errorSent = false;
  private boolean responseFinalized = false;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public NettyResponseHandler(ChannelHandlerContext ctx, NettyMetrics nettyMetrics) {
    this.ctx = ctx;
    this.nettyMetrics = nettyMetrics;
    this.response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
  }

  // header helpers
  public void setContentType(String type) {
    verifyResponseAlive();
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, type);
  }

  public void finalizeResponse() {
    finalizeResponse(false);
  }

  public void finalizeResponseAndFlush() {
    finalizeResponse(true);
  }

  private void finalizeResponse(boolean flush) {
    // no locking needed (for responseFinalized) here since exactly one message handler thread has a
    // reference to this response handler.
    verifyChannelOpen();
    verifyResponseAlive();
    // This ugly if else might change once I have a better understanding of the ChannelFuture offered by write
    if (flush) {
      ctx.writeAndFlush(response);
    } else {
      ctx.write(response);
    }
    responseFinalized = true;
  }

  public void addToBody(byte[] data, boolean isLast) {
    addToBody(data, isLast, false);
  }

  public void addToBodyAndFlush(byte[] data, boolean isLast) {
    addToBody(data, isLast, true);
  }

  private void addToBody(byte[] data, boolean isLast, boolean flush) {
    verifyChannelOpen();
    /*
     TODO: When we return data via gets, we need to be careful not to modify data while ctx.write() is in flight.
     TODO: Working on getting a future implementation that can wait for the write to finish.
     TODO: Will do this with the handleGet() API.
     */
    ByteBuf buf = Unpooled.wrappedBuffer(data);
    HttpContent content;
    if (isLast) {
      content = new DefaultLastHttpContent(buf);
    } else {
      content = new DefaultHttpContent(buf);
    }
    // This ugly if else might change once I have a better understanding of the ChannelFuture offered by write
    if (flush) {
      ctx.writeAndFlush(content);
    } else {
      ctx.write(content);
    }
  }

  public void flush() {
    verifyChannelOpen();
    ctx.flush();
  }

  public void close() {
    ChannelFuture future = ctx.close();
    close(future);
  }

  private synchronized void close(ChannelFuture future) {
    verifyChannelOpen();
    future.addListener(ChannelFutureListener.CLOSE);
    channelClosed = true;
  }

  public synchronized void onError(Throwable cause) {
    if (!errorSent) {
      errorSent = true;
      buildAndSendError(cause);
    }
  }

  public void onRequestComplete()
      throws Exception {
    //nothing to do for now
  }

  private void buildAndSendError(Throwable cause) {
    nettyMetrics.errorStateCount.inc();
    HttpResponseStatus status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
    String msg = "";
    if (cause instanceof RestServiceException) {
      status = getHttpEquivalentErrorCode(((RestServiceException) cause).getErrorCode());
      if (status == HttpResponseStatus.BAD_REQUEST) {
        msg = cause.getMessage();
      }
    } else {
      nettyMetrics.unknownExceptionCount.inc();
      logger.error("Unknown exception received while processing error response - " + cause.getCause() + " - " + cause
          .getMessage());
    }

    if (ctx.channel().isActive()) {
      sendError(status, msg);
    }
  }

  private HttpResponseStatus getHttpEquivalentErrorCode(RestServiceErrorCode restServiceErrorCode) {
    switch (restServiceErrorCode) {
      case BadExecutionData:
      case BadRequest:
      case DuplicateRequest:
      case NoRequest:
      case UnknownCustomOperationType:
      case UnknownRestMethod:
        nettyMetrics.badRequestErrorCount.inc();
        return HttpResponseStatus.BAD_REQUEST;
      case ChannelActiveTasksFailure:
      case HandlerSelectionError:
      case HttpObjectConversionFailure:
      case InternalServerError:
      case MessageHandleFailure:
      case MessageQueueingFailure:
      case RequestProcessingFailure:
      case ResponseBuildingFailure:
      case ReponseHandlerMissing:
      case RestObjectMissing:
      case RestRequestMissing:
        nettyMetrics.internalServerErrorCount.inc();
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
      default:
        nettyMetrics.unknownRestExceptionCount.inc();
        return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    }
  }

  private void sendError(HttpResponseStatus status, String msg) {
    String fullMsg = "Failure: " + status;
    if (msg != null && !msg.isEmpty()) {
      fullMsg += ". Reason - " + msg;
    }
    fullMsg += "\r\n";

    FullHttpResponse response =
        new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.copiedBuffer(fullMsg, CharsetUtil.UTF_8));
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

    ChannelFuture future = ctx.writeAndFlush(response);
    close(future);
  }

  private void verifyResponseAlive() {
    if (responseFinalized) {
      nettyMetrics.deadResponseAccess.inc();
      throw new IllegalStateException("Cannot re-finalize response");
    }
  }

  private void verifyChannelOpen() {
    if (channelClosed || !(ctx.channel().isActive())) {
      nettyMetrics.channelOperationAfterCloseErrorCount.inc();
      throw new IllegalStateException("Channel " + ctx.channel() + " has already been closed before write");
    }
  }
}
