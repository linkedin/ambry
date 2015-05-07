package com.github.ambry.rest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  public void setContentType(String type)
      throws Exception {
    verifyResponseAlive();
    response.headers().set(HttpHeaders.Names.CONTENT_TYPE, type);
  }

  public void finalizeResponse()
      throws Exception {
    finalizeResponse(false);
  }

  public void finalizeResponseAndFlush()
      throws Exception {
    finalizeResponse(true);
  }

  private void finalizeResponse(boolean flush)
      throws Exception {
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

  public void addToBody(byte[] data, boolean isLast)
      throws Exception {
    addToBody(data, isLast, false);
  }

  public void addToBodyAndFlush(byte[] data, boolean isLast)
      throws Exception {
    addToBody(data, isLast, true);
  }

  private void addToBody(byte[] data, boolean isLast, boolean flush)
      throws Exception {
    verifyChannelOpen();
    /*
     TODO: When we return data via gets, we need to be careful not to modify data while ctx.write() is in flight.
     TODO: Working on getting a future implementation that can wait for the write to finish.
     TODO: Will do this with the getExipredBlob() or getDeletedBlob() API.
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

  public void flush()
      throws Exception {
    verifyChannelOpen();
    ctx.flush();
  }

  public void close()
      throws Exception {
    verifyChannelOpen();
    // no locking needed here (for channelClosed) since exactly one message handler thread has a
    // reference to this response handler.
    ChannelFuture future = ctx.close();
    future.addListener(ChannelFutureListener.CLOSE);
    channelClosed = true;
  }

  public void onError(Exception e) {
    // no locking needed here (for errorSent) since exactly one message handler thread has a
    // reference to this response handler.
    if (!errorSent) {
      errorSent = true;
      NettyMessageProcessor.onError(ctx, e, logger, nettyMetrics);
    }
  }

  public void onRequestComplete()
      throws Exception {
    //nothing to do for now
  }

  private void verifyResponseAlive()
      throws IllegalStateException {
    if (responseFinalized) {
      nettyMetrics.deadResponseAccess.inc();
      throw new IllegalStateException("Cannot re-finalize response");
    }
  }

  private void verifyChannelOpen()
      throws IllegalStateException {
    if (channelClosed || !(ctx.channel().isOpen())) {
      nettyMetrics.channelOperationAfterCloseErrorCount.inc();
      throw new IllegalStateException("Channel " + ctx.channel() + " has already been closed before write");
    }
  }
}
