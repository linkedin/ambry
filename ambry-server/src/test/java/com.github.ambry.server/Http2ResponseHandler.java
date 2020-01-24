/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.github.ambry.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http2.HttpConversionUtil;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Process {@link io.netty.handler.codec.http.FullHttpResponse} translated from HTTP/2 frames
 */
@ChannelHandler.Sharable
public class Http2ResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

  protected final Logger logger = LoggerFactory.getLogger(getClass());
  private StreamResult streamResult;

  public Http2ResponseHandler() {

  }

  /**
   * Track the {@link ChannelFuture} and {@link ChannelPromise}
   *
   * @param writeFuture A future that represent the request write operation
   * @param promise The promise object that will be used to wait/notify events
   * @return The previous object associated with {@code streamId}
   * @see Http2ResponseHandler#awaitResponses(long, TimeUnit)
   */
  public void put(ChannelFuture writeFuture, ChannelPromise promise) {
    streamResult = new StreamResult(writeFuture, promise);
  }

  /**
   * Wait (sequentially) for a time duration for each anticipated response
   *
   * @param timeout Value of time to wait for each response
   * @param unit Units associated with {@code timeout}
   * @see Http2ResponseHandler#put(ChannelFuture, ChannelPromise)
   */
  public StreamResult awaitResponses(long timeout, TimeUnit unit) {
    ChannelFuture writeFuture = streamResult.getChannelFuture();
    if (!writeFuture.awaitUninterruptibly(timeout, unit)) {
      throw new IllegalStateException("Timed out waiting to write for stream id " + streamResult.getChannelFuture());
    }
    if (!writeFuture.isSuccess()) {
      throw new RuntimeException(writeFuture.cause());
    }
    ChannelPromise promise = streamResult.getChannelPromise();
    if (!promise.awaitUninterruptibly(timeout, unit)) {
      throw new IllegalStateException("Timed out waiting for response on stream id " + streamResult.getChannelFuture());
    }
    if (!promise.isSuccess()) {
      throw new RuntimeException(promise.cause());
    }
    return streamResult;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
    Integer streamId = msg.headers().getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
    if (streamId == null) {
      logger.error("Http2ResponseHandler unexpected message received: " + msg);
      return;
    }
    logger.trace("Stream response received.");
    streamResult.setByteBuf(msg.content().retainedDuplicate());
    streamResult.getChannelPromise().setSuccess();
  }

  class StreamResult {
    private final ChannelFuture channelFuture;
    private final ChannelPromise channelPromise;
    private ByteBuf byteBuf;

    StreamResult(ChannelFuture channelFuture, ChannelPromise channelPromise) {
      this.channelFuture = channelFuture;
      this.channelPromise = channelPromise;
    }

    public ChannelFuture getChannelFuture() {
      return channelFuture;
    }

    public ChannelPromise getChannelPromise() {
      return channelPromise;
    }

    public void setByteBuf(ByteBuf byteBuf) {
      this.byteBuf = byteBuf;
    }

    public ByteBuf getByteBuf() {
      return byteBuf;
    }
  }
}
