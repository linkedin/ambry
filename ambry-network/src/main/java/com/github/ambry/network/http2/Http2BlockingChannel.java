/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network.http2;

import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Send;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Promise;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A HTTP2 implementation of {@link ConnectedChannel} used in replication and test.
 * This implementation reuses connections maintained by {@link Http2MultiplexedChannelPool}(a pool for a single host:port) and creates a stream channel for each send.
 */
public class Http2BlockingChannel implements ConnectedChannel {
  private static final Logger logger = LoggerFactory.getLogger(Http2BlockingChannel.class);
  private final static AttributeKey<Promise<ByteBuf>> RESPONSE_PROMISE = AttributeKey.newInstance("ResponsePromise");
  private Promise<ByteBuf> responsePromise;
  private Http2MultiplexedChannelPool http2MultiplexedChannelPool;

  public Http2BlockingChannel(Http2MultiplexedChannelPool http2MultiplexedChannelPool) {
    this.http2MultiplexedChannelPool = http2MultiplexedChannelPool;
  }

  /**
   * Constructor for test purpose.
   */
  public Http2BlockingChannel(String hostName, int port, SSLConfig sslConfig, Http2ClientConfig http2ClientConfig,
      Http2ClientMetrics http2ClientMetrics) {
    NettySslHttp2Factory nettySslHttp2Factory;
    try {
      nettySslHttp2Factory = new NettySslHttp2Factory(sslConfig);
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException("Can't create NettySslHttp2Factory: ", e);
    }
    this.http2MultiplexedChannelPool =
        new Http2MultiplexedChannelPool(new InetSocketAddress(hostName, port), nettySslHttp2Factory,
            Epoll.isAvailable() ? new EpollEventLoopGroup() : new NioEventLoopGroup(), http2ClientConfig,
            http2ClientMetrics);
  }

  @Override
  public void connect() throws IOException {

  }

  @Override
  public void disconnect() throws IOException {

  }

  /**
   * A new stream channel is created for each send.
   */
  @Override
  public void send(Send request) throws IOException {
    Channel streamChannel;
    try {
      streamChannel = http2MultiplexedChannelPool.acquire().get(500, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException("Can't acquire stream channel: ", e);
    }

    ChannelPipeline p = streamChannel.pipeline();
    p.addLast(new Http2StreamFrameToHttpObjectCodec(false));
    p.addLast(new HttpObjectAggregator(http2MultiplexedChannelPool.getMaxContentLength()));
    p.addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
      @Override
      protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
        ctx.channel().attr(RESPONSE_PROMISE).getAndSet(null).setSuccess(msg.content().retainedDuplicate());
        // Stream channel can't be reused. Release it here.
        ctx.channel()
            .parent()
            .attr(Http2MultiplexedChannelPool.HTTP2_MULTIPLEXED_CHANNEL_POOL)
            .get()
            .release(ctx.channel());
      }
    });
    streamChannel.pipeline().addLast(new AmbrySendToHttp2Adaptor());

    responsePromise = streamChannel.eventLoop().newPromise();
    streamChannel.attr(RESPONSE_PROMISE).set(responsePromise);
    streamChannel.writeAndFlush(request).awaitUninterruptibly(3, TimeUnit.SECONDS);
  }

  @Override
  public ChannelOutput receive() throws IOException {
    ByteBuf responseByteBuf;
    try {
      responseByteBuf = responsePromise.get(3, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException("No response received in 3 seconds.");
    }
    DataInputStream dataInputStream = new NettyByteBufDataInputStream(responseByteBuf);
    return new ChannelOutput(dataInputStream, dataInputStream.readLong());
  }

  @Override
  public String getRemoteHost() {
    return http2MultiplexedChannelPool.getInetSocketAddress().getHostName();
  }

  @Override
  public int getRemotePort() {
    return http2MultiplexedChannelPool.getInetSocketAddress().getPort();
  }
}
