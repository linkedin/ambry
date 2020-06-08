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
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
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
  private Promise<ByteBuf> responsePromise;
  private final Http2MultiplexedChannelPool http2MultiplexedChannelPool;
  private final Http2ClientConfig http2ClientConfig;
  final static AttributeKey<Promise<ByteBuf>> RESPONSE_PROMISE = AttributeKey.newInstance("ResponsePromise");

  public Http2BlockingChannel(Http2MultiplexedChannelPool http2MultiplexedChannelPool) {
    this.http2MultiplexedChannelPool = http2MultiplexedChannelPool;
    this.http2ClientConfig = http2MultiplexedChannelPool.getHttp2ClientConfig();
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
    this.http2ClientConfig = http2ClientConfig;
    this.http2MultiplexedChannelPool =
        new Http2MultiplexedChannelPool(new InetSocketAddress(hostName, port), nettySslHttp2Factory,
            Epoll.isAvailable() ? new EpollEventLoopGroup() : new NioEventLoopGroup(), http2ClientConfig,
            http2ClientMetrics,
            new Http2BlockingChannelStreamChannelInitializer(http2ClientConfig.http2MaxContentLength));
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
      streamChannel = http2MultiplexedChannelPool.acquire()
          .get(http2ClientConfig.http2BlockingChannelAcquireTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new IOException("Can't acquire stream channel from " + getRemoteHost() + ":" + getRemotePort(), e);
    }

    responsePromise = streamChannel.eventLoop().newPromise();
    streamChannel.attr(RESPONSE_PROMISE).set(responsePromise);
    streamChannel.writeAndFlush(request)
        .awaitUninterruptibly(http2ClientConfig.http2BlockingChannelSendTimeoutMs, TimeUnit.MILLISECONDS);
  }

  @Override
  public ChannelOutput receive() throws IOException {
    ByteBuf responseByteBuf;
    try {
      responseByteBuf =
          responsePromise.get(http2ClientConfig.http2BlockingChannelReceiveTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new IOException("Failed to receive response from " + getRemoteHost() + ":" + getRemotePort(), e);
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
