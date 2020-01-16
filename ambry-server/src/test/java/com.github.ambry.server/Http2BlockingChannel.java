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
package com.github.ambry.server;

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Send;
import com.github.ambry.rest.NettySslHttp2Factory;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A HTTP2 implementation of {@link ConnectedChannel}. This implementation is for test now. It will be imporved to
 * to support replication in the future.
 */
public class Http2BlockingChannel implements ConnectedChannel {
  private static final Logger logger = LoggerFactory.getLogger(Http2BlockingChannel.class);
  private final Http2ResponseHandler http2ResponseHandler;
  private final String hostName;
  private final int port;
  private EventLoopGroup workerGroup;
  private Channel channel;

  public Http2BlockingChannel(String hostName, int port) {
    http2ResponseHandler = new Http2ResponseHandler();
    this.hostName = hostName;
    this.port = port;
  }

  @Override
  public void connect() throws IOException {
    workerGroup = new NioEventLoopGroup();
    Bootstrap b = new Bootstrap();
    b.group(workerGroup);
    b.channel(NioSocketChannel.class);
    b.option(ChannelOption.SO_KEEPALIVE, true);
    b.remoteAddress(hostName, port);
    SSLFactory sslFactory;
    try {
      sslFactory = new NettySslHttp2Factory(new SSLConfig(new VerifiableProperties(new Properties())));
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
    b.handler(new Http2ClientChannelInitializer(sslFactory, hostName, port));

    // Start the client.
    channel = b.connect().syncUninterruptibly().channel();
    logger.info("Connected to remote host");
  }

  @Override
  public void disconnect() throws IOException {
    channel.disconnect().syncUninterruptibly();
    workerGroup.shutdownGracefully();
  }

  @Override
  public void send(Send request) throws IOException {
    ByteBufferChannel byteBufferChannel = new ByteBufferChannel(ByteBuffer.allocate((int) request.sizeInBytes()));
    while (!request.isSendComplete()) {
      request.writeTo(byteBufferChannel);
    }
    byteBufferChannel.getBuffer().position(0);
    ByteBuf byteBuf = Unpooled.wrappedBuffer(byteBufferChannel.getBuffer());

    Http2ClientStreamInitializer initializer = new Http2ClientStreamInitializer(http2ResponseHandler);
    Http2StreamChannel childChannel =
        new Http2StreamChannelBootstrap(channel).handler(initializer).open().syncUninterruptibly().getNow();
    Http2Headers http2Headers = new DefaultHttp2Headers().method(HttpMethod.POST.asciiName()).scheme("https").path("/");

    DefaultHttp2HeadersFrame headersFrame = new DefaultHttp2HeadersFrame(http2Headers, false);
    DefaultHttp2DataFrame dataFrame = new DefaultHttp2DataFrame(byteBuf, true);
    ChannelPromise childChannelPromise = childChannel.newPromise();
    childChannel.write(headersFrame);
    ChannelFuture channelFuture = childChannel.write(dataFrame);
    childChannel.flush();
    http2ResponseHandler.put(channelFuture, childChannelPromise);
  }

  @Override
  public ChannelOutput receive() throws IOException {
    Http2ResponseHandler.StreamResult streamResult = http2ResponseHandler.awaitResponses(5, TimeUnit.SECONDS);
    DataInputStream dataInputStream = new NettyByteBufDataInputStream(streamResult.getByteBuf());
    return new ChannelOutput(dataInputStream, dataInputStream.readLong());
  }

  @Override
  public String getRemoteHost() {
    return hostName;
  }

  @Override
  public int getRemotePort() {
    return port;
  }
}
