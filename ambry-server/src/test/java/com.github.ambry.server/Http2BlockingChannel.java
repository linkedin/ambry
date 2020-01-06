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


/**
 * A HTTP2 implementation of {@link ConnectedChannel}
 */
public class Http2BlockingChannel implements ConnectedChannel {
  private Channel channel;
  private Http2ResponseHandler http2ResponseHandler;
  private String hostName;
  private int port;

  public Http2BlockingChannel(String hostName, int port) {
    http2ResponseHandler = new Http2ResponseHandler();
    this.hostName = hostName;
    this.port = port;
  }

  @Override
  public void connect() throws IOException {
    Bootstrap b = new Bootstrap();
    b.group(new NioEventLoopGroup());
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
    System.out.println("Connected to remote host");
  }

  @Override
  public void disconnect() throws IOException {

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
    return null;
  }

  @Override
  public int getRemotePort() {
    return 0;
  }
}
