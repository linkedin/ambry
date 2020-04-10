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
package com.github.ambry.network.http2;

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.network.ChannelOutput;
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.Send;
import com.github.ambry.commons.NettySslHttp2Factory;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Promise;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A HTTP2 implementation of {@link ConnectedChannel}. This implementation is for test now. It will be imporved to
 * to support replication in the future.
 */
public class Http2BlockingChannel implements ConnectedChannel {
  private static final Logger logger = LoggerFactory.getLogger(Http2BlockingChannel.class);
  private final static AttributeKey<Promise<ByteBuf>> RESPONSE_PROMISE = AttributeKey.newInstance("ResponsePromise");
  private final String hostName;
  private final SSLFactory sslFactory;
  private final int port;
  private EventLoopGroup workerGroup;
  private Channel channel;
  private Promise<ByteBuf> responsePromise;
  private Http2StreamChannelBootstrap http2StreamChannelBootstrap;

  public Http2BlockingChannel(String hostName, int port, SSLConfig sslConfig) {
    this.hostName = hostName;
    this.port = port;
    try {
      sslFactory = new NettySslHttp2Factory(sslConfig);
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void connect() throws IOException {
    workerGroup = Epoll.isAvailable() ? new EpollEventLoopGroup() : new NioEventLoopGroup();
    Bootstrap b = new Bootstrap();
    b.group(workerGroup);
    b.channel(Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class);
    b.option(ChannelOption.SO_KEEPALIVE, true);
    b.remoteAddress(hostName, port);
    b.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        SslHandler sslHandler = new SslHandler(sslFactory.createSSLEngine(hostName, port, SSLFactory.Mode.CLIENT));
        pipeline.addLast(sslHandler);
        pipeline.addLast(Http2FrameCodecBuilder.forClient().initialSettings(Http2Settings.defaultSettings()).build());
        pipeline.addLast(new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()));
      }
    });

    // Start the client.
    channel = b.connect().syncUninterruptibly().channel();
    logger.info("Connected to remote host");
    http2StreamChannelBootstrap = new Http2StreamChannelBootstrap(channel).handler(new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new Http2StreamFrameToHttpObjectCodec(false));
        p.addLast(new HttpObjectAggregator(1024 * 1024));
        p.addLast(new SimpleChannelInboundHandler<FullHttpResponse>() {
          @Override
          protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) throws Exception {
            Integer streamId = msg.headers().getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text());
            if (streamId == null) {
              logger.error("Http2ResponseHandler unexpected message received: " + msg);
              return;
            }
            ctx.channel().attr(RESPONSE_PROMISE).getAndSet(null).setSuccess(msg.content().retainedDuplicate());
            logger.trace("Stream response received.");
          }
        });
      }
    });
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

    Http2StreamChannel childChannel = http2StreamChannelBootstrap.open().syncUninterruptibly().getNow();
    Http2Headers http2Headers = new DefaultHttp2Headers().method(HttpMethod.POST.asciiName()).scheme("https").path("/");
    responsePromise = childChannel.eventLoop().newPromise();
    childChannel.attr(RESPONSE_PROMISE).set(responsePromise);

    DefaultHttp2HeadersFrame headersFrame = new DefaultHttp2HeadersFrame(http2Headers, false);
    DefaultHttp2DataFrame dataFrame = new DefaultHttp2DataFrame(byteBuf, true);
    childChannel.write(headersFrame);
    childChannel.write(dataFrame);
    childChannel.flush();
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
    return hostName;
  }

  @Override
  public int getRemotePort() {
    return port;
  }
}
