/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.Send;
import com.github.ambry.rest.Http2ClientChannelInitializer;
import com.github.ambry.rest.Http2ClientStreamInitializer;
import com.github.ambry.rest.Http2ResponseHandler;
import com.github.ambry.rest.NettySslHttp2Factory;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.ByteBufferChannel;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link NetworkClient} that provides a method for sending a list of requests in the form of {@link Send} to a host:port,
 * and receive responses for sent requests. Requests that come in via {@link #sendAndPoll(List, Set, int)} call,
 * that could not be immediately sent are queued, and an attempt will be made in subsequent invocations of the call (or
 * until they time out).
 * (Note: We will empirically determine whether, rather than queueing a request,
 * a request should be failed if connections could not be checked out if pool limit for its hostPort has been reached
 * and all connections to the hostPort are unavailable).
 *
 * This class is not thread safe.
 */
public class Http2NetworkClient implements NetworkClient {
  private static final Logger logger = LoggerFactory.getLogger(Http2NetworkClient.class);
  private EventLoopGroup workerGroup;
  private Map<String, Channel> hostToChannel = new ConcurrentHashMap<>();

  public Http2NetworkClient() {

    workerGroup = new NioEventLoopGroup();
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    return null;
  }

  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {

    for (DataNodeId dataNodeId : dataNodeIds) {
      Bootstrap b = new Bootstrap();
      b.group(workerGroup);
      b.channel(NioSocketChannel.class);
      b.option(ChannelOption.SO_KEEPALIVE, true);
      b.remoteAddress(dataNodeId.getHostname(), dataNodeId.getHttp2Port());
      SSLFactory sslFactory;
      try {
        sslFactory = new NettySslHttp2Factory(new SSLConfig(new VerifiableProperties(new Properties())));
      } catch (Exception e) {
        logger.error("Exception: ", e);
        return 0;
      }
      b.handler(new Http2ClientChannelInitializer(sslFactory, dataNodeId.getHostname(), dataNodeId.getHttp2Port()));

      b.connect().addListener(future -> {
        if (future.isSuccess()) {
          hostToChannel.put(dataNodeId.getHostname(), (Channel) future.get());
          logger.info("Connected to remote host " + dataNodeId.getHostname());
        } else {
          logger.error("Connection failed to " + dataNodeId.getHostname());
        }
      });
    }
    return 0;
  }

  public void sendRequest(RequestInfo requestInfo) throws IOException {

    Send send = requestInfo.getRequest();
    ByteBufferChannel byteBufferChannel = new ByteBufferChannel(ByteBuffer.allocate((int) send.sizeInBytes()));
    while (!send.isSendComplete()) {
      send.writeTo(byteBufferChannel);
    }
    byteBufferChannel.getBuffer().position(0);
    ByteBuf byteBuf = Unpooled.wrappedBuffer(byteBufferChannel.getBuffer());



    Channel channel = hostToChannel.get(requestInfo.getHost());
    Http2ClientStreamInitializer initializer = new Http2ClientStreamInitializer(new Http2ResponseHandler());
    Http2StreamChannelBootstrap http2StreamChannelBootstrap =
        new Http2StreamChannelBootstrap(channel).handler(initializer);

    Http2StreamChannel childChannel = http2StreamChannelBootstrap.open().syncUninterruptibly().getNow();
    Http2Headers http2Headers = new DefaultHttp2Headers().method(HttpMethod.POST.asciiName()).scheme("https").path("/");
    http2Headers.set(RestUtils.Headers.HTTP2_FRONTEND_REQUEST, "true");
  }

  @Override
  public void wakeup() {

  }

  @Override
  public void close() {

  }
}
