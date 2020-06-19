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
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.network.http2.AmbryNetworkRequestHandler;
import com.github.ambry.network.http2.AmbrySendToHttp2Adaptor;
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.network.http2.Http2ServerStreamHandler;
import com.github.ambry.rest.ConnectionStatsHandler;
import com.github.ambry.rest.ServerSecurityHandler;
import com.github.ambry.network.RequestResponseChannel;
import com.github.ambry.rest.NettyMetrics;
import com.github.ambry.rest.NettyServer;
import com.github.ambry.rest.NioServer;
import com.github.ambry.rest.NioServerFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link NioServerFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NettyServer} and returns a new instance on
 * {@link #getNioServer()}.
 */
public class StorageServerNettyFactory implements NioServerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageServerNettyFactory.class);

  private final NettyConfig nettyConfig;
  private final NettyMetrics nettyMetrics;
  final Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers;

  /**
   * Creates a new instance of StorageServerNettyFactory.
   * @param http2Port the port for HTTP2 request.
   * @param requestResponseChannel the {@link RequestResponseChannel} to receive original ambry request and send
   *                               original ambry response.
   * @param sslFactory the {@link SSLFactory} used to construct the {@link javax.net.ssl.SSLEngine} used for
   *                          handling http2 requests.
   * @param nettyConfig the nettyConfig
   * @param http2ClientConfig the http2ClientConfig
   * @param serverMetrics the serverMetrics
   * @param nettyMetrics the nettyMetrics
   * @param http2ServerMetrics the http2ServerMetrics
   * @param serverSecurityService the serverSecurityService used to create ServerSecurityHandler
   * @throws IllegalArgumentException if any of the arguments are null.
   */

  public StorageServerNettyFactory(int http2Port, RequestResponseChannel requestResponseChannel, SSLFactory sslFactory,
      NettyConfig nettyConfig, Http2ClientConfig http2ClientConfig, ServerMetrics serverMetrics,
      NettyMetrics nettyMetrics, Http2ServerMetrics http2ServerMetrics, ServerSecurityService serverSecurityService) {
    if (requestResponseChannel == null || sslFactory == null || nettyConfig == null || http2ClientConfig == null
        || serverMetrics == null || nettyMetrics == null || http2ServerMetrics == null
        || serverSecurityService == null) {
      throw new IllegalArgumentException("Null arg(s) received during instantiation of StorageServerNettyFactory");
    }
    this.nettyConfig = nettyConfig;
    this.nettyMetrics = nettyMetrics;

    // For ServerSecurityHandler, Http2ServerStreamHandler, AmbryNetworkRequestHandler,
    // Http2StreamFrameToHttpObjectCodec and AmbrySendToHttp2Adaptor, each of them should only have one instance.

    ServerSecurityHandler serverSecurityHandler = new ServerSecurityHandler(serverSecurityService, serverMetrics);
    Http2ServerStreamHandler http2ServerStreamHandler =
        new Http2ServerStreamHandler(new AmbryNetworkRequestHandler(requestResponseChannel, http2ServerMetrics),
            new Http2StreamFrameToHttpObjectCodec(true), new AmbrySendToHttp2Adaptor(true), http2ClientConfig);
    ConnectionStatsHandler connectionStatsHandler = new ConnectionStatsHandler(nettyMetrics);
    Map<Integer, ChannelInitializer<SocketChannel>> initializers = Collections.singletonMap(http2Port,
        new StorageServerNettyChannelInitializer(http2ClientConfig, http2ServerMetrics, sslFactory,
            connectionStatsHandler, http2ServerStreamHandler, serverSecurityHandler));
    channelInitializers = Collections.unmodifiableMap(initializers);
  }

  /**
   * Returns a new instance of {@link NettyServer}.
   * @return a new instance of {@link NettyServer}.
   */
  @Override
  public NioServer getNioServer() {
    return new NettyServer(nettyConfig, nettyMetrics, channelInitializers);
  }
}
