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
package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.VerifiableProperties;
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.LinkedHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link NioServerFactory}.
 * <p/>
 * Sets up all the supporting cast required for the operation of {@link NettyServer} and returns a new instance on
 * {@link #getNioServer()}.
 */
public class NettyServerFactory implements NioServerFactory {

  private final NettyConfig nettyConfig;
  private final NettyMetrics nettyMetrics;
  // linked hashmap as we need a deterministic order while iterating through the map
  private final LinkedHashMap<String, ChannelHandler> channelHandlerInfoList = new LinkedHashMap<>();
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Creates a new instance of NettyServerFactory.
   * @param verifiableProperties the in-memory {@link VerifiableProperties} to use.
   * @param metricRegistry the {@link MetricRegistry} to use.
   * @param requestHandler the {@link RestRequestHandler} that handles general requests.
   * @param publicAccessLogger the {@link PublicAccessLogger} that can be used for public access logging
   * @param restServerState the {@link RestServerState} that can be used to check the health of the system
   *                              to respond to health check requests
   * @throws IllegalArgumentException if any of the arguments are null.
   */
  public NettyServerFactory(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestHandler requestHandler, PublicAccessLogger publicAccessLogger, RestServerState restServerState) {
    if (verifiableProperties != null && metricRegistry != null && requestHandler != null && publicAccessLogger != null
        && restServerState != null) {
      this.nettyConfig = new NettyConfig(verifiableProperties);
      this.nettyMetrics = new NettyMetrics(metricRegistry);
      initializeChannelHandlers(nettyMetrics, nettyConfig, requestHandler, publicAccessLogger, restServerState);
    } else {
      StringBuilder errorMessage =
          new StringBuilder("Null arg(s) received during instantiation of NettyServerFactory -");
      if (verifiableProperties == null) {
        errorMessage.append(" [VerifiableProperties] ");
      }
      if (metricRegistry == null) {
        errorMessage.append(" [MetricRegistry] ");
      }
      if (requestHandler == null) {
        errorMessage.append(" [RestRequestHandler] ");
      }
      if (publicAccessLogger == null) {
        errorMessage.append(" [PublicAccessLogger] ");
      }
      if (restServerState == null) {
        errorMessage.append(" [RestServerState] ");
      }
      throw new IllegalArgumentException(errorMessage.toString());
    }
    logger.trace("Instantiated NettyServerFactory");
  }

  /**
   * Initialize the {@link ChannelHandler}s to be used in the netty pipeline
   * @param nettyMetrics the {@link NettyMetrics} instance to use to record metrics.
   * @param nettyConfig the {@link NettyConfig} instance that defines the configuration parameters for the NettyServer.
   * @param requestHandler the {@link RestRequestHandler} that can be used to submit requests that need to be handled.
   * @param publicAccessLogger the {@link PublicAccessLogger} that can be used for public access logging
   * @param restServerState the {@link RestServerState} that can be used to check the health of the system
   *                              to respond to health check requests
   */
  private void initializeChannelHandlers(NettyMetrics nettyMetrics, NettyConfig nettyConfig,
      RestRequestHandler requestHandler, PublicAccessLogger publicAccessLogger, RestServerState restServerState) {
    // for http encoding/decoding. Note that we get content in 8KB chunks and a change to that number has
    // to go here.
    channelHandlerInfoList.put("codec", new HttpServerCodec());
    // for health check request handling
    channelHandlerInfoList.put("HealthCheckHandler", new HealthCheckHandler(restServerState, nettyMetrics));
    // for public access logging
    channelHandlerInfoList
        .put("PublicAccessLogHandler", new PublicAccessLogRequestHandler(publicAccessLogger, nettyMetrics));
    // for detecting connections that have been idle too long - probably because of an error.
    channelHandlerInfoList.put("idleStateHandler", new IdleStateHandler(0, 0, nettyConfig.nettyServerIdleTimeSeconds));
    // for safe writing of chunks for responses
    channelHandlerInfoList.put("chunker", new ChunkedWriteHandler());
    // custom processing class that interfaces with a BlobStorageService.
    channelHandlerInfoList.put("processor", new NettyMessageProcessor(nettyMetrics, nettyConfig, requestHandler));
  }

  /**
   * Returns a new instance of {@link NettyServer}.
   * @return a new instance of {@link NettyServer}.
   */
  @Override
  public NioServer getNioServer() {
    return new NettyServer(nettyConfig, nettyMetrics, channelHandlerInfoList);
  }
}
