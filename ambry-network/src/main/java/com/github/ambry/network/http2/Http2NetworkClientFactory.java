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

import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.utils.Time;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory class used to get new instances of a {@link Http2NetworkClient}
 */
public class Http2NetworkClientFactory implements NetworkClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(Http2NetworkClientFactory.class);
  private final Http2ClientMetrics http2ClientMetrics;
  private final Http2ClientConfig http2ClientConfig;
  private final SSLFactory sslFactory;
  private final EventLoopGroup eventLoopGroup;

  /**
   * Construct a factory using the given parameters.
   * @param http2ClientMetrics the metrics for HTTP/2 Client.
   * @param http2ClientConfig the configs for HTTP/2 Client.
   * @param sslFactory the sslFactory for HTTP/2 Client.
   */
  public Http2NetworkClientFactory(Http2ClientMetrics http2ClientMetrics, Http2ClientConfig http2ClientConfig,
      SSLFactory sslFactory, Time time) {
    this.http2ClientMetrics = http2ClientMetrics;
    this.http2ClientConfig = http2ClientConfig;
    this.sslFactory = sslFactory;
    if (Epoll.isAvailable()) {
      logger.info("Create EpollEventLoopGroup in Http2NetworkClientFactory.");
      this.eventLoopGroup = new EpollEventLoopGroup(http2ClientConfig.http2NettyEventLoopGroupThreads);
    } else {
      logger.info("Create NioEventLoopGroup in Http2NetworkClientFactory.");
      this.eventLoopGroup = new NioEventLoopGroup(http2ClientConfig.http2NettyEventLoopGroupThreads);
    }
    this.http2ClientMetrics.registerNettyPendingTasksGauge(eventLoopGroup);
  }

  /**
   * Construct and return a new {@link Http2NetworkClient}
   * @return return a new {@link Http2NetworkClient}
   */
  @Override
  public Http2NetworkClient getNetworkClient() throws IOException {
    return new Http2NetworkClient(http2ClientMetrics, http2ClientConfig, sslFactory, eventLoopGroup);
  }
}

