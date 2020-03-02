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
package com.github.ambry.network.http2;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolMap;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A HTTP/2 implementation of {@link NetworkClient}. More implementation details will be added in next PR.
 */
public class Http2NetworkClient implements NetworkClient {
  private static final Logger logger = LoggerFactory.getLogger(Http2NetworkClient.class);
  private EventLoopGroup eventLoopGroup;
  private final ChannelPoolMap<InetSocketAddress, ChannelPool> pools;

  public Http2NetworkClient(Http2ClientMetrics http2ClientMetrics, Http2ClientConfig http2ClientConfig,
      SSLFactory sslFactory) {
    this.eventLoopGroup = new NioEventLoopGroup(http2ClientConfig.http2NettyEventLoopGroupThreads);
    this.pools = new Http2ChannelPoolMap(sslFactory, eventLoopGroup, http2ClientConfig);
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    return null;
  }

  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {
    return 0;
  }

  @Override
  public void wakeup() {

  }

  @Override
  public void close() {

  }
}
