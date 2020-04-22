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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.Http2ClientConfig;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientErrorCode;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A HTTP/2 implementation of {@link NetworkClient}.
 */
public class Http2NetworkClient implements NetworkClient {
  private static final Logger logger = LoggerFactory.getLogger(Http2NetworkClient.class);
  private final EventLoopGroup eventLoopGroup;
  private final ChannelPoolMap<InetSocketAddress, ChannelPool> pools;
  private final Http2ClientResponseHandler http2ClientResponseHandler;
  private final Http2ClientStreamStatsHandler http2ClientStreamStatsHandler;
  private final Http2ClientMetrics http2ClientMetrics;
  private final Http2ClientConfig http2ClientConfig;
  private static final ResponseInfo WAKEUP_MARKER = new ResponseInfo(null, null, null);
  static final AttributeKey<RequestInfo> REQUEST_INFO = AttributeKey.newInstance("RequestInfo");

  public Http2NetworkClient(Http2ClientMetrics http2ClientMetrics, Http2ClientConfig http2ClientConfig,
      SSLFactory sslFactory) {
    logger.info("Http2NetworkClient started");
    this.http2ClientConfig = http2ClientConfig;
    if (Epoll.isAvailable()) {
      logger.info("Using EpollEventLoopGroup in Http2NetworkClient.");
      this.eventLoopGroup = new EpollEventLoopGroup(http2ClientConfig.http2NettyEventLoopGroupThreads);
    } else {
      this.eventLoopGroup = new NioEventLoopGroup(http2ClientConfig.http2NettyEventLoopGroupThreads);
    }
    this.pools = new Http2ChannelPoolMap(sslFactory, eventLoopGroup, http2ClientConfig, http2ClientMetrics);
    this.http2ClientResponseHandler = new Http2ClientResponseHandler(this);
    this.http2ClientStreamStatsHandler = new Http2ClientStreamStatsHandler(this);
    this.http2ClientMetrics = http2ClientMetrics;
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    long startTime = System.currentTimeMillis();
    List<ResponseInfo> readyResponseInfos = new ArrayList<>();
    // Send request
    http2ClientMetrics.http2ClientSendRate.mark(requestsToSend.size());
    for (RequestInfo requestInfo : requestsToSend) {
      long streamInitiateTime = System.currentTimeMillis();
      this.pools.get(InetSocketAddress.createUnresolved(requestInfo.getHost(), requestInfo.getPort().getPort()))
          .acquire()
          .addListener((GenericFutureListener<Future<Channel>>) future -> {
            if (future.isSuccess()) {
              http2ClientMetrics.http2StreamAcquireTime.update(System.currentTimeMillis() - streamInitiateTime);
              long streamAcquiredTime = System.currentTimeMillis();
              Channel streamChannel = future.getNow();
              streamChannel.pipeline().addLast(http2ClientStreamStatsHandler);
              // TODO: implement ourselves' aggregator. Http2Streams to Response Object
              streamChannel.pipeline().addLast(new Http2StreamFrameToHttpObjectCodec(false));
              streamChannel.pipeline().addLast(new HttpObjectAggregator(http2ClientConfig.http2MaxContentLength));
              streamChannel.pipeline().addLast(http2ClientResponseHandler);
              streamChannel.pipeline().addLast(new AmbrySendToHttp2Adaptor());
              streamChannel.attr(REQUEST_INFO).set(requestInfo);
              streamChannel.writeAndFlush(requestInfo.getRequest()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                  // Listener will be notified after data is removed from ChannelOutboundBuffer (netty's send buffer)
                  // After removing from ChannelOutboundBuffer, it goes to OS send buffer.
                  if (future.isSuccess()) {
                    http2ClientMetrics.http2StreamWriteAndFlushTime.update(
                        System.currentTimeMillis() - streamAcquiredTime);
                    requestInfo.setStreamSendTime(System.currentTimeMillis());
                  } else {
                    http2ClientMetrics.http2StreamWriteAndFlushErrorCount.inc();
                    logger.warn("Stream writeAndFlush fail: {}", future.cause());
                  }
                  // release related bytebuf
                  requestInfo.getRequest().release();
                }
              });
            } else {
              logger.error("Couldn't acquire stream channel to {}:{} . Cause: {}.", requestInfo.getHost(),
                  requestInfo.getPort().getPort(), future.cause());
              // release related bytebuf
              requestInfo.getRequest().release();
              readyResponseInfos.add(new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null));
            }
          });
    }
    http2ClientMetrics.http2ClientSendTime.update(System.currentTimeMillis() - startTime);
    // TODO: close stream channel for requestsToDrop. Need a hashmap from corelationId to streamChannel

    ResponseInfo firstResponse = null;
    try {
      firstResponse = http2ClientResponseHandler.getResponseInfoQueue().poll(pollTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      logger.debug("Interrupted polling responses");
    }
    if (firstResponse == null) {
      return Collections.emptyList();
    }
    // add responseInfo to readyResponseInfos
    FilteredInserter<ResponseInfo> filteredInserter =
        new FilteredInserter<>(readyResponseInfos, r -> r != WAKEUP_MARKER);
    filteredInserter.add(firstResponse);
    http2ClientResponseHandler.getResponseInfoQueue().drainTo(filteredInserter);

    // wakeup markers will be detected in NonBlockingRouter#onResponse()
    http2ClientMetrics.http2ClientSendRate.mark(readyResponseInfos.size());

    http2ClientMetrics.http2ClientSendAndPollTime.update(System.currentTimeMillis() - startTime);
    return readyResponseInfos;
  }

  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {
    long startTime = System.currentTimeMillis();
    AtomicInteger successCount = new AtomicInteger();
    AtomicInteger failCount = new AtomicInteger();
    int warmUpConnectionPerPort =
        http2ClientConfig.http2MinConnectionPerPort * connectionWarmUpPercentagePerDataNode / 100;
    int expectedConnections = dataNodeIds.size() * warmUpConnectionPerPort;
    for (DataNodeId dataNodeId : dataNodeIds) {
      for (int i = 0; i < warmUpConnectionPerPort; i++) {
        this.pools.get(InetSocketAddress.createUnresolved(dataNodeId.getHostname(), dataNodeId.getHttp2Port()))
            .acquire()
            .addListener((GenericFutureListener<Future<Channel>>) future -> {
              if (future.isSuccess()) {
                Channel streamChannel = future.getNow();
                streamChannel.parent()
                    .attr(Http2MultiplexedChannelPool.HTTP2_MULTIPLEXED_CHANNEL_POOL)
                    .get()
                    .release(streamChannel);
                successCount.incrementAndGet();
              } else {
                failCount.incrementAndGet();
                responseInfoList.add(new ResponseInfo(null, NetworkClientErrorCode.NetworkError, null, dataNodeId));
                logger.error("Couldn't acquire stream channel to {}:{} . Cause: {}.", dataNodeId.getHostname(),
                    dataNodeId.getHttp2Port(), future.cause());
              }
            });
      }
    }

    while (System.currentTimeMillis() - startTime < timeForWarmUp) {
      if (successCount.get() + failCount.get() == expectedConnections) {
        break;
      } else {
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          break;
        }
      }
    }

    logger.info("HTTP2 connection warm up done. Tried: {}, Succeeded: {}, Failed: {}, Time elapsed: {} ms",
        expectedConnections, successCount, failCount, System.currentTimeMillis() - startTime);

    return successCount.get();
  }

  @Override
  public void wakeup() {
    // if sendAndPoll is currently executing a timed poll on the blocking queue, we need to put WAKEUP_MARKER
    // to wake it up before a response comes
    try {
      http2ClientResponseHandler.getResponseInfoQueue().put(WAKEUP_MARKER);
    } catch (InterruptedException e) {
      logger.warn("Interrupted while waking up", e);
    }
  }

  @Override
  public void close() {

  }

  public Http2ClientMetrics getHttp2ClientMetrics() {
    return http2ClientMetrics;
  }

  /**
   * An implementation of AbstractCollection to be used for non-{@link Http2NetworkClient#WAKEUP_MARKER} insertion.
   */
  private static class FilteredInserter<T> extends AbstractCollection<T> {
    private final Collection<T> data;
    private final Predicate<T> shouldAdd;

    FilteredInserter(Collection<T> data, Predicate<T> shouldAdd) {
      this.data = data;
      this.shouldAdd = shouldAdd;
    }

    @Override
    public boolean add(T t) {
      if (shouldAdd.test(t)) {
        return data.add(t);
      }
      return false;
    }

    @Override
    public Iterator<T> iterator() {
      return data.iterator();
    }

    @Override
    public int size() {
      return data.size();
    }
  }
}
