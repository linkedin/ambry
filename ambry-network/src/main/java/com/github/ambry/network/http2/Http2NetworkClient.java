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
import com.github.ambry.protocol.RequestOrResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.network.http2.Http2Utils.*;


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
  private final Http2StreamFrameToHttpObjectCodec http2StreamFrameToHttpObjectCodec;
  private final AmbrySendToHttp2Adaptor ambrySendToHttp2Adaptor;
  private final Map<Integer, Channel> correlationIdInFlightToChannelMap;
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
    this.http2ClientResponseHandler = new Http2ClientResponseHandler(http2ClientMetrics);
    this.http2ClientStreamStatsHandler = new Http2ClientStreamStatsHandler(http2ClientMetrics);
    this.http2StreamFrameToHttpObjectCodec = new Http2StreamFrameToHttpObjectCodec(false);
    this.ambrySendToHttp2Adaptor = new AmbrySendToHttp2Adaptor(false);

    this.pools = new Http2ChannelPoolMap(sslFactory, eventLoopGroup, http2ClientConfig, http2ClientMetrics,
        new StreamChannelInitializer());
    this.http2ClientMetrics = http2ClientMetrics;
    correlationIdInFlightToChannelMap = new ConcurrentHashMap<>();
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {

    List<ResponseInfo> readyResponseInfos = new ArrayList<>();
    if (requestsToDrop.size() != 0) {
      logger.warn("Number of requestsToDrop: {}", requestsToDrop.size());
      http2ClientMetrics.http2RequestsToDropCount.inc(requestsToDrop.size());
      for (int correlationId : requestsToDrop) {
        Channel streamChannel = correlationIdInFlightToChannelMap.remove(correlationId);
        if (streamChannel != null) {
          logger.warn("Drop request on streamChannel: {}", streamChannel);
          // Drop a request triggers exception on writeAndFlush failure or Http2ClientResponseHandler.exceptionCaught()
          // We can't release a stream channel twice because a stream counter is maintained per physical channel, but we
          // could add multiple ResponseInfos for same RequestInfo, because router will ignore these after the first one.
          RequestInfo requestInfo = releaseAndCloseStreamChannel(streamChannel);
          if (requestInfo != null) {
            readyResponseInfos.add(new ResponseInfo(requestInfo, NetworkClientErrorCode.TimeoutError, null));
          }
        }
      }
    }

    long sendStartTime = System.currentTimeMillis();
    // Send request
    http2ClientMetrics.http2ClientSendRate.mark(requestsToSend.size());
    for (RequestInfo requestInfo : requestsToSend) {
      long streamInitiateTime = System.currentTimeMillis();

      RequestOrResponse request = (RequestOrResponse) (requestInfo.getRequest());
      long waitingTime = streamInitiateTime - request.requestCreateTime;
      http2ClientMetrics.requestToNetworkClientLatencyMs.update(waitingTime);

      this.pools.get(InetSocketAddress.createUnresolved(requestInfo.getHost(), requestInfo.getPort().getPort()))
          .acquire()
          .addListener((GenericFutureListener<Future<Channel>>) future -> {
            if (future.isSuccess()) {
              http2ClientMetrics.http2StreamAcquireTime.update(System.currentTimeMillis() - streamInitiateTime);
              long streamAcquiredTime = System.currentTimeMillis();
              Channel streamChannel = future.getNow();
              correlationIdInFlightToChannelMap.put(requestInfo.getRequest().getCorrelationId(), streamChannel);
              streamChannel.attr(REQUEST_INFO).set(requestInfo);
              if (!streamChannel.isWritable() || !streamChannel.parent().isWritable()) {
                http2ClientMetrics.http2StreamNotWritableCount.inc();
                logger.debug("Stream {} {} not writable. BytesBeforeWritable {} {}", streamChannel.hashCode(),
                    streamChannel, streamChannel.bytesBeforeWritable(), streamChannel.parent().bytesBeforeWritable());
              }
              streamChannel.writeAndFlush(requestInfo.getRequest()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                  // Listener will be notified right after data is removed from ChannelOutboundBuffer (netty's send buffer)
                  // After removing from ChannelOutboundBuffer, it goes to OS send buffer.
                  if (future.isSuccess()) {
                    long writeAndFlushUsedTime = System.currentTimeMillis() - streamAcquiredTime;
                    http2ClientMetrics.http2StreamWriteAndFlushTime.update(writeAndFlushUsedTime);
                    requestInfo.setStreamSendTime(System.currentTimeMillis());
                    if (writeAndFlushUsedTime > http2ClientConfig.http2WriteAndFlushTimeoutMs) {
                      // This usually happens if remote can't accept data in time.
                      logger.debug(
                          "WriteAndFlush exceeds http2RequestTimeoutMs {}ms, used time: {}ms, stream channel {}",
                          http2ClientConfig.http2WriteAndFlushTimeoutMs, writeAndFlushUsedTime, streamChannel);
                      if (http2ClientConfig.http2DropRequestOnWriteAndFlushTimeout) {
                        RequestInfo requestInfoFromChannelAttr = releaseAndCloseStreamChannel(streamChannel);
                        if (requestInfoFromChannelAttr != null) {
                          http2ClientResponseHandler.getResponseInfoQueue()
                              .put(new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null));
                          // Don't need to call requestInfo.getRequest().release(), because netty write handler decreases refcnt.
                        }
                      }
                    }
                    // When success, handler would release the request.
                  } else {
                    http2ClientMetrics.http2StreamWriteAndFlushErrorCount.inc();
                    logger.warn("Stream {} {} writeAndFlush fail. Cause: ", streamChannel.hashCode(), streamChannel,
                        future.cause());
                    RequestInfo requestInfoFromChannelAttr = releaseAndCloseStreamChannel(streamChannel);
                    if (requestInfoFromChannelAttr != null) {
                      http2ClientResponseHandler.getResponseInfoQueue()
                          .put(new ResponseInfo(requestInfoFromChannelAttr, NetworkClientErrorCode.NetworkError, null));
                    }
                    if (!(future.cause() instanceof ClosedChannelException)) {
                      // If it's ClosedChannelException caused by drop request, it's probably refCnt has been decreased.
                      // TODO: a round solution is needed.
                      requestInfo.getRequest().release();
                    }
                  }
                }
              });
            } else {
              logger.error("Couldn't acquire stream channel to {}:{} . Cause:", requestInfo.getHost(),
                  requestInfo.getPort().getPort(), future.cause());
              // release related bytebuf
              requestInfo.getRequest().release();
              http2ClientResponseHandler.getResponseInfoQueue()
                  .put(new ResponseInfo(requestInfo, NetworkClientErrorCode.NetworkError, null));
            }
          });
    }
    http2ClientMetrics.http2ClientSendTime.update(System.currentTimeMillis() - sendStartTime);

    http2ClientResponseHandler.getResponseInfoQueue().poll(readyResponseInfos, pollTimeoutMs);
    for (ResponseInfo responseInfo : readyResponseInfos) {
      correlationIdInFlightToChannelMap.remove(responseInfo.getRequestInfo().getRequest().getCorrelationId());
    }

    http2ClientMetrics.http2ClientSendRate.mark(readyResponseInfos.size());
    http2ClientMetrics.http2ClientSendAndPollTime.update(System.currentTimeMillis() - sendStartTime);
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
                releaseAndCloseStreamChannel(streamChannel);
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
    http2ClientResponseHandler.getResponseInfoQueue().wakeup();
  }

  @Override
  public void close() {

  }

  private class StreamChannelInitializer extends ChannelInitializer {

    public void initChannel(Channel channel) {
      channel.pipeline().addLast(http2ClientStreamStatsHandler);
      // TODO: implement ourselves' aggregator. Http2Streams to Response Object
      channel.pipeline().addLast(http2StreamFrameToHttpObjectCodec);
      channel.pipeline().addLast(new HttpObjectAggregator(http2ClientConfig.http2MaxContentLength));
      channel.pipeline().addLast(http2ClientResponseHandler);
      channel.pipeline().addLast(ambrySendToHttp2Adaptor);
      // We log hashCode because frame id is -1 at this time.
      logger.trace("Handlers added to channel: {} {} ", channel.hashCode(), channel);
    }
  }
}
