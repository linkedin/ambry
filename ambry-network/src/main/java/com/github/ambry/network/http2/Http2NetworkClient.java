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
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
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
  private final ChannelPoolMap<InetSocketAddress, ChannelPool> pools;
  private final Http2ClientResponseHandler http2ClientResponseHandler;
  private final Http2ClientStreamStatsHandler http2ClientStreamStatsHandler;
  private final Http2ClientMetrics http2ClientMetrics;
  private final Http2ClientConfig http2ClientConfig;
  private final Http2StreamFrameToHttpObjectCodec http2StreamFrameToHttpObjectCodec;
  private final AmbrySendToHttp2Adaptor ambrySendToHttp2Adaptor;
  private final RequestsStatsHandler requestsStatsHandler;
  private final Map<Integer, Channel> correlationIdInFlightToChannelMap;
  static final AttributeKey<RequestInfo> REQUEST_INFO = AttributeKey.newInstance("RequestInfo");

  public Http2NetworkClient(Http2ClientMetrics http2ClientMetrics, Http2ClientConfig http2ClientConfig,
      SSLFactory sslFactory, EventLoopGroup eventLoopGroup) {
    this.http2ClientConfig = http2ClientConfig;
    this.http2ClientResponseHandler = new Http2ClientResponseHandler(http2ClientMetrics);
    this.http2ClientStreamStatsHandler = new Http2ClientStreamStatsHandler(http2ClientMetrics);
    this.http2StreamFrameToHttpObjectCodec = new Http2StreamFrameToHttpObjectCodec(false);
    this.ambrySendToHttp2Adaptor = new AmbrySendToHttp2Adaptor(false, http2ClientConfig.http2FrameMaxSize);
    this.requestsStatsHandler = new RequestsStatsHandler(http2ClientMetrics);
    this.pools = new Http2ChannelPoolMap(sslFactory, eventLoopGroup, http2ClientConfig, http2ClientMetrics,
        new StreamChannelInitializer());
    this.http2ClientMetrics = http2ClientMetrics;
    correlationIdInFlightToChannelMap = new ConcurrentHashMap<>();
    logger.info("Http2NetworkClient started");
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
          RequestInfo requestInfo = releaseAndCloseStreamChannel(streamChannel);
          // Drop request just generates a ResponseInfo with TimeoutError to router.
          // The stream is still transmitting, but router will ignore ResponseInfo with the same correlationId.
          // We need stream reset to cancel the stream in transmitting.
          if (requestInfo != null) {
            logger.warn("Drop request {}:{} on streamChannel: {}", requestInfo.getRequest().getRequestOrResponseType(),
                correlationId, streamChannel);
            NetworkClientErrorCode errorCode = NetworkClientErrorCode.TimeoutError;
            if (http2ClientConfig.http2TimeoutAsNetworkError) {
              errorCode = NetworkClientErrorCode.NetworkError;
            }
            readyResponseInfos.add(new ResponseInfo(requestInfo, errorCode, null));
            // We don't have to release the request here, since it should already be released when client sent it out.
          } else {
            logger.warn("Drop request {} on streamChannel: {}", correlationId, streamChannel);
          }
        }
      }
    }

    long sendStartTime = System.currentTimeMillis();
    // Send request
    http2ClientMetrics.http2ClientSendRate.mark(requestsToSend.size());
    for (RequestInfo requestInfo : requestsToSend) {
      long streamInitiateTime = System.currentTimeMillis();
      long waitingTime = streamInitiateTime - requestInfo.getRequestCreateTime();
      http2ClientMetrics.requestToNetworkClientLatencyMs.update(waitingTime);
      requestInfo.setRequestEnqueueTime(streamInitiateTime);
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

              // Set approximate additional time that may be needed for the response when under heavy load.
              increaseTimeoutForRequestIfNeeded(requestInfo, streamInitiateTime, streamAcquiredTime);

              streamChannel.writeAndFlush(requestInfo.getRequest()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                  // Listener will be notified right after data is removed from ChannelOutboundBuffer (netty's send buffer)
                  // After removing from ChannelOutboundBuffer, it goes to OS send buffer.
                  if (future.isSuccess()) {
                    long writeAndFlushUsedTime = System.currentTimeMillis() - streamAcquiredTime;
                    http2ClientMetrics.http2StreamWriteAndFlushTime.update(writeAndFlushUsedTime);
                    requestInfo.setRequestSendTime(System.currentTimeMillis());
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
                  } else {
                    http2ClientMetrics.http2StreamWriteAndFlushErrorCount.inc();
                    logger.warn("Stream {} {} writeAndFlush fail. Cause: {}", streamChannel.hashCode(), streamChannel,
                        future.cause().toString());
                    RequestInfo requestInfoFromChannelAttr = releaseAndCloseStreamChannel(streamChannel);
                    if (requestInfoFromChannelAttr != null) {
                      http2ClientResponseHandler.getResponseInfoQueue()
                          .put(new ResponseInfo(requestInfoFromChannelAttr, NetworkClientErrorCode.NetworkError, null));
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
                responseInfoList.add(
                    new ResponseInfo(null, NetworkClientErrorCode.NetworkError, null, dataNodeId, false));
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

  private void increaseTimeoutForRequestIfNeeded(RequestInfo requestInfo, long streamInitiateTime,
      long streamAcquiredTime) {
    long inFlightRequestCount = requestsStatsHandler.getInFlightRequestCount();
    long additionalRequestTimeOutMs = (streamAcquiredTime - streamInitiateTime) + (
        (inFlightRequestCount / http2ClientConfig.http2RequestCountForScalingTimeout)
            * http2ClientConfig.http2RequestAdditionalTimeoutMs);
    requestInfo.incrementNetworkTimeOutMs(additionalRequestTimeOutMs);
  }

  private class StreamChannelInitializer extends ChannelInitializer {

    public void initChannel(Channel channel) {
      channel.pipeline().addLast(http2ClientStreamStatsHandler);
      // TODO: implement ourselves' aggregator. Http2Streams to Response Object
      channel.pipeline().addLast(http2StreamFrameToHttpObjectCodec);
      channel.pipeline().addLast(new HttpObjectAggregator(http2ClientConfig.http2MaxContentLength));
      channel.pipeline().addLast(ambrySendToHttp2Adaptor);
      // Add requests stats handler before response handler since the latter closes the channel after processing the message.
      channel.pipeline().addLast(requestsStatsHandler);
      channel.pipeline().addLast(http2ClientResponseHandler);
      // We log hashCode because frame id is -1 at this time.
      logger.trace("Handlers added to channel: {} {} ", channel.hashCode(), channel);
    }
  }
}
