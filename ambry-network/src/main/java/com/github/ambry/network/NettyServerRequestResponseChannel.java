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
package com.github.ambry.network;

import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.NetworkConfig;
import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.protocol.RequestOrResponse;
import com.github.ambry.protocol.ServerRequestResponseUtil;
import com.github.ambry.protocol.Response;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.SystemTime;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RequestResponseChannel for Netty based server
 */
public class NettyServerRequestResponseChannel implements RequestResponseChannel {
  private static final Logger logger = LoggerFactory.getLogger(NettyServerRequestResponseChannel.class);
  private final Http2ServerMetrics http2ServerMetrics;
  private final NetworkRequestQueue networkRequestQueue;
  private final ServerMetrics serverMetrics;
  private final ServerRequestResponseUtil serverRequestResponseUtil;

  public NettyServerRequestResponseChannel(NetworkConfig config, Http2ServerMetrics http2ServerMetrics,
      ServerMetrics serverMetrics, ServerRequestResponseUtil serverRequestResponseUtil) {
    this.serverMetrics = serverMetrics;
    this.serverRequestResponseUtil = serverRequestResponseUtil;
    switch (config.requestQueueType) {
      case ADAPTIVE_QUEUE_WITH_LIFO_CO_DEL:
        this.networkRequestQueue = new AdaptiveLifoCoDelNetworkRequestQueue(config.adaptiveLifoQueueThreshold,
            config.adaptiveLifoQueueCodelTargetDelayMs, config.requestQueueTimeoutMs, SystemTime.getInstance(),
            config.requestQueueCapacity);
        break;
      case BASIC_QUEUE_WITH_FIFO:
        this.networkRequestQueue = new FifoNetworkRequestQueue(config.requestQueueTimeoutMs, SystemTime.getInstance(),
            config.requestQueueCapacity);
        break;
      default:
        throw new IllegalArgumentException("Queue type not supported by channel: " + config.requestQueueType);
    }
    this.http2ServerMetrics = http2ServerMetrics;
    serverMetrics.registerRequestQueuesMetrics(networkRequestQueue::size);
  }

  /**
   * Send a request to be handled
   */
  @Override
  public void sendRequest(NetworkRequest networkRequest) throws InterruptedException {
    if (networkRequestQueue.offer(networkRequest)) {
      http2ServerMetrics.requestEnqueueTime.update(System.currentTimeMillis() - networkRequest.getStartTimeInMs());
    } else {
      // If incoming request queue is full, reject the request.
      rejectRequest(networkRequest);
    }
  }

  /** Send a response back via netty outbound handlers */
  @Override
  public void sendResponse(Send payloadToSend, NetworkRequest originalRequest, ServerNetworkResponseMetrics metrics)
      throws InterruptedException {

    if (!(originalRequest instanceof NettyServerRequest)) {
      throw new IllegalArgumentException("NetworkRequest should be NettyRequest");
    }
    ChannelHandlerContext ctx = ((NettyServerRequest) originalRequest).getCtx();
    http2ServerMetrics.requestTotalProcessingTime.update(
        System.currentTimeMillis() - originalRequest.getStartTimeInMs());
    long sendStartTime = System.currentTimeMillis();
    ChannelFutureListener channelFutureListener = new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        long responseFlushTime = System.currentTimeMillis() - sendStartTime;
        http2ServerMetrics.responseFlushTime.update(responseFlushTime);
        if (metrics != null) {
          metrics.updateSendTime(responseFlushTime);
        }
      }
    };
    ctx.channel().writeAndFlush(payloadToSend).addListener(channelFutureListener);
  }

  /**
   * Closes the connection and does not send any response
   */
  @Override
  public void closeConnection(NetworkRequest originalRequest) {
    ChannelHandlerContext context = ((NettyServerRequest) originalRequest).getCtx();
    if (context != null) {
      logger.trace("close connection " + context.channel());
      context.channel().close();
    }
  }

  @Override
  public NetworkRequest receiveRequest() throws InterruptedException {
    while (true) {
      NetworkRequest request = networkRequestQueue.take();
      http2ServerMetrics.requestQueuingTime.update(System.currentTimeMillis() - request.getStartTimeInMs());
      if (networkRequestQueue.isExpired(request)) {
        // If the request is stale, it means that server is overloaded and unable to process them in time. Reject the
        // request with backoff error code.
        rejectRequest(request);
        continue;
      }
      return request;
    }
  }

  /**
   * Shuts down the request response channel and release any resources in {@link NetworkRequestQueue}.
   */
  @Override
  public void shutdown() {
    networkRequestQueue.close();
  }

  /**
   * Reject request with backoff error code.
   * @param networkRequest incoming request
   */
  private void rejectRequest(NetworkRequest networkRequest) {
    RequestOrResponse request;
    try {
      request = serverRequestResponseUtil.getDecodedRequest(networkRequest);
      Response response = serverRequestResponseUtil.createErrorResponse(request, ServerErrorCode.Retry_After_Backoff);
      serverMetrics.totalRequestDroppedRate.mark();
      sendResponse(response, networkRequest, null);
    } catch (IOException | InterruptedException e) {
      closeConnection(networkRequest);
    } finally {
      networkRequest.release();
    }
  }
}


