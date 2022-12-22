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
import com.github.ambry.server.EmptyRequest;
import com.github.ambry.utils.SystemTime;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RequestResponseChannel for Netty based server
 */
public class NettyServerRequestResponseChannel implements RequestResponseChannel {
  private static final Logger logger = LoggerFactory.getLogger(NettyServerRequestResponseChannel.class);
  private final Http2ServerMetrics http2ServerMetrics;
  private final NetworkRequestQueue networkRequestQueue;

  public NettyServerRequestResponseChannel(NetworkConfig config, Http2ServerMetrics http2ServerMetrics,
      ServerMetrics serverMetrics) {
    switch (config.requestQueueType) {
      case ADAPTIVE_QUEUE_WITH_LIFO_CO_DEL:
        this.networkRequestQueue = new AdaptiveLifoCoDelNetworkRequestQueue(config.adaptiveLifoQueueThreshold,
            config.adaptiveLifoQueueCodelTargetDelayMs, config.requestQueueTimeoutMs, SystemTime.getInstance());
        break;
      case BASIC_QUEUE_WITH_FIFO:
        this.networkRequestQueue = new FifoNetworkRequestQueue(config.requestQueueTimeoutMs, SystemTime.getInstance());
        break;
      default:
        throw new IllegalArgumentException("Queue type not supported by channel: " + config.requestQueueType);
    }
    this.http2ServerMetrics = http2ServerMetrics;
    serverMetrics.registerRequestQueuesMetrics(networkRequestQueue::numActiveRequests,
        networkRequestQueue::numDroppedRequests);
  }

  /** Send a request to be handled */
  @Override
  public void sendRequest(NetworkRequest request) throws InterruptedException {
    networkRequestQueue.offer(request);
    http2ServerMetrics.requestEnqueueTime.update(System.currentTimeMillis() - request.getStartTimeInMs());
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
        metrics.updateSendTime(responseFlushTime);
      }
    };
    ctx.channel().writeAndFlush(payloadToSend).addListener(channelFutureListener);
  }

  /**
   * Closes the connection and does not send any response
   */
  @Override
  public void closeConnection(NetworkRequest originalRequest) throws InterruptedException {
    ChannelHandlerContext context = ((NettyServerRequest) originalRequest).getCtx();
    if (context != null) {
      logger.trace("close connection " + context.channel());
      context.channel().close();
    }
  }

  @Override
  public NetworkRequest receiveRequest() throws InterruptedException {
    NetworkRequest request;
    while (true) {
      request = networkRequestQueue.take();
      http2ServerMetrics.requestQueuingTime.update(System.currentTimeMillis() - request.getStartTimeInMs());
      if (consumeSizeHeader(request)) {
        return request;
      }
    }
  }

  @Override
  public List<NetworkRequest> getDroppedRequests() throws InterruptedException {
    while (true) {
      List<NetworkRequest> droppedRequests = networkRequestQueue.getDroppedRequests();
      List<NetworkRequest> validDroppedRequests = new ArrayList<>();
      for (NetworkRequest requestToDrop : droppedRequests) {
        if (consumeSizeHeader(requestToDrop)) {
          validDroppedRequests.add(requestToDrop);
        }
      }
      if (!validDroppedRequests.isEmpty()) {
        return validDroppedRequests;
      }
    }
  }

  /**
   * Consume the first 8 bytes of the input stream which represents the size of the request. This is added to the
   * request since Ambry's {@link SocketServer} stack needs to know the number of the bytes to read for a request
   * (see {@link BoundedNettyByteBufReceive#readFrom(ReadableByteChannel)} method). This is not needed when using
   * Netty HTTP2 server stack. We can remove this once {@link SocketServer} stack is retired. For now, we are
   * consuming the bytes here so that rest of request handling in server is same.
   * @param request incoming network request.
   * @return false if there was any error while reading the first 8 bytes. Else, return true.
   */
  private boolean consumeSizeHeader(NetworkRequest request) throws InterruptedException {
    if (request.equals(EmptyRequest.getInstance())) {
      logger.debug("Request handler {} received shut down command ", request);
    } else {
      DataInputStream stream = new DataInputStream(request.getInputStream());
      try {
        stream.readLong();
      } catch (IOException e) {
        logger.error("Encountered an error while reading length out of request {}, close the connection", request, e);
        closeConnection(request);
        // Release the memory held by this request.
        request.release();
        return false;
      }
    }
    return true;
  }

  /**
   * Shuts down the request response channel and release any resources in {@link NetworkRequestQueue}.
   */
  @Override
  public void shutdown() {
    networkRequestQueue.close();
  }
}


