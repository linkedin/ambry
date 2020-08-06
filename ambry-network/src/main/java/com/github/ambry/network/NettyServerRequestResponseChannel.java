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

import com.github.ambry.network.http2.Http2ServerMetrics;
import com.github.ambry.server.EmptyRequest;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ArrayBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RequestResponseChannel for Netty based server
 */
public class NettyServerRequestResponseChannel implements RequestResponseChannel {
  private static final Logger logger = LoggerFactory.getLogger(NettyServerRequestResponseChannel.class);
  private final ArrayBlockingQueue<NetworkRequest> requestQueue;
  private final Http2ServerMetrics http2ServerMetrics;

  public NettyServerRequestResponseChannel(int queueSize, Http2ServerMetrics http2ServerMetrics) {
    requestQueue = new ArrayBlockingQueue<>(queueSize);
    this.http2ServerMetrics = http2ServerMetrics;
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  @Override
  public void sendRequest(NetworkRequest request) throws InterruptedException {
    requestQueue.put(request);
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
    ChannelFutureListener channelFutureListener = new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {
          if (!(future.cause() instanceof ClosedChannelException)) {
            // TODO: a round solution is needed. For now, use same logic as Http2NetworkClient in frontend.
            payloadToSend.release();
          }
        }
      }
    };
    ctx.channel().writeAndFlush(payloadToSend).addListener(channelFutureListener);
  }

  /**
   * Closes the connection and does not send any response
   */
  @Override
  public void closeConnection(NetworkRequest originalRequest) throws InterruptedException {
    //TODO: close connection
  }

  /** Get the next request or block until there is one */
  @Override
  public NetworkRequest receiveRequest() throws InterruptedException {

    NetworkRequest request = requestQueue.take();
    http2ServerMetrics.requestQueuingTime.update(System.currentTimeMillis() - request.getStartTimeInMs());
    if (request.equals(EmptyRequest.getInstance())) {
      logger.debug("Request handler {} received shut down command ", request);
    } else {
      DataInputStream stream = new DataInputStream(request.getInputStream());
      try {
        // The first 8 bytes is size of the request. TCP implementation uses this size to allocate buffer. See {@link BoundedReceive}
        // Here we just need to consume it.
        stream.readLong();
      } catch (IOException e) {
        throw new IllegalStateException("stream read error." + e);
      }
    }
    return request;
  }

  /**
   * Shuts down the request response channel
   */
  @Override
  public void shutdown() {
    return;
  }
}


