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

import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.Closeable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Netty client to send requests and receive responses in tests. A single instance can handle only a single request in
 * flight at any point in time.
 */
public class NettyClient implements Closeable {
  private final String hostname;
  private final int port;

  private final EventLoopGroup group = new NioEventLoopGroup();
  private final Bootstrap b = new Bootstrap();
  private final Queue<HttpObject> responseParts = new LinkedList<HttpObject>();
  private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
  private final RequestSender requestSender = new RequestSender();
  private final WriteResultListener writeResultListener = new WriteResultListener();
  private final ChannelCloseListener channelCloseListener = new ChannelCloseListener();
  private final CommunicationHandler communicationHandler = new CommunicationHandler();
  private final AtomicBoolean isOpen = new AtomicBoolean(true);

  private HttpRequest request;
  private ChunkedInput<HttpContent> content;
  private FutureResult<Queue<HttpObject>> responseFuture;
  private Callback<Queue<HttpObject>> callback;

  private volatile ChannelFuture channelConnectFuture;
  private volatile Exception exception = null;
  private volatile boolean isKeepAlive = false;

  /**
   * Create a NettyClient.
   * @param hostname the host to connect to.
   * @param port the port to connect to.
   */
  public NettyClient(String hostname, int port) throws InterruptedException {
    this.hostname = hostname;
    this.port = port;
    b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new HttpClientCodec()).addLast(new ChunkedWriteHandler()).addLast(communicationHandler);
      }
    });
    createChannel();
  }

  /**
   * Sends the request and content to the server and returns a {@link Future} that tracks the arrival of a response for
   * the request. The {@link Future} and {@code callback} are triggered when the response is complete.
   * <p/>
   * Be sure to decrease the reference counts of each of the received response parts via a call to
   * {@link ReferenceCountUtil#release(Object)} once the part has been processed. Neglecting to do so might result in
   * OOM.
   * @param request the request that needs to be sent.
   * @param content the content accompanying the request. Can be null.
   * @param callback the callback to invoke when the response is available. Can be null.
   * @return a {@link Future} that tracks the arrival of the response for this request.
   */
  public Future<Queue<HttpObject>> sendRequest(HttpRequest request, ChunkedInput<HttpContent> content,
      Callback<Queue<HttpObject>> callback) {
    this.request = request;
    this.content = content;
    this.callback = callback;
    resetState();
    channelConnectFuture.addListener(requestSender);
    return responseFuture;
  }

  /**
   * Closes the netty client.
   * @throws IllegalStateException if the client did not close within a timeout or was interrupted while closing.
   */
  @Override
  public void close() {
    if (isOpen.compareAndSet(true, false) && !group.isTerminated()) {
      group.shutdownGracefully();
      try {
        if (!group.awaitTermination(30, TimeUnit.SECONDS)) {
          throw new IllegalStateException("Client did not close within timeout");
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Resets the request tracking state of the client.
   */
  private void resetState() {
    responseFuture = new FutureResult<Queue<HttpObject>>();
    responseParts.clear();
    exception = null;
    callbackInvoked.set(false);
  }

  /**
   * Connects to the server at {@link #hostname}:{@link #port} and sets {@link #channelConnectFuture} that tracks the
   * success of the connect.
   * @throws InterruptedException if the connect is interrupted.
   */
  private void createChannel() throws InterruptedException {
    channelConnectFuture = b.connect(hostname, port);
    // add a listener to create a new channel if this channel disconnects.
    ChannelFuture channelCloseFuture = channelConnectFuture.channel().closeFuture();
    channelCloseFuture.addListener(channelCloseListener);
  }

  /**
   * Invokes the future and callback associated with the current request.
   */
  private void invokeFutureAndCallback() {
    if (callbackInvoked.compareAndSet(false, true)) {
      responseFuture.done(responseParts, exception);
      if (callback != null) {
        callback.onCompletion(responseParts, exception);
      }
    }
  }

  /**
   * Sends the {@link #request} and {@link #content} if the channel connects successfully.
   */
  private class RequestSender implements GenericFutureListener<ChannelFuture> {

    @Override
    public void operationComplete(ChannelFuture future) {
      if (future.isSuccess()) {
        future.channel().write(request).addListener(writeResultListener);
        if (content != null) {
          future.channel().write(content).addListener(writeResultListener);
        }
        future.channel().flush();
      } else {
        exception = (Exception) future.cause();
        invokeFutureAndCallback();
      }
    }
  }

  /**
   * Listens to results of the write to channel.
   */
  private class WriteResultListener implements GenericFutureListener<ChannelFuture> {

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        exception = (Exception) future.cause();
        invokeFutureAndCallback();
      }
    }
  }

  /**
   * Listens to close of a channel and initiates connect for a new channel.
   */
  private class ChannelCloseListener implements GenericFutureListener<ChannelFuture> {

    @Override
    public void operationComplete(ChannelFuture future) throws InterruptedException {
      if (isOpen.get()) {
        createChannel();
      }
      invokeFutureAndCallback();
    }
  }

  /**
   * Custom handler that receives the response.
   */
  @ChannelHandler.Sharable
  private class CommunicationHandler extends SimpleChannelInboundHandler<HttpObject> {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject in) {
      // Make sure that we increase refCnt because we are going to process it async. The other end has to release
      // after processing.
      responseParts.offer(ReferenceCountUtil.retain(in));
      if (in instanceof HttpResponse && in.getDecoderResult().isSuccess()) {
        isKeepAlive = HttpHeaders.isKeepAlive((HttpResponse) in);
      } else if (in.getDecoderResult().isFailure()) {
        Throwable cause = in.getDecoderResult().cause();
        if (cause instanceof Exception) {
          exception = (Exception) cause;
        } else {
          exception =
              new Exception("Encountered Throwable when trying to decode response. Message: " + cause.getMessage());
        }
        invokeFutureAndCallback();
      }
      if (in instanceof LastHttpContent) {
        if (isKeepAlive) {
          invokeFutureAndCallback();
        } else {
          // if not, the future will be invoked when the channel is closed.
          ctx.close();
        }
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (cause instanceof Exception) {
        exception = (Exception) cause;
        ctx.close();
      } else {
        ctx.fireExceptionCaught(cause);
      }
    }
  }
}
