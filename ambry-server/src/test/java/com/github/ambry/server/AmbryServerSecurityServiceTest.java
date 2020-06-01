/*
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

package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.ServerMetrics;
import com.github.ambry.config.ServerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.protocol.AmbryRequests;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingConsumer;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelProgressivePromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Test;


public class AmbryServerSecurityServiceTest {
  private final ServerSecurityService serverSecurityService =
      new AmbryServerSecurityService(new ServerConfig(new VerifiableProperties(new Properties())),
          new ServerMetrics(new MetricRegistry(), AmbryRequests.class, AmbryServer.class));


  @Test
  public void validateConnectionTest() throws Exception {
    //ctx is null
    TestUtils.assertException(IllegalArgumentException.class,
        () -> serverSecurityService.validateConnection(null).get(), null);

    //success case
    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelHandlerContext ctx = new MockChannelHandlerContext(channel);
    try {
      serverSecurityService.validateConnection(ctx, (r, e) -> {
        Assert.assertNull("result not null", r);
        Assert.assertNull("exception not null", e);
      });
    } catch (Exception e) {
      Assert.fail("unexpected exception happened" + e);
    }


    //service is closed
    serverSecurityService.close();
    ThrowingConsumer<ExecutionException> errorAction = e -> {
      Assert.assertTrue("Exception should have been an instance of RestServiceException",
          e.getCause() instanceof RestServiceException);
      RestServiceException re = (RestServiceException) e.getCause();
      Assert.assertEquals("Unexpected RestServerErrorCode (Future)", RestServiceErrorCode.ServiceUnavailable,
          re.getErrorCode());
    };
    TestUtils.assertException(ExecutionException.class, () -> serverSecurityService.validateConnection(ctx).get(),
        errorAction);
  }


  @Test
  public void validateRequestTest() throws Exception {
    //request is null
    TestUtils.assertException(IllegalArgumentException.class,
        () -> serverSecurityService.validateRequest(null).get(), null);

    //success case
    RestRequest request = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    EmbeddedChannel channel = new EmbeddedChannel();
    ChannelHandlerContext ctx = new MockChannelHandlerContext(channel);
    try {
      serverSecurityService.validateRequest(request, (r, e) -> {
        Assert.assertNull("result not null", r);
        Assert.assertNull("exception not null", e);
      });
    } catch (Exception e) {
      Assert.fail("unexpected exception happened" + e);
    }


    //service is closed
    serverSecurityService.close();
    ThrowingConsumer<ExecutionException> errorAction = e -> {
      Assert.assertTrue("Exception should have been an instance of RestServiceException",
          e.getCause() instanceof RestServiceException);
      RestServiceException re = (RestServiceException) e.getCause();
      Assert.assertEquals("Unexpected RestServerErrorCode (Future)", RestServiceErrorCode.ServiceUnavailable,
          re.getErrorCode());
    };
    TestUtils.assertException(ExecutionException.class, () -> serverSecurityService.validateRequest(request).get(),
        errorAction);
  }


}

/**
 * Mock class for ChannelHandlerContext used in channelInactiveTest.
 */
class MockChannelHandlerContext implements ChannelHandlerContext {
  private final EmbeddedChannel embeddedChannel;

  MockChannelHandlerContext(EmbeddedChannel embeddedChannel) {
    this.embeddedChannel = embeddedChannel;
  }

  @Override
  public Channel channel() {
    return embeddedChannel;
  }

  @Override
  public EventExecutor executor() {
    return null;
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public ChannelHandler handler() {
    return null;
  }

  @Override
  public boolean isRemoved() {
    return false;
  }

  @Override
  public ChannelHandlerContext fireChannelRegistered() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelUnregistered() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelActive() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelInactive() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
    return null;
  }

  @Override
  public ChannelHandlerContext fireUserEventTriggered(Object evt) {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelRead(Object msg) {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelReadComplete() {
    return null;
  }

  @Override
  public ChannelHandlerContext fireChannelWritabilityChanged() {
    return null;
  }

  @Override
  public ChannelFuture bind(SocketAddress localAddress) {
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress) {
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
    return null;
  }

  @Override
  public ChannelFuture disconnect() {
    return null;
  }

  @Override
  public ChannelFuture close() {
    return null;
  }

  @Override
  public ChannelFuture deregister() {
    return null;
  }

  @Override
  public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
    return null;
  }

  @Override
  public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
    return null;
  }

  @Override
  public ChannelFuture disconnect(ChannelPromise promise) {
    return null;
  }

  @Override
  public ChannelFuture close(ChannelPromise promise) {
    return null;
  }

  @Override
  public ChannelFuture deregister(ChannelPromise promise) {
    return null;
  }

  @Override
  public ChannelHandlerContext read() {
    return null;
  }

  @Override
  public ChannelFuture write(Object msg) {
    return null;
  }

  @Override
  public ChannelFuture write(Object msg, ChannelPromise promise) {
    return null;
  }

  @Override
  public ChannelHandlerContext flush() {
    return null;
  }

  @Override
  public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
    return null;
  }

  @Override
  public ChannelFuture writeAndFlush(Object msg) {
    return null;
  }

  @Override
  public ChannelPromise newPromise() {
    return null;
  }

  @Override
  public ChannelProgressivePromise newProgressivePromise() {
    return new DefaultChannelProgressivePromise(embeddedChannel);
  }

  @Override
  public ChannelFuture newSucceededFuture() {
    return null;
  }

  @Override
  public ChannelFuture newFailedFuture(Throwable cause) {
    return null;
  }

  @Override
  public ChannelPromise voidPromise() {
    return null;
  }

  @Override
  public ChannelPipeline pipeline() {
    return embeddedChannel.pipeline();
  }

  @Override
  public ByteBufAllocator alloc() {
    return null;
  }

  @Override
  public <T> Attribute<T> attr(AttributeKey<T> key) {
    return null;
  }

  @Override
  public <T> boolean hasAttr(AttributeKey<T> key) {
    return false;
  }
}
