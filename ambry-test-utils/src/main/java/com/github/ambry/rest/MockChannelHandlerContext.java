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

/**
 * Mock class for ChannelHandlerContext
 */

public class MockChannelHandlerContext implements ChannelHandlerContext {
  private final EmbeddedChannel embeddedChannel;

  public MockChannelHandlerContext(EmbeddedChannel embeddedChannel) {
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
