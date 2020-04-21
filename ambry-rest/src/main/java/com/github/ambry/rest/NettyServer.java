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

import com.github.ambry.config.NettyConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link NioServer}.
 * <p/>
 * Responsible for accepting connections from clients, decoding HTTP data and passing them on to services that can
 * generate responses via {@link NettyMessageProcessor}.
 * <p/>
 * The accompanying framework also provides a Netty specific implementation of {@link RestResponseChannel}
 * ({@link NettyResponseChannel}) for writing responses to clients.
 * <p/>
 * This implementation creates a pipeline of handlers for every connection that it accepts and the last inbound handler,
 * {@link NettyMessageProcessor}, is responsible for processing the inbound requests and passing them to services that
 * can generate a response.
 */
public class NettyServer implements NioServer {
  private final NettyConfig nettyConfig;
  private final NettyMetrics nettyMetrics;
  private final Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  /**
   * Creates a new instance of NettyServer.
   * @param nettyConfig the {@link NettyConfig} instance that defines the configuration parameters for the NettyServer.
   * @param nettyMetrics the {@link NettyMetrics} instance to use to record metrics.
   * @param channelInitializers a {@link Map} from port number to the {@link ChannelInitializer} used to initialize
   *                            a new channel on that port.
   */
  public NettyServer(NettyConfig nettyConfig, NettyMetrics nettyMetrics,
      Map<Integer, ChannelInitializer<SocketChannel>> channelInitializers) {
    this.nettyConfig = nettyConfig;
    this.nettyMetrics = nettyMetrics;
    this.channelInitializers = channelInitializers;
    NettyRequest.bufferWatermark = nettyConfig.nettyServerRequestBufferWatermark;
    logger.trace("Instantiated NettyServer");
  }

  @Override
  public void start() throws InstantiationException {
    long startupBeginTime = System.currentTimeMillis();
    try {
      logger.trace("Starting NettyServer deployment");
      if (Epoll.isAvailable()) {
        logger.trace("Using EpollEventLoopGroup in NettyServer.");
        bossGroup = new EpollEventLoopGroup(nettyConfig.nettyServerBossThreadCount);
        workerGroup = new EpollEventLoopGroup(nettyConfig.nettyServerWorkerThreadCount);
      } else {
        bossGroup = new NioEventLoopGroup(nettyConfig.nettyServerBossThreadCount);
        workerGroup = new NioEventLoopGroup(nettyConfig.nettyServerWorkerThreadCount);
      }
      for (Map.Entry<Integer, ChannelInitializer<SocketChannel>> entry : channelInitializers.entrySet()) {
        bindServer(entry.getKey(), entry.getValue(), bossGroup, workerGroup);
      }
    } catch (InterruptedException e) {
      logger.error("NettyServer start await was interrupted", e);
      nettyMetrics.nettyServerStartError.inc();
      throw new InstantiationException(
          "Netty server bind to port [" + nettyConfig.nettyServerPort + "] was interrupted");
    } finally {
      long startupTime = System.currentTimeMillis() - startupBeginTime;
      logger.info("NettyServer start took {} ms", startupTime);
      nettyMetrics.nettyServerStartTimeInMs.update(startupTime);
    }
  }

  @Override
  public void shutdown() {
    logger.info("Shutting down NettyServer");
    if (bossGroup != null && workerGroup != null && (!bossGroup.isTerminated() || !workerGroup.isTerminated())) {
      long shutdownBeginTime = System.currentTimeMillis();
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
      try {
        if (!(workerGroup.awaitTermination(30, TimeUnit.SECONDS) && bossGroup.awaitTermination(30, TimeUnit.SECONDS))) {
          logger.error("NettyServer shutdown failed after waiting for 30 seconds");
          nettyMetrics.nettyServerShutdownError.inc();
        }
      } catch (InterruptedException e) {
        logger.error("NettyServer termination await was interrupted. Shutdown may have been unsuccessful", e);
        nettyMetrics.nettyServerShutdownError.inc();
      } finally {
        long shutdownTime = System.currentTimeMillis() - shutdownBeginTime;
        logger.info("NettyServer shutdown took {} ms", shutdownTime);
        nettyMetrics.nettyServerShutdownTimeInMs.update(shutdownTime);
      }
    }
  }

  /**
   * Bootstrap a new server with a {@link ChannelInitializer} and bind it to a port.
   * @param port the port number to bind this server to.
   * @param channelInitializer the {@link ChannelInitializer} for request handling on this server.
   * @param bossGroup the pool of boss threads that this server uses.
   * @param workerGroup the pool of worker threads that this server uses.
   * @throws InterruptedException if binding to the port failed.
   */
  private void bindServer(int port, ChannelInitializer<SocketChannel> channelInitializer, EventLoopGroup bossGroup,
      EventLoopGroup workerGroup) throws InterruptedException {
    ServerBootstrap b = new ServerBootstrap();
    Class<? extends ServerSocketChannel> channelClass =
        Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    // Note: ServerSocketChannel option doesn't apply to SocketChannel
    b.group(bossGroup, workerGroup)
        .channel(channelClass)
        .option(ChannelOption.SO_BACKLOG, nettyConfig.nettyServerSoBacklog)
        .handler(new LoggingHandler(LogLevel.DEBUG))
        .childHandler(channelInitializer);
    b.bind(port).sync();
    logger.info("NettyServer now listening on port {}", port);
  }
}
