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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
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
  private final ChannelInitializer<SocketChannel> channelInitializer;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  /**
   * Creates a new instance of NettyServer.
   * @param nettyConfig the {@link NettyConfig} instance that defines the configuration parameters for the NettyServer.
   * @param nettyMetrics the {@link NettyMetrics} instance to use to record metrics.
   * @param channelInitializer A {@link ChannelInitializer} that is used to initialize new channels.
   */
  public NettyServer(NettyConfig nettyConfig, NettyMetrics nettyMetrics,
      ChannelInitializer<SocketChannel> channelInitializer) {
    this.nettyConfig = nettyConfig;
    this.nettyMetrics = nettyMetrics;
    this.channelInitializer = channelInitializer;
    NettyRequest.bufferWatermark = nettyConfig.nettyServerRequestBufferWatermark;
    logger.trace("Instantiated NettyServer");
  }

  @Override
  public void start() throws InstantiationException {
    long startupBeginTime = System.currentTimeMillis();
    try {
      logger.trace("Starting NettyServer deployment");
      bossGroup = new NioEventLoopGroup(nettyConfig.nettyServerBossThreadCount);
      workerGroup = new NioEventLoopGroup(nettyConfig.nettyServerWorkerThreadCount);
      ServerBootstrap b = new ServerBootstrap();
      // Netty creates a new instance of every class in the pipeline for every connection
      // i.e. if there are a 1000 active connections there will be a 1000 NettyMessageProcessor instances.
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, nettyConfig.nettyServerSoBacklog)
          .handler(new LoggingHandler(LogLevel.DEBUG))
          .childHandler(channelInitializer);
      b.bind(nettyConfig.nettyServerPort).sync();
      logger.info("NettyServer now listening on port {}", nettyConfig.nettyServerPort);
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
}
