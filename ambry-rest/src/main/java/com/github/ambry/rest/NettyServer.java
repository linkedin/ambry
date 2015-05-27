package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of RestServer. Handles Http for Ambry.
 */
public class NettyServer implements RestServer {
  private final NettyConfig serverConfig;
  private final NettyMetrics nettyMetrics;
  private final RestRequestDelegator requestDelegator;

  private Logger logger = LoggerFactory.getLogger(getClass());

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  public NettyServer(VerifiableProperties verifiableProperties, MetricRegistry metricRegistry,
      RestRequestDelegator requestDelegator)
      throws Exception {
    serverConfig = new NettyConfig(verifiableProperties);
    nettyMetrics = new NettyMetrics(metricRegistry);
    this.requestDelegator = requestDelegator;
  }

  public void start()
      throws Exception {
    logger.info("Netty server starting up");

    bossGroup = new NioEventLoopGroup(serverConfig.getBossThreadCount());
    workerGroup = new NioEventLoopGroup(serverConfig.getWorkerThreadCount());

    try {
      ServerBootstrap b = new ServerBootstrap();

      // Right now this pipeline suffices. The old AmbryFrontEnd has a factory for this. Will investigate
      // if we need any new functionality.
      b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, serverConfig.getSoBacklog()).handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch)
                throws Exception {
              ch.pipeline().addLast("codec", new HttpServerCodec()).addLast("chunker", new ChunkedWriteHandler())
                  .addLast("idleStateHandler", new IdleStateHandler(0, 0, serverConfig.getIdleTimeSeconds()))
                  .addLast("processor", new NettyMessageProcessor(requestDelegator, nettyMetrics));
            }
          });

      ChannelFuture f = b.bind(serverConfig.getPort()).sync();
      logger.info("Netty server started on port " + serverConfig.getPort());

      f.channel().closeFuture().sync();
    } catch (Exception e) {
      logger.error("Netty server start failed - " + e);
      throw new InstantiationException("Netty server start failed");
    } finally {
      // this is recommended in netty. But will see if this is something that the rest server user will need to handle.
      shutdown();
    }
  }

  public void shutdown()
      throws Exception {
    logger.info("Shutting down netty server..");
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    logger.info("Netty server shutdown complete..");
  }
}
