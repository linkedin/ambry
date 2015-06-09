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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
      throws InstantiationException {
    serverConfig = new NettyConfig(verifiableProperties);
    nettyMetrics = new NettyMetrics(metricRegistry);
    this.requestDelegator = requestDelegator;
  }

  public void start()
      throws InstantiationException {
    logger.info("Netty server starting up");

    bossGroup = new NioEventLoopGroup(serverConfig.getBossThreadCount());
    workerGroup = new NioEventLoopGroup(serverConfig.getWorkerThreadCount());

    try {
      AtomicReference<Exception> startupException = new AtomicReference<Exception>();
      NettyServerDeployer nettyServerDeployer = new NettyServerDeployer(startupException);
      Thread deploymentThread = new Thread(nettyServerDeployer);
      deploymentThread.start();
      long startWaitSecs = serverConfig.getStartupWaitSeconds();
      if(!(nettyServerDeployer.awaitStartup(startWaitSecs, TimeUnit.SECONDS))) {
        throw new InstantiationException("Netty server failed to start in " + startWaitSecs + " seconds" );
      } else if (startupException.get() != null) {
        throw new InstantiationException("Netty server start failed - " + startupException.get());
      }
    } catch (InterruptedException e) {
      logger.error("Netty server start might have failed - " + e);
      throw new InstantiationException("Netty server start might have failed - " + e);
    }
  }

  public void shutdown()
      throws Exception {
    if (bossGroup != null && workerGroup != null) {
      logger.info("Shutting down netty server..");
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
      if (!awaitTermination(60, TimeUnit.SECONDS)) {
        throw new Exception("NettyServer shutdown failed after waiting for 60 seconds");
      }
      bossGroup = null;
      workerGroup = null;
      logger.info("Netty server shutdown");
    }
  }

  private boolean awaitTermination(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return workerGroup.awaitTermination(timeout / 2, timeUnit) && bossGroup.awaitTermination(timeout / 2, timeUnit);
  }

  private class NettyServerDeployer implements Runnable {
    private CountDownLatch startupDone = new CountDownLatch(1);
    private AtomicReference<Exception> exception;

    public NettyServerDeployer(AtomicReference<Exception> exception) {
      this.exception = exception;
    }

    public void run() {
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
        startupDone.countDown();

        f.channel().closeFuture().sync(); // this is blocking
      } catch (Exception e) {
        logger.error("Netty server start failed - " + e);
        exception.set(e);
        startupDone.countDown();
      }
    }

    public boolean awaitStartup(long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      return startupDone.await(timeout, timeUnit);
    }
  }
}
