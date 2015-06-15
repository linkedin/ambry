package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.NioServer;
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
 * Netty specific implementation of NioServer. Handles Http for the underlying BlobStorageService.
 */
public class NettyServer implements NioServer {
  private final NettyConfig serverConfig;
  private final NettyMetrics nettyMetrics;
  /**
   * This the object through which a RestMessageHandler can be requested.
   */
  private final RestRequestDelegator requestDelegator;

  private final Logger logger = LoggerFactory.getLogger(getClass());

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
      // deploy netty server as a thread and catch startup exceptions if any.
      AtomicReference<Exception> startupException = new AtomicReference<Exception>();
      NettyServerDeployer nettyServerDeployer = new NettyServerDeployer(startupException);
      Thread deploymentThread = new Thread(nettyServerDeployer);
      deploymentThread.start();

      long startWaitSecs = serverConfig.getStartupWaitSeconds();
      if (!(nettyServerDeployer.awaitStartup(startWaitSecs, TimeUnit.SECONDS))) {
        throw new InstantiationException("Netty server failed to start in " + startWaitSecs + " seconds");
      } else if (startupException.get() != null) {
        throw new InstantiationException("Netty server start failed - " + startupException.get());
      }
    } catch (InterruptedException e) {
      logger.error("Netty server start might have failed - " + e);
      throw new InstantiationException("Netty server start might have failed - " + e);
    }
  }

  public void shutdown() {
    if (bossGroup != null && workerGroup != null) {
      logger.info("Shutting down netty server..");
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
      try {
        if (!awaitTermination(60, TimeUnit.SECONDS)) {
          logger.error("NettyServer shutdown failed after waiting for 60 seconds");
        } else {
          bossGroup = null;
          workerGroup = null;
          logger.info("Netty server shutdown");
        }
      } catch (InterruptedException e) {
        logger.error("Termination await interrupted while attempting to shutdown NettyServer - " + e);
      }
    }
  }

  private boolean awaitTermination(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return workerGroup.awaitTermination(timeout / 2, timeUnit) && bossGroup.awaitTermination(timeout / 2, timeUnit);
  }

  /**
   * Deploys netty http server.
   */
  private class NettyServerDeployer implements Runnable {
    private CountDownLatch startupDone = new CountDownLatch(1);
    /**
     * To record an exceptions at startup.
     */
    private AtomicReference<Exception> exception;

    public NettyServerDeployer(AtomicReference<Exception> exception) {
      this.exception = exception;
    }

    public void run() {
      try {
        ServerBootstrap b = new ServerBootstrap();

        // Netty creates a new instance of every class in the pipeline for every connection
        // i.e. if there are a 1000 active connections there will be a 1000 NettyMessageProcessor instances.
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, serverConfig.getSoBacklog()).handler(new LoggingHandler(LogLevel.INFO))
            .childHandler(new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(SocketChannel ch)
                  throws Exception {
                ch.pipeline()
                    // for http encoding/decoding.
                    .addLast("codec", new HttpServerCodec())
                        // for chunking. TODO: this is not leveraged by us yet. We will do it when doing GET blob.
                    .addLast("chunker", new ChunkedWriteHandler())
                        // for detecting connections that have been idle too long - probably because of an error.
                    .addLast("idleStateHandler", new IdleStateHandler(0, 0, serverConfig.getIdleTimeSeconds()))
                        // custom processing class that interfaces with a BlobStorageService.
                    .addLast("processor", new NettyMessageProcessor(requestDelegator, nettyMetrics));
              }
            });

        ChannelFuture f = b.bind(serverConfig.getPort()).sync();
        logger.info("Netty server started on port " + serverConfig.getPort());
        startupDone.countDown(); // let the parent know that startup is complete

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
