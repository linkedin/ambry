package com.github.ambry.rest;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Netty specific implementation of {@link NioServer}.
 * <p/>
 * Responsible for accepting connections from clients, decoding HTTP data, passing them on the underlying
 * {@link BlobStorageService} and providing a Netty specific implementation of
 * {@link RestResponseHandler} ({@link NettyResponseHandler}) for writing responses to
 * clients.
 * <p/>
 * This implementation creates a pipeline of handlers for every connection that it accepts and the last inbound handler,
 * {@link NettyMessageProcessor}, is responsible for invoking a {@link RestRequestHandler}.
 * <p/>
 * Each {@link NettyMessageProcessor} instance makes use of the {@link RestRequestHandlerController} provided to request
 * a {@link RestRequestHandler}.
 */
class NettyServer implements NioServer {
  private final NettyConfig nettyConfig;
  private final NettyMetrics nettyMetrics;
  private final NettyServerDeployer nettyServerDeployer;
  private final Thread nettyServerDeployerThread;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public NettyServer(NettyConfig nettyConfig, NettyMetrics nettyMetrics,
      RestRequestHandlerController requestHandlerController) {
    this.nettyConfig = nettyConfig;
    this.nettyMetrics = nettyMetrics;
    nettyServerDeployer = new NettyServerDeployer(nettyConfig, nettyMetrics, requestHandlerController);
    nettyServerDeployerThread = new Thread(nettyServerDeployer);
  }

  @Override
  public void start()
      throws InstantiationException {
    if (!nettyServerDeployerThread.isAlive()) {
      logger.info("Starting NettyServer..");
      try {
        nettyServerDeployerThread.start();
        if (!(nettyServerDeployer.awaitStartup(nettyConfig.nettyServerStartupWaitSeconds, TimeUnit.SECONDS))) {
          throw new InstantiationException(
              "NettyServer failed to start in " + nettyConfig.nettyServerStartupWaitSeconds + " seconds");
        } else if (nettyServerDeployer.getException() != null) {
          throw new InstantiationException("NettyServer start failed - " + nettyServerDeployer.getException());
        }
      } catch (InterruptedException e) {
        logger.error("NettyServer start await was interrupted. It might not have started", e);
        throw new InstantiationException("Netty server start might have failed - " + e);
      }
    }
  }

  @Override
  public void shutdown() {
    if (nettyServerDeployerThread.isAlive()) {
      nettyServerDeployer.shutdown();
    }
  }
}

/**
 * Deploys netty HTTP server in a separate thread so that the main thread is unblocked.
 */
class NettyServerDeployer implements Runnable {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final CountDownLatch startupDone = new CountDownLatch(1);
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final NettyConfig nettyConfig;
  private final NettyMetrics nettyMetrics;
  private final RestRequestHandlerController requestHandlerController;
  private Exception exception = null;

  public NettyServerDeployer(NettyConfig nettyConfig, NettyMetrics nettyMetrics,
      RestRequestHandlerController requestHandlerController) {
    this.nettyConfig = nettyConfig;
    this.nettyMetrics = nettyMetrics;
    this.requestHandlerController = requestHandlerController;
    bossGroup = new NioEventLoopGroup(nettyConfig.nettyServerBossThreadCount);
    workerGroup = new NioEventLoopGroup(nettyConfig.nettyServerWorkerThreadCount);
  }

  @Override
  public void run() {
    try {
      ServerBootstrap b = new ServerBootstrap();
      // Netty creates a new instance of every class in the pipeline for every connection
      // i.e. if there are a 1000 active connections there will be a 1000 NettyMessageProcessor instances.
      b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, nettyConfig.nettyServerSoBacklog).handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch)
                throws Exception {
              ch.pipeline()
                  // for http encoding/decoding.
                  .addLast("codec", new HttpServerCodec())
                      // for chunking.
                  .addLast("chunker", new ChunkedWriteHandler())
                      // for detecting connections that have been idle too long - probably because of an error.
                  .addLast("idleStateHandler", new IdleStateHandler(0, 0, nettyConfig.nettyServerIdleTimeSeconds))
                      // custom processing class that interfaces with a BlobStorageService.
                  .addLast("processor", new NettyMessageProcessor(nettyMetrics, requestHandlerController));
            }
          });
      ChannelFuture f = b.bind(nettyConfig.nettyServerPort).sync();
      logger.info("NettyServer has started on port " + nettyConfig.nettyServerPort);

      // let the parent know that startup is complete and so that it can proceed.
      startupDone.countDown();
      // this is blocking
      f.channel().closeFuture().sync();
    } catch (Exception e) {
      logger.error("Netty server start failed", e);
      exception = e;
      startupDone.countDown();
    }
  }

  /**
   * Wait for the specified time for the startup to complete.
   * @param timeout - time to wait.
   * @param timeUnit - unit of timeout
   * @return - true if startup was done within the timeout, false otherwise.
   * @throws InterruptedException
   */
  public boolean awaitStartup(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return startupDone.await(timeout, timeUnit);
  }

  /**
   * Gets exceptions that occurred during startup if any.
   * @return - null if no exception occurred during startup, the exception that occurred otherwise.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * Shuts down the NettyServerDeployer.
   */
  public void shutdown() {
    if (!bossGroup.isTerminated() || !workerGroup.isTerminated()) {
      logger.info("Shutting down NettyServer..");
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
      try {
        // magic number
        if (workerGroup.awaitTermination(30, TimeUnit.SECONDS) && bossGroup.awaitTermination(30, TimeUnit.SECONDS)) {
          logger.info("NettyServer shutdown complete");
        } else {
          logger.error("NettyServer shutdown failed after waiting for 60 seconds");
        }
      } catch (InterruptedException e) {
        logger.error("Termination await interrupted while attempting to shutdown NettyServer. Shutdown may have been"
            + " unsuccessful", e);
      }
    }
  }
}
