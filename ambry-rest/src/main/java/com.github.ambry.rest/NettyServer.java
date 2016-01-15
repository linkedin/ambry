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
import io.netty.handler.timeout.IdleStateHandler;
import java.util.concurrent.CountDownLatch;
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
class NettyServer implements NioServer {
  private final NettyConfig nettyConfig;
  private final NettyMetrics nettyMetrics;
  private final NettyServerDeployer nettyServerDeployer;
  private final Thread nettyServerDeployerThread;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public NettyServer(NettyConfig nettyConfig, NettyMetrics nettyMetrics,
      RequestResponseHandlerController requestResponseHandlerController) {
    this.nettyConfig = nettyConfig;
    this.nettyMetrics = nettyMetrics;
    nettyServerDeployer = new NettyServerDeployer(nettyConfig, nettyMetrics, requestResponseHandlerController);
    nettyServerDeployerThread = new Thread(nettyServerDeployer);
    logger.trace("Instantiated NettyServer");
  }

  @Override
  public void start()
      throws InstantiationException {
    if (!nettyServerDeployerThread.isAlive()) {
      logger.info("Starting NettyServer");
      long startupBeginTime = System.currentTimeMillis();
      try {
        nettyServerDeployerThread.start();
        if (!(nettyServerDeployer.awaitStartup(nettyConfig.nettyServerStartupWaitSeconds, TimeUnit.SECONDS))) {
          nettyMetrics.nettyServerStartError.inc();
          throw new InstantiationException("Netty server start timed out");
        } else if (nettyServerDeployer.getException() != null) {
          logger.error("Exception during deployment of NettyServer", nettyServerDeployer.getException());
          nettyMetrics.nettyServerStartError.inc();
          throw new InstantiationException(
              "NettyServer start failed - " + nettyServerDeployer.getException().getLocalizedMessage());
        }
      } catch (InterruptedException e) {
        logger.error("NettyServer start await was interrupted. It might not have started", e);
        nettyMetrics.nettyServerStartError.inc();
        throw new InstantiationException("Netty server start might have failed - " + e.getLocalizedMessage());
      } finally {
        long startupTime = System.currentTimeMillis() - startupBeginTime;
        logger.info("NettyServer start took {} ms", startupTime);
        nettyMetrics.nettyServerStartTimeInMs.update(startupTime);
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
  private final RequestResponseHandlerController requestResponseHandlerController;
  private Exception exception = null;

  public NettyServerDeployer(NettyConfig nettyConfig, NettyMetrics nettyMetrics,
      RequestResponseHandlerController requestResponseHandlerController) {
    this.nettyConfig = nettyConfig;
    this.nettyMetrics = nettyMetrics;
    this.requestResponseHandlerController = requestResponseHandlerController;
    bossGroup = new NioEventLoopGroup(nettyConfig.nettyServerBossThreadCount);
    workerGroup = new NioEventLoopGroup(nettyConfig.nettyServerWorkerThreadCount);
    logger.trace("Instantiated NettyServerDeployer");
  }

  @Override
  public void run() {
    try {
      logger.trace("Starting NettyServer deployment");
      ServerBootstrap b = new ServerBootstrap();
      // Netty creates a new instance of every class in the pipeline for every connection
      // i.e. if there are a 1000 active connections there will be a 1000 NettyMessageProcessor instances.
      b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, nettyConfig.nettyServerSoBacklog)
          .handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        public void initChannel(SocketChannel ch)
            throws Exception {
          ch.pipeline()
              // for http encoding/decoding. Note that we get content in 8KB chunks and a change to that number has
              // to go here.
              .addLast("codec", new HttpServerCodec())
                  // for detecting connections that have been idle too long - probably because of an error.
              .addLast("idleStateHandler", new IdleStateHandler(0, 0, nettyConfig.nettyServerIdleTimeSeconds))
                  // custom processing class that interfaces with a BlobStorageService.
              .addLast("processor",
                  new NettyMessageProcessor(nettyMetrics, nettyConfig, requestResponseHandlerController));
        }
      });
      ChannelFuture f = b.bind(nettyConfig.nettyServerPort).sync();
      logger.info("NettyServer now listening on port {}", nettyConfig.nettyServerPort);
      // let the parent know that startup is complete and so that it can proceed.
      startupDone.countDown();
      // this is blocking
      f.channel().closeFuture().sync();
    } catch (Exception e) {
      exception = e;
      startupDone.countDown();
    }
  }

  /**
   * Wait for the specified time for the startup to complete.
   * @param timeout time to wait for startup.
   * @param timeUnit unit of {@code timeout}.
   * @return {@code true} if startup was completed within the {@code timeout}, {@code false} otherwise.
   * @throws InterruptedException if the wait for startup is interrupted.
   */
  public boolean awaitStartup(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return startupDone.await(timeout, timeUnit);
  }

  /**
   * Gets exceptions that occurred during startup if any.
   * @return null if no {@link Exception} occurred during startup, the exception that occurred otherwise.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * Shuts down the NettyServerDeployer.
   */
  public void shutdown() {
    if (!bossGroup.isTerminated() || !workerGroup.isTerminated()) {
      logger.info("Shutting down NettyServer");
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
