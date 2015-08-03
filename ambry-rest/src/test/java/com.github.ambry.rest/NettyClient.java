package com.github.ambry.rest;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Netty client to send requests and receive responses in tests.
 */
class NettyClient {
  private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();
  private final NettyClientDeployer deployer;
  private final Thread deployerThread;

  public NettyClient(int serverPort, LinkedBlockingQueue<HttpObject> contentQueue,
      LinkedBlockingQueue<HttpObject> responseQueue) {
    deployer = new NettyClientDeployer(serverPort, contentQueue, responseQueue, cause);
    deployerThread = new Thread(deployer);
  }

  /**
   * Starts the netty client. Returns after startup is FULLY complete.
   * <p/>
   * For now all content has to be loaded before start() is called. Once start() is called, all the contents in the
   * content queue are sent to the server and any that are enqueued after this call are ignored.
   * @throws InstantiationException
   */
  public void start()
      throws InstantiationException {
    try {
      deployerThread.start();
      if (!deployer.awaitStartup(30, TimeUnit.SECONDS)) {
        throw new InstantiationException("NettyClient did not start in 30 seconds");
      } else if (cause.get() != null) {
        throw new InstantiationException("NettyClient startup threw exception" + cause.get());
      }
    } catch (InterruptedException e) {
      throw new InstantiationException("Startup await of NettyClient was interrupted. Client may not have started");
    }
  }

  /**
   * Shuts down the netty client. Returns after shutdown is FULLY complete.
   * @throws Exception
   */
  public void shutdown()
      throws Exception {
    if (deployerThread.isAlive()) {
      deployer.shutdown();
    }
  }

  /**
   * Returns exceptions thrown by the {@link NettyClientDeployer} or {@link CommunicationHandler} if any.
   * @return - null if no errors, the error that occurred otherwise.
   */
  public Throwable getCause() {
    return cause.get();
  }
}

/**
 * Deploys the Netty client as a separate thread.
 */
class NettyClientDeployer implements Runnable {
  private final CountDownLatch startupComplete = new CountDownLatch(1);
  private final EventLoopGroup group = new NioEventLoopGroup();

  private final int serverPort;
  private final LinkedBlockingQueue<HttpObject> contentQueue;
  private final LinkedBlockingQueue<HttpObject> responseQueue;
  private AtomicReference<Throwable> cause;

  public NettyClientDeployer(int serverPort, LinkedBlockingQueue<HttpObject> contentQueue,
      LinkedBlockingQueue<HttpObject> responseQueue, AtomicReference<Throwable> cause) {
    this.serverPort = serverPort;
    this.contentQueue = contentQueue;
    this.responseQueue = responseQueue;
    this.cause = cause;
  }

  @Override
  public void run() {
    try {
      Bootstrap b = new Bootstrap();
      b.group(group).channel(NioSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, false)
          .option(ChannelOption.TCP_NODELAY, false).handler(new ChannelInitializer<SocketChannel>() {
        @Override
        public void initChannel(SocketChannel ch)
            throws Exception {
          ch.pipeline().addLast(new HttpClientCodec()).addLast(new ChunkedWriteHandler())
              // custom handler
              .addLast(new CommunicationHandler(contentQueue, responseQueue, cause));
        }
      });

      ChannelFuture f = b.connect("localhost", serverPort).sync();
      startupComplete.countDown();
      f.channel().closeFuture().sync();
    } catch (Exception e) {
      cause.set(e);
      startupComplete.countDown();
    }
  }

  /**
   * Waits until client is deployed.
   * @param timeout
   * @param timeUnit
   * @return
   * @throws InterruptedException
   */
  public boolean awaitStartup(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return startupComplete.await(timeout, timeUnit);
  }

  /**
   * Shuts down netty client. Returns after shutdown is complete.
   * @throws Exception
   */
  public void shutdown()
      throws Exception {
    if (!group.isTerminated()) {
      group.shutdownGracefully();
      if (!group.awaitTermination(30, TimeUnit.SECONDS)) {
        throw new Exception("Client did not shutdown within timeout");
      }
    }
  }
}

/**
 * Custom handler that sends out the request and receives responses. Any exceptions are bubbled up to
 * {@link NettyClient}.
 */
class CommunicationHandler extends SimpleChannelInboundHandler<Object> {
  private final AtomicReference<Throwable> cause;
  private final LinkedBlockingQueue<HttpObject> contentQueue;
  private final LinkedBlockingQueue<HttpObject> responseQueue;

  public CommunicationHandler(LinkedBlockingQueue<HttpObject> contentQueue,
      LinkedBlockingQueue<HttpObject> responseQueue, AtomicReference<Throwable> cause) {
    this.cause = cause;
    this.contentQueue = contentQueue;
    this.responseQueue = responseQueue;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx)
      throws Exception {
    while (!contentQueue.isEmpty()) {
      ctx.writeAndFlush(contentQueue.remove());
    }
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Object in)
      throws Exception {
    if (in instanceof HttpObject) {
      // Make sure that we increase refCnt because we are going to process it async. The other end has to release after
      // processing.
      ReferenceCountUtil.retain(in);
      responseQueue.offer((HttpObject) in);
    } else {
      throw new IllegalStateException("Read object is not a HTTPObject");
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    ctx.close();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    this.cause.set(cause);
  }
}
