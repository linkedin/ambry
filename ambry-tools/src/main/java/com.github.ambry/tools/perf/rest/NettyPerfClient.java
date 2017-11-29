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
package com.github.ambry.tools.perf.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.github.ambry.commons.SSLFactory;
import com.github.ambry.config.SSLConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Netty based client to evaluate performance of the front end.
 */
public class NettyPerfClient {
  private static final String GET = "GET";
  private static final String POST = "POST";
  private static final List<String> SUPPORTED_REQUEST_TYPES = Arrays.asList(GET, POST);
  private static final Logger logger = LoggerFactory.getLogger(NettyPerfClient.class);

  private final String host;
  private final int port;
  private final String uri;
  private final int concurrency;
  private final long totalSize;
  private final byte[] chunk;
  private final SSLFactory sslFactory;
  private final String serviceId;
  private final String targetAccountName;
  private final String targetContainerName;
  private final Bootstrap b = new Bootstrap();
  private final ChannelConnectListener channelConnectListener = new ChannelConnectListener();
  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final JmxReporter reporter = JmxReporter.forRegistry(metricRegistry).build();
  private final PerfClientMetrics perfClientMetrics = new PerfClientMetrics(metricRegistry);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicLong totalRequestCount = new AtomicLong(0);

  private EventLoopGroup group;
  private long perfClientStartTime;
  private volatile boolean isRunning = false;

  /**
   * Abstraction class for all the parameters that are expected.
   */
  private static class ClientArgs {
    final String host;
    final Integer port;
    final String path;
    final String requestType;
    final Integer concurrency;
    final Long postBlobTotalSize;
    final Integer postBlobChunkSize;
    final String sslPropsFilePath;
    final String targetAccountName;
    final String targetContainerName;
    final String serviceId;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Parses the arguments provided and extracts them into variables that can be retrieved.
     * @param args the command line argument list.
     */
    protected ClientArgs(String args[]) {
      OptionParser parser = new OptionParser();
      ArgumentAcceptingOptionSpec<String> host = parser.accepts("host", "Front end host to contact")
          .withOptionalArg()
          .describedAs("host")
          .ofType(String.class)
          .defaultsTo("localhost");
      ArgumentAcceptingOptionSpec<Integer> port = parser.accepts("port", "Front end port")
          .withOptionalArg()
          .describedAs("port")
          .ofType(Integer.class)
          .defaultsTo(1174);
      ArgumentAcceptingOptionSpec<String> path = parser.accepts("path", "Resource path (prefix with a '/')")
          .withOptionalArg()
          .describedAs("path")
          .ofType(String.class)
          .defaultsTo("/");
      ArgumentAcceptingOptionSpec<String> requestType =
          parser.accepts("requestType", "The type of request to make (POST, GET)")
              .withOptionalArg()
              .describedAs("requestType")
              .ofType(String.class)
              .defaultsTo(GET);
      ArgumentAcceptingOptionSpec<Integer> concurrency = parser.accepts("concurrency", "Number of parallel requests")
          .withOptionalArg()
          .describedAs("concurrency")
          .ofType(Integer.class)
          .defaultsTo(1);
      ArgumentAcceptingOptionSpec<Long> postBlobTotalSize =
          parser.accepts("postBlobTotalSize", "Total size in bytes of blob to be POSTed")
              .withOptionalArg()
              .describedAs("postBlobTotalSize")
              .ofType(Long.class);
      ArgumentAcceptingOptionSpec<Integer> postBlobChunkSize =
          parser.accepts("postBlobChunkSize", "Size in bytes of each chunk that will be POSTed")
              .withOptionalArg()
              .describedAs("postBlobChunkSize")
              .ofType(Integer.class);
      ArgumentAcceptingOptionSpec<String> targetAccountName =
          parser.accepts("targetAccountName", "Target account name for POSTs")
              .withOptionalArg()
              .describedAs("targetAccountName")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> targetContainerName =
          parser.accepts("targetContainerName", "Target container name for POSTs")
              .withOptionalArg()
              .describedAs("targetContainerName")
              .ofType(String.class);
      ArgumentAcceptingOptionSpec<String> serviceId = parser.accepts("serviceId", "serviceId representing the caller")
          .withOptionalArg()
          .describedAs("serviceId")
          .ofType(String.class)
          .defaultsTo("NettyPerfClient");
      ArgumentAcceptingOptionSpec<String> sslPropsFilePath =
          parser.accepts("sslPropsFilePath", "The path to the properties file with SSL settings")
              .withOptionalArg()
              .describedAs("sslPropsFilePath")
              .ofType(String.class);

      OptionSet options = parser.parse(args);
      this.host = options.valueOf(host);
      this.port = options.valueOf(port);
      this.path = options.valueOf(path);
      this.requestType = options.valueOf(requestType);
      this.concurrency = options.valueOf(concurrency);
      this.postBlobTotalSize = options.valueOf(postBlobTotalSize);
      this.postBlobChunkSize = options.valueOf(postBlobChunkSize);
      this.sslPropsFilePath = options.valueOf(sslPropsFilePath);
      this.targetAccountName = options.valueOf(targetAccountName);
      this.targetContainerName = options.valueOf(targetContainerName);
      this.serviceId = options.valueOf(serviceId);
      validateArgs();

      logger.info("Host: {}", this.host);
      logger.info("Port: {}", this.port);
      logger.info("Path: {}", this.path);
      logger.info("Request type: {}", this.requestType);
      logger.info("Concurrency: {}", this.concurrency);
      logger.info("Post blob total size: {}", this.postBlobTotalSize);
      logger.info("Post blob chunk size: {}", this.postBlobChunkSize);
      logger.info("SSL properties file path: {}", this.sslPropsFilePath);
    }

    /**
     * Validates the arguments given and verifies relationships b/w them if any exist.
     */
    private void validateArgs() {
      if (!SUPPORTED_REQUEST_TYPES.contains(requestType)) {
        throw new IllegalArgumentException("Unsupported request type: " + requestType);
      } else if (requestType.equals(POST)) {
        if (postBlobTotalSize == null || postBlobTotalSize <= 0 || postBlobChunkSize == null
            || postBlobChunkSize <= 0) {
          throw new IllegalArgumentException(
              "Total size to be posted and size of each chunk need to be specified with POST and have to be > 0");
        } else if (targetAccountName == null || targetAccountName.isEmpty() || targetContainerName == null
            || targetContainerName.isEmpty()) {
          throw new IllegalArgumentException("TargetAccountName and TargetContainerName cannot be empty with POST");
        }
      }
      if (serviceId == null || serviceId.isEmpty()) {
        throw new IllegalArgumentException("serviceId cannot be empty");
      }
    }
  }

  /**
   * Invokes the {@link NettyPerfClient} with the command line arguments.
   * @param args command line arguments.
   */
  public static void main(String[] args) {
    try {
      ClientArgs clientArgs = new ClientArgs(args);
      final NettyPerfClient nettyPerfClient =
          new NettyPerfClient(clientArgs.host, clientArgs.port, clientArgs.path, clientArgs.concurrency,
              clientArgs.postBlobTotalSize, clientArgs.postBlobChunkSize, clientArgs.sslPropsFilePath,
              clientArgs.serviceId, clientArgs.targetAccountName, clientArgs.targetContainerName);
      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          logger.info("Received shutdown signal. Requesting NettyPerfClient shutdown");
          nettyPerfClient.shutdown();
        }
      });
      nettyPerfClient.start();
      nettyPerfClient.awaitShutdown();
    } catch (Exception e) {
      logger.error("Exception during execution of NettyPerfClient", e);
    }
  }

  /**
   * Creates an instance of NettyPerfClient
   * @param host host to contact.
   * @param port port to contact.
   * @param path resource path.
   * @param concurrency number of parallel requests.
   * @param totalSize the total size in bytes of a blob to be POSTed ({@code null} if non-POST).
   * @param chunkSize size in bytes of each chunk to be POSTed ({@code null} if non-POST).
   * @param sslPropsFilePath the path to the SSL properties, or {@code null} to disable SSL.
   * @param serviceId serviceId of the caller to represent the identity
   * @param targetAccountName target account name for POST
   * @param targetContainerName target container name for POST
   * @throws IOException
   * @throws GeneralSecurityException
   */
  private NettyPerfClient(String host, int port, String path, int concurrency, Long totalSize, Integer chunkSize,
      String sslPropsFilePath, String serviceId, String targetAccountName, String targetContainerName)
      throws IOException, GeneralSecurityException {
    this.host = host;
    this.port = port;
    this.uri = "http://" + host + ":" + port + path;
    this.concurrency = concurrency;
    if (chunkSize != null) {
      this.totalSize = totalSize;
      chunk = new byte[chunkSize];
      new Random().nextBytes(chunk);
    } else {
      this.totalSize = 0;
      chunk = null;
    }
    sslFactory = sslPropsFilePath != null ? new SSLFactory(
        new SSLConfig(new VerifiableProperties(Utils.loadProps(sslPropsFilePath)))) : null;
    this.serviceId = serviceId;
    this.targetAccountName = targetAccountName;
    this.targetContainerName = targetContainerName;
    logger.info("Instantiated NettyPerfClient which will interact with host {}, port {}, uri {} with concurrency {}",
        this.host, this.port, uri, this.concurrency);
  }

  /**
   * Starts the NettyPerfClient.
   * @throws InterruptedException
   */
  protected void start() throws InterruptedException {
    logger.info("Starting NettyPerfClient");
    reporter.start();
    group = new NioEventLoopGroup(concurrency);
    b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        if (sslFactory != null) {
          ch.pipeline()
              .addLast("sslHandler", new SslHandler(sslFactory.createSSLEngine(host, port, SSLFactory.Mode.CLIENT)));
        }
        ch.pipeline().addLast(new HttpClientCodec()).addLast(new ChunkedWriteHandler()).addLast(new ResponseHandler());
      }
    });
    logger.info("Connecting to {}:{}", host, port);
    b.remoteAddress(host, port);
    perfClientStartTime = System.currentTimeMillis();
    for (int i = 0; i < concurrency; i++) {
      b.connect().addListener(channelConnectListener);
    }
    isRunning = true;
    logger.info("Created {} channel(s)", concurrency);
    logger.info("NettyPerfClient started");
  }

  /**
   * Shuts down the NettyPerfClient.
   */
  protected void shutdown() {
    logger.info("Shutting down NettyPerfClient");
    isRunning = false;
    group.shutdownGracefully();
    long totalRunTimeInMs = System.currentTimeMillis() - perfClientStartTime;
    try {
      if (!group.awaitTermination(5, TimeUnit.SECONDS)) {
        logger.error("Netty worker did not shutdown within timeout");
      } else {
        logger.info("NettyPerfClient shutdown complete");
      }
    } catch (InterruptedException e) {
      logger.error("NettyPerfClient shutdown interrupted", e);
    } finally {
      logger.info("Executed for approximately {} s and sent {} requests ({} requests/sec)",
          (float) totalRunTimeInMs / (float) Time.MsPerSec, totalRequestCount.get(),
          (float) totalRequestCount.get() * (float) Time.MsPerSec / (float) totalRunTimeInMs);
      Snapshot rttStatsSnapshot = perfClientMetrics.requestRoundTripTimeInMs.getSnapshot();
      logger.info("RTT stats: Min - {} ms, Mean - {} ms, Max - {} ms", rttStatsSnapshot.getMin(),
          rttStatsSnapshot.getMean(), rttStatsSnapshot.getMax());
      logger.info("RTT stats: 95th percentile - {} ms, 99th percentile - {} ms, 999th percentile - {} ms",
          rttStatsSnapshot.get95thPercentile(), rttStatsSnapshot.get99thPercentile(),
          rttStatsSnapshot.get999thPercentile());
      reporter.stop();
      shutdownLatch.countDown();
    }
  }

  /**
   * Blocking function to wait on the NettyPerfClient shutting down.
   * @throws InterruptedException
   */
  protected void awaitShutdown() throws InterruptedException {
    shutdownLatch.await();
  }

  /**
   * Custom handler that sends out the request and receives and processes the response.
   */
  private class ResponseHandler extends SimpleChannelInboundHandler<HttpObject> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private HttpRequest request;
    private HttpResponse response;
    private ChunkedInput<HttpContent> chunkedInput;

    private int chunksReceived;
    private long sizeReceived;
    private long lastChunkReceiveTime;
    private long requestStartTime;
    private long requestId = 0;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      perfClientMetrics.channelCreationRate.mark();
      logger.trace("Channel {} active", ctx.channel());
      sendRequest(ctx);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, HttpObject in) {
      long currentChunkReceiveTime = System.currentTimeMillis();
      boolean recognized = false;
      if (in instanceof HttpResponse) {
        recognized = true;
        long responseReceiveStart = currentChunkReceiveTime - requestStartTime;
        perfClientMetrics.timeToFirstResponseChunkInMs.update(responseReceiveStart);
        logger.trace("Response receive has started on channel {}. Took {} ms", ctx.channel(), responseReceiveStart);
        response = (HttpResponse) in;
      }
      if (in instanceof HttpContent) {
        recognized = true;
        perfClientMetrics.delayBetweenChunkReceiveInMs.update(currentChunkReceiveTime - lastChunkReceiveTime);
        chunksReceived++;
        int bytesReceivedThisTime = ((HttpContent) in).content().readableBytes();
        sizeReceived += bytesReceivedThisTime;
        perfClientMetrics.bytesReceiveRate.mark(bytesReceivedThisTime);
        if (in instanceof LastHttpContent) {
          long requestRoundTripTime = currentChunkReceiveTime - requestStartTime;
          perfClientMetrics.requestRoundTripTimeInMs.update(requestRoundTripTime);
          perfClientMetrics.getContentSizeInBytes.update(sizeReceived);
          perfClientMetrics.getChunkCount.update(chunksReceived);
          logger.trace(
              "Final content received on channel {}. Took {} ms. Total chunks received - {}. Total size received - {}",
              ctx.channel(), requestRoundTripTime, chunksReceived, sizeReceived);
          if (HttpUtil.isKeepAlive(response) && isRunning) {
            logger.trace("Sending new request on channel {}", ctx.channel());
            sendRequest(ctx);
          } else if (!isRunning) {
            logger.info("Closing channel {} because NettyPerfClient has been shutdown", ctx.channel());
            ctx.close();
          } else {
            perfClientMetrics.requestResponseError.inc();
            logger.error("Channel {} not kept alive. Last response status was {}", ctx.channel(), response.status());
            ctx.close();
          }
        }
      }
      if (!recognized) {
        throw new IllegalStateException("Unexpected HttpObject - " + in.getClass());
      }
      lastChunkReceiveTime = currentChunkReceiveTime;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      logger.trace("Channel {} inactive", ctx.channel());
      ctx.close();
      if (isRunning) {
        perfClientMetrics.unexpectedDisconnectionError.inc();
        logger.info("Creating a new channel to keep up concurrency");
        b.connect().addListener(channelConnectListener);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      perfClientMetrics.requestResponseError.inc();
      logger.error("Exception caught on channel {} while processing request/response", ctx.channel(), cause);
      ctx.close();
    }

    /**
     * Sends the request according to the configuration.
     * @param ctx the {@link ChannelHandlerContext} to use to send the request.
     */
    private void sendRequest(ChannelHandlerContext ctx) {
      requestId++;
      long globalId = totalRequestCount.incrementAndGet();
      logger.trace("Sending request with global ID {} and local ID {} on channel {}", globalId, requestId,
          ctx.channel());
      reset();
      perfClientMetrics.requestRate.mark();
      ctx.writeAndFlush(request);
      if (request.method().equals(HttpMethod.POST)) {
        ctx.writeAndFlush(chunkedInput);
      }
      logger.trace("Request {} scheduled to be sent on channel {}", requestId, ctx.channel());
    }

    /**
     * Resets all state in preparation for the next request-response.
     */
    private void reset() {
      if (chunk != null) {
        chunkedInput = new HttpChunkedInput(new RepeatedBytesInput());
        request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
        HttpUtil.setContentLength(request, totalSize);
        request.headers().add(RestUtils.Headers.BLOB_SIZE, totalSize);
        request.headers().add(RestUtils.Headers.SERVICE_ID, serviceId);
        request.headers().add(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
        request.headers().add(RestUtils.Headers.TARGET_ACCOUNT_NAME, targetAccountName);
        request.headers().add(RestUtils.Headers.TARGET_CONTAINER_NAME, targetContainerName);
      } else {
        request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
      }
      chunksReceived = 0;
      sizeReceived = 0;
      lastChunkReceiveTime = 0;
      requestStartTime = System.currentTimeMillis();
      response = null;
    }

    /**
     * Returns a chunk with the same data again and again until a fixed size is reached.
     */
    private class RepeatedBytesInput implements ChunkedInput<ByteBuf> {
      private final AtomicBoolean metricRecorded = new AtomicBoolean(false);

      private long streamed = 0;
      private long startTime;
      private long lastChunkSendTime = 0;
      private final Logger logger = LoggerFactory.getLogger(getClass());

      /**
       * Creates an instance that repeatedly sends the same chunk up to the configured size.
       */
      protected RepeatedBytesInput() {
        if (totalSize < 0 || (totalSize > 0 && chunk.length < 1)) {
          throw new IllegalArgumentException("Invalid argument(s)");
        }
      }

      @Override
      public boolean isEndOfInput() {
        boolean isEndOfInput = streamed >= totalSize;
        if (isEndOfInput && metricRecorded.compareAndSet(false, true)) {
          long postChunksTime = System.currentTimeMillis() - startTime;
          perfClientMetrics.postChunksTimeInMs.update(postChunksTime);
          logger.debug("Took {} ms to POST the blob of size {}", postChunksTime, streamed);
        }
        return isEndOfInput;
      }

      @Override
      public void close() {
        logger.debug("Size streamed - {}", streamed);
      }

      @Override
      public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
        return readChunk(ctx.alloc());
      }

      @Override
      public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
        ByteBuf buf = null;
        if (streamed == 0) {
          startTime = System.currentTimeMillis();
        }
        if (!isEndOfInput()) {
          long currentChunkSendTime = System.currentTimeMillis();
          int remaining = (totalSize - streamed) > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) (totalSize - streamed);
          int toWrite = Math.min(chunk.length, remaining);
          buf = Unpooled.wrappedBuffer(chunk, 0, toWrite);
          streamed += toWrite;
          if (lastChunkSendTime > 0) {
            perfClientMetrics.delayBetweenChunkSendInMs.update(currentChunkSendTime - lastChunkSendTime);
          }
          lastChunkSendTime = currentChunkSendTime;
        }
        return buf;
      }

      @Override
      public long length() {
        return totalSize;
      }

      @Override
      public long progress() {
        return streamed;
      }
    }
  }

  /**
   * Channel connection listener that prints error if channel could not be connected.
   */
  private class ChannelConnectListener implements GenericFutureListener<ChannelFuture> {

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        perfClientMetrics.connectError.inc();
        logger.error("Channel {} to {}:{} could not be connected.", future.channel(), host, port, future.cause());
      }
    }
  }

  /**
   * Metrics that track peformance.
   */
  private static class PerfClientMetrics {
    public final Meter bytesReceiveRate;
    public final Meter channelCreationRate;
    public final Meter requestRate;

    public final Histogram delayBetweenChunkReceiveInMs;
    public final Histogram delayBetweenChunkSendInMs;
    public final Histogram getContentSizeInBytes;
    public final Histogram getChunkCount;
    public final Histogram postChunksTimeInMs;
    public final Histogram requestRoundTripTimeInMs;
    public final Histogram timeToFirstResponseChunkInMs;

    public final Counter connectError;
    public final Counter requestResponseError;
    public final Counter unexpectedDisconnectionError;

    /**
     * Creates an instance of PerfClientMetrics.
     * @param metricRegistry the {@link MetricRegistry} instance to use.
     */
    protected PerfClientMetrics(MetricRegistry metricRegistry) {
      bytesReceiveRate = metricRegistry.meter(MetricRegistry.name(ResponseHandler.class, "BytesReceiveRate"));
      channelCreationRate = metricRegistry.meter(MetricRegistry.name(ResponseHandler.class, "ChannelCreationRate"));
      requestRate = metricRegistry.meter(MetricRegistry.name(ResponseHandler.class, "RequestRate"));

      delayBetweenChunkReceiveInMs =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.class, "DelayBetweenChunkReceiveInMs"));
      delayBetweenChunkSendInMs = metricRegistry.histogram(
          MetricRegistry.name(ResponseHandler.RepeatedBytesInput.class, "DelayBetweenChunkSendInMs"));
      getContentSizeInBytes = metricRegistry.histogram(
          MetricRegistry.name(ResponseHandler.RepeatedBytesInput.class, "GetContentSizeInBytes"));
      getChunkCount =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.RepeatedBytesInput.class, "GetChunkCount"));
      postChunksTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.RepeatedBytesInput.class, "PostChunksTimeInMs"));
      requestRoundTripTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.class, "RequestRoundTripTimeInMs"));
      timeToFirstResponseChunkInMs =
          metricRegistry.histogram(MetricRegistry.name(ResponseHandler.class, "TimeToFirstResponseChunkInMs"));

      connectError = metricRegistry.counter(MetricRegistry.name(ResponseHandler.class, "ConnectError"));
      requestResponseError = metricRegistry.counter(MetricRegistry.name(ResponseHandler.class, "RequestResponseError"));
      unexpectedDisconnectionError =
          metricRegistry.counter(MetricRegistry.name(ResponseHandler.class, "UnexpectedDisconnectionError"));
    }
  }
}
