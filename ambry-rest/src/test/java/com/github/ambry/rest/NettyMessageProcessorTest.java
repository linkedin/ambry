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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.frontend.ContainerMetrics;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.TestUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.FileUpload;
import io.netty.handler.codec.http.multipart.HttpDataFactory;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import io.netty.handler.codec.http.multipart.MemoryFileUpload;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Test;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.Headers.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link NettyMessageProcessor}.
 */
public class NettyMessageProcessorTest {
  private static final String CONTAINER_METRIC_PREFIX =
      ContainerMetrics.class.getCanonicalName() + ".account___container___PostBlob";
  private final InMemoryRouter router;
  private final RestRequestService restRequestService;
  private final MockRestRequestResponseHandler requestHandler;
  private final HelperNotificationSystem notificationSystem = new HelperNotificationSystem();

  private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong(0);
  private static final NettyMetrics NETTY_METRICS = new NettyMetrics(new MetricRegistry());
  private static final NettyConfig NETTY_CONFIG = new NettyConfig(new VerifiableProperties(new Properties()));
  private static final PerformanceConfig PERFORMANCE_CONFIG =
      new PerformanceConfig(new VerifiableProperties(new Properties()));
  // Minimum time a request is held in flight before it is terminated, so that the recorded duration is provably
  // non-zero rather than merely non-negative.
  private static final long MIN_IN_FLIGHT_MS = 2;

  /**
   * Sets up the mock services that {@link NettyMessageProcessor} can use.
   * @throws InstantiationException
   * @throws IOException
   */
  public NettyMessageProcessorTest() throws InstantiationException, IOException {
    VerifiableProperties verifiableProperties = new VerifiableProperties(new Properties());
    RestRequestMetricsTracker.setDefaults(new MetricRegistry());
    router = new InMemoryRouter(verifiableProperties, notificationSystem, new MockClusterMap(), null);
    restRequestService = new MockRestRequestService(verifiableProperties, router);
    requestHandler = new MockRestRequestResponseHandler(restRequestService);
    restRequestService.setupResponseHandler(requestHandler);
    restRequestService.start();
    requestHandler.start();
  }

  /**
   * Clean up task.
   */
  @After
  public void cleanUp() throws IOException {
    restRequestService.shutdown();
    router.close();
    notificationSystem.close();
  }

  /**
   * Tests for the common case request handling flow.
   * @throws IOException
   */
  @Test
  public void requestHandleWithGoodInputTest() throws IOException {
    doRequestHandleWithoutKeepAlive(HttpMethod.GET, RestMethod.GET);
    doRequestHandleWithoutKeepAlive(HttpMethod.DELETE, RestMethod.DELETE);
    doRequestHandleWithoutKeepAlive(HttpMethod.HEAD, RestMethod.HEAD);

    EmbeddedChannel channel = createChannel();
    doRequestHandleWithKeepAlive(channel, HttpMethod.GET, RestMethod.GET);
    doRequestHandleWithKeepAlive(channel, HttpMethod.DELETE, RestMethod.DELETE);
    doRequestHandleWithKeepAlive(channel, HttpMethod.HEAD, RestMethod.HEAD);
  }

  /**
   * Tests the case where raw bytes are POSTed as chunks.
   * @throws InterruptedException
   */
  @Test
  public void rawBytesPostTest() throws InterruptedException {
    Random random = new Random();
    // request also contains content.
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(random.nextInt(128) + 128));
    HttpRequest postRequest =
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", Unpooled.wrappedBuffer(content));
    postRequest.headers().set(RestUtils.Headers.SERVICE_ID, "rawBytesPostTest");
    postRequest = ReferenceCountUtil.retain(postRequest);
    ByteBuffer receivedContent = doPostTest(postRequest, null);
    compareContent(receivedContent, Collections.singletonList(content));

    // request and content separate.
    final int NUM_CONTENTS = 5;
    postRequest = RestTestUtils.createRequest(HttpMethod.POST, "/", null);
    List<ByteBuffer> contents = new ArrayList<ByteBuffer>(NUM_CONTENTS);
    int blobSize = 0;
    for (int i = 0; i < NUM_CONTENTS; i++) {
      ByteBuffer buffer = ByteBuffer.wrap(TestUtils.getRandomBytes(random.nextInt(128) + 128));
      blobSize += buffer.remaining();
      contents.add(i, buffer);
    }
    postRequest.headers().set(RestUtils.Headers.SERVICE_ID, "rawBytesPostTest");
    receivedContent = doPostTest(postRequest, contents);
    compareContent(receivedContent, contents);
  }

  /**
   * Tests the case where multipart upload is used.
   * @throws Exception
   */
  @Test
  public void multipartPostTest() throws Exception {
    Random random = new Random();
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(random.nextInt(128) + 128));
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.POST, "/", null);
    httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "rawBytesPostTest");
    HttpPostRequestEncoder encoder = createEncoder(httpRequest, content);
    HttpRequest postRequest = encoder.finalizeRequest();
    List<ByteBuffer> contents = new ArrayList<ByteBuffer>();
    while (!encoder.isEndOfInput()) {
      // Sending null for ctx because the encoder is OK with that.
      contents.add(encoder.readChunk(PooledByteBufAllocator.DEFAULT).content().nioBuffer());
    }
    ByteBuffer receivedContent = doPostTest(postRequest, contents);
    compareContent(receivedContent, Collections.singletonList(content));
  }

  /**
   * Test the case where Except == 100-continue.
   * @throws Exception
   */
  @Test
  public void continueHeaderPutTest() throws Exception {
    notificationSystem.reset();
    Properties properties = new Properties();
    properties.put(NettyConfig.NETTY_ENABLE_ONE_HUNDRED_CONTINUE, "true");
    NettyConfig nettyConfig = new NettyConfig(new VerifiableProperties(properties));
    EmbeddedChannel channel = createChannel(nettyConfig);
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(EXPECT, CONTINUE);
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/s3/", headers);
    httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "rawBytesPostTest");
    httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
    channel.writeInbound(httpRequest);

    Random random = new Random();
    HttpResponse response = channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.CONTINUE, response.status());

    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(random.nextInt(128) + 128));
    channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(content)));
    channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

    if (!notificationSystem.operationCompleted.await(1000, TimeUnit.MILLISECONDS)) {
      fail("Put did not succeed after 1000ms. There is an error or timeout needs to increase");
    }
    ByteBuffer receivedContent = router.getActiveBlobs().get(notificationSystem.blobIdOperatedOn).getBlob();
    compareContent(receivedContent, Collections.singletonList(content));
  }

  /**
   * Test the case where Except == 100-continue.
   * @throws Exception
   */
  @Test
  public void continueHeaderPostTest() throws Exception {
    notificationSystem.reset();
    Properties properties = new Properties();
    properties.put(NettyConfig.NETTY_ENABLE_ONE_HUNDRED_CONTINUE, "true");
    NettyConfig nettyConfig = new NettyConfig(new VerifiableProperties(properties));
    EmbeddedChannel channel = createChannel(nettyConfig);
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(EXPECT, CONTINUE);
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.POST, "/s3/", headers);
    httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "rawBytesPostTest");
    httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
    channel.writeInbound(httpRequest);

    Random random = new Random();
    HttpResponse response = channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.CONTINUE, response.status());

    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(random.nextInt(128) + 128));
    channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(content)));
    channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

    if (!notificationSystem.operationCompleted.await(1000, TimeUnit.MILLISECONDS)) {
      fail("Post did not succeed after 1000ms. There is an error or timeout needs to increase");
    }
    ByteBuffer receivedContent = router.getActiveBlobs().get(notificationSystem.blobIdOperatedOn).getBlob();
    compareContent(receivedContent, Collections.singletonList(content));
  }

  /**
   * Tests for error handling flow when bad input streams are provided to the {@link NettyMessageProcessor}.
   */
  @Test
  public void requestHandleWithBadInputTest() throws IOException {
    String content = "@@randomContent@@@";
    // content without request.
    EmbeddedChannel channel = createChannel();
    channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(content.getBytes())));
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.status());
    assertFalse("Channel is not closed", channel.isOpen());

    // content without request on a channel that was kept alive
    channel = createChannel();
    // send and receive response for a good request and keep the channel alive
    channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, MockRestRequestService.ECHO_REST_METHOD, null));
    channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    // drain the content
    while (channel.readOutbound() != null) {
      ;
    }
    assertTrue("Channel is not active", channel.isActive());
    // send content without request
    channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.status());
    assertFalse("Channel is not closed", channel.isOpen());

    // content when no content is expected.
    channel = createChannel();
    channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, "/", null));
    channel.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(content.getBytes())));
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.status());
    assertFalse("Channel is not closed", channel.isOpen());

    // wrong HTTPObject.
    channel = createChannel();
    channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.status());
    assertFalse("Channel is not closed", channel.isOpen());

    // request while another request is in progress.
    channel = createChannel();
    channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, "/", null));
    channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, "/", null));
    // channel should be closed by now
    assertFalse("Channel is not closed", channel.isOpen());
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.status());

    // decoding failure
    channel = createChannel();
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.GET, "/", null);
    httpRequest.setDecoderResult(DecoderResult.failure(new IllegalStateException("Induced failure")));
    channel.writeInbound(httpRequest);
    // channel should be closed by now
    assertFalse("Channel is not closed", channel.isOpen());
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.status());

    // unsupported method
    channel = createChannel();
    channel.writeInbound(RestTestUtils.createRequest(HttpMethod.TRACE, "/", null));
    // channel should be closed by now
    assertFalse("Channel is not closed", channel.isOpen());
    response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.BAD_REQUEST, response.status());
  }

  /**
   * Tests for error handling flow when the {@link RestRequestHandler} throws exceptions.
   */
  @Test
  public void requestHandlerExceptionTest() {
    try {
      // RuntimeException
      Properties properties = new Properties();
      properties.setProperty(MockRestRequestResponseHandler.RUNTIME_EXCEPTION_ON_HANDLE, "true");
      requestHandler.breakdown(new VerifiableProperties(properties));
      doRequestHandlerExceptionTest(HttpMethod.GET, HttpResponseStatus.INTERNAL_SERVER_ERROR);

      // RestServiceException
      properties.clear();
      properties.setProperty(MockRestRequestResponseHandler.REST_EXCEPTION_ON_HANDLE,
          RestServiceErrorCode.InternalServerError.toString());
      requestHandler.breakdown(new VerifiableProperties(properties));
      doRequestHandlerExceptionTest(HttpMethod.GET, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    } finally {
      requestHandler.fix();
    }
  }

  /**
   * Tests that the 100-continue PUT works correctly even when the write promise for the
   * 100-continue response completes AFTER {@code handleContent()} has cleared the EXPECT header.
   * This simulates the production scenario where the security service callback runs on a
   * non-event-loop thread, causing the write promise to complete asynchronously.
   *
   * <p>Before the fix (capturing {@code shouldCloseRequest} at listener construction time), the
   * {@code ResponseMetadataWriteListener} would re-evaluate {@code
   * isPutOrPostS3RequestAndExpectContinue(request)} at completion time, see EXPECT="" (mutated by
   * handleContent), and incorrectly call {@code request.close()}.
   *
   * <p>The fix captures the close decision when the listener is created (while EXPECT is still
   * "100-continue"), so it correctly evaluates to {@code shouldCloseRequest=false} regardless of
   * when the promise completes.
   *
   * @throws Exception
   */
  @Test
  public void continueHeaderPutRequestCloseRaceWithFixTest() throws Exception {
    notificationSystem.reset();
    Properties properties = new Properties();
    properties.put(NettyConfig.NETTY_ENABLE_ONE_HUNDRED_CONTINUE, "true");
    NettyConfig nettyConfig = new NettyConfig(new VerifiableProperties(properties));

    DelayedContinueWriteHandler delayHandler = new DelayedContinueWriteHandler();
    NettyMessageProcessor processor =
        new NettyMessageProcessor(NETTY_METRICS, nettyConfig, PERFORMANCE_CONFIG, requestHandler);
    EmbeddedChannel channel = new EmbeddedChannel(delayHandler, new ChunkedWriteHandler(), processor);

    HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(EXPECT, CONTINUE);
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/s3/", headers);
    httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "continueHeaderPutRequestCloseRaceWithFixTest");
    httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");

    // Step 1: Send the HTTP request. This triggers handlePut() which synchronously writes a 100-continue
    // FullHttpResponse. The DelayedContinueWriteHandler intercepts the write: the message itself is forwarded
    // to the channel buffer (readable via readOutbound()), but the original promise is held.
    // Crucially, the ResponseMetadataWriteListener captures shouldCloseRequest=false at this point
    // because EXPECT is still "100-continue".
    channel.writeInbound(httpRequest);

    // Step 2: Read and verify the 100-continue response is buffered.
    HttpResponse response = channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.CONTINUE, response.status());

    // Step 3: Send content (not LastHttpContent). This triggers handleContent() which:
    //   - Adds content to the request
    //   - Detects hasContinueAndIsPutOrPost = true (EXPECT is still "100-continue")
    //   - Clears EXPECT: request.setArg(EXPECT, "")
    //   - Creates a new NettyResponseChannel (responseChannel2)
    //   - Calls handleRequest(request, responseChannel2) to start the actual PUT
    //   The PUT is now in progress but still waiting for more content (LastHttpContent not yet
    // sent).
    Random random = new Random();
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(random.nextInt(128) + 128));
    channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(content)));

    // Step 4: Complete the held continue write promise AFTER EXPECT has been cleared.
    // This fires ResponseMetadataWriteListener.operationComplete() on responseChannel1.
    // With the fix, shouldCloseRequest was captured as false at construction time, so
    // request.close() is NOT called despite EXPECT now being "".
    // Without the fix, this would re-read EXPECT="" and incorrectly close the request.
    delayHandler.completeContinueWrite();

    // Step 5: Send last content chunk. With the fix, the request is still open so
    // addContent() succeeds and the PUT can complete normally.
    // Without the fix, addContent() would throw RestServiceException(RequestChannelClosed) because
    // the request was prematurely closed in step 4, causing exceptionCaught() to close the channel.
    channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

    // Step 6: Assert the channel is still open. This is the key assertion that differentiates
    // the fixed vs unfixed behavior. Without the fix, the channel would be closed here because
    // addContent() threw RestServiceException -> exceptionCaught() -> ctx.close().
    assertTrue("Channel should still be open — the fix prevents the stale 100-continue listener from "
        + "closing the request prematurely", channel.isOpen());

    // Step 7: Assert the PUT succeeds with correct content.
    if (!notificationSystem.operationCompleted.await(1000, TimeUnit.MILLISECONDS)) {
      fail("Put did not succeed after 1000ms. The 100-continue write promise race condition was not fixed.");
    }
    ByteBuffer receivedContent = router.getActiveBlobs().get(notificationSystem.blobIdOperatedOn).getBlob();
    compareContent(receivedContent, Collections.singletonList(content));
  }

  /**
   * Tests that the 100-continue PUT works correctly under normal timing (no delayed promise). This
   * is the baseline case where the write promise completes synchronously before {@code
   * handleContent()} runs, so the {@code ResponseMetadataWriteListener} sees EXPECT="100-continue"
   * and correctly sets {@code shouldCloseRequest=false}.
   *
   * <p>This test is identical to {@link #continueHeaderPutTest()} but uses the same {@link
   * DelayedContinueWriteHandler} pipeline with the promise completed immediately (before content is
   * sent), verifying the non-racy code path.
   *
   * @throws Exception
   */
  @Test
  public void continueHeaderPutRequestCloseRaceWithoutDelayTest() throws Exception {
    notificationSystem.reset();
    Properties properties = new Properties();
    properties.put(NettyConfig.NETTY_ENABLE_ONE_HUNDRED_CONTINUE, "true");
    NettyConfig nettyConfig = new NettyConfig(new VerifiableProperties(properties));

    DelayedContinueWriteHandler delayHandler = new DelayedContinueWriteHandler();
    NettyMessageProcessor processor =
        new NettyMessageProcessor(NETTY_METRICS, nettyConfig, PERFORMANCE_CONFIG, requestHandler);
    EmbeddedChannel channel = new EmbeddedChannel(delayHandler, new ChunkedWriteHandler(), processor);

    HttpHeaders headers = new DefaultHttpHeaders();
    headers.set(EXPECT, CONTINUE);
    HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/s3/", headers);
    httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "continueHeaderPutRequestCloseRaceWithoutDelayTest");
    httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");

    // Step 1: Send the HTTP request. The DelayedContinueWriteHandler holds the promise.
    channel.writeInbound(httpRequest);

    // Step 2: Read the 100-continue response.
    HttpResponse response = channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.CONTINUE, response.status());

    // Step 3: Complete the promise BEFORE sending content (simulating normal synchronous timing).
    // The listener fires while EXPECT is still "100-continue", so shouldCloseRequest=false
    // regardless of whether the fix is applied.
    delayHandler.completeContinueWrite();

    // Step 4: Send content and last content chunk.
    Random random = new Random();
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(random.nextInt(128) + 128));
    channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(content)));
    channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);

    // Step 5: Assert the PUT succeeds normally.
    if (!notificationSystem.operationCompleted.await(1000, TimeUnit.MILLISECONDS)) {
      fail("Put did not succeed after 1000ms. There is an error or timeout needs to increase");
    }
    ByteBuffer receivedContent = router.getActiveBlobs().get(notificationSystem.blobIdOperatedOn).getBlob();
    compareContent(receivedContent, Collections.singletonList(content));
  }

  /**
   * Tests that a failed 100-Continue response write records one client abort and one termination duration even though
   * the interim response channel has already completed.
   */
  @Test
  public void testContinueResponseWriteFailureRecordsClientAbortOnce() {
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    NettyMetrics nettyMetrics = new NettyMetrics(metricRegistry);
    ContainerMetrics containerMetrics =
        new ContainerMetrics("account", "container", "PostBlob", metricRegistry, false, null);
    Properties properties = new Properties();
    properties.put(NettyConfig.NETTY_ENABLE_ONE_HUNDRED_CONTINUE, "true");
    NettyConfig nettyConfig = new NettyConfig(new VerifiableProperties(properties));
    RestRequestHandler continueRequestHandler = new RestRequestHandler() {
      @Override
      public void start() {
      }

      @Override
      public void shutdown() {
      }

      @Override
      public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel)
          throws RestServiceException {
        restRequest.getMetricsTracker().injectContainerMetrics(containerMetrics);
        restResponseChannel.setStatus(ResponseStatus.Continue);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        restResponseChannel.onResponseComplete(null);
      }
    };
    NettyMessageProcessor processor =
        new NettyMessageProcessor(nettyMetrics, nettyConfig, PERFORMANCE_CONFIG, continueRequestHandler);
    EmbeddedChannel channel =
        new EmbeddedChannel(new PublicAccessLogHandler(new MockPublicAccessLogger(new String[0], new String[0], false),
            nettyMetrics), new FailingContinueWriteHandler(), new ChunkedWriteHandler(), processor);
    try {
      HttpHeaders headers = new DefaultHttpHeaders();
      headers.set(EXPECT, CONTINUE);
      HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/s3/", headers);
      httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "continueWriteFailureTest");
      httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");

      channel.writeInbound(httpRequest);
      channel.runPendingTasks();

      assertEquals("The failed continue write should record one client abort", 1,
          metricRegistry.getCounters().get(CONTAINER_METRIC_PREFIX + "ClientAbortCount").getCount());
      assertEquals("The continue response should not enter the server-error abort subset", 0,
          metricRegistry.getCounters().get(CONTAINER_METRIC_PREFIX + "ServerErrorClientAbortCount").getCount());
      assertEquals("The failed continue write should record the termination duration exactly once", 1,
          nettyMetrics.clientTerminatedRequestTimeInMs.getCount());
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  /**
   * Tests that a request still in flight when the channel goes inactive is recorded in
   * {@link NettyMetrics#clientTerminatedRequestTimeInMs} with the time it had been in flight for, and not in the
   * idle histogram.
   */
  @Test
  public void testAbortedRequestRecordsTimeInFlight() throws Exception {
    try (RealChannelFixture fixture = new RealChannelFixture()) {
      long testStartMs = System.currentTimeMillis();
      fixture.sendOpenRequest();
      assertEquals("Nothing should have been recorded while the request is still in flight", 0,
          fixture.nettyMetrics.clientTerminatedRequestTimeInMs.getCount());
      awaitClockAdvance(MIN_IN_FLIGHT_MS);

      fixture.client.close().sync();
      fixture.awaitServerCloseAndDrain();

      assertFalse("A remote close must not be marked as server initiated",
          PublicAccessLogHandler.isServerCloseInitiated(fixture.server()));
      assertEquals("Termination of the in-flight request should have been recorded exactly once", 1,
          fixture.nettyMetrics.clientTerminatedRequestTimeInMs.getCount());
      assertEquals("A client termination should not also be recorded as an idle termination", 0,
          fixture.nettyMetrics.idleTerminatedRequestTimeInMs.getCount());
      assertEquals("The remote close should record one per-container client abort", 1,
          fixture.metricRegistry.getCounters().get(CONTAINER_METRIC_PREFIX + "ClientAbortCount").getCount());
      assertEquals("The 400-classified remote close should not enter the server-error abort subset", 0,
          fixture.metricRegistry.getCounters()
              .get(CONTAINER_METRIC_PREFIX + "ServerErrorClientAbortCount")
              .getCount());
      assertRecordedTimeInFlight(fixture.nettyMetrics.clientTerminatedRequestTimeInMs, testStartMs);
    }
  }

  /**
   * Tests that a server-initiated close still cleans up an open request but is not attributed to the remote client.
   */
  @Test
  public void testServerInitiatedCloseDoesNotRecordClientAbort() throws Exception {
    try (RealChannelFixture fixture = new RealChannelFixture()) {
      fixture.sendOpenRequest();

      fixture.server().close().sync();
      fixture.awaitServerCloseAndDrain();

      assertTrue("The outbound close should be marked as server initiated",
          PublicAccessLogHandler.isServerCloseInitiated(fixture.server()));
      assertEquals("A server close should not record a client-termination duration", 0,
          fixture.nettyMetrics.clientTerminatedRequestTimeInMs.getCount());
      assertEquals("A server close should not record a client abort", 0,
          fixture.metricRegistry.getCounters().get(CONTAINER_METRIC_PREFIX + "ClientAbortCount").getCount());
      assertEquals("A server close should not enter the server-error abort subset", 0,
          fixture.metricRegistry.getCounters()
              .get(CONTAINER_METRIC_PREFIX + "ServerErrorClientAbortCount")
              .getCount());
    }
  }

  /**
   * Tests that a request still in flight when the channel is closed for being idle is recorded in
   * {@link NettyMetrics#idleTerminatedRequestTimeInMs}, since an idle timeout aborts the longest-lived requests and
   * would otherwise be missing from the distribution. It is kept separate from
   * {@link NettyMetrics#clientTerminatedRequestTimeInMs} because the two have different causes: an idle termination is
   * the server enforcing its own timeout, and so cannot be observed sooner than that timeout, whereas a client
   * termination can happen at any point. Pooling them would inject a floor at the idle timeout into a distribution
   * whose signal is at the low end.
   */
  @Test
  public void testIdleChannelAbortRecordsTimeInFlight() {
    NettyMetrics nettyMetrics = new NettyMetrics(new MetricRegistry());
    NettyMessageProcessor processor =
        new NettyMessageProcessor(nettyMetrics, NETTY_CONFIG, PERFORMANCE_CONFIG, requestHandler);
    EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), processor);

    long testStartMs = System.currentTimeMillis();
    channel.writeInbound(RestTestUtils.createRequest(HttpMethod.GET, "/", null));
    awaitClockAdvance(MIN_IN_FLIGHT_MS);

    channel.pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);

    assertEquals("Termination of the idle in-flight request should have been recorded exactly once", 1,
        nettyMetrics.idleTerminatedRequestTimeInMs.getCount());
    assertEquals("An idle termination should not also be recorded as a client termination", 0,
        nettyMetrics.clientTerminatedRequestTimeInMs.getCount());
    assertRecordedTimeInFlight(nettyMetrics.idleTerminatedRequestTimeInMs, testStartMs);
  }

  /**
   * Tests that an idle timeout abort records the time the request was in flight for on a real event loop, where
   * {@code fireChannelInactive} is deferred to a later task and the request is already closed by the time
   * {@link NettyMessageProcessor#channelInactive} runs. {@link EmbeddedChannel} runs that task inline and so cannot
   * exercise this ordering.
   */
  @Test
  public void testIdleChannelAbortOnRealEventLoopRecordsTimeInFlight() throws Exception {
    try (RealChannelFixture fixture = new RealChannelFixture()) {
      long testStartMs = System.currentTimeMillis();
      fixture.sendOpenRequest();
      awaitClockAdvance(MIN_IN_FLIGHT_MS);

      fixture.server()
          .eventLoop()
          .submit(() -> fixture.server().pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT))
          .sync();
      fixture.awaitServerCloseAndDrain();

      assertTrue("The idle timeout should close the channel through the outbound server path",
          PublicAccessLogHandler.isServerCloseInitiated(fixture.server()));
      assertEquals("Termination of the idle in-flight request should have been recorded exactly once", 1,
          fixture.nettyMetrics.idleTerminatedRequestTimeInMs.getCount());
      assertEquals("The deferred channelInactive should not double record the same termination", 0,
          fixture.nettyMetrics.clientTerminatedRequestTimeInMs.getCount());
      assertEquals("An idle timeout should not record a client abort", 0,
          fixture.metricRegistry.getCounters().get(CONTAINER_METRIC_PREFIX + "ClientAbortCount").getCount());
      assertRecordedTimeInFlight(fixture.nettyMetrics.idleTerminatedRequestTimeInMs, testStartMs);
    }
  }

  /**
   * Tests that closing a channel with no request in flight records nothing.
   */
  @Test
  public void testInactiveChannelWithNoRequestInFlightRecordsNothing() {
    NettyMetrics nettyMetrics = new NettyMetrics(new MetricRegistry());
    NettyMessageProcessor processor =
        new NettyMessageProcessor(nettyMetrics, NETTY_CONFIG, PERFORMANCE_CONFIG, requestHandler);
    EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), processor);

    channel.close().awaitUninterruptibly();

    assertEquals("A channel that never carried a request should record nothing", 0,
        nettyMetrics.clientTerminatedRequestTimeInMs.getCount());
    assertEquals("A channel that never carried a request should record no idle termination either", 0,
        nettyMetrics.idleTerminatedRequestTimeInMs.getCount());
  }

  // helpers
  // general

  /**
   * Busy waits until {@link System#currentTimeMillis()} has advanced by at least {@code durationMs}. Used instead of
   * {@link Thread#sleep(long)}, which cannot return early but is disallowed for test synchronization.
   * @param durationMs the number of ms the clock must advance by.
   */
  private static void awaitClockAdvance(long durationMs) {
    long deadlineMs = System.currentTimeMillis() + durationMs;
    while (System.currentTimeMillis() < deadlineMs) {
      Thread.yield();
    }
  }

  /**
   * Asserts that the single time in flight recorded in {@code histogram} is bounded below by
   * {@link #MIN_IN_FLIGHT_MS} and above by the time the test itself has taken. The upper bound is what
   * distinguishes an elapsed time from a wall clock timestamp.
   * @param histogram the {@link Histogram} the value was recorded in.
   * @param testStartMs the value of {@link System#currentTimeMillis()} from before the request was sent.
   */
  private static void assertRecordedTimeInFlight(Histogram histogram, long testStartMs) {
    long recordedMs = histogram.getSnapshot().getMin();
    long testDurationMs = System.currentTimeMillis() - testStartMs;
    assertTrue("Time in flight " + recordedMs + " ms should be at least the " + MIN_IN_FLIGHT_MS
        + " ms the request was held for", recordedMs >= MIN_IN_FLIGHT_MS);
    assertTrue("Time in flight " + recordedMs + " ms should not exceed the " + testDurationMs
        + " ms the test has taken", recordedMs <= testDurationMs);
  }

  /**
   * Creates an {@link EmbeddedChannel} that incorporates an instance of {@link NettyMessageProcessor}.
   * @return an {@link EmbeddedChannel} that incorporates an instance of {@link NettyMessageProcessor}.
   */
  private EmbeddedChannel createChannel() {
    NettyMessageProcessor processor =
        new NettyMessageProcessor(NETTY_METRICS, NETTY_CONFIG, PERFORMANCE_CONFIG, requestHandler);
    return new EmbeddedChannel(new ChunkedWriteHandler(), processor);
  }

  private EmbeddedChannel createChannel(NettyConfig nettyConfig) {
    NettyMessageProcessor processor =
        new NettyMessageProcessor(NETTY_METRICS, nettyConfig, PERFORMANCE_CONFIG, requestHandler);
    return new EmbeddedChannel(new ChunkedWriteHandler(), processor);
  }

  /**
   * Sends the provided {@code httpRequest} and verifies that the response is an echo of the {@code restMethod}.
   * @param channel the {@link EmbeddedChannel} to send the request over.
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param restMethod the equivalent {@link RestMethod} for {@code httpMethod}. Used to check for correctness of
   *                   response.
   * @param isKeepAlive if the request needs to be keep-alive.
   * @throws IOException
   */
  private void sendRequestCheckResponse(EmbeddedChannel channel, HttpMethod httpMethod, RestMethod restMethod,
      boolean isKeepAlive) throws IOException {
    long requestId = REQUEST_ID_GENERATOR.getAndIncrement();
    String uri = MockRestRequestService.ECHO_REST_METHOD + requestId;
    HttpRequest httpRequest = RestTestUtils.createRequest(httpMethod, uri, null);
    HttpUtil.setKeepAlive(httpRequest, isKeepAlive);
    channel.writeInbound(httpRequest);
    channel.writeInbound(new DefaultLastHttpContent());
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", HttpResponseStatus.OK, response.status());
    // MockRestRequestService echoes the RestMethod + request id.
    String expectedResponse = restMethod.toString() + requestId;
    assertEquals("Unexpected content", expectedResponse,
        RestTestUtils.getContentString((HttpContent) channel.readOutbound()));
    assertTrue("End marker was expected", channel.readOutbound() instanceof LastHttpContent);
  }

  /**
   * Does the post test by sending the request and content to {@link NettyMessageProcessor} through an
   * {@link EmbeddedChannel} and returns the data stored in the {@link InMemoryRouter} as a result of the post.
   * @param postRequest the POST request as a {@link HttpRequest}.
   * @param contentToSend the content to be sent as a part of the POST.
   * @return the data stored in the {@link InMemoryRouter} as a result of the POST.
   * @throws InterruptedException
   */
  private ByteBuffer doPostTest(HttpRequest postRequest, List<ByteBuffer> contentToSend) throws InterruptedException {
    EmbeddedChannel channel = createChannel();

    // POST
    notificationSystem.reset();
    postRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
    HttpUtil.setKeepAlive(postRequest, false);
    channel.writeInbound(postRequest);
    if (contentToSend != null) {
      for (ByteBuffer content : contentToSend) {
        channel.writeInbound(new DefaultHttpContent(Unpooled.wrappedBuffer(content)));
      }
      channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
    }
    if (!notificationSystem.operationCompleted.await(1000, TimeUnit.MILLISECONDS)) {
      fail("Post did not succeed after 1000ms. There is an error or timeout needs to increase");
    }
    assertNotNull("Blob id operated on cannot be null", notificationSystem.blobIdOperatedOn);
    return router.getActiveBlobs().get(notificationSystem.blobIdOperatedOn).getBlob();
  }

  /**
   * Compares {@code contentToCheck} to {@code srcOfTruth}.
   * @param contentToCheck the content that needs to be checked against the {@code srcOfTruth}.
   * @param srcOfTruth the original content.
   */
  private void compareContent(ByteBuffer contentToCheck, List<ByteBuffer> srcOfTruth) {
    ByteBuffer truth;
    int counter = 0;
    truth = srcOfTruth.get(counter++);
    while (contentToCheck.hasRemaining()) {
      if (!truth.hasRemaining()) {
        truth = srcOfTruth.get(counter++);
      }
      assertEquals("Byte in actual content differs from original content", truth.get(), contentToCheck.get());
    }
  }

  // requestHandleWithGoodInputTest() helpers

  /**
   * Does a test to see that request handling with good input succeeds when channel is not keep alive.
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param restMethod the equivalent {@link RestMethod} for {@code httpMethod}. Used to check for correctness of
   *                   response.
   * @throws IOException
   */
  private void doRequestHandleWithoutKeepAlive(HttpMethod httpMethod, RestMethod restMethod) throws IOException {
    EmbeddedChannel channel = createChannel();
    sendRequestCheckResponse(channel, httpMethod, restMethod, false);
    assertFalse("Channel not closed", channel.isOpen());
  }

  /**
   * Does a test to see that request handling with good input succeeds when channel is keep alive.
   * @param channel the {@link EmbeddedChannel} to use.
   * @param httpMethod the {@link HttpMethod} for the request.
   * @param restMethod the equivalent {@link RestMethod} for {@code httpMethod}. Used to check for correctness of
   *                   response.
   * @throws IOException
   */
  private void doRequestHandleWithKeepAlive(EmbeddedChannel channel, HttpMethod httpMethod, RestMethod restMethod)
      throws IOException {
    for (int i = 0; i < 5; i++) {
      sendRequestCheckResponse(channel, httpMethod, restMethod, true);
      assertTrue("Channel is closed", channel.isOpen());
    }
  }

  // multipartPostTest() helpers.

  /**
   * Creates a {@link HttpPostRequestEncoder} that encodes the given {@code request} and {@code blobContent}.
   * @param request the {@link HttpRequest} containing headers and other metadata about the request.
   * @param blobContent the {@link ByteBuffer} that represents the content of the blob.
   * @return a {@link HttpPostRequestEncoder} that can encode the {@code request} and {@code blobContent}.
   * @throws HttpPostRequestEncoder.ErrorDataEncoderException
   * @throws IOException
   */
  private HttpPostRequestEncoder createEncoder(HttpRequest request, ByteBuffer blobContent)
      throws HttpPostRequestEncoder.ErrorDataEncoderException, IOException {
    HttpDataFactory httpDataFactory = new DefaultHttpDataFactory(false);
    HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(httpDataFactory, request, true);
    FileUpload fileUpload = new MemoryFileUpload(RestUtils.MultipartPost.BLOB_PART, RestUtils.MultipartPost.BLOB_PART,
        "application/octet-stream", "", Charset.forName("UTF-8"), blobContent.remaining());
    fileUpload.setContent(Unpooled.wrappedBuffer(blobContent));
    encoder.addBodyHttpData(fileUpload);
    return encoder;
  }

  // requestHandlerExceptionTest() helpers.

  /**
   * Does a test where the request handler inside {@link NettyMessageProcessor} fails. Checks for the right error code
   * in the response.
   * @param httpMethod the {@link HttpMethod} to use for the request.
   * @param expectedStatus the excepted {@link HttpResponseStatus} in the response.
   */
  private void doRequestHandlerExceptionTest(HttpMethod httpMethod, HttpResponseStatus expectedStatus) {
    EmbeddedChannel channel = createChannel();
    channel.writeInbound(RestTestUtils.createRequest(httpMethod, "/", null));
    channel.writeInbound(new DefaultLastHttpContent());
    // first outbound has to be response.
    HttpResponse response = (HttpResponse) channel.readOutbound();
    assertEquals("Unexpected response status", expectedStatus, response.status());
  }

  /**
   * A notification system that helps track events in the {@link InMemoryRouter}. Not thread safe and has to be
   * {@link #reset()} before every operation for which it is used.
   */
  private class HelperNotificationSystem implements NotificationSystem {
    /**
     * The blob id of the blob that the last operation was on.
     */
    protected volatile String blobIdOperatedOn = null;
    /**
     * Latch for awaiting the completion of an operation.
     */
    protected volatile CountDownLatch operationCompleted = new CountDownLatch(1);

    @Override
    public void onBlobCreated(String blobId, BlobProperties blobProperties, Account account, Container container,
        NotificationBlobType notificationBlobType) {
      blobIdOperatedOn = blobId;
      operationCompleted.countDown();
    }

    @Override
    public void onBlobTtlUpdated(String blobId, String serviceId, long expiresAtMs, Account account,
        Container container) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void onBlobDeleted(String blobId, String serviceId, Account account, Container container) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void onBlobUndeleted(String blobId, String serviceId, Account account, Container container) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void onBlobReplicated(String blobId, String serviceId, Account account, Container container,
        DataNodeId sourceHost) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void onBlobReplicaCreated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void onBlobReplicaDeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void onBlobReplicaPurged(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void onBlobReplicaUpdated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType,
        UpdateType updateType, MessageInfo info) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void onBlobReplicaUndeleted(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void onBlobReplicaReplicated(String sourceHost, int port, String blobId, BlobReplicaSourceType sourceType) {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void close() {
      // no op.
    }

    /**
     * Resets the state and prepares this instance for another operation.
     */
    protected void reset() {
      blobIdOperatedOn = null;
      operationCompleted = new CountDownLatch(1);
    }
  }

  /**
   * A {@link ChannelOutboundHandlerAdapter} that intercepts the 100-continue {@link
   * FullHttpResponse} write and delays its promise completion. This simulates the production
   * scenario where the security service callback runs on a non-event-loop thread, causing {@code
   * ctx.writeAndFlush()} to be scheduled asynchronously so the write promise completes after
   * subsequent inbound processing has mutated shared state.
   */
  private static class DelayedContinueWriteHandler extends ChannelOutboundHandlerAdapter {
    private ChannelPromise heldPromise;

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (msg instanceof FullHttpResponse && ((FullHttpResponse) msg).status().equals(HttpResponseStatus.CONTINUE)) {
        // Forward the message with a void promise so it is buffered in the channel (readable via
        // readOutbound()),
        // but the original promise is NOT completed — the ResponseMetadataWriteListener won't fire
        // yet.
        heldPromise = promise;
        ctx.write(msg, ctx.voidPromise());
      } else {
        ctx.write(msg, promise);
      }
    }

    /**
     * Completes the held continue write promise, triggering the {@code
     * ResponseMetadataWriteListener} that was attached to the original 100-continue response write.
     */
    void completeContinueWrite() {
      if (heldPromise != null) {
        heldPromise.setSuccess();
      }
    }
  }

  /**
   * Fails the 100-Continue write through its promise, matching an asynchronous response-write failure.
   */
  private static class FailingContinueWriteHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
      if (msg instanceof FullHttpResponse && ((FullHttpResponse) msg).status().equals(HttpResponseStatus.CONTINUE)) {
        ReferenceCountUtil.release(msg);
        promise.setFailure(new ClosedChannelException());
      } else {
        ctx.write(msg, promise);
      }
    }
  }

  /**
   * Holds a request open after injecting the container metrics used by the termination tests.
   */
  private static class HoldingRequestHandler implements RestRequestHandler {
    private final ContainerMetrics containerMetrics;
    private final CountDownLatch requestHandled;

    HoldingRequestHandler(ContainerMetrics containerMetrics, CountDownLatch requestHandled) {
      this.containerMetrics = containerMetrics;
      this.requestHandled = requestHandled;
    }

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      restRequest.getMetricsTracker().injectContainerMetrics(containerMetrics);
      requestHandled.countDown();
    }
  }

  /**
   * Production-shaped local transport used to distinguish remote close from outbound server close.
   */
  private static class RealChannelFixture implements AutoCloseable {
    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final NettyMetrics nettyMetrics = new NettyMetrics(metricRegistry);
    private final EventLoopGroup group = new DefaultEventLoopGroup(1);
    private final AtomicReference<Channel> serverChannel = new AtomicReference<>();
    private final CountDownLatch serverInitialized = new CountDownLatch(1);
    private final CountDownLatch requestHandled = new CountDownLatch(1);
    private final Channel listener;
    private final Channel client;

    RealChannelFixture() throws Exception {
      RestRequestMetricsTracker.setDefaults(metricRegistry);
      ContainerMetrics containerMetrics =
          new ContainerMetrics("account", "container", "PostBlob", metricRegistry, false, null);
      HoldingRequestHandler requestHandler = new HoldingRequestHandler(containerMetrics, requestHandled);
      LocalAddress address = new LocalAddress("client-abort-" + REQUEST_ID_GENERATOR.incrementAndGet());
      listener = new ServerBootstrap().group(group)
          .channel(LocalServerChannel.class)
          .childHandler(new ChannelInitializer<LocalChannel>() {
            @Override
            protected void initChannel(LocalChannel channel) {
              channel.pipeline()
                  .addLast(new PublicAccessLogHandler(
                      new MockPublicAccessLogger(new String[0], new String[0], false), nettyMetrics))
                  .addLast(new ChunkedWriteHandler())
                  .addLast(new NettyMessageProcessor(nettyMetrics, NETTY_CONFIG, PERFORMANCE_CONFIG, requestHandler));
              serverChannel.set(channel);
              serverInitialized.countDown();
            }
          })
          .bind(address)
          .sync()
          .channel();
      client = new Bootstrap().group(group)
          .channel(LocalChannel.class)
          .handler(new ChannelInboundHandlerAdapter())
          .connect(address)
          .sync()
          .channel();
      assertTrue("Server child channel should have initialized", serverInitialized.await(5, TimeUnit.SECONDS));
    }

    void sendOpenRequest() throws Exception {
      HttpRequest request = RestTestUtils.createRequest(HttpMethod.POST, "/", null);
      HttpUtil.setKeepAlive(request, true);
      client.writeAndFlush(request).sync();
      assertTrue("Request handler should have received the request", requestHandled.await(5, TimeUnit.SECONDS));
    }

    Channel server() {
      return serverChannel.get();
    }

    void awaitServerCloseAndDrain() throws Exception {
      assertTrue("Server side channel should have closed", server().closeFuture().await(5, TimeUnit.SECONDS));
      server().eventLoop().submit(() -> {
      }).sync();
      server().eventLoop().submit(() -> {
      }).sync();
    }

    @Override
    public void close() throws Exception {
      client.close().awaitUninterruptibly();
      listener.close().awaitUninterruptibly();
      group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
    }
  }

}
