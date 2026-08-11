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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.NettyConfig;
import com.github.ambry.config.PerformanceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.notification.BlobReplicaSourceType;
import com.github.ambry.notification.NotificationBlobType;
import com.github.ambry.notification.NotificationSystem;
import com.github.ambry.notification.UpdateType;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.ClientChannelCloseException;
import com.github.ambry.utils.PossibleClientChannelCloseException;
import com.github.ambry.utils.TestUtils;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
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
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Test;

import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.Headers.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link NettyMessageProcessor}.
 */
public class NettyMessageProcessorTest {
  private final InMemoryRouter router;
  private final RestRequestService restRequestService;
  private final MockRestRequestResponseHandler requestHandler;
  private final HelperNotificationSystem notificationSystem = new HelperNotificationSystem();

  private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong(0);
  private static final NettyMetrics NETTY_METRICS = new NettyMetrics(new MetricRegistry());
  private static final NettyConfig NETTY_CONFIG = new NettyConfig(new VerifiableProperties(new Properties()));
  private static final PerformanceConfig PERFORMANCE_CONFIG =
      new PerformanceConfig(new VerifiableProperties(new Properties()));
  // A RestServerState that reports the service as up, matching normal request-serving conditions. Only isServiceUp()
  // is read by NettyMessageProcessor, so a single shared instance is safe across tests.
  private static final RestServerState SERVICE_UP_STATE = createServiceUpState();

  private static RestServerState createServiceUpState() {
    RestServerState state = new RestServerState("/healthCheck");
    state.markServiceUp();
    return state;
  }

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
        new NettyMessageProcessor(NETTY_METRICS, nettyConfig, PERFORMANCE_CONFIG, requestHandler, SERVICE_UP_STATE);
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
        new NettyMessageProcessor(NETTY_METRICS, nettyConfig, PERFORMANCE_CONFIG, requestHandler, SERVICE_UP_STATE);
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
   * Verifies that {@link NettyMessageProcessor#channelInactive} flips {@code NettyRequest.channelOpen} to
   * false so downstream callbacks that later check {@code restRequest.isOpen()} observe the disconnect.
   * <p>
   * Regression: previously {@code channelInactive} only called {@code onRequestAborted(...)}, which routes
   * an exception into the response channel but does NOT close the {@link NettyRequest}. Best-effort race
   * guards elsewhere (e.g. named-blob metadata commit in {@code AmbryIdConverterFactory}) depend on
   * {@code restRequest.isOpen() == false} to short-circuit; without this fix they would still run.
   *
   * @throws Exception
   */
  @Test
  public void channelInactiveClosesInflightRequestTest() throws Exception {
    // Custom handler that captures the RestRequest so we can inspect isOpen() after channelInactive fires.
    CapturingRestRequestHandler capturingHandler = new CapturingRestRequestHandler();
    capturingHandler.start();
    try {
      NettyMessageProcessor processor =
          new NettyMessageProcessor(NETTY_METRICS, NETTY_CONFIG, PERFORMANCE_CONFIG, capturingHandler, SERVICE_UP_STATE);
      EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), processor);

      // Send a PUT header only (no LastHttpContent) so the request stays in-flight when we close the channel.
      HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/", null);
      httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "channelInactiveClosesInflightRequestTest");
      httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
      channel.writeInbound(httpRequest);

      RestRequest capturedRequest = capturingHandler.getCapturedRequest();
      assertNotNull("Handler should have received the in-flight RestRequest", capturedRequest);
      assertTrue("RestRequest must be open before channelInactive", capturedRequest.isOpen());

      // Simulate the client TCP disconnect / channel becoming inactive mid-request.
      channel.close().awaitUninterruptibly();

      assertFalse("RestRequest.isOpen() must be false after channelInactive so downstream callbacks "
          + "(e.g. named-blob metadata commit) can observe the disconnect", capturedRequest.isOpen());
    } finally {
      capturingHandler.shutdown();
    }
  }

  /**
   * Verifies that a client TCP disconnect ({@link NettyMessageProcessor#channelInactive}) while a PUT request is
   * still in-flight delivers a {@link ClientChannelCloseException} to the pending {@code readInto} callback, so
   * downstream consumers can recognize the termination as client-rooted via {@code instanceof} or
   * {@link com.github.ambry.utils.Utils#isPossibleClientTermination(Throwable)}.
   * <p/>
   * Also verifies that the same typed exception is now delivered to the separate response-completion path
   * (via {@code onRequestAborted}) - not just to {@code readInto} - and that doing so is behavior-neutral. Unlike
   * the idle-timeout case below, this is NOT independently verified by a runtime assertion in this test: by the
   * time {@code onRequestAborted} runs here, the network channel has already begun closing (this test simulates
   * the abort via {@code channel.close()} itself), so {@code NettyResponseChannel} never gets to actually write an
   * error response to the outbound queue - {@code channel.readOutbound()} is always {@code null} in this scenario.
   * Behavior-neutrality for this call site is instead established by code inspection (see the PR description):
   * {@code NettyResponseChannel#getErrorResponse} routes through {@code Utils.isPossibleClientTermination(cause)},
   * which recognizes {@link ClientChannelCloseException} unconditionally via {@code instanceof} - identically to
   * how the legacy {@code Utils.convertToClientTerminationException(...)} message wrap it replaces always matched
   * that same check - so the response status code and {@code clientEarlyTerminationCount} metric this call site
   * would have produced are provably unchanged, even though no response is actually observable in this test.
   * @throws Exception
   */
  @Test
  public void channelInactiveDeliversClientTerminationToReadIntoTest() throws Exception {
    CapturingRestRequestHandler capturingHandler = new CapturingRestRequestHandler();
    capturingHandler.start();
    try {
      NettyMessageProcessor processor =
          new NettyMessageProcessor(NETTY_METRICS, NETTY_CONFIG, PERFORMANCE_CONFIG, capturingHandler, SERVICE_UP_STATE);
      EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), processor);

      HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/", null);
      httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "channelInactiveDeliversClientTerminationToReadIntoTest");
      httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
      channel.writeInbound(httpRequest);

      RestRequest capturedRequest = capturingHandler.getCapturedRequest();
      assertNotNull("Handler should have received the in-flight RestRequest", capturedRequest);

      ReadIntoCallback callback = new ReadIntoCallback();
      capturedRequest.readInto(new com.github.ambry.commons.ByteBufferAsyncWritableChannel(), callback);

      // Simulate the client TCP disconnect / channel becoming inactive mid-request.
      channel.close().awaitUninterruptibly();

      callback.awaitCallback();
      assertNotNull("readInto callback should have received an exception", callback.exception);
      assertTrue("readInto callback exception should be a ClientChannelCloseException",
          callback.exception instanceof ClientChannelCloseException);

      // No outbound error response is observable in this scenario - see the class-level javadoc note above for why
      // this call site's Path B behavior-neutrality is verified by code inspection instead.
      assertNull("No outbound response is expected once the channel is already closing", channel.readOutbound());
    } finally {
      capturingHandler.shutdown();
    }
  }

  /**
   * Verifies that when the channel becomes inactive while the service is DOWN (e.g. a server-initiated close during
   * {@link RestServer#shutdown()}, where the Netty worker event loop's {@code shutdownGracefully()} closes in-flight
   * connections), {@link NettyMessageProcessor#channelInactive} delivers only the "possible" tier
   * ({@link PossibleClientChannelCloseException}) - NOT the high-confidence {@link ClientChannelCloseException} - to
   * the pending {@code readInto} callback. {@code channelInactive} fires for both client- and server-initiated closes,
   * so a server-shutdown abort must not be mislabeled as a proven client termination (which would incorrectly suppress
   * router health metrics for a genuine server-side event). The request is still open when the channel goes inactive,
   * exactly as in the sure-tier case; only {@code restServerState.isServiceUp()} distinguishes the two.
   * @throws Exception
   */
  @Test
  public void channelInactiveWhileServiceDownDeliversPossibleClientTerminationTest() throws Exception {
    CapturingRestRequestHandler capturingHandler = new CapturingRestRequestHandler();
    capturingHandler.start();
    RestServerState serviceDownState = new RestServerState("/healthCheck");
    // Leave the service marked down (the default) to simulate a server shutdown already in progress.
    try {
      NettyMessageProcessor processor =
          new NettyMessageProcessor(NETTY_METRICS, NETTY_CONFIG, PERFORMANCE_CONFIG, capturingHandler, serviceDownState);
      EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), processor);

      HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/", null);
      httpRequest.headers()
          .set(RestUtils.Headers.SERVICE_ID, "channelInactiveWhileServiceDownDeliversPossibleClientTerminationTest");
      httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
      channel.writeInbound(httpRequest);

      RestRequest capturedRequest = capturingHandler.getCapturedRequest();
      assertNotNull("Handler should have received the in-flight RestRequest", capturedRequest);
      assertTrue("RestRequest must be open before channelInactive", capturedRequest.isOpen());

      ReadIntoCallback callback = new ReadIntoCallback();
      capturedRequest.readInto(new com.github.ambry.commons.ByteBufferAsyncWritableChannel(), callback);

      // Simulate the channel becoming inactive while the server is shutting down.
      channel.close().awaitUninterruptibly();

      callback.awaitCallback();
      assertNotNull("readInto callback should have received an exception", callback.exception);
      assertFalse("A server-initiated close (service down) must NOT be classified as the high-confidence "
              + "ClientChannelCloseException", callback.exception instanceof ClientChannelCloseException);
      assertTrue("A server-initiated close (service down) should deliver a PossibleClientChannelCloseException",
          callback.exception instanceof PossibleClientChannelCloseException);
      assertFalse("RestRequest.isOpen() must be false after channelInactive", capturedRequest.isOpen());
    } finally {
      capturingHandler.shutdown();
    }
  }

  /**
   * Verifies that a client idle/stall timeout (the {@link IdleState#ALL_IDLE} branch of
   * {@link NettyMessageProcessor#userEventTriggered}) delivers a {@link PossibleClientChannelCloseException} - not
   * the high-confidence {@link ClientChannelCloseException} - to the pending {@code readInto} callback. This is
   * intentional: {@link NettyRequest} suspends reads (autoRead=false) on the channel while a slow/backpressured
   * downstream consumer keeps buffered data above {@code nettyServerRequestBufferWatermark} (see
   * {@link NettyRequest#continueReadIfPossible}), so {@code ALL_IDLE} can fire purely due to a server/destination-side
   * stall rather than genuine client inactivity. Since this ambiguity cannot be cleanly disambiguated with high
   * confidence, idle-timeout is tagged with the "possible" tier rather than the "sure" tier; only
   * {@code channelInactive} has been proven exclusively client-rooted.
   * @throws Exception
   */
  @Test
  public void idleTimeoutDeliversPossibleClientTerminationToReadIntoTest() throws Exception {
    CapturingRestRequestHandler capturingHandler = new CapturingRestRequestHandler();
    capturingHandler.start();
    try {
      NettyMessageProcessor processor =
          new NettyMessageProcessor(NETTY_METRICS, NETTY_CONFIG, PERFORMANCE_CONFIG, capturingHandler, SERVICE_UP_STATE);
      EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), processor);

      HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/", null);
      httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "idleTimeoutDeliversPossibleClientTerminationToReadIntoTest");
      httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
      channel.writeInbound(httpRequest);

      RestRequest capturedRequest = capturingHandler.getCapturedRequest();
      assertNotNull("Handler should have received the in-flight RestRequest", capturedRequest);

      ReadIntoCallback callback = new ReadIntoCallback();
      capturedRequest.readInto(new com.github.ambry.commons.ByteBufferAsyncWritableChannel(), callback);

      // Simulate the connection going idle past the configured timeout.
      channel.pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);
      channel.runPendingTasks();

      callback.awaitCallback();
      assertNotNull("readInto callback should have received an exception", callback.exception);
      assertFalse(
          "readInto callback exception must NOT be the high-confidence ClientChannelCloseException for idle-timeout "
              + "(ambiguous - could be a server/destination-side stall) - see channelInactive for the proven "
              + "client-exclusive case", callback.exception instanceof ClientChannelCloseException);
      assertTrue("readInto callback exception should be a PossibleClientChannelCloseException for idle-timeout",
          callback.exception instanceof PossibleClientChannelCloseException);

      // The idle-timeout channel is still active when onRequestAborted fires, so an error response is written
      // before network teardown; assert it is BAD_REQUEST - unchanged from the pre-existing message-based wrap -
      // proving the typed-exception propagation into onRequestAborted for this call site is behavior-neutral.
      HttpResponse outboundResponse = channel.readOutbound();
      assertNotNull("An error response should have been written for the idle-timeout abort", outboundResponse);
      assertEquals("Response status for a possible-client abort must remain BAD_REQUEST", HttpResponseStatus.BAD_REQUEST,
          outboundResponse.status());
    } finally {
      capturingHandler.shutdown();
    }
  }

  /**
   * Verifies (and documents) that an {@link IOException} reaching {@link NettyMessageProcessor#exceptionCaught}
   * while a PUT request is still in-flight (e.g. "connection reset"/"broken pipe" while reading further request
   * content from the client) delivers a {@link PossibleClientChannelCloseException} - not the high-confidence
   * {@link ClientChannelCloseException} - to the pending {@code readInto} callback. This path is a plausible
   * client-rooted signal, but exclusivity isn't proven the way it is for {@code channelInactive} - a destination-write
   * failure could in principle propagate here via Netty's implicit exception routing - so it is tagged with the
   * "possible" tier.
   * @throws Exception
   */
  @Test
  public void exceptionCaughtIOExceptionDeliversPossibleClientTerminationToReadIntoTest() throws Exception {
    CapturingRestRequestHandler capturingHandler = new CapturingRestRequestHandler();
    capturingHandler.start();
    try {
      NettyMessageProcessor processor =
          new NettyMessageProcessor(NETTY_METRICS, NETTY_CONFIG, PERFORMANCE_CONFIG, capturingHandler, SERVICE_UP_STATE);
      EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), processor);

      HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/", null);
      httpRequest.headers()
          .set(RestUtils.Headers.SERVICE_ID, "exceptionCaughtIOExceptionDeliversPossibleClientTerminationToReadIntoTest");
      httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
      channel.writeInbound(httpRequest);

      RestRequest capturedRequest = capturingHandler.getCapturedRequest();
      assertNotNull("Handler should have received the in-flight RestRequest", capturedRequest);

      ReadIntoCallback callback = new ReadIntoCallback();
      capturedRequest.readInto(new com.github.ambry.commons.ByteBufferAsyncWritableChannel(), callback);

      // Simulate an IOException surfacing on the client-facing channel while the request is still open (e.g. a
      // broken pipe / connection reset detected while trying to read further content from the client).
      channel.pipeline().fireExceptionCaught(new IOException("Simulated connection reset by peer"));
      channel.runPendingTasks();

      callback.awaitCallback();
      assertNotNull("readInto callback should have received an exception", callback.exception);
      assertFalse(
          "readInto callback exception must NOT be the high-confidence ClientChannelCloseException for "
              + "exceptionCaught's IOException branch (exclusivity unproven) - see channelInactive for the proven "
              + "client-exclusive case", callback.exception instanceof ClientChannelCloseException);
      assertTrue(
          "readInto callback exception should be a PossibleClientChannelCloseException for exceptionCaught's "
              + "IOException branch", callback.exception instanceof PossibleClientChannelCloseException);
    } finally {
      capturingHandler.shutdown();
    }
  }

  /**
   * Verifies that a server-side abort (e.g. {@link NettyMessageProcessor#exceptionCaught} triggered by an internal
   * {@link RestServiceException}, with no client disconnect) does NOT deliver a {@link ClientChannelCloseException}
   * or a {@link PossibleClientChannelCloseException} to the pending {@code readInto} callback - it must land in the
   * unclassified "other" tier. This is the positive-tagging counterpart to
   * {@link #channelInactiveDeliversClientTerminationToReadIntoTest()}, and complements
   * {@link #idleTimeoutDeliversPossibleClientTerminationToReadIntoTest()} and
   * {@link #exceptionCaughtIOExceptionDeliversPossibleClientTerminationToReadIntoTest()}, together proving
   * server/internal terminations are never mis-tagged as client-rooted (at either the "sure" or "possible" tier).
   * @throws Exception
   */
  @Test
  public void serverAbortDoesNotDeliverClientTerminationToReadIntoTest() throws Exception {
    CapturingRestRequestHandler capturingHandler = new CapturingRestRequestHandler();
    capturingHandler.start();
    try {
      NettyMessageProcessor processor =
          new NettyMessageProcessor(NETTY_METRICS, NETTY_CONFIG, PERFORMANCE_CONFIG, capturingHandler, SERVICE_UP_STATE);
      EmbeddedChannel channel = new EmbeddedChannel(new ChunkedWriteHandler(), processor);

      HttpRequest httpRequest = RestTestUtils.createRequest(HttpMethod.PUT, "/", null);
      httpRequest.headers().set(RestUtils.Headers.SERVICE_ID, "serverAbortDoesNotDeliverClientTerminationToReadIntoTest");
      httpRequest.headers().set(RestUtils.Headers.AMBRY_CONTENT_TYPE, "application/octet-stream");
      channel.writeInbound(httpRequest);

      RestRequest capturedRequest = capturingHandler.getCapturedRequest();
      assertNotNull("Handler should have received the in-flight RestRequest", capturedRequest);

      ReadIntoCallback callback = new ReadIntoCallback();
      capturedRequest.readInto(new com.github.ambry.commons.ByteBufferAsyncWritableChannel(), callback);

      // Simulate a purely server-side/internal abort - no client disconnect, no idle timeout.
      channel.pipeline()
          .fireExceptionCaught(new RestServiceException("Simulated internal error", RestServiceErrorCode.InternalServerError));
      channel.runPendingTasks();

      callback.awaitCallback();
      assertNotNull("readInto callback should have received an exception", callback.exception);
      assertFalse("readInto callback exception must NOT be a ClientChannelCloseException for a server-rooted abort",
          callback.exception instanceof ClientChannelCloseException);
      assertFalse(
          "readInto callback exception must NOT be a PossibleClientChannelCloseException for a server-rooted abort",
          callback.exception instanceof PossibleClientChannelCloseException);
    } finally {
      capturingHandler.shutdown();
    }
  }

  /**
   * {@link RestRequestHandler} that captures the first {@link RestRequest} passed to
   * {@link #handleRequest(RestRequest, RestResponseChannel)} and does nothing else. Used by
   * {@link #channelInactiveClosesInflightRequestTest()} to hold a reference to an in-flight request
   * so its {@code isOpen()} state can be observed after {@code channelInactive}.
   */
  private static class CapturingRestRequestHandler implements RestRequestHandler {
    private final java.util.concurrent.atomic.AtomicReference<RestRequest> captured =
        new java.util.concurrent.atomic.AtomicReference<>();

    @Override
    public void start() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void handleRequest(RestRequest restRequest, RestResponseChannel restResponseChannel) {
      captured.compareAndSet(null, restRequest);
      // Do NOT complete the request — leave it in-flight so channelInactive fires before completion.
    }

    RestRequest getCapturedRequest() {
      return captured.get();
    }
  }

  // helpers
  // general

  /**
   * Creates an {@link EmbeddedChannel} that incorporates an instance of {@link NettyMessageProcessor}.
   * @return an {@link EmbeddedChannel} that incorporates an instance of {@link NettyMessageProcessor}.
   */
  private EmbeddedChannel createChannel() {
    NettyMessageProcessor processor =
        new NettyMessageProcessor(NETTY_METRICS, NETTY_CONFIG, PERFORMANCE_CONFIG, requestHandler, SERVICE_UP_STATE);
    return new EmbeddedChannel(new ChunkedWriteHandler(), processor);
  }

  private EmbeddedChannel createChannel(NettyConfig nettyConfig) {
    NettyMessageProcessor processor =
        new NettyMessageProcessor(NETTY_METRICS, nettyConfig, PERFORMANCE_CONFIG, requestHandler, SERVICE_UP_STATE);
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
}
