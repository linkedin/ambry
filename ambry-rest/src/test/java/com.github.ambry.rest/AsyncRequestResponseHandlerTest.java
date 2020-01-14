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
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.ByteBufferRSC;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.utils.TestUtils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLSession;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link AsyncRequestResponseHandler}.
 */
public class AsyncRequestResponseHandlerTest {
  private static VerifiableProperties verifiableProperties;
  private static Router router;
  private static MockRestRequestService restRequestService;
  private static AsyncRequestResponseHandler asyncRequestResponseHandler;

  /**
   * Sets up all the tests by providing a started {@link AsyncRequestResponseHandler} for their use.
   * @throws InstantiationException
   * @throws IOException
   */
  @BeforeClass
  public static void startRequestResponseHandler() throws InstantiationException, IOException {
    verifiableProperties = new VerifiableProperties(new Properties());
    router = new InMemoryRouter(verifiableProperties, new MockClusterMap());
    restRequestService = new MockRestRequestService(verifiableProperties, router);
    asyncRequestResponseHandler =
        new AsyncRequestResponseHandler(new RequestResponseHandlerMetrics(new MetricRegistry()), 5, restRequestService);
    restRequestService.setupResponseHandler(asyncRequestResponseHandler);
    restRequestService.start();
    asyncRequestResponseHandler.start();
  }

  /**
   * Shuts down the created {@link AsyncRequestResponseHandler}.
   * @throws IOException
   */
  @AfterClass
  public static void shutdownRequestResponseHandler() throws IOException {
    asyncRequestResponseHandler.shutdown();
    restRequestService.shutdown();
    router.close();
  }

  /**
   * Tests {@link AsyncRequestResponseHandler#start()} and {@link AsyncRequestResponseHandler#shutdown()}.
   * @throws IOException
   */
  @Test
  public void startShutdownTest() throws IOException {
    final int EXPECTED_WORKER_COUNT = new Random().nextInt(10);
    AsyncRequestResponseHandler handler = getAsyncRequestResponseHandler(EXPECTED_WORKER_COUNT);
    assertEquals("Number of workers alive is incorrect", 0, handler.getWorkersAlive());
    assertFalse("IsRunning should be false", handler.isRunning());
    handler.start();
    try {
      assertTrue("IsRunning should be true", handler.isRunning());
      assertEquals("Number of workers alive is incorrect", EXPECTED_WORKER_COUNT, handler.getWorkersAlive());
    } finally {
      handler.shutdown();
    }
    assertFalse("IsRunning should be false", handler.isRunning());
  }

  /**
   * Tests for {@link AsyncRequestResponseHandler#shutdown()} when {@link AsyncRequestResponseHandler#start()} has not
   * been called previously. This test is for cases where {@link AsyncRequestResponseHandler#start()} has failed and
   * {@link AsyncRequestResponseHandler#shutdown()} needs to be run.
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStart() throws IOException {
    AsyncRequestResponseHandler handler = getAsyncRequestResponseHandler(1);
    handler.shutdown();
  }

  /**
   * This tests for exceptions thrown when a {@link AsyncRequestResponseHandler} is used without calling
   * {@link AsyncRequestResponseHandler#start()}first.
   * @throws IOException
   * @throws JSONException
   * @throws URISyntaxException
   */
  @Test
  public void useServiceWithoutStartTest() throws IOException, JSONException, URISyntaxException {
    AsyncRequestResponseHandler handler = getAsyncRequestResponseHandler(1);
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    try {
      handler.handleRequest(restRequest, new MockRestResponseChannel());
      fail("Should have thrown RestServiceException because the AsyncRequestResponseHandler has not been started");
    } catch (RestServiceException e) {
      assertEquals("The RestServiceErrorCode does not match", RestServiceErrorCode.ServiceUnavailable,
          e.getErrorCode());
    }

    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    try {
      handler.handleResponse(restRequest, new MockRestResponseChannel(),
          new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0)), null);
      fail("Should have thrown RestServiceException because the AsyncRequestResponseHandler has not been started");
    } catch (RestServiceException e) {
      assertEquals("The RestServiceErrorCode does not match", RestServiceErrorCode.ServiceUnavailable,
          e.getErrorCode());
    }
  }

  /**
   * Tests the behavior of {@link AsyncRequestResponseHandler} when request worker count or restRequestService is invalid.
   * @throws Exception
   */
  @Test
  public void edgeCaseTest() throws Exception {
    AsyncRequestResponseHandler requestResponseHandler;
    RequestResponseHandlerMetrics metrics;

    // set request workers < 0
    try {
      requestResponseHandler = getAsyncRequestResponseHandler(-1);
      fail("Setting request workers < 0 should have thrown exception");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    // set null RestRequestService
    try {
      RestRequestService restRequestService = new MockRestRequestService(verifiableProperties, router);
      metrics = new RequestResponseHandlerMetrics(new MetricRegistry());
      new AsyncRequestResponseHandler(metrics, 1, null);
      fail("Setting RestRequestService to null should have thrown exception");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }
  }

  /**
   * Tests handling of all {@link RestMethod}s. The {@link MockRestRequestService} instance being used is
   * asked to only echo the method.
   * @throws Exception
   */
  @Test
  public void allRestMethodsSuccessTest() throws Exception {
    for (int i = 0; i < 25; i++) {
      for (RestMethod restMethod : RestMethod.values()) {
        // PUT is not supported, so it always fails.
        if (restMethod != RestMethod.UNKNOWN && restMethod != RestMethod.PUT) {
          doHandleRequestSuccessTest(restMethod, asyncRequestResponseHandler);
        }
      }
    }
  }

  /**
   * Tests that right exceptions are thrown on bad input to
   * {@link AsyncRequestResponseHandler#handleRequest(RestRequest, RestResponseChannel)}. These are exceptions that get
   * thrown before queuing.
   * @throws Exception
   */
  @Test
  public void handleRequestFailureBeforeQueueTest() throws Exception {
    // RestRequest null.
    try {
      asyncRequestResponseHandler.handleRequest(null, new MockRestResponseChannel());
      fail("Test should have thrown exception, but did not");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // RestResponseChannel null.
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject(), null);
    try {
      asyncRequestResponseHandler.handleRequest(restRequest, null);
      fail("Test should have thrown exception, but did not");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // AsyncRequestResponseHandler should still be alive and serving requests
    assertTrue("AsyncRequestResponseHandler is dead", asyncRequestResponseHandler.isRunning());
  }

  /**
   * Tests that right exceptions are thrown on bad input while a de-queued {@link RestRequest} is being handled.
   * @throws Exception
   */
  @Test
  public void handleRequestFailureOnDequeueTest() throws Exception {
    unknownRestMethodTest(asyncRequestResponseHandler);
    putRestMethodTest(asyncRequestResponseHandler);
    delayedHandleRequestThatThrowsRestException(asyncRequestResponseHandler);
    delayedHandleRequestThatThrowsRuntimeException(asyncRequestResponseHandler);
  }

  /**
   * Tests {@link AsyncRequestResponseHandler#handleResponse(RestRequest, RestResponseChannel, ReadableStreamChannel, Exception)} with good input.
   * @throws Exception
   */
  @Test
  public void handleResponseSuccessTest() throws Exception {
    for (int i = 0; i < 100; i++) {
      doHandleResponseSuccessTest(asyncRequestResponseHandler);
    }
  }

  /**
   * Tests the reaction of {@link AsyncRequestResponseHandler#handleResponse(RestRequest, RestResponseChannel, ReadableStreamChannel, Exception)} to some misbehaving components.
   * @throws Exception
   */
  @Test
  public void handleResponseExceptionTest() throws Exception {
    ByteBufferRSC response = new ByteBufferRSC(ByteBuffer.allocate(0));
    // RestRequest null.
    try {
      asyncRequestResponseHandler.handleResponse(null, new MockRestResponseChannel(), response, null);
      fail("Test should have thrown exception, but did not");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // RestResponseChannel null.
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject(), null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    try {
      asyncRequestResponseHandler.handleResponse(restRequest, null, response, null);
      fail("Test should have thrown exception, but did not");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // Writing response throws exception.
    MockRestRequest goodRestRequest = createRestRequest(RestMethod.GET, "/", null, null);
    goodRestRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    String exceptionMsg = "@@randomMsg@@";
    ReadableStreamChannel badResponse = new IncompleteReadReadableStreamChannel(null, new Exception(exceptionMsg));
    awaitResponse(asyncRequestResponseHandler, goodRestRequest, restResponseChannel, badResponse, null);
    assertNotNull("MockRestResponseChannel would have been passed an exception", restResponseChannel.getException());
    assertEquals("Exception message does not match", exceptionMsg, restResponseChannel.getException().getMessage());

    // Writing response is incomplete
    goodRestRequest = createRestRequest(RestMethod.GET, "/", null, null);
    goodRestRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    restResponseChannel = new MockRestResponseChannel();
    badResponse = new IncompleteReadReadableStreamChannel(0L, null);
    awaitResponse(asyncRequestResponseHandler, goodRestRequest, restResponseChannel, badResponse, null);
    assertNotNull("MockRestResponseChannel would have been passed an exception", restResponseChannel.getException());
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getException().getClass());

    // No bytes read and no exception
    goodRestRequest = createRestRequest(RestMethod.GET, "/", null, null);
    goodRestRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    restResponseChannel = new MockRestResponseChannel();
    badResponse = new IncompleteReadReadableStreamChannel(null, null);
    awaitResponse(asyncRequestResponseHandler, goodRestRequest, restResponseChannel, badResponse, null);
    assertNotNull("MockRestResponseChannel would have been passed an exception", restResponseChannel.getException());
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getException().getClass());

    // RestRequest is bad.
    BadRestRequest badRestRequest = new BadRestRequest();
    badRestRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    restResponseChannel = new MockRestResponseChannel();
    ByteBufferRSC goodResponse = new ByteBufferRSC(ByteBuffer.allocate(0));
    EventMonitor<ByteBufferRSC.Event> responseCloseMonitor =
        new EventMonitor<ByteBufferRSC.Event>(ByteBufferRSC.Event.Close);
    goodResponse.addListener(responseCloseMonitor);
    awaitResponse(asyncRequestResponseHandler, badRestRequest, restResponseChannel, goodResponse, null);
    assertTrue("Response is not closed", responseCloseMonitor.awaitEvent(1, TimeUnit.SECONDS));

    // AsyncRequestResponseHandler should still be alive and serving requests
    assertTrue("AsyncRequestResponseHandler is dead", asyncRequestResponseHandler.isRunning());
  }

  /**
   * Tests various functions when multiple responses are in flight.
   * @throws Exception
   */
  @Test
  public void multipleResponsesInFlightTest() throws Exception {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    AsyncRequestResponseHandler responseHandler = getAsyncRequestResponseHandler(0);
    responseHandler.start();
    try {
      final int EXPECTED_QUEUE_SIZE = 5;

      // test for duplicate request submission
      // submit a few responses that halt on read
      CountDownLatch releaseRead = new CountDownLatch(1);
      byte[] data = null;
      RestRequest restRequest = null;
      MockRestResponseChannel restResponseChannel = null;
      ReadableStreamChannel response = null;
      for (int i = 0; i < EXPECTED_QUEUE_SIZE; i++) {
        data = TestUtils.getRandomBytes(32);
        response = new HaltingRSC(ByteBuffer.wrap(data), releaseRead, executorService);
        restRequest = createRestRequest(RestMethod.GET, "/", null, null);
        restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
        restResponseChannel = new MockRestResponseChannel(restRequest);
        responseHandler.handleResponse(restRequest, restResponseChannel, response, null);
      }
      assertEquals("Response set size unexpected", EXPECTED_QUEUE_SIZE, responseHandler.getResponseSetSize());
      // attach a listener to the last MockRestResponseChannel
      EventMonitor<MockRestResponseChannel.Event> eventMonitor =
          new EventMonitor<MockRestResponseChannel.Event>(MockRestResponseChannel.Event.OnRequestComplete);
      restResponseChannel.addListener(eventMonitor);
      // we try to re submit the last response. This should fail.
      try {
        responseHandler.handleResponse(restRequest, restResponseChannel, response, null);
        fail("Trying to resubmit a response that is already in flight should have failed");
      } catch (RestServiceException e) {
        assertEquals("Unexpected error code", RestServiceErrorCode.RequestResponseQueuingFailure, e.getErrorCode());
      }
      assertEquals("Response set size unexpected", EXPECTED_QUEUE_SIZE, responseHandler.getResponseSetSize());

      releaseRead.countDown();
      if (!eventMonitor.awaitEvent(1, TimeUnit.SECONDS)) {
        fail("awaitResponse took too long. There might be a problem or the timeout may need to be increased");
      }
      // compare the output. We care only about the last one because we want to make sure the duplicate submission
      // did not mess with the original
      assertArrayEquals("Unexpected data in the response", data, restResponseChannel.getResponseBody());

      // test for shutdown when responses are still in progress
      releaseRead = new CountDownLatch(1);
      List<MockRestResponseChannel> responseChannels = new ArrayList<MockRestResponseChannel>(EXPECTED_QUEUE_SIZE);
      for (int i = 0; i < EXPECTED_QUEUE_SIZE; i++) {
        response = new HaltingRSC(ByteBuffer.allocate(0), releaseRead, executorService);
        restRequest = createRestRequest(RestMethod.GET, "/", null, null);
        restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
        restResponseChannel = new MockRestResponseChannel(restRequest);
        responseHandler.handleResponse(restRequest, restResponseChannel, response, null);
        responseChannels.add(restResponseChannel);
      }
      assertEquals("Response set size unexpected", EXPECTED_QUEUE_SIZE, responseHandler.getResponseSetSize());
      responseHandler.shutdown();
      assertEquals("Response set size unexpected", 0, responseHandler.getResponseSetSize());
      releaseRead.countDown();
      // all of the responses should have exceptions
      for (MockRestResponseChannel channel : responseChannels) {
        assertNotNull("There should be an exception", channel.getException());
        if (channel.getException() instanceof RestServiceException) {
          RestServiceException rse = (RestServiceException) channel.getException();
          assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.ServiceUnavailable, rse.getErrorCode());
        } else {
          throw channel.getException();
        }
      }
    } finally {
      responseHandler.shutdown();
      executorService.shutdown();
    }
  }

  /**
   * Tests various functions when multiple requests are in queue.
   * @throws Exception
   */
  @Test
  public void multipleRequestsInQueueTest() throws Exception {
    final int EXPECTED_MIN_QUEUE_SIZE = 5;
    restRequestService.blockAllOperations();
    try {
      // the first request that each worker processes will block.
      for (int i = 0; i < EXPECTED_MIN_QUEUE_SIZE + asyncRequestResponseHandler.getWorkersAlive(); i++) {
        MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
        restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
        MockRestResponseChannel restResponseChannel = new MockRestResponseChannel(restRequest);
        asyncRequestResponseHandler.handleRequest(restRequest, restResponseChannel);
      }
      assertTrue("Request queue size should be at least " + EXPECTED_MIN_QUEUE_SIZE + ". Is "
              + asyncRequestResponseHandler.getRequestQueueSize(),
          asyncRequestResponseHandler.getRequestQueueSize() >= EXPECTED_MIN_QUEUE_SIZE);
    } finally {
      restRequestService.releaseAllOperations();
    }
  }

  // helpers
  // general

  /**
   * Creates a {@link MockRestRequest} with the given parameters.
   * @param method the {@link RestMethod} desired.
   * @param uri the URI to hit.
   * @param headers the headers that will accompany the request.
   * @param contents the content associated with the request.
   * @return a {@link MockRestRequest} based on the given parameters.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  private MockRestRequest createRestRequest(RestMethod method, String uri, JSONObject headers,
      List<ByteBuffer> contents) throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, method.name());
    data.put(MockRestRequest.URI_KEY, uri);
    if (headers != null) {
      data.put(MockRestRequest.HEADERS_KEY, headers);
    }
    return new MockRestRequest(data, contents);
  }

  /**
   * Sends a the {@code restRequest} to the {@code requestHandler} for handling and waits for
   * {@link RestResponseChannel#onResponseComplete(Exception)} to be called in {@code restResponseChannel}.
   * If there was an exception input to the function, throws that exception.
   * @param requestHandler the {@link AsyncRequestResponseHandler} instance to use.
   * @param restRequest the {@link RestRequest} to send to the {@code requestHandler}.
   * @param restResponseChannel the {@link RestResponseChannel} for responses.
   * @throws InterruptedException
   * @throws RestServiceException
   */
  private void sendRequestAwaitResponse(AsyncRequestResponseHandler requestHandler, RestRequest restRequest,
      MockRestResponseChannel restResponseChannel) throws InterruptedException, RestServiceException {
    EventMonitor<MockRestResponseChannel.Event> eventMonitor =
        new EventMonitor<MockRestResponseChannel.Event>(MockRestResponseChannel.Event.OnRequestComplete);
    restResponseChannel.addListener(eventMonitor);
    requestHandler.handleRequest(restRequest, restResponseChannel);
    if (!eventMonitor.awaitEvent(1, TimeUnit.SECONDS)) {
      fail("sendRequestAwaitResponse took too long. There might be a problem or the timeout may need to be increased");
    }
  }

  /**
   * Queues a response and waits while the response is completely sent out.
   * @param responseHandler the {@link AsyncRequestResponseHandler} instance to use.
   * @param restRequest the {@link RestRequest} to send to the {@code requestHandler}.
   * @param restResponseChannel the {@link RestResponseChannel} for responses.
   * @param response the response to send as a {@link ReadableStreamChannel}.
   * @param exception the exception that occurred while building the response, if any.
   * @throws InterruptedException
   * @throws RestServiceException
   */
  private void awaitResponse(AsyncRequestResponseHandler responseHandler, RestRequest restRequest,
      MockRestResponseChannel restResponseChannel, ReadableStreamChannel response, Exception exception)
      throws InterruptedException, RestServiceException {
    EventMonitor<MockRestResponseChannel.Event> eventMonitor =
        new EventMonitor<MockRestResponseChannel.Event>(MockRestResponseChannel.Event.OnRequestComplete);
    restResponseChannel.addListener(eventMonitor);
    responseHandler.handleResponse(restRequest, restResponseChannel, response, exception);
    if (!eventMonitor.awaitEvent(1, TimeUnit.SECONDS)) {
      fail("awaitResponse took too long. There might be a problem or the timeout may need to be increased");
    }
  }

  /**
   * Sends a {@link RestRequest} to the {@code requestHandler} with the specified {@code restMethod} and checks the
   * response to see that the {@code restMethod} has been echoed.
   * @param restMethod the {@link RestMethod} required.
   * @param requestHandler the {@link AsyncRequestResponseHandler} instance to use.
   * @throws Exception
   */
  private void doHandleRequestSuccessTest(RestMethod restMethod, AsyncRequestResponseHandler requestHandler)
      throws Exception {
    RestRequest restRequest = createRestRequest(restMethod, MockRestRequestService.ECHO_REST_METHOD, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestAwaitResponse(requestHandler, restRequest, restResponseChannel);
    if (restResponseChannel.getException() == null) {
      byte[] response = restResponseChannel.getResponseBody();
      assertArrayEquals("Unexpected response", restMethod.toString().getBytes(), response);
    } else {
      throw restResponseChannel.getException();
    }
  }

  /**
   * Tests {@link AsyncRequestResponseHandler#handleResponse(RestRequest, RestResponseChannel, ReadableStreamChannel, Exception)} with good input.
   * @throws Exception
   */
  private void doHandleResponseSuccessTest(AsyncRequestResponseHandler asyncRequestResponseHandler) throws Exception {
    // both response and exception null
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    awaitResponse(asyncRequestResponseHandler, restRequest, restResponseChannel, null, null);
    if (restResponseChannel.getException() != null) {
      throw restResponseChannel.getException();
    }

    // both response and exception not null
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    restResponseChannel = new MockRestResponseChannel();
    ByteBuffer responseBuffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1024));
    ByteBufferRSC response = new ByteBufferRSC(responseBuffer);
    EventMonitor<ByteBufferRSC.Event> responseCloseMonitor =
        new EventMonitor<ByteBufferRSC.Event>(ByteBufferRSC.Event.Close);
    response.addListener(responseCloseMonitor);
    Exception e = new Exception();
    awaitResponse(asyncRequestResponseHandler, restRequest, restResponseChannel, response, e);
    // make sure exception was correctly sent to the RestResponseChannel.
    assertEquals("Exception was not piped correctly", e, restResponseChannel.getException());
    assertTrue("Response is not closed", responseCloseMonitor.awaitEvent(1, TimeUnit.SECONDS));

    // response null but exception not null.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    restResponseChannel = new MockRestResponseChannel();
    e = new Exception();
    awaitResponse(asyncRequestResponseHandler, restRequest, restResponseChannel, null, e);
    // make sure exception was correctly sent to the RestResponseChannel.
    assertEquals("Exception was not piped correctly", e, restResponseChannel.getException());

    // response not null.
    // steady response - full response available.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    restResponseChannel = new MockRestResponseChannel();
    responseBuffer = ByteBuffer.wrap(TestUtils.getRandomBytes(1024));
    response = new ByteBufferRSC(responseBuffer);
    responseCloseMonitor = new EventMonitor<ByteBufferRSC.Event>(ByteBufferRSC.Event.Close);
    response.addListener(responseCloseMonitor);
    awaitResponse(asyncRequestResponseHandler, restRequest, restResponseChannel, response, null);
    if (restResponseChannel.getException() == null) {
      assertArrayEquals("Response does not match", responseBuffer.array(), restResponseChannel.getResponseBody());
      assertTrue("Response is not closed", responseCloseMonitor.awaitEvent(1, TimeUnit.SECONDS));
    } else {
      throw restResponseChannel.getException();
    }
  }

  // BeforeClass helpers

  /**
   * Gets a new instance of {@link AsyncRequestResponseHandler}.
   * @param requestWorkers the number of request workers.
   * @return a new instance of {@link AsyncRequestResponseHandler}.
   * @throws IOException
   */
  private static AsyncRequestResponseHandler getAsyncRequestResponseHandler(int requestWorkers) throws IOException {
    RestRequestService restRequestService = new MockRestRequestService(verifiableProperties, router);
    RequestResponseHandlerMetrics metrics = new RequestResponseHandlerMetrics(new MetricRegistry());
    return new AsyncRequestResponseHandler(metrics, requestWorkers, restRequestService);
  }

  // useWithoutSettingWorkerCountTest() and zeroScalingUnitsTest() helpers

  /**
   * Uses the {@code requestResponseHandler} with zero request workers and one response worker and verifies that
   * responses are sent, but requests are not served.
   * @throws Exception
   */
  @Test
  public void noRequestHandlersTest() throws Exception {
    AsyncRequestResponseHandler requestResponseHandler = getAsyncRequestResponseHandler(0);
    // ok for start
    requestResponseHandler.start();
    try {
      // using for responses OK.
      MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      awaitResponse(requestResponseHandler, restRequest, restResponseChannel, null, null);
      if (restResponseChannel.getException() != null) {
        throw restResponseChannel.getException();
      }

      // using for request is not OK.
      try {
        doHandleRequestSuccessTest(RestMethod.GET, requestResponseHandler);
        fail("Handling request should have failed because no RestRequestService was set.");
      } catch (RestServiceException e) {
        assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.ServiceUnavailable, e.getErrorCode());
      }
    } finally {
      requestResponseHandler.shutdown();
    }
  }

  // handleRequestFailureOnDequeueTest() helpers

  /**
   * Sends a {@link RestRequest} with an unknown {@link RestMethod}. The failure will happen once the request is
   * dequeued.
   * @param requestResponseHandler The {@link AsyncRequestResponseHandler} instance to use.
   * @throws Exception
   */
  private void unknownRestMethodTest(AsyncRequestResponseHandler requestResponseHandler) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.UNKNOWN, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestAwaitResponse(requestResponseHandler, restRequest, restResponseChannel);
    if (restResponseChannel.getException() == null) {
      fail("Request handling would have failed and an exception should have been generated");
    } else {
      // it's ok if this conversion fails - the test should fail anyway.
      RestServiceException e = (RestServiceException) restResponseChannel.getException();
      assertEquals("Did not get expected RestServiceErrorCode", RestServiceErrorCode.UnsupportedRestMethod,
          e.getErrorCode());
      assertTrue("AsyncRequestResponseHandler is dead", requestResponseHandler.isRunning());
    }
  }

  /**
   * Sends a {@link RestRequest} with an PUT {@link RestMethod}. The failure will happen once the request is
   * dequeued.
   * @param requestResponseHandler The {@link AsyncRequestResponseHandler} instance to use.
   * @throws Exception
   */
  private void putRestMethodTest(AsyncRequestResponseHandler requestResponseHandler) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.PUT, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestAwaitResponse(requestResponseHandler, restRequest, restResponseChannel);
    if (restResponseChannel.getException() == null) {
      fail("Request handling would have failed and an exception should have been generated");
    } else {
      // it's ok if this conversion fails - the test should fail anyway.
      RestServiceException e = (RestServiceException) restResponseChannel.getException();
      assertEquals("Did not get expected RestServiceErrorCode", RestServiceErrorCode.UnsupportedHttpMethod,
          e.getErrorCode());
      assertTrue("AsyncRequestResponseHandler is dead", requestResponseHandler.isRunning());
    }
  }

  /**
   * Sends a {@link RestRequest} that requests a {@link RestServiceException}. The failure will happen once the request
   * is dequeued.
   * @param requestResponseHandler The {@link AsyncRequestResponseHandler} instance to use.
   * @throws Exception
   */
  private void delayedHandleRequestThatThrowsRestException(AsyncRequestResponseHandler requestResponseHandler)
      throws Exception {
    RestRequest restRequest =
        createRestRequest(RestMethod.GET, MockRestRequestService.SEND_RESPONSE_REST_SERVICE_EXCEPTION, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestAwaitResponse(requestResponseHandler, restRequest, restResponseChannel);
    if (restResponseChannel.getException() == null) {
      fail("Request handling would have failed and an exception should have been generated");
    } else {
      // it's ok if this conversion fails - the test should fail anyway.
      RestServiceException e = (RestServiceException) restResponseChannel.getException();
      assertEquals("Did not get expected RestServiceErrorCode", RestServiceErrorCode.InternalServerError,
          e.getErrorCode());
      assertTrue("AsyncRequestResponseHandler is dead", requestResponseHandler.isRunning());
    }
  }

  /**
   * Sends a {@link RestRequest} that requests a {@link RuntimeException}. The failure will happen once the request is
   * dequeued.
   * @param requestResponseHandler The {@link AsyncRequestResponseHandler} instance to use.
   * @throws Exception
   */
  private void delayedHandleRequestThatThrowsRuntimeException(AsyncRequestResponseHandler requestResponseHandler)
      throws Exception {
    RestRequest restRequest =
        createRestRequest(RestMethod.GET, MockRestRequestService.THROW_RUNTIME_EXCEPTION, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestAwaitResponse(requestResponseHandler, restRequest, restResponseChannel);
    if (restResponseChannel.getException() == null) {
      fail("Request handling would have failed and an exception should have been generated");
    } else {
      // it's ok if this conversion fails - the test should fail anyway.
      RuntimeException e = (RuntimeException) restResponseChannel.getException();
      assertEquals("Failure message does not match expectation", MockRestRequestService.THROW_RUNTIME_EXCEPTION,
          e.getMessage());
      assertTrue("AsyncRequestResponseHandler is dead", requestResponseHandler.isRunning());
    }
  }
}

/**
 * Implementation of many event listeners that checks for the expected event and records the parameters received. Can be
 * used with one event only.
 * @param <T> the type of event that this is a monitor for.
 */
class EventMonitor<T>
    implements MockRestResponseChannel.EventListener, MockRestRequest.EventListener, ByteBufferRSC.EventListener {
  private final T eventOfInterest;
  private final CountDownLatch eventFired = new CountDownLatch(1);
  private final AtomicBoolean eventOccurred = new AtomicBoolean(false);

  /**
   * Creates a listener that listens to the {@code eventOfInterest}.
   * @param eventOfInterest the event that this EventMonitor listens to.
   */
  public EventMonitor(T eventOfInterest) {
    this.eventOfInterest = eventOfInterest;
  }

  @Override
  public void onEventComplete(MockRestResponseChannel mockRestResponseChannel, MockRestResponseChannel.Event event) {
    if (eventOfInterest.equals(event) && eventOccurred.compareAndSet(false, true)) {
      eventFired.countDown();
    }
  }

  @Override
  public void onEventComplete(MockRestRequest mockRestRequest, MockRestRequest.Event event) {
    if (eventOfInterest.equals(event) && eventOccurred.compareAndSet(false, true)) {
      eventFired.countDown();
    }
  }

  @Override
  public void onEventComplete(ByteBufferRSC byteBufferRSC, ByteBufferRSC.Event event) {
    if (eventOfInterest.equals(event) && eventOccurred.compareAndSet(false, true)) {
      eventFired.countDown();
    }
  }

  /**
   * Wait for a finite amount of time for event to occur.
   * @param timeout the length of time to wait for.
   * @param timeUnit the time unit of timeout.
   * @return {@code true} if the the event occurred within the {@code timeout}. Otherwise {@code false}.
   * @throws InterruptedException
   */
  public boolean awaitEvent(long timeout, TimeUnit timeUnit) throws InterruptedException {
    return eventFired.await(timeout, timeUnit);
  }
}

/**
 * Object that wraps another {@link ReadableStreamChannel} and simply blocks on read until released.
 */
class HaltingRSC extends ByteBufferRSC implements ReadableStreamChannel {
  private final CountDownLatch release;
  private final ExecutorService executorService;

  public HaltingRSC(ByteBuffer buffer, CountDownLatch releaseRead, ExecutorService executorService) {
    super(buffer);
    this.release = releaseRead;
    this.executorService = executorService;
  }

  @Override
  public Future<Long> readInto(final AsyncWritableChannel asyncWritableChannel, final Callback<Long> callback) {
    if (!isOpen()) {
      throw new IllegalStateException("Channel is not open");
    }

    final FutureResult<Long> future = new FutureResult<Long>();
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          release.await();
        } catch (InterruptedException e) {
          // move on.
        }
        long result = 0;
        Exception exception = null;
        try {
          result = asyncWritableChannel.write(buffer, callback).get();
        } catch (Exception e) {
          exception = e;
        }
        future.done(result, exception);
      }
    });
    return future;
  }
}

/**
 * {@link ReadableStreamChannel} implementation that either has an {@link Exception} on
 * {@link #readInto(AsyncWritableChannel, Callback)} or executes an incomplete read.
 */
class IncompleteReadReadableStreamChannel implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final Long bytesRead;
  private final Exception exceptionToThrow;

  /**
   * Create an instance of {@link IncompleteReadReadableStreamChannel} with {@code bytesRead} and
   * {@code exceptionToThrow} that will be returned via the future and callback.
   * @param bytesRead The number of bytes read that needs to be returned. Can be null.
   * @param exceptionToThrow if desired, provide an exception that will thrown on read. Can be null.
   */
  public IncompleteReadReadableStreamChannel(Long bytesRead, Exception exceptionToThrow) {
    this.bytesRead = bytesRead;
    this.exceptionToThrow = exceptionToThrow;
  }

  @Override
  public long getSize() {
    return 1;
  }

  /**
   * Either throws the exception provided or returns immediately saying no bytes were read.
   * @param asyncWritableChannel the {@link AsyncWritableChannel} to read the data into.
   * @param callback the {@link Callback} that will be invoked either when all the data in the channel has been emptied
   *                 into the {@code asyncWritableChannel} or if there is an exception in doing so. This can be null.
   * @return a {@link Future} that will eventually contain the result of the operation.
   */
  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    Exception exception;
    if (!channelOpen.get()) {
      exception = new ClosedChannelException();
    } else {
      exception = exceptionToThrow;
    }
    FutureResult<Long> futureResult = new FutureResult<Long>();
    futureResult.done(bytesRead, exception);
    if (callback != null) {
      callback.onCompletion(bytesRead, exception);
    }
    return futureResult;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() throws IOException {
    channelOpen.set(false);
  }
}

/**
 * A bad implementation of {@link RestRequest}. Just throws exceptions.
 */
class BadRestRequest implements RestRequest {
  private final RestRequestMetricsTracker restRequestMetricsTracker = new RestRequestMetricsTracker();

  @Override
  public RestMethod getRestMethod() {
    return null;
  }

  @Override
  public String getPath() {
    return null;
  }

  @Override
  public String getUri() {
    return null;
  }

  @Override
  public Map<String, Object> getArgs() {
    return null;
  }

  @Override
  public Object setArg(String key, Object value) {
    return null;
  }

  @Override
  public SSLSession getSSLSession() {
    return null;
  }

  @Override
  public void prepare() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean isOpen() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void close() throws IOException {
    throw new IOException("Not implemented");
  }

  @Override
  public RestRequestMetricsTracker getMetricsTracker() {
    return restRequestMetricsTracker;
  }

  @Override
  public long getSize() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void setDigestAlgorithm(String digestAlgorithm) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public byte[] getDigest() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public long getBytesReceived() {
    return 0;
  }
}
