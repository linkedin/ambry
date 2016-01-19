package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.ByteBufferRSC;
import com.github.ambry.router.Callback;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static Router router;
  private static BlobStorageService blobStorageService;
  private static AsyncRequestResponseHandler asyncRequestResponseHandler;

  /**
   * Sets up all the tests by providing a started {@link AsyncRequestResponseHandler} for their use.
   * @throws InstantiationException
   * @throws IOException
   */
  @BeforeClass
  public static void startRequestResponseHandler()
      throws InstantiationException, IOException {
    VerifiableProperties verifiableProperties = new VerifiableProperties(new Properties());
    router = new InMemoryRouter(verifiableProperties);
    blobStorageService = new MockBlobStorageService(verifiableProperties, router);
    asyncRequestResponseHandler = getAsyncRequestResponseHandler(blobStorageService);
    blobStorageService.start();
    asyncRequestResponseHandler.start();
  }

  /**
   * Shuts down the created {@link AsyncRequestResponseHandler}.
   * @throws IOException
   */
  @AfterClass
  public static void shutdownRequestResponseHandler()
      throws IOException {
    asyncRequestResponseHandler.shutdown();
    blobStorageService.shutdown();
    router.close();
  }

  /**
   * Tests {@link AsyncRequestResponseHandler#start()} and {@link AsyncRequestResponseHandler#shutdown()}.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startShutdownTest()
      throws InstantiationException, IOException {
    AsyncRequestResponseHandler handler = getAsyncRequestResponseHandler(blobStorageService);
    assertFalse("IsRunning should be false", handler.isRunning());
    handler.start();
    assertTrue("IsRunning should be true", handler.isRunning());
    handler.shutdown();
    assertFalse("IsRunning should be false", handler.isRunning());
  }

  /**
   * Tests for {@link AsyncRequestResponseHandler#shutdown()} when {@link AsyncRequestResponseHandler#start()} has not
   * been called previously. This test is for cases where {@link AsyncRequestResponseHandler#start()} has failed and
   * {@link AsyncRequestResponseHandler#shutdown()} needs to be run.
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStart()
      throws IOException {
    AsyncRequestResponseHandler handler = getAsyncRequestResponseHandler(blobStorageService);
    handler.shutdown();
  }

  /**
   * This tests for exceptions thrown when a {@link AsyncRequestResponseHandler} is used without calling
   * {@link AsyncRequestResponseHandler#start()}first.
   * @throws InstantiationException
   * @throws IOException
   * @throws JSONException
   * @throws URISyntaxException
   */
  @Test
  public void useServiceWithoutStartTest()
      throws InstantiationException, IOException, JSONException, URISyntaxException {
    AsyncRequestResponseHandler handler = getAsyncRequestResponseHandler(blobStorageService);
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    try {
      handler.handleRequest(restRequest, new MockRestResponseChannel());
      fail("Should have thrown RestServiceException because the AsyncRequestResponseHandler has not been started");
    } catch (RestServiceException e) {
      assertEquals("The RestServiceErrorCode does not match", RestServiceErrorCode.ServiceUnavailable,
          e.getErrorCode());
    }

    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
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
   * This tests for exceptions thrown when a {@link AsyncRequestResponseHandler} is started without setting a
   * {@link BlobStorageService}.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startWithoutBlobStorageServiceTest()
      throws InstantiationException, IOException {
    AsyncRequestResponseHandler requestResponseHandler = getAsyncRequestResponseHandler(null);
    try {
      requestResponseHandler.start();
      fail("Start should have failed because no BlobStorageService was set.");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    } finally {
      requestResponseHandler.shutdown();
    }
  }

  /**
   * Tests handling of all {@link RestMethod}s. The {@link MockBlobStorageService} instance being used is
   * asked to only echo the method.
   * @throws Exception
   */
  @Test
  public void allRestMethodsSuccessTest()
      throws Exception {
    for (RestMethod restMethod : RestMethod.values()) {
      if (restMethod != RestMethod.UNKNOWN) {
        doHandleRequestSuccessTest(restMethod, asyncRequestResponseHandler);
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
  public void handleRequestFailureBeforeQueueTest()
      throws Exception {
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
  public void handleRequestFailureOnDequeueTest()
      throws Exception {
    unknownRestMethodTest(asyncRequestResponseHandler);
    delayedHandleRequestThatThrowsRestException(asyncRequestResponseHandler);
    delayedHandleRequestThatThrowsRuntimeException(asyncRequestResponseHandler);
  }

  /**
   * Tests {@link AsyncRequestResponseHandler#handleResponse(RestRequest, RestResponseChannel, ReadableStreamChannel,
   * Exception)} with good input.
   * @throws Exception
   */
  @Test
  public void handleResponseSuccessTest()
      throws Exception {
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
    try {
      asyncRequestResponseHandler.handleResponse(restRequest, null, response, null);
      fail("Test should have thrown exception, but did not");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // AsyncRequestResponseHandler should still be alive and serving requests
    assertTrue("AsyncRequestResponseHandler is dead", asyncRequestResponseHandler.isRunning());

    // both response and exception null
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    EventMonitor<MockRestRequest.Event> requestCloseMonitor =
        new EventMonitor<MockRestRequest.Event>(MockRestRequest.Event.Close);
    restRequest.addListener(requestCloseMonitor);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    awaitResponse(asyncRequestResponseHandler, restRequest, restResponseChannel, null, null);
    if (restResponseChannel.getCause() != null) {
      throw (Exception) restResponseChannel.getCause();
    }

    // response null but exception not null.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    requestCloseMonitor = new EventMonitor<MockRestRequest.Event>(MockRestRequest.Event.Close);
    restRequest.addListener(requestCloseMonitor);
    restResponseChannel = new MockRestResponseChannel();
    Exception e = new Exception();
    awaitResponse(asyncRequestResponseHandler, restRequest, restResponseChannel, null, e);
    // make sure exception was correctly sent to the RestResponseChannel.
    assertEquals("Exception was not piped correctly", e, restResponseChannel.getCause());

    // response not null.
    // steady response - full response available.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    requestCloseMonitor = new EventMonitor<MockRestRequest.Event>(MockRestRequest.Event.Close);
    restRequest.addListener(requestCloseMonitor);
    restResponseChannel = new MockRestResponseChannel();
    ByteBuffer responseBuffer = ByteBuffer.wrap(getRandomBytes(1024));
    response = new ByteBufferRSC(responseBuffer);
    EventMonitor<ByteBufferRSC.Event> responseCloseMonitor =
        new EventMonitor<ByteBufferRSC.Event>(ByteBufferRSC.Event.Close);
    response.addListener(responseCloseMonitor);
    awaitResponse(asyncRequestResponseHandler, restRequest, restResponseChannel, response, null);
    if (restResponseChannel.getCause() == null) {
      assertArrayEquals("Response does not match", responseBuffer.array(),
          restResponseChannel.getResponseBody(MockRestResponseChannel.DataStatus.Flushed));
      assertTrue("Response is not closed", responseCloseMonitor.awaitEvent(1, TimeUnit.SECONDS));
    } else {
      throw (Exception) restResponseChannel.getCause();
    }

    // halting response - response not available in one shot
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    restResponseChannel = new MockRestResponseChannel();
    responseBuffer = ByteBuffer.wrap(getRandomBytes(1024));
    awaitResponse(asyncRequestResponseHandler, restRequest, restResponseChannel,
        new IntermittentRSC(responseBuffer, 128), null);
    if (restResponseChannel.getCause() == null) {
      assertArrayEquals("Response does not match", responseBuffer.array(),
          restResponseChannel.getResponseBody(MockRestResponseChannel.DataStatus.Flushed));
    } else {
      throw (Exception) restResponseChannel.getCause();
    }
  }

  /**
   * Tests the reaction of {@link AsyncRequestResponseHandler#handleResponse(RestRequest, RestResponseChannel,
   * ReadableStreamChannel, Exception)} to some misbehaving components.
   * @throws Exception
   */
  @Test
  public void handleResponseExceptionTest()
      throws Exception {
    // Response is bad.
    MockRestRequest goodRestRequest = createRestRequest(RestMethod.GET, "/", null, null);
    goodRestRequest.getMetricsTracker().scalingMetricsTracker.markRequestReceived();
    EventMonitor<MockRestRequest.Event> requestCloseMonitor =
        new EventMonitor<MockRestRequest.Event>(MockRestRequest.Event.Close);
    goodRestRequest.addListener(requestCloseMonitor);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    ReadableStreamChannel badResponse = new BadRSC();
    awaitResponse(asyncRequestResponseHandler, goodRestRequest, restResponseChannel, badResponse, null);
    assertNotNull("MockRestResponseChannel would have been passed an exception", restResponseChannel.getCause());

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
   * This test halts the processing pipeline by introducing a blocking response and then fills the request and response
   * queues and tests various functions like request, response queue sizes and reactions to shutdown.
   * <p/>
   * This test could have assumptions about implementation.
   * @throws Exception
   */
  @Test
  public void midOccupancyTest()
      throws Exception {
    final CountDownLatch releaseRead = new CountDownLatch(1);
    AsyncRequestResponseHandler handler = getAsyncRequestResponseHandler(blobStorageService);
    handler.start();
    List<AsyncRequestInfo> requests = new ArrayList<AsyncRequestInfo>();
    Map<RestRequest, AsyncResponseInfo> responses = new HashMap<RestRequest, AsyncResponseInfo>();

    try {
      // queue a response that will halt AsyncHandlerWorker
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      HaltingRSC haltingRSC = new HaltingRSC(new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0)), releaseRead);
      responses.put(restRequest, new AsyncResponseInfo(haltingRSC, restResponseChannel));
      handler.handleResponse(restRequest, restResponseChannel, haltingRSC, null);
      if (haltingRSC.awaitHalt(1, TimeUnit.SECONDS)) {
        // queue a few requests
        for (int i = 0; i < 5; i++) {
          restRequest = createRestRequest(RestMethod.GET, "/", null, null);
          restResponseChannel = new MockRestResponseChannel();
          requests.add(new AsyncRequestInfo(restRequest, restResponseChannel));
          handler.handleRequest(restRequest, restResponseChannel);
        }

        // queue a request that will halt AsyncHandlerWorker.

        // queue a few more responses
        for (int i = 0; i < 5; i++) {
          restRequest = createRestRequest(RestMethod.GET, "/", null, null);
          restResponseChannel = new MockRestResponseChannel();
          ReadableStreamChannel response = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
          responses.put(restRequest, new AsyncResponseInfo(response, restResponseChannel));
          handler.handleResponse(restRequest, restResponseChannel, response, null);
        }

        // check sizes
        assertEquals("Requests queue size mismatch", requests.size(), handler.getRequestQueueSize());
        assertEquals("Responses queue size mismatch", responses.size(), handler.getResponseSetSize());

        // try queuing a response for a request that is already queued
        try {
          handler.handleResponse(restRequest, restResponseChannel,
              new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0)), null);
          fail("Response queuing should have thrown exception since the request is duplicate");
        } catch (RestServiceException e) {
          assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.RequestResponseQueuingFailure,
              e.getErrorCode());
        }
      } else {
        fail("Response was not processed within timeout. There might be an error or time out may need to increase");
      }
    } finally {
      releaseRead.countDown();
      handler.shutdown();

      // since the shutdown is complete, all closes would have happened synchronously.
      for (Map.Entry<RestRequest, AsyncResponseInfo> entry : responses.entrySet()) {
        assertFalse("Response channel not closed", entry.getValue().getResponse().isOpen());
      }
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
      List<ByteBuffer> contents)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, method);
    data.put(MockRestRequest.URI_KEY, uri);
    if (headers != null) {
      data.put(MockRestRequest.HEADERS_KEY, headers);
    }
    return new MockRestRequest(data, contents);
  }

  /**
   * Gets a byte array of length {@code size} with random bytes.
   * @param size the required length of the random byte array.
   * @return a byte array of length {@code size} with random bytes.
   */
  private byte[] getRandomBytes(int size) {
    byte[] bytes = new byte[size];
    new Random().nextBytes(bytes);
    return bytes;
  }

  /**
   * Sends a the {@code restRequest} to the {@code requestHandler} for handling and waits for
   * {@link RestResponseChannel#onResponseComplete(Throwable)} to be called in {@code restResponseChannel}.
   * If there was an exception input to the function, throws that exception.
   * @param requestHandler the {@link AsyncRequestResponseHandler} instance to use.
   * @param restRequest the {@link RestRequest} to send to the {@code requestHandler}.
   * @param restResponseChannel the {@link RestResponseChannel} for responses.
   * @throws InterruptedException
   * @throws RestServiceException
   */
  private void sendRequestAwaitResponse(AsyncRequestResponseHandler requestHandler, RestRequest restRequest,
      MockRestResponseChannel restResponseChannel)
      throws InterruptedException, RestServiceException {
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

  // BeforeClass helpers

  /**
   * Gets a new instance of {@link AsyncRequestResponseHandler}.
   * @param blobStorageService the {@link BlobStorageService} that will back the {@link AsyncRequestResponseHandler}.
   * @return a new instance of {@link AsyncRequestResponseHandler}.
   * @throws IOException
   */
  private static AsyncRequestResponseHandler getAsyncRequestResponseHandler(BlobStorageService blobStorageService)
      throws IOException {
    RestServerMetrics serverMetrics = new RestServerMetrics(new MetricRegistry());
    AsyncRequestResponseHandler handler = new AsyncRequestResponseHandler(serverMetrics);
    if (blobStorageService != null) {
      handler.setBlobStorageService(blobStorageService);
    }
    return handler;
  }

  // handleRequestTest() helpers

  /**
   * Sends a {@link RestRequest} to the {@code requestHandler} with the specified {@code restMethod} and checks the
   * response to see that the {@code restMethod} has been echoed.
   * @param restMethod the {@link RestMethod} required.
   * @param requestHandler the {@link AsyncRequestResponseHandler} instance to use.
   * @throws Exception
   */
  private void doHandleRequestSuccessTest(RestMethod restMethod, AsyncRequestResponseHandler requestHandler)
      throws Exception {
    RestRequest restRequest = createRestRequest(restMethod, MockBlobStorageService.ECHO_REST_METHOD, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestAwaitResponse(requestHandler, restRequest, restResponseChannel);
    if (restResponseChannel.getCause() == null) {
      byte[] response = restResponseChannel.getResponseBody(MockRestResponseChannel.DataStatus.Flushed);
      assertArrayEquals("Unexpected response", restMethod.toString().getBytes(), response);
    } else {
      // don't care if the conversion to Exception fails. The test has failed anyway.
      throw (Exception) restResponseChannel.getCause();
    }
  }

  // handleRequestFailureOnDequeueTest() helpers

  /**
   * Sends a {@link RestRequest} with an unknown {@link RestMethod}. The failure will happen once the request is
   * dequeued.
   * @param requestResponseHandler The {@link AsyncRequestResponseHandler} instance to use.
   * @throws Exception
   */
  private void unknownRestMethodTest(AsyncRequestResponseHandler requestResponseHandler)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.UNKNOWN, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestAwaitResponse(requestResponseHandler, restRequest, restResponseChannel);
    if (restResponseChannel.getCause() == null) {
      fail("Request handling would have failed and an exception should have been generated");
    } else {
      // it's ok if this conversion fails - the test should fail anyway.
      RestServiceException e = (RestServiceException) restResponseChannel.getCause();
      assertEquals("Did not get expected RestServiceErrorCode", RestServiceErrorCode.UnsupportedRestMethod,
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
        createRestRequest(RestMethod.GET, MockBlobStorageService.SEND_RESPONSE_REST_SERVICE_EXCEPTION, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestAwaitResponse(requestResponseHandler, restRequest, restResponseChannel);
    if (restResponseChannel.getCause() == null) {
      fail("Request handling would have failed and an exception should have been generated");
    } else {
      // it's ok if this conversion fails - the test should fail anyway.
      RestServiceException e = (RestServiceException) restResponseChannel.getCause();
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
        createRestRequest(RestMethod.GET, MockBlobStorageService.THROW_RUNTIME_EXCEPTION, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestAwaitResponse(requestResponseHandler, restRequest, restResponseChannel);
    if (restResponseChannel.getCause() == null) {
      fail("Request handling would have failed and an exception should have been generated");
    } else {
      // it's ok if this conversion fails - the test should fail anyway.
      RuntimeException e = (RuntimeException) restResponseChannel.getCause();
      assertEquals("Failure message does not match expectation", MockBlobStorageService.THROW_RUNTIME_EXCEPTION,
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
class EventMonitor<T> implements MockRestResponseChannel.EventListener, MockRestRequest.EventListener, ByteBufferRSC.EventListener {
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
  public boolean awaitEvent(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return eventFired.await(timeout, timeUnit);
  }
}

/**
 * Class that returns 0 bytes on read sometimes.
 */
class IntermittentRSC implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ByteBuffer data;
  private final int haltSize;
  private volatile boolean halt = false;

  /**
   * Create an instance of {@link IntermittentRSC}.
   * @param data the data that will be returned on read.
   * @param haltSize the number of bytes after which the read will return 0 temporarily.
   */
  public IntermittentRSC(ByteBuffer data, int haltSize) {
    this.data = data;
    this.haltSize = haltSize;
  }

  @Override
  public long getSize() {
    return data.limit();
  }

  @Override
  @Deprecated
  public int read(WritableByteChannel channel)
      throws IOException {
    int bytesWritten;
    if (!channelOpen.get()) {
      throw new ClosedChannelException();
    } else if (!data.hasRemaining()) {
      bytesWritten = -1;
    } else if (halt) {
      bytesWritten = 0;
      halt = false;
    } else {
      int length = Math.min(haltSize, data.remaining());
      ByteBuffer dataView = ByteBuffer.wrap(data.array(), data.arrayOffset() + data.position(), length);
      bytesWritten = channel.write(dataView);
      data.position(data.position() + bytesWritten);
      halt = true;
    }
    return bytesWritten;
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    // NOTE: This class will be removed once changes are made to AsyncRequestResponseHandler that make this test
    // obsolete. No need to implement this function.
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    channelOpen.set(false);
  }
}

/**
 * Object that wraps another {@link ReadableStreamChannel} and simply blocks on read until released.
 */
class HaltingRSC implements ReadableStreamChannel {
  private final ReadableStreamChannel rsc;
  private final CountDownLatch release;
  private final CountDownLatch halted = new CountDownLatch(1);

  public HaltingRSC(ReadableStreamChannel rsc, CountDownLatch releaseRead) {
    this.rsc = rsc;
    this.release = releaseRead;
  }

  @Override
  public long getSize() {
    return rsc.getSize();
  }

  @Override
  @Deprecated
  public int read(WritableByteChannel channel)
      throws IOException {
    try {
      halted.countDown();
      // halt until released.
      release.await();
    } catch (InterruptedException e) {
      // move on.
    }
    return rsc.read(channel);
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    try {
      halted.countDown();
      // halt until released.
      release.await();
    } catch (InterruptedException e) {
      // move on.
    }
    return rsc.readInto(asyncWritableChannel, callback);
  }

  @Override
  public boolean isOpen() {
    return rsc.isOpen();
  }

  @Override
  public void close()
      throws IOException {
    rsc.close();
  }

  /**
   * Used to wait until this RSC is actually picked up for processing by the AsyncHandlerWorker.
   * @param timeout the time to wait for.
   * @param timeUnit the time unit of {@code timeout}
   * @return {@code true} if the wait was released within the timeout. {@code false} otherwise.
   * @throws InterruptedException
   */
  public boolean awaitHalt(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return halted.await(timeout, timeUnit);
  }
}

/**
 * A bad implementation of {@link ReadableStreamChannel}. Just throws exceptions.
 */
class BadRSC implements ReadableStreamChannel {

  @Override
  public long getSize() {
    return -1;
  }

  @Override
  @Deprecated
  public int read(WritableByteChannel channel)
      throws IOException {
    throw new IOException("Not implemented");
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public boolean isOpen() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void close()
      throws IOException {
    throw new IOException("Not implemented");
  }
}

/**
 * A bad implementation of {@link RestResponseChannel}. Just throws exceptions.
 */
class BadResponseChannel implements RestResponseChannel {

  @Override
  public int write(ByteBuffer src)
      throws IOException {
    throw new IOException("Not implemented");
  }

  @Override
  public void flush()
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void onResponseComplete(Throwable cause) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void setStatus(ResponseStatus status)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void setContentType(String type)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void setContentLength(long length)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void setLocation(String location)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void setLastModified(Date lastModified)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void setExpires(Date expireTime)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void setCacheControl(String cacheControl)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void setPragma(String pragma)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void setDate(Date date)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public void setHeader(String headerName, Object headerValue)
      throws RestServiceException {
    throw new RestServiceException("Not implemented", RestServiceErrorCode.InternalServerError);
  }

  @Override
  public boolean isOpen() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void close()
      throws IOException {
    throw new IOException("Not implemented");
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
  public Map<String, List<String>> getArgs() {
    return null;
  }

  @Override
  public boolean isOpen() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void close()
      throws IOException {
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
  @Deprecated
  public int read(WritableByteChannel channel)
      throws IOException {
    throw new IOException("Not implemented");
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    throw new IllegalStateException("Not implemented");
  }
}
