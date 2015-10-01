package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Tests functionality of {@link AsyncRequestHandler}.
 */
public class AsyncRequestHandlerTest {
  private static RestRequestHandler asyncRequestHandler;

  /**
   * Sets up all the tests by providing a started {@link AsyncRequestHandler} for their use.
   * @throws InstantiationException
   * @throws IOException
   */
  @BeforeClass
  public static void startRequestHandler()
      throws InstantiationException, IOException {
    asyncRequestHandler = getAsyncRequestHandler();
    asyncRequestHandler.start();
  }

  /**
   * Shuts down the created {@link AsyncRequestHandler}.
   */
  @AfterClass
  public static void shutdownRequestHandler() {
    asyncRequestHandler.shutdown();
  }

  /**
   * Tests {@link AsyncRequestHandler#start()} and {@link AsyncRequestHandler#shutdown()}.
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startShutdownTest()
      throws InstantiationException, IOException {
    RestRequestHandler handler = getAsyncRequestHandler();
    handler.start();
    handler.shutdown();
  }

  /**
   * Tests for {@link AsyncRequestHandler#shutdown()} when {@link AsyncRequestHandler#start()} has not been called
   * previously. This test is for cases where {@link AsyncRequestHandler#start()} has failed and
   * {@link AsyncRequestHandler#shutdown()} needs to be run.
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStart()
      throws IOException {
    RestRequestHandler handler = getAsyncRequestHandler();
    handler.shutdown();
  }

  /**
   * This tests for exceptions thrown when a {@link AsyncRequestHandler} is used without calling
   * {@link AsyncRequestHandler#start()}first.
   * @throws InstantiationException
   * @throws IOException
   * @throws JSONException
   * @throws URISyntaxException
   */
  @Test
  public void useServiceWithoutStartTest()
      throws InstantiationException, IOException, JSONException, URISyntaxException {
    RestRequestHandler handler = getAsyncRequestHandler();
    RestRequestInfo restRequestInfo = createRestRequestInfo(RestMethod.GET, "/", null);
    try {
      handler.handleRequest(restRequestInfo);
      fail("The test should have thrown RestServiceException because the AsyncRequestHandler has not been started");
    } catch (RestServiceException e) {
      assertEquals("The RestServiceErrorCode does not match", RestServiceErrorCode.RequestHandlerUnavailable,
          e.getErrorCode());
    }
  }

  /**
   * Tests handling of requests given good input.
   * @throws Exception
   */
  @Test
  public void handleRequestTest()
      throws Exception {
    for (RestMethod restMethod : RestMethod.values()) {
      if (restMethod != RestMethod.UNKNOWN) {
        doHandleRequestSuccessTest(restMethod, asyncRequestHandler);
      }
    }
  }

  /**
   * Tests that right exceptions are thrown on bad input while handling (before queueing).
   * @throws Exception
   */
  @Test
  public void handleRequestWithBadInputTest()
      throws Exception {
    handleRequestInfoWithRestRequestInfoNull(asyncRequestHandler);
    handleRequestInfoWithRestRequestNull(asyncRequestHandler);
    handleRequestInfoWithRestResponseChannelNull(asyncRequestHandler);
  }

  /**
   * Tests that right exceptions are thrown on bad input while a de-queued {@link RestRequestInfo} is being handled.
   * @throws Exception
   */
  @Test
  public void delayedHandleRequestWithBadInputTest()
      throws Exception {
    unknownRestMethodTest(asyncRequestHandler);
    delayedHandleRequestThatThrowsRestException(asyncRequestHandler);
    delayedHandleRequestThatThrowsRuntimeException(asyncRequestHandler);
  }

  /**
   * Tests that implementations of {@link RestRequestInfoEventListener} that throw exceptions do not kill the
   * {@link AsyncRequestHandler}.
   * @throws Exception
   */
  @Test
  public void badRequestInfoHandleListenersTest()
      throws Exception {
    badOnHandleSuccessBehaviourTest(asyncRequestHandler);
    badOnHandleFailureBehaviourTest(asyncRequestHandler);
  }

  /**
   * Checks {@link AsyncRequestHandler#onRequestComplete(RestRequest)} functionality with good input.
   * @throws IOException
   * @throws JSONException
   * @throws URISyntaxException
   */
  @Test
  public void onRequestCompleteWithRequestNotNullTest()
      throws IOException, JSONException, URISyntaxException {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    // Does nothing for now so this is a dummy test. If functionality is introduced in onRequestComplete(),
    // this has to be updated with a check to verify state.
    asyncRequestHandler.onRequestComplete(restRequest);
  }

  /**
   * Checks {@link AsyncRequestHandler#onRequestComplete(RestRequest)} functionality with bad input.
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void onRequestCompleteWithRequestNullTest()
      throws IOException, JSONException {
    // Does nothing for now so this is a dummy test. If functionality is introduced in onRequestComplete(),
    // this has to be updated with a check to verify state.
    asyncRequestHandler.onRequestComplete(null);
  }

  // helpers
  // general
  private RestRequest createRestRequest(RestMethod method, String uri, JSONObject headers)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, method);
    data.put(MockRestRequest.URI_KEY, uri);
    if (headers != null) {
      data.put(MockRestRequest.HEADERS_KEY, headers);
    }
    return new MockRestRequest(data);
  }

  private RestRequestContent createRestContent(boolean isLast)
      throws JSONException {
    JSONObject data = new JSONObject();
    data.put(MockRestRequestContent.IS_LAST_KEY, isLast);
    data.put(MockRestRequestContent.CONTENT_KEY, "");
    return new MockRestRequestContent(data);
  }

  private RestRequestInfo createRestRequestInfo(RestMethod method, String uri, JSONObject headers)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    RestRequest restRequest = createRestRequest(method, uri, headers);
    return new RestRequestInfo(restRequest, null, new MockRestResponseChannel(), true);
  }

  private void finishRequest(RestRequestInfo restRequestInfo, RestRequestInfoEventListener resultListener,
      RestRequestHandler requestHandler)
      throws JSONException, RestServiceException {
    RestRequestInfo lastRestRequestInfo = new RestRequestInfo(restRequestInfo.getRestRequest(), createRestContent(true),
        restRequestInfo.getRestResponseChannel());
    lastRestRequestInfo.addListener(resultListener);
    requestHandler.handleRequest(lastRestRequestInfo);
  }

  // BeforeClass helpers
  private static RestRequestHandler getAsyncRequestHandler()
      throws IOException {
    BlobStorageService blobStorageService = new MockBlobStorageService(new MockClusterMap());
    RestServerMetrics serverMetrics = new RestServerMetrics(new MetricRegistry());
    return new AsyncRequestHandler(blobStorageService, serverMetrics);
  }

  // handleRequestTest() helpers

  /**
   * Sends a {@link RestRequestInfo} to the {@link AsyncRequestHandler} and adds a {@link RestRequestInfoEventListener}
   * to it to listen for handling results.
   * </p>
   * The listener determines success or failure of the test (handling success expected). Failure of test is bubbled up
   * to this function.
   * @param restMethod - the {@link RestMethod} required.
   * @param requestHandler - the {@link RestRequestHandler} to use.
   * @throws Exception
   */
  private void doHandleRequestSuccessTest(RestMethod restMethod, RestRequestHandler requestHandler)
      throws Exception {
    RestRequestInfo restRequestInfo = createRestRequestInfo(restMethod, "/", new JSONObject());
    requestHandler.handleRequest(restRequestInfo);

    RestRequestInfoHandlingSuccessMonitor restRequestInfoHandlingSuccessMonitor =
        new RestRequestInfoHandlingSuccessMonitor((MockRestResponseChannel) restRequestInfo.getRestResponseChannel(),
            restMethod);
    finishRequest(restRequestInfo, restRequestInfoHandlingSuccessMonitor, requestHandler);

    if (restRequestInfoHandlingSuccessMonitor.await(10, TimeUnit.SECONDS)) {
      Exception e = restRequestInfoHandlingSuccessMonitor.getException();
      AssertionError ae = restRequestInfoHandlingSuccessMonitor.getAssertionError();
      if (e != null) {
        throw e;
      } else if (ae != null) {
        throw ae;
      }
    } else {
      fail("Test took too long. There might be a problem or the timeout may need to be increased");
    }
  }

  // handleRequestWithBadInputTest() helpers
  private void handleRequestInfoWithRestRequestInfoNull(RestRequestHandler requestHandler)
      throws Exception {
    doHandleRequestFailureTest(null, RestServiceErrorCode.RestRequestInfoNull, requestHandler);
  }

  private void handleRequestInfoWithRestRequestNull(RestRequestHandler requestHandler)
      throws Exception {
    MockRestResponseChannel responseChannel = new MockRestResponseChannel();
    RestRequestInfo restRequestInfo = new RestRequestInfo(null, null, responseChannel, true);
    doHandleRequestFailureTest(restRequestInfo, RestServiceErrorCode.RequestNull, requestHandler);
  }

  private void handleRequestInfoWithRestResponseChannelNull(RestRequestHandler requestHandler)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    RestRequestInfo restRequestInfo = new RestRequestInfo(restRequest, null, null, true);
    doHandleRequestFailureTest(restRequestInfo, RestServiceErrorCode.ResponseChannelNull, requestHandler);
  }

  /**
   * Sends a {@link RestRequestInfo} to the {@link AsyncRequestHandler} and expects the handling to fail. Tests for the
   * proper {@link RestServiceErrorCode}.
   * @param restRequestInfo - The {@link RestRequestInfo} that has to be handled.
   * @param expectedCode - The expected {@link RestServiceErrorCode}.
   * @param requestHandler - The {@link RestRequestHandler} to use.
   * @throws Exception
   */
  private void doHandleRequestFailureTest(RestRequestInfo restRequestInfo, RestServiceErrorCode expectedCode,
      RestRequestHandler requestHandler)
      throws Exception {
    try {
      requestHandler.handleRequest(restRequestInfo);
      fail("Test should have thrown exception, but did not");
    } catch (RestServiceException e) {
      assertEquals("Did not get expected RestServiceErrorCode", expectedCode, e.getErrorCode());

      // AsyncRequestHandler should still be alive and serving requests
      doHandleRequestSuccessTest(RestMethod.GET, requestHandler);
    }
  }

  // delayedHandleRequestWithBadInputTest() helpers
  private void unknownRestMethodTest(RestRequestHandler requestHandler)
      throws Exception {
    RestRequestInfo restRequestInfo = createRestRequestInfo(RestMethod.UNKNOWN, "/", null);
    doDelayedHandleRequestFailureTest(restRequestInfo, RestServiceErrorCode.UnsupportedRestMethod, requestHandler);
  }

  private void delayedHandleRequestThatThrowsRestException(RestRequestHandler requestHandler)
      throws Exception {
    RestRequestInfo restRequestInfo =
        createRestRequestInfo(RestMethod.GET, MockBlobStorageService.OPERATION_THROW_HANDLING_REST_EXCEPTION, null);
    doDelayedHandleRequestFailureTest(restRequestInfo, RestServiceErrorCode.InternalServerError, requestHandler);
  }

  private void delayedHandleRequestThatThrowsRuntimeException(RestRequestHandler requestHandler)
      throws Exception {
    RestRequestInfo restRequestInfo =
        createRestRequestInfo(RestMethod.GET, MockBlobStorageService.OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION, null);
    try {
      doDelayedHandleRequestFailureTest(restRequestInfo, RestServiceErrorCode.ResponseBuildingFailure, requestHandler);
    } catch (RuntimeException e) {
      // expected. Check message.
      assertEquals("Failure message does not match expectation",
          MockBlobStorageService.OPERATION_THROW_HANDLING_RUNTIME_EXCEPTION, e.getMessage());
    }
  }

  /**
   * Sends a {@link RestRequestInfo} to the {@link AsyncRequestHandler} and adds a {@link RestRequestInfoEventListener}
   * to it to listen for handling results.
   * </p>
   * The listener determines success or failure of the test (handling failure expected and the
   * {@link RestServiceErrorCode} has to match expectation). Failure of test is bubbled up to this function.
   * @param restRequestInfo - The {@link RestRequestInfo} that has to be handled.
   * @param expectedCode - The expected {@link RestServiceErrorCode}.
   * @param requestHandler - The {@link RestRequestHandler} to use.
   * @throws Exception
   */
  private void doDelayedHandleRequestFailureTest(RestRequestInfo restRequestInfo, RestServiceErrorCode expectedCode,
      RestRequestHandler requestHandler)
      throws Exception {
    RestRequestInfoHandlingFailureMonitor restRequestInfoHandlingFailureMonitor =
        new RestRequestInfoHandlingFailureMonitor(expectedCode);
    restRequestInfo.addListener(restRequestInfoHandlingFailureMonitor);
    requestHandler.handleRequest(restRequestInfo);

    if (restRequestInfoHandlingFailureMonitor.await(30, TimeUnit.SECONDS)) {
      Exception e = restRequestInfoHandlingFailureMonitor.getException();
      AssertionError ae = restRequestInfoHandlingFailureMonitor.getAssertionError();
      if (e != null) {
        throw e;
      } else if (ae != null) {
        throw ae;
      }
      // AsyncRequestHandler should still be alive and serving requests
      doHandleRequestSuccessTest(RestMethod.GET, requestHandler);
    } else {
      fail("Test took too long. There might be a problem or the timeout may need to be increased");
    }
  }

  // badRequestInfoHandleListenersTest() helpers

  /**
   * Sends a request to the {@link AsyncRequestHandler} that will be handled successfully but the implementation of
   * {@link RestRequestInfoEventListener} throws exceptions. Tests that {@link AsyncRequestHandler} does not get killed.
   * @param requestHandler - The {@link RestRequestHandler} to use.
   * @throws Exception
   */
  public void badOnHandleSuccessBehaviourTest(RestRequestHandler requestHandler)
      throws Exception {
    doBadHandlerTest(createRestRequestInfo(RestMethod.GET, "/", new JSONObject()), requestHandler);
  }

  /**
   * Sends a request to the {@link AsyncRequestHandler} that will be handled unsuccessfully and the implementation of
   * {@link RestRequestInfoEventListener} throws exceptions. Tests that {@link AsyncRequestHandler} does not get killed.
   * @param requestHandler - The {@link RestRequestHandler} to use.
   * @throws Exception
   */
  public void badOnHandleFailureBehaviourTest(RestRequestHandler requestHandler)
      throws Exception {
    doBadHandlerTest(
        createRestRequestInfo(RestMethod.GET, MockBlobStorageService.OPERATION_THROW_HANDLING_REST_EXCEPTION, null),
        requestHandler);
  }

  /**
   * Sends a request to the {@link AsyncRequestHandler} and attaches the implementation of
   * {@link RestRequestInfoEventListener} that throws exceptions. Tests that {@link AsyncRequestHandler} is alive and
   * well at the end of test.
   * @param restRequestInfo - The {@link RestRequestInfo} that has to be handled.
   * @param requestHandler - The {@link RestRequestHandler} to use.
   * @throws Exception
   */
  public void doBadHandlerTest(RestRequestInfo restRequestInfo, RestRequestHandler requestHandler)
      throws Exception {
    RestRequestInfoHandlingBadMonitor restRequestInfoHandlingBadMonitor = new RestRequestInfoHandlingBadMonitor();
    restRequestInfo.addListener(restRequestInfoHandlingBadMonitor);
    requestHandler.handleRequest(restRequestInfo);

    if (!restRequestInfoHandlingBadMonitor.await(10, TimeUnit.SECONDS)) {
      fail("Test took too long. There might be a problem or the timeout may need to be increased");
    }

    // AsyncRequestHandler should still be alive and serving requests
    doHandleRequestSuccessTest(RestMethod.GET, requestHandler);
  }
}

// monitor classes

/**
 * Implementation of {@link RestRequestInfoEventListener} that throws exceptions while handling result events.
 */
class RestRequestInfoHandlingBadMonitor implements RestRequestInfoEventListener {
  private final CountDownLatch toBeProcessed = new CountDownLatch(1);

  @Override
  public void onHandlingComplete(RestRequestInfo restRequestInfo, Exception e) {
    toBeProcessed.countDown();
    throw new RuntimeException("This is bad handler");
  }

  /**
   * Wait for a finite amount of time for handling to complete.
   * @param timeout - the length of time to wait for.
   * @param timeUnit - the time unit of timeout.
   * @return
   * @throws InterruptedException
   */
  public boolean await(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return toBeProcessed.await(timeout, timeUnit);
  }
}

/**
 * Implementation of {@link RestRequestInfoEventListener} that expects success and checks the expected response. Fails
 * and records exception otherwise.
 */
class RestRequestInfoHandlingSuccessMonitor implements RestRequestInfoEventListener {
  private final CountDownLatch toBeProcessed = new CountDownLatch(1);
  private final MockRestResponseChannel restResponseChannel;
  private final RestMethod restMethod;

  private Exception exception = null;
  private AssertionError assertionError = null;

  public RestRequestInfoHandlingSuccessMonitor(MockRestResponseChannel responseChannel, RestMethod restMethod) {
    this.restResponseChannel = responseChannel;
    this.restMethod = restMethod;
  }

  @Override
  public void onHandlingComplete(RestRequestInfo restRequestInfo, Exception e) {
    try {
      if (e == null) {
        String responseBody = restResponseChannel.getFlushedResponseBody();
        // expect MockBlobStorageService to echo the method back.
        assertEquals("Unexpected response for " + restMethod, restMethod.toString(), responseBody);
      } else {
        exception = e;
      }
    } catch (Exception ee) {
      exception = ee;
    } catch (AssertionError ae) {
      assertionError = ae;
    } finally {
      toBeProcessed.countDown();
    }
  }

  /**
   * Wait for a finite amount of time for handling to complete.
   * @param timeout - the length of time to wait for.
   * @param timeUnit - the time unit of timeout.
   * @return
   * @throws InterruptedException
   */
  public boolean await(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return toBeProcessed.await(timeout, timeUnit);
  }

  /**
   * Returns any Exception that occurred.
   * @return - Exception that occurred if any, null otherwise.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * Returns any AssertionError that occurred.
   * @return - AssertionError that occurred if any, null otherwise.
   */
  public AssertionError getAssertionError() {
    return assertionError;
  }
}

/**
 * Implementation of {@link RestRequestInfoEventListener} that expects failure and checks the expected error code. Fails
 * and records exception otherwise.
 */
class RestRequestInfoHandlingFailureMonitor implements RestRequestInfoEventListener {
  private final CountDownLatch toBeProcessed = new CountDownLatch(1);
  private final RestServiceErrorCode expectedCode;

  private Exception exception = null;
  private AssertionError assertionError = null;

  public RestRequestInfoHandlingFailureMonitor(RestServiceErrorCode expectedCode) {
    this.expectedCode = expectedCode;
  }

  @Override
  public void onHandlingComplete(RestRequestInfo restRequestInfo, Exception e) {
    try {
      if (e == null) {
        exception = new Exception("Request handling success when it should have failed");
      } else {
        if (e instanceof RestServiceException) {
          assertEquals("Did not get expected RestServiceErrorCode", expectedCode,
              ((RestServiceException) e).getErrorCode());
        } else {
          exception = e;
        }
      }
    } catch (Exception ee) {
      exception = ee;
    } catch (AssertionError ae) {
      assertionError = ae;
    } finally {
      toBeProcessed.countDown();
    }
  }

  /**
   * Wait for a finite amount of time for handling to complete.
   * @param timeout - the length of time to wait for.
   * @param timeUnit - the time unit of timeout.
   * @return
   * @throws InterruptedException
   */
  public boolean await(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return toBeProcessed.await(timeout, timeUnit);
  }

  /**
   * Returns any Exception that occurred.
   * @return - Exception that occurred if any, null otherwise.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * Returns any AssertionError that occurred.
   * @return - AssertionError that occurred if any, null otherwise.
   */
  public AssertionError getAssertionError() {
    return assertionError;
  }
}

