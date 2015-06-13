package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.HandleMessageResultListener;
import com.github.ambry.restservice.MessageInfo;
import com.github.ambry.restservice.MockBlobStorageService;
import com.github.ambry.restservice.MockRestRequest;
import com.github.ambry.restservice.MockRestResponseHandler;
import com.github.ambry.restservice.RestMethod;
import com.github.ambry.restservice.RestRequest;
import com.github.ambry.restservice.RestServiceErrorCode;
import com.github.ambry.restservice.RestServiceException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Tests functionality of RestMessageHandler
 */
public class RestMessageHandlerTest {

  /**
   * Tests handling and processing of messages given good input.
   * @throws InterruptedException
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleAndProcessMessageTest()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    RestMessageHandler restMessageHandler = getRestMessageHandler();
    Thread messageHandlerRunner = new Thread(restMessageHandler);
    messageHandlerRunner.start();
    for (RestMethod restMethod : RestMethod.values()) {
      doProcessMessageSuccessTest(restMethod, restMessageHandler);
    }
    restMessageHandler.shutdownGracefully(null);
  }

  /**
   * Tests that right exceptions are thrown on bad input while handling.
   * @throws InterruptedException
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void handleMessageWithBadInputTest()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    handleMessageWithRestRequestNull();
    handleMessageWithRestObjectNull();
    handleMessageWithRestResponseHandlerNull();
  }

  /**
   * Tests that right exception are thrown on bad input while processing.
   * @throws Exception
   */
  @Test
  public void processMessageWithBadInputTest()
      throws Exception {
    processMessageThatThrowsRuntimeException();
  }

  /**
   * Tests that bad handlers (ones that throw exceptions) do not kill the RestMessageHandler.
   * @throws InterruptedException
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  @Test
  public void badMessageHandleListenersTest()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    badOnHandleSuccessTest();
    badOnHandleFailureTest();
  }

  /**
   * Check onError with good input - should not kill RestMessageHandler and expected response should be correct
   * @throws IOException
   * @throws JSONException
   * @throws URISyntaxException
   */
  @Test
  public void onErrorWithResponseHandlerNotNullTest()
      throws IOException, JSONException, URISyntaxException {
    RestMessageHandler messageHandler = getRestMessageHandler();
    MessageInfo messageInfo = createMessageInfo(RestMethod.GET, "/", new JSONObject());
    Exception placeholderException = new Exception("placeHolderException");
    messageHandler.onError(messageInfo, placeholderException);

    MockRestResponseHandler restResponseHandler = ((MockRestResponseHandler) messageInfo.getResponseHandler());
    JSONObject response = restResponseHandler.getFlushedResponse();
    assertEquals("Response status error ", MockRestResponseHandler.STATUS_ERROR,
        response.getString(MockRestResponseHandler.RESPONSE_STATUS_KEY));
    assertEquals("Response error message mismatch", placeholderException.toString(),
        response.getString(MockRestResponseHandler.ERROR_MESSAGE_KEY));
  }

  /**
   * Checks onError with badInput - should not kill RestMessageHandler
   * @throws IOException
   * @throws JSONException
   * @throws URISyntaxException
   */
  @Test
  public void onErrorWithBadInputTest()
      throws IOException, JSONException, URISyntaxException {
    RestMessageHandler messageHandler = getRestMessageHandler();
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    Exception placeholderException = new Exception("placeHolderException");

    // no exceptions should be thrown
    // no response handler
    messageHandler.onError(new MessageInfo(restRequest, restRequest, null), placeholderException);
    // no message info
    messageHandler.onError(null, placeholderException);
  }

  /**
   * Checks onRequestComplete functionality with good input
   * @throws IOException
   * @throws JSONException
   * @throws URISyntaxException
   */
  @Test
  public void onRequestCompleteWithRequestNotNullTest()
      throws IOException, JSONException, URISyntaxException {
    RestMessageHandler messageHandler = getRestMessageHandler();
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    /**
     * Does nothing for now so this is a dummy test. If functionality is introduced in onRequestComplete(),
     * this has to be updated with a check to verify state
     */
    messageHandler.onRequestComplete(restRequest);
  }

  /**
   * Checks onRequestComplete functionality with bad input
   * @throws IOException
   * @throws JSONException
   */
  @Test
  public void onRequestCompleteWithRequestNullTest()
      throws IOException, JSONException {
    RestMessageHandler messageHandler = getRestMessageHandler();
    /**
     * Does nothing for now so this is a dummy test. If functionality is introduced in onRequestComplete(),
     * this has to be updated with a check to verify state
     */
    messageHandler.onRequestComplete(null);
  }

  // helpers
  // general
  private RestMessageHandler getRestMessageHandler()
      throws IOException {
    BlobStorageService blobStorageService = getBlobStorageService();
    RestServerMetrics serverMetrics = new RestServerMetrics(new MetricRegistry());
    return new RestMessageHandler(blobStorageService, serverMetrics);
  }

  private BlobStorageService getBlobStorageService()
      throws IOException {
    // dud properties. should pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    return new MockBlobStorageService(verifiableProperties, new MockClusterMap(), new MetricRegistry());
  }

  private RestRequest createRestRequest(RestMethod method, String uri, JSONObject headers)
      throws JSONException, URISyntaxException {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, method);
    data.put(MockRestRequest.URI_KEY, uri);
    data.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(data);
  }

  private MessageInfo createMessageInfo(RestMethod method, String uri, JSONObject headers)
      throws JSONException, URISyntaxException {
    RestRequest restRequest = createRestRequest(method, uri, headers);
    return new MessageInfo(restRequest, restRequest, new MockRestResponseHandler());
  }

  // handleAndProcessMessageTest() helpers

  /**
   * Sends message to the RestMessageHandler and adds a listener to the message to listen for processing results.
   * The listener determines success or failure of the test (processing success expected).
   * Failure of test is bubbled up to this function.
   * @param restMethod
   * @param restMessageHandler
   * @throws InterruptedException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  private void doProcessMessageSuccessTest(RestMethod restMethod, RestMessageHandler restMessageHandler)
      throws InterruptedException, JSONException, RestServiceException, URISyntaxException {
    MessageInfo messageInfo = createMessageInfo(restMethod, "/", new JSONObject());
    MessageProcessingSuccessMonitor messageProcessingSuccessMonitor =
        new MessageProcessingSuccessMonitor((MockRestResponseHandler) messageInfo.getResponseHandler(), restMethod);
    messageInfo.addListener(messageProcessingSuccessMonitor);
    restMessageHandler.handleMessage(messageInfo);

    if (messageProcessingSuccessMonitor.await(10, TimeUnit.SECONDS)) {
      Throwable e = messageProcessingSuccessMonitor.getCause();
      if (e != null) {
        fail("Test for " + restMethod + " failed - " + e);
      }
    } else {
      fail("Test took too long. There might be a problem or the timeout may need to be increased");
    }
  }

  // handleMessageWithBadInputTest() helpers
  private void handleMessageWithRestRequestNull()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(null, restRequest, restResponseHandler);
    doHandleMessageFailureTest(messageInfo, RestServiceErrorCode.RestRequestMissing);
  }

  private void handleMessageWithRestObjectNull()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(restRequest, null, restResponseHandler);
    doHandleMessageFailureTest(messageInfo, RestServiceErrorCode.RestObjectMissing);
  }

  private void handleMessageWithRestResponseHandlerNull()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MessageInfo messageInfo = new MessageInfo(restRequest, restRequest, null);
    doHandleMessageFailureTest(messageInfo, RestServiceErrorCode.ReponseHandlerMissing);
  }

  /**
   * Sends message to the RestMessageHandler and expects the handling to fail. Tests for the proper error code.
   * @param messageInfo
   * @param expectedCode
   * @throws InterruptedException
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  private void doHandleMessageFailureTest(MessageInfo messageInfo, RestServiceErrorCode expectedCode)
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    RestMessageHandler restMessageHandler = getRestMessageHandler();
    Thread messageHandlerRunner = new Thread(restMessageHandler);
    messageHandlerRunner.start();

    try {
      restMessageHandler.handleMessage(messageInfo);
      fail("Test should have thrown exception, but did not");
    } catch (RestServiceException e) {
      assertEquals("Did not get expected RestServiceErrorCode", expectedCode, e.getErrorCode());

      // message handler should still be alive and serving requests
      assertTrue("Message handler is not alive", messageHandlerRunner.isAlive());
      doProcessMessageSuccessTest(RestMethod.GET, restMessageHandler);
    } finally {
      restMessageHandler.shutdownGracefully(null);
    }
  }

  // processMessageWithBadInputTest() helpers
  private void processMessageThatThrowsRuntimeException()
      throws Exception {
    MessageInfo messageInfo = createMessageInfoThatThrowsProcessingRuntimeException();
    try {
      doProcessMessageFailureTest(messageInfo, null);
      fail("Did not get an exception even though one was requested");
    } catch (RuntimeException e) {
      // nothing to do. expected.
    }
  }

  private MessageInfo createMessageInfoThatThrowsProcessingRuntimeException()
      throws JSONException, URISyntaxException {
    JSONObject headers = new JSONObject();
    JSONObject executionData = new JSONObject();
    executionData.put(MockBlobStorageService.OPERATION_TYPE_KEY,
        MockBlobStorageService.OPERATION_THROW_PROCESSING_UNCHECKED_EXCEPTION);
    executionData.put(MockBlobStorageService.OPERATION_DATA_KEY, new JSONObject());
    headers.put(MockBlobStorageService.EXECUTION_DATA_HEADER_KEY, executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

  /**
   * Sends message to the RestMessageHandler and adds a listener to the message to listen for processing results.
   * The listener determines success or failure of the test (processing failure expected and error code has to match).
   * Failure of test is bubbled up to this function.
   * @param messageInfo
   * @param expectedCode
   * @throws Exception
   */
  private void doProcessMessageFailureTest(MessageInfo messageInfo, RestServiceErrorCode expectedCode)
      throws Exception {
    RestMessageHandler restMessageHandler = getRestMessageHandler();
    Thread messageHandlerRunner = new Thread(restMessageHandler);
    messageHandlerRunner.start();

    MessageProcessingFailureMonitor messageProcessingFailureMonitor =
        new MessageProcessingFailureMonitor(messageHandlerRunner, expectedCode);
    messageInfo.addListener(messageProcessingFailureMonitor);
    restMessageHandler.handleMessage(messageInfo);

    if (messageProcessingFailureMonitor.await(30, TimeUnit.SECONDS)) {
      restMessageHandler.shutdownGracefully(null);
      Throwable cause = messageProcessingFailureMonitor.getCause();
      if (cause != null && !(cause instanceof Exception)) {
        fail("Failure scenario test failed - " + cause);
      } else if (cause != null) {
        throw (Exception) cause;
      }
    } else {
      fail("Test took too long. There might be a problem or the timeout may need to be increased");
    }
  }

  // badMessageHandleListenersTest() helpers

  /**
   * Sends a request that will be processed successfully but the listener is bad. Tests that RestMessageHandler does
   * not get killed.
   * @throws InterruptedException
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  public void badOnHandleSuccessTest()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    doBadHandlerTest(createMessageInfo(RestMethod.GET, "/", new JSONObject()));
  }

  /**
   * Sends a request that will be processed unsuccessfully and the listener is bad. Tests that RestMessageHandler does
   * not get killed.
   * @throws InterruptedException
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  public void badOnHandleFailureTest()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    doBadHandlerTest(createMessageInfoThatThrowsProcessingRuntimeException());
  }

  /**
   * Sends a message and attaches a listener that throws an exception. Checks that the RestMessageHandler is alive and
   * well at the end of the test.
   * @param messageInfo
   * @throws InterruptedException
   * @throws IOException
   * @throws JSONException
   * @throws RestServiceException
   * @throws URISyntaxException
   */
  public void doBadHandlerTest(MessageInfo messageInfo)
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    RestMessageHandler restMessageHandler = getRestMessageHandler();
    Thread messageHandlerRunner = new Thread(restMessageHandler);
    messageHandlerRunner.start();

    MessageProcessingBadMonitor messageProcessingBadMonitor = new MessageProcessingBadMonitor();
    messageInfo.addListener(messageProcessingBadMonitor);
    restMessageHandler.handleMessage(messageInfo);

    if (!messageProcessingBadMonitor.await(10, TimeUnit.SECONDS)) {
      fail("Test took too long. There might be a problem or the timeout may need to be increased");
    }

    // message handler should still be alive and serving requests
    assertTrue("Message handler is not alive", messageHandlerRunner.isAlive());
    doProcessMessageSuccessTest(RestMethod.GET, restMessageHandler);
    restMessageHandler.shutdownGracefully(null);
  }

  // monitor classes

  /**
   * Listener that throws exceptions while handling result events.
   */
  private class MessageProcessingBadMonitor implements HandleMessageResultListener {
    private final CountDownLatch toBeProcessed = new CountDownLatch(1);

    public void onMessageHandleSuccess(MessageInfo messageInfo) {
      toBeProcessed.countDown();
      throw new RuntimeException("This is bad handler");
    }

    public void onMessageHandleFailure(MessageInfo messageInfo, Exception e) {
      toBeProcessed.countDown();
      throw new RuntimeException("This is bad handler");
    }

    public boolean await(long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      return toBeProcessed.await(timeout, timeUnit);
    }
  }

  /**
   * Listener that expects success and checks the expected response. Fails and records exception otherwise.
   */
  private class MessageProcessingSuccessMonitor implements HandleMessageResultListener {
    private final CountDownLatch toBeProcessed = new CountDownLatch(1);
    private final MockRestResponseHandler restResponseHandler;
    private final RestMethod restMethod;

    private Throwable cause = null;

    public Throwable getCause() {
      return cause;
    }

    public MessageProcessingSuccessMonitor(MockRestResponseHandler restResponseHandler, RestMethod restMethod) {
      this.restResponseHandler = restResponseHandler;
      this.restMethod = restMethod;
    }

    public void onMessageHandleSuccess(MessageInfo messageInfo) {
      try {
        String responseBody = restResponseHandler.getFlushedBody();
        // expect MockBlobStorageService to echo the method back.
        assertEquals("Unexpected response for " + restMethod, restMethod.toString(), responseBody);
      } catch (Throwable e) {
        cause = e;
      } finally {
        toBeProcessed.countDown();
      }
    }

    public void onMessageHandleFailure(MessageInfo messageInfo, Exception e) {
      cause = e;
      toBeProcessed.countDown();
    }

    public boolean await(long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      return toBeProcessed.await(timeout, timeUnit);
    }
  }

  /**
   * Listener that expects failure and checks the expected error code. Fails and records exception otherwise.
   */
  private class MessageProcessingFailureMonitor implements HandleMessageResultListener {
    private final CountDownLatch toBeProcessed = new CountDownLatch(1);
    private final RestServiceErrorCode expectedCode;
    private final Thread messageHandlerRunner;

    private Throwable cause = null;

    public Throwable getCause() {
      return cause;
    }

    public MessageProcessingFailureMonitor(Thread messageHandlerRunner, RestServiceErrorCode expectedCode) {
      this.expectedCode = expectedCode;
      this.messageHandlerRunner = messageHandlerRunner;
    }

    public void onMessageHandleSuccess(MessageInfo messageInfo) {
      cause = new Exception("MessageHandle success when it should have failed");
      toBeProcessed.countDown();
    }

    public void onMessageHandleFailure(MessageInfo messageInfo, Exception e) {
      try {
        if (e instanceof RestServiceException) {
          assertEquals("Did not get expected RestServiceErrorCode", expectedCode,
              ((RestServiceException) e).getErrorCode());
        } else {
          cause = e;
        }
        // message handler should still be alive
        assertTrue("Message handler is not alive", messageHandlerRunner.isAlive());
      } catch (Throwable ne) {
        cause = ne;
      }
      toBeProcessed.countDown();
    }

    public boolean await(long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      return toBeProcessed.await(timeout, timeUnit);
    }
  }
}
