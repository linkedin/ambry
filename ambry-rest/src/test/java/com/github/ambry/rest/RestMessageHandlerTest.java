package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.restservice.BlobStorageService;
import com.github.ambry.restservice.HandleMessageEventListener;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * TODO: write description
 */
public class RestMessageHandlerTest {

  @Test
  public void processMessageTest()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    RestMessageHandler restMessageHandler = getRestMessageHandler();
    Thread messageHandlerRunner = new Thread(restMessageHandler);
    messageHandlerRunner.start();
    for (RestMethod restMethod : RestMethod.values()) {
      doProcessMessageSuccessTest(restMethod, restMessageHandler);
    }
    restMessageHandler.shutdownGracefully(null);
  }

  @Test
  public void handleMessageWithBadInputTest()
      throws IOException, JSONException, URISyntaxException {
    handleMessageWithRestRequestNull();
    handleMessageWithRestObjectNull();
    handleMessageWithRestResponseHandlerNull();
  }

  @Test
  public void processMessageWithBadInputTest()
      throws Exception {
    processMessageThatThrowsRuntimeException();
  }

  @Test
  public void badMessageHandleListenersTest()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    badOnHandleSuccessTest();
    badOnHandleFailureTest();
  }

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

  @Test
  public void onErrorWithBadInputTest()
      throws IOException, JSONException, URISyntaxException {
    RestMessageHandler messageHandler = getRestMessageHandler();
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    Exception placeholderException = new Exception("placeHolderException");

    // no exceptions should be thrown
    messageHandler.onError(new MessageInfo(restRequest, restRequest, null), placeholderException);
    messageHandler.onError(null, placeholderException);
  }

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
    BlobStorageService blobStorageService = new MockBlobStorageService();
    RestServerMetrics serverMetrics = new RestServerMetrics(new MetricRegistry());
    return new RestMessageHandler(blobStorageService, serverMetrics);
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

  // processMessageTest() helpers
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
      throws IOException, JSONException, URISyntaxException {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(null, restRequest, restResponseHandler);
    doHandleMessageFailureTest(messageInfo, RestServiceErrorCode.RestRequestMissing);
  }

  private void handleMessageWithRestObjectNull()
      throws IOException, JSONException, URISyntaxException {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(restRequest, null, restResponseHandler);
    doHandleMessageFailureTest(messageInfo, RestServiceErrorCode.RestObjectMissing);
  }

  private void handleMessageWithRestResponseHandlerNull()
      throws IOException, JSONException, URISyntaxException {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MessageInfo messageInfo = new MessageInfo(restRequest, restRequest, null);
    doHandleMessageFailureTest(messageInfo, RestServiceErrorCode.ReponseHandlerMissing);
  }

  private void doHandleMessageFailureTest(MessageInfo messageInfo, RestServiceErrorCode expectedCode)
      throws IOException {
    RestMessageHandler restMessageHandler = getRestMessageHandler();
    Thread messageHandlerRunner = new Thread(restMessageHandler);
    messageHandlerRunner.start();

    try {
      restMessageHandler.handleMessage(messageInfo);
      fail("Test should have thrown exception, but did not");
    } catch (RestServiceException e) {
      // message handler should still be alive
      assertTrue("Message handler is not alive", messageHandlerRunner.isAlive());
      assertEquals("Did not get expected RestServiceErrorCode", expectedCode, e.getErrorCode());
    }
  }

  // processMessageWithBadInputTest() helpers
  private void processMessageThatThrowsRuntimeException()
      throws Exception {
    MessageInfo messageInfo = createMessageInfoThatThrowsRuntimeException();
    try {
      doProcessMessageFailureTest(messageInfo, RestServiceErrorCode.RequestProcessingFailure);
      fail("Did not get an exception even though one was requested");
    } catch (RuntimeException e) {
      // nothing to do. expected.
    }
  }

  private MessageInfo createMessageInfoThatThrowsRuntimeException()
      throws JSONException, URISyntaxException {
    JSONObject headers = new JSONObject();
    JSONObject executionData = new JSONObject();
    executionData.put(MockBlobStorageService.OPERATION_TYPE_KEY,
        MockBlobStorageService.OPERATION_THROW_PROCESSING_UNCHECKED_EXCEPTION);
    executionData.put(MockBlobStorageService.OPERATION_DATA_KEY, new JSONObject());
    headers.put(MockBlobStorageService.EXECUTION_DATA_HEADER_KEY, executionData);

    return createMessageInfo(RestMethod.GET, "/", headers);
  }

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
  public void badOnHandleSuccessTest()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    doBadHandlerTest(createMessageInfo(RestMethod.GET, "/", new JSONObject()));
  }

  public void badOnHandleFailureTest()
      throws InterruptedException, IOException, JSONException, RestServiceException, URISyntaxException {
    doBadHandlerTest(createMessageInfoThatThrowsRuntimeException());
  }

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

    // make sure that the message handler is still alive and well
    assertTrue("RestMessageHandler is dead", messageHandlerRunner.isAlive());
    for (RestMethod restMethod : RestMethod.values()) {
      doProcessMessageSuccessTest(restMethod, restMessageHandler);
    }
    restMessageHandler.shutdownGracefully(null);
  }

  // monitor classes
  private class MessageProcessingBadMonitor implements HandleMessageEventListener {
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

  private class MessageProcessingSuccessMonitor implements HandleMessageEventListener {
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
      } catch (Exception e) {
        cause = e;
      } catch (AssertionError e) {
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

  private class MessageProcessingFailureMonitor implements HandleMessageEventListener {
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
      } catch (Exception ne) {
        cause = ne;
      } catch (AssertionError ae) {
        cause = ae;
      }
      toBeProcessed.countDown();
    }

    public boolean await(long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      return toBeProcessed.await(timeout, timeUnit);
    }
  }
}
