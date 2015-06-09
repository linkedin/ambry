package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.storageservice.ExecutionData;
import com.github.ambry.storageservice.MockBlobStorageService;
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
      throws Exception {
    for (RestMethod restMethod : RestMethod.values()) {
      doProcessMessageSuccessTest(restMethod);
    }
  }

  @Test
  public void processMessageWithBadInputTest()
      throws Exception {
    processMessageWithRestRequestNull();
    processMessageWithRestObjectNull();
    processMessageWithRestResponseHandlerNull();
    processMessageThatThrowsRuntimeException();
  }

  @Test
  public void badMessageHandleListenersTest()
      throws Exception {
    badOnHandleSuccessTest();
    badOnHandleFailureTest();
  }

  // helpers
  // general
  private MockRestMessageHandler getMockRestMessageHandler() {
    MockBlobStorageService blobStorageService = new MockBlobStorageService();
    MockExternalServerMetrics serverMetrics = new MockExternalServerMetrics(new MetricRegistry());
    return new MockRestMessageHandler(blobStorageService, serverMetrics);
  }

  private MockRestRequest createRestRequest(RestMethod method, String uri, JSONObject headers)
      throws JSONException {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, method);
    data.put(MockRestRequest.URI_KEY, uri);
    data.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(data);
  }

  // processMessageTest() helpers
  private void doProcessMessageSuccessTest(RestMethod restMethod)
      throws Exception {
    MockRestMessageHandler restMessageHandler = getMockRestMessageHandler();
    Thread messageHandlerRunner = new Thread(restMessageHandler);
    messageHandlerRunner.start();
    sendRequestAndExpectSuccess(restMethod, restMessageHandler);
    restMessageHandler.shutdownGracefully(null);
  }

  private void sendRequestAndExpectSuccess(RestMethod restMethod, MockRestMessageHandler restMessageHandler)
      throws Exception {
    MockRestRequest restRequest = createRestRequest(restMethod, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(restRequest, restRequest, restResponseHandler);
    MessageProcessingSuccessMonitor messageProcessingSuccessMonitor =
        new MessageProcessingSuccessMonitor(restResponseHandler, restMethod);
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

  // processMessageWithBadInputTest() helpers
  private void processMessageWithRestRequestNull()
      throws Exception {
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(null, restRequest, restResponseHandler);
    doProcessMessageFailureTest(messageInfo, RestErrorCode.RestRequestMissing);
  }

  private void processMessageWithRestObjectNull()
      throws Exception {
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(restRequest, null, restResponseHandler);
    doProcessMessageFailureTest(messageInfo, RestErrorCode.RestObjectMissing);
  }

  private void processMessageWithRestResponseHandlerNull()
      throws Exception {
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MessageInfo messageInfo = new MessageInfo(restRequest, restRequest, null);
    doProcessMessageFailureTest(messageInfo, RestErrorCode.ReponseHandlerMissing);
  }

  private void processMessageThatThrowsRuntimeException()
      throws Exception {
    JSONObject headers = new JSONObject();
    JSONObject executionData = new JSONObject();
    executionData.put(ExecutionData.OPERATION_TYPE_KEY,
        MockRestMessageHandler.OPERATION_THROW_PROCESSING_UNCHECKED_EXCEPTION);
    executionData.put(ExecutionData.OPERATION_DATA_KEY, new JSONObject());
    headers.put(RestMessageHandler.EXECUTION_DATA_HEADER_KEY, executionData);
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", headers);
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(restRequest, restRequest, restResponseHandler);
    try {
      doProcessMessageFailureTest(messageInfo, RestErrorCode.RequestProcessingFailure);
      fail("Did not get an exception even though one was requested");
    } catch (RuntimeException e) {
      // nothing to do. expected.
    }
  }

  private void doProcessMessageFailureTest(MessageInfo messageInfo, RestErrorCode expectedCode)
      throws Exception {
    MockRestMessageHandler restMessageHandler = getMockRestMessageHandler();
    Thread messageHandlerRunner = new Thread(restMessageHandler);
    messageHandlerRunner.start();

    RestResponseHandler restResponseHandler = messageInfo.getResponseHandler();
    if (restResponseHandler != null && !(restResponseHandler instanceof MockRestResponseHandler)) {
      fail("Provided response handler is not a MockRestResponseHandler");
    }

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
      throws Exception {
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(restRequest, restRequest, restResponseHandler);
    doBadHandlerTest(messageInfo);
  }

  public void badOnHandleFailureTest()
      throws Exception {
    MockRestRequest restRequest = createRestRequest(RestMethod.GET, "/", new JSONObject());
    MockRestResponseHandler restResponseHandler = new MockRestResponseHandler();
    MessageInfo messageInfo = new MessageInfo(restRequest, null, restResponseHandler);
    doBadHandlerTest(messageInfo);
  }

  public void doBadHandlerTest(MessageInfo messageInfo)
      throws Exception {
    MockRestMessageHandler restMessageHandler = getMockRestMessageHandler();
    Thread messageHandlerRunner = new Thread(restMessageHandler);
    messageHandlerRunner.start();

    MessageProcessingBadMonitor messageProcessingBadMonitor = new MessageProcessingBadMonitor(messageHandlerRunner);
    messageInfo.addListener(messageProcessingBadMonitor);
    restMessageHandler.handleMessage(messageInfo);

    if (!messageProcessingBadMonitor.await(10, TimeUnit.SECONDS)) {
      fail("Test took too long. There might be a problem or the timeout may need to be increased");
    }

    // make sure that the message handler is still alive and well
    assertTrue("RestMessageHandler is dead", messageHandlerRunner.isAlive());
    for (RestMethod restMethod : RestMethod.values()) {
      sendRequestAndExpectSuccess(restMethod, restMessageHandler);
    }
    restMessageHandler.shutdownGracefully(null);
  }

  // monitor classes
  private class MessageProcessingBadMonitor implements HandleMessageEventListener {
    private final CountDownLatch toBeProcessed = new CountDownLatch(1);
    private final Thread messageHandlerRunner;

    public MessageProcessingBadMonitor(Thread messageHandlerRunner)
        throws Exception {
      this.messageHandlerRunner = messageHandlerRunner;
    }

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

  private class MessageProcessingFailureMonitor implements HandleMessageEventListener {
    private final CountDownLatch toBeProcessed = new CountDownLatch(1);
    private final RestErrorCode expectedCode;
    private final Thread messageHandlerRunner;

    private Throwable cause = null;

    public Throwable getCause() {
      return cause;
    }

    public MessageProcessingFailureMonitor(Thread messageHandlerRunner, RestErrorCode expectedCode)
        throws Exception {
      this.expectedCode = expectedCode;
      this.messageHandlerRunner = messageHandlerRunner;
    }

    public void onMessageHandleSuccess(MessageInfo messageInfo) {
      cause = new Exception("MessageHandle success when it should have failed");
      toBeProcessed.countDown();
    }

    public void onMessageHandleFailure(MessageInfo messageInfo, Exception e) {
      try {
        if (e instanceof RestException) {
          assertEquals("Did not get expected RestErrorCode", expectedCode, ((RestException) e).getErrorCode());
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

  private class MessageProcessingSuccessMonitor implements HandleMessageEventListener {
    private final CountDownLatch toBeProcessed = new CountDownLatch(1);
    private final MockRestResponseHandler restResponseHandler;
    private final RestMethod restMethod;

    private Throwable cause = null;

    public Throwable getCause() {
      return cause;
    }

    public MessageProcessingSuccessMonitor(MockRestResponseHandler restResponseHandler, RestMethod restMethod)
        throws Exception {
      this.restResponseHandler = restResponseHandler;
      this.restMethod = restMethod;
    }

    public void onMessageHandleSuccess(MessageInfo messageInfo) {
      try {
        String responseBody = restResponseHandler.getFlushedBody().getString(MockRestResponseHandler.BODY_STRING_KEY);
        // expect MockRestMessageHandler to echo the method back.
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
}
