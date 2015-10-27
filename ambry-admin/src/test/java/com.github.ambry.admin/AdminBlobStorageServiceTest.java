package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.AsyncRequestResponseHandler;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RequestResponseHandlerController;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestConstants;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServerMetrics;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.ByteBufferChannel;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link AdminBlobStorageService}.
 */
public class AdminBlobStorageServiceTest {

  private static final ClusterMap CLUSTER_MAP;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private final InMemoryRouter router;
  private final AdminResponseHandlerController adminResponseHandlerController;
  private final AdminBlobStorageService adminBlobStorageService;

  /**
   * Sets up the {@link AdminBlobStorageService} instance before a test.
   * @throws InstantiationException
   * @throws IOException
   */
  public AdminBlobStorageServiceTest()
      throws InstantiationException, IOException {
    router = new InMemoryRouter(new VerifiableProperties(new Properties()));
    adminResponseHandlerController = new AdminResponseHandlerController();
    adminBlobStorageService = getAdminBlobStorageService();
    adminBlobStorageService.start();
    adminResponseHandlerController.start();
  }

  /**
   * Shuts down the {@link AdminBlobStorageService} instance after all tests.
   * @throws IOException
   */
  @After
  public void shutdownAdminBlobStorageService()
      throws IOException {
    adminResponseHandlerController.shutdown();
    adminBlobStorageService.shutdown();
    router.close();
  }

  /**
   * Tests basic startup and shutdown functionality (no exceptions).
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startShutDownTest()
      throws InstantiationException, IOException {
    adminBlobStorageService.start();
    adminBlobStorageService.shutdown();
  }

  /**
   * Tests for {@link AdminBlobStorageService#shutdown()} when {@link AdminBlobStorageService#start()} has not been
   * called previously.
   * <p/>
   * This test is for  cases where {@link AdminBlobStorageService#start()} has failed and
   * {@link AdminBlobStorageService#shutdown()} needs to be run.
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStartTest()
      throws IOException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    adminBlobStorageService.shutdown();
  }

  /**
   * This tests for exceptions thrown when an {@link AdminBlobStorageService} instance is used without calling
   * {@link AdminBlobStorageService#start()} first.
   * @throws Exception
   */
  @Test
  public void useServiceWithoutStartTest()
      throws Exception {
    // simulating by shutting down first.
    adminBlobStorageService.shutdown();
    // fine to use without start.
    echoTest();
  }

  /**
   * Checks for reactions of all methods in {@link AdminBlobStorageService} to null arguments.
   * @throws Exception
   */
  @Test
  public void nullInputsForFunctionsTest()
      throws Exception {
    doNullInputsForFunctionsTest("handleGet");
    doNullInputsForFunctionsTest("handlePost");
    doNullInputsForFunctionsTest("handleDelete");
    doNullInputsForFunctionsTest("handleHead");
  }

  /**
   * Checks reactions of all methods in {@link AdminBlobStorageService} to a {@link Router} that throws
   * {@link RuntimeException}.
   * @throws Exception
   */
  @Test
  public void runtimeExceptionRouterTest()
      throws Exception {
    // set InMemoryRouter up to throw RuntimeException
    Properties properties = new Properties();
    properties.setProperty(InMemoryRouter.OPERATION_THROW_EARLY_RUNTIME_EXCEPTION, "true");
    router.setVerifiableProperties(new VerifiableProperties(properties));

    doRuntimeExceptionRouterTest(RestMethod.GET);
    doRuntimeExceptionRouterTest(RestMethod.POST);
    doRuntimeExceptionRouterTest(RestMethod.DELETE);
    doRuntimeExceptionRouterTest(RestMethod.HEAD);
  }

  /**
   * Tests reactions of various methods of {@link AdminBlobStorageService} to a {@link RequestResponseHandlerController}
   * that throws {@link RuntimeException}.
   * <p/>
   * In reality, nothing can be done if this happens - the call will fail with an exception.
   * @throws Exception
   */
  @Test
  public void badControllerTest()
      throws Exception {
    doBadControllerTest("handleGet");
    doBadControllerTest("handlePost");
    doBadControllerTest("handleDelete");
    doBadControllerTest("handleHead");
  }

  /**
   * Checks reactions of all methods in {@link AdminBlobStorageService} to bad {@link AsyncRequestResponseHandler} and
   * {@link RestRequest} implementations.
   * @throws Exception
   */
  @Test
  public void badResponseHandlerAndRestRequestTest()
      throws Exception {
    RestRequest restRequest = new BadRestRequest();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();

    // What happens inside AdminBlobStorageService during this test?
    // 1. Since the RestRequest throws errors, AdminBlobStorageService will attempt to submit response with exception
    //      to AdminTestResponseHandler.
    // 2. The submission will fail because AdminTestResponseHandler has been shutdown.
    // 3. AdminBlobStorageService will directly complete the request over the RestResponseChannel with the *original*
    //      exception.
    // 4. It will then try to release resources but closing the RestRequest will also throw an exception. This exception
    //      is swallowed.
    // What the test is looking for -> No exceptions thrown when the handle is run and the original exception arrives
    // safely.
    adminResponseHandlerController.getHandler().shutdown();

    adminBlobStorageService.handleGet(restRequest, restResponseChannel);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getCause().getClass());

    adminResponseHandlerController.getHandler().reset();
    restResponseChannel = new MockRestResponseChannel();
    adminBlobStorageService.handlePost(restRequest, restResponseChannel);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getCause().getClass());

    adminResponseHandlerController.getHandler().reset();
    restResponseChannel = new MockRestResponseChannel();
    adminBlobStorageService.handleDelete(restRequest, restResponseChannel);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getCause().getClass());

    adminResponseHandlerController.getHandler().reset();
    restResponseChannel = new MockRestResponseChannel();
    adminBlobStorageService.handleHead(restRequest, restResponseChannel);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getCause().getClass());
  }

  /**
   * Tests {@link AdminBlobStorageService#submitResponse(RestRequest, RestResponseChannel, AsyncRequestResponseHandler,
   * ReadableStreamChannel, Exception)}.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void submitResponseTest()
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    String exceptionMsg = new String(getRandomBytes(10));
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    // handleResponse of AdminTestResponseHandler throws exception because it has not been started.

    // there is an exception already.
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    adminBlobStorageService.submitResponse(restRequest, restResponseChannel, adminTestResponseHandler, null,
        new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());

    // there is no exception and exception thrown when the response is submitted.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    ReadableStreamChannel response = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    assertTrue("Response channel is not open", response.isOpen());
    adminBlobStorageService.submitResponse(restRequest, restResponseChannel, adminTestResponseHandler, response, null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());
    assertFalse("Response channel is not cleaned up", response.isOpen());
  }

  /**
   * Tests releasing of resources if response submission fails.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void releaseResourcesTest()
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    // handleResponse of AdminTestResponseHandler throws exception because it has not been started.

    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    assertTrue("RestRequest channel not open", restRequest.isOpen());
    assertTrue("ReadableStreamChannel not open", channel.isOpen());
    adminBlobStorageService.submitResponse(restRequest, restResponseChannel, adminTestResponseHandler, channel, null);
    assertFalse("RestRequest channel is still open", restRequest.isOpen());
    assertFalse("ReadableStreamChannel is still open", channel.isOpen());

    // null ReadableStreamChannel
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    assertTrue("RestRequest channel not open", restRequest.isOpen());
    adminBlobStorageService.submitResponse(restRequest, restResponseChannel, adminTestResponseHandler, null, null);
    assertFalse("RestRequest channel is still open", restRequest.isOpen());

    // bad RestRequest (close() throws IOException)
    channel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    restResponseChannel = new MockRestResponseChannel();
    assertTrue("ReadableStreamChannel not open", channel.isOpen());
    adminBlobStorageService
        .submitResponse(new BadRestRequest(), restResponseChannel, adminTestResponseHandler, channel, null);
    assertFalse("ReadableStreamChannel is still open", channel.isOpen());

    // bad ReadableStreamChannel (close() throws IOException)
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    assertTrue("RestRequest channel not open", restRequest.isOpen());
    adminBlobStorageService
        .submitResponse(restRequest, restResponseChannel, adminTestResponseHandler, new BadRSC(), null);
    assertFalse("RestRequest channel is still open", restRequest.isOpen());
  }

  /**
   * Tests {@link AdminBlobStorageService#getOperationOrBlobIdFromUri(RestRequest)}.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void getOperationOrBlobIdFromUriTest()
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    String path = "expectedPath";
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/" + path, null, null);
    assertEquals("Unexpected path", path, AdminBlobStorageService.getOperationOrBlobIdFromUri(restRequest));
  }

  /**
   * Tests blob POST, GET, HEAD and DELETE operations.
   * @throws Exception
   */
  @Test
  public void postGetHeadDeleteTest()
      throws Exception {
    final int CONTENT_LENGTH = 1024;
    ByteBuffer content = ByteBuffer.wrap(getRandomBytes(CONTENT_LENGTH));
    String serviceId = "postGetHeadDeleteServiceID";
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    JSONObject headers = new JSONObject();
    setAmbryHeaders(headers, CONTENT_LENGTH, 7200, false, serviceId, contentType, ownerId);

    String blobId = postBlobAndVerify(headers, content);
    getBlobAndVerify(blobId, headers, content);
    getHeadAndVerify(blobId, headers);
    deleteBlobAndVerify(blobId);
  }

  /**
   * Tests the {@link AdminOperationType#echo} admin operation. Checks to see that the echo matches input text.
   * @throws Exception
   */
  @Test
  public void echoTest()
      throws Exception {
    String inputText = "textToBeEchoed";
    RestRequest restRequest = createEchoGetRestRequest(inputText);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    ReadableStreamChannel channel = doGet(restRequest, restResponseChannel);
    String echoedText = getJsonizedResponseBody(channel).getString(EchoHandler.TEXT_KEY);
    assertEquals("Unexpected Content-Type", "application/json",
        restResponseChannel.getHeader("Content-Type", MockRestResponseChannel.DataStatus.Flushed));
    assertEquals("Did not get expected response", inputText, echoedText);
  }

  /**
   * Tests reactions of the {@link AdminOperationType#echo} operation to bad input - specifically if we do not include
   * required parameters.
   * @throws Exception
   */
  @Test
  public void echoWithBadInputTest()
      throws Exception {
    // bad input - uri does not have text that needs to be echoed.
    RestRequest restRequest = createRestRequest(RestMethod.GET, AdminOperationType.echo.toString(), null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      doGet(restRequest, restResponseChannel);
      fail("Exception should have been thrown because some required parameters were missing");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.MissingArgs, e.getErrorCode());
    }
  }

  /**
   * Tests the {@link AdminOperationType#getReplicasForBlobId} admin operation.
   * <p/>
   * For the each {@link PartitionId} in the {@link ClusterMap}, a {@link BlobId} is created.
   * The string representation is sent to the {@link AdminBlobStorageService} as a part of getReplicasForBlobId request.
   * The returned replica list is checked for equality against a locally obtained replica list.
   * @throws Exception
   */
  @Test
  public void getReplicasForBlobIdTest()
      throws Exception {
    List<PartitionId> partitionIds = CLUSTER_MAP.getWritablePartitionIds();
    for (PartitionId partitionId : partitionIds) {
      String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
      BlobId blobId = new BlobId(partitionId);
      RestRequest restRequest = createGetReplicasForBlobIdRestRequest(blobId.getID());
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = doGet(restRequest, restResponseChannel);
      assertEquals("Unexpected Content-Type", "application/json",
          restResponseChannel.getHeader("Content-Type", MockRestResponseChannel.DataStatus.Flushed));
      String returnedReplicasStr =
          getJsonizedResponseBody(channel).getString(GetReplicasForBlobIdHandler.REPLICAS_KEY).replace("\"", "");
      assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
          originalReplicaStr, returnedReplicasStr);
    }
  }

  /**
   * Tests reactions of the {@link AdminOperationType#getReplicasForBlobId} operation to bad input - specifically if we
   * do not include required parameters.
   * @throws Exception
   */
  @Test
  public void getReplicasForBlobIdWithBadInputTest()
      throws Exception {
    try {
      // bad input - uri missing the blob id whose replicas need to be returned.
      RestRequest restRequest =
          createRestRequest(RestMethod.GET, AdminOperationType.getReplicasForBlobId.toString(), null, null);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doGet(restRequest, restResponseChannel);
      fail("Exception should have been thrown because some required parameters were missing");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.MissingArgs, e.getErrorCode());
    }

    try {
      // bad input - invalid blob id.
      RestRequest restRequest = createGetReplicasForBlobIdRestRequest("12345");
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doGet(restRequest, restResponseChannel);
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.NotFound, e.getErrorCode());
    }

    try {
      // bad input - invalid blob id for this cluster map.
      String blobId = "AAEAAQAAAAAAAADFAAAAJDMyYWZiOTJmLTBkNDYtNDQyNS1iYzU0LWEwMWQ1Yzg3OTJkZQ.gif";
      RestRequest restRequest = createGetReplicasForBlobIdRestRequest(blobId);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doGet(restRequest, restResponseChannel);
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.NotFound, e.getErrorCode());
    }
  }

  /**
   * Tests reaction of GET to operations that are unknown/not defined.
   * @throws Exception
   */
  @Test
  public void unknownCustomGetOperationTest()
      throws Exception {
    try {
      RestRequest restRequest = createRestRequest(RestMethod.GET, "unknownOperation?dummyParam=dummyValue", null, null);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doGet(restRequest, restResponseChannel);
      fail("Exception should have been thrown because an unknown operation has been specified");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadRequest, e.getErrorCode());
    }
  }

  /**
   * Tests reaction of GET to operations that have bad input.
   * 1. No operation specified
   * @throws Exception
   */
  @Test
  public void customGetWithBadUriTest()
      throws Exception {
    try {
      // bad input - no operation specified
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doGet(restRequest, restResponseChannel);
      fail("Exception should have been thrown because no operation has been specified");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.BadRequest, e.getErrorCode());
    }
  }

  /**
   * Tests for non common case scenarios for {@link HeadForGetCallback}.
   * @throws Exception
   */
  @Test
  public void headForGetCallbackTest()
      throws Exception {
    String exceptionMsg = new String(getRandomBytes(10));
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    adminTestResponseHandler.start();

    // the good case is tested through the postGetHeadDeleteTest() (result non-null, exception null)
    // Both arguments null
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    HeadForGetCallback callback =
        new HeadForGetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler,
            router, 0);
    callback.onCompletion(null, null);
    // there should be an exception
    assertEquals("Both arguments null should have thrown exception", IllegalStateException.class,
        adminTestResponseHandler.getException().getClass());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is not null.
    adminTestResponseHandler.reset();
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback =
        new HeadForGetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler,
            router, 0);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    adminTestResponseHandler.reset();
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback =
        new HeadForGetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler,
            router, 0);
    callback.onCompletion(null, new RouterException(exceptionMsg, RouterErrorCode.UnexpectedInternalError));
    assertEquals("RouterException not converted to RestServiceException", RestServiceException.class,
        adminTestResponseHandler.getException().getClass());
    if (!adminTestResponseHandler.getException().getMessage().contains(exceptionMsg)) {
      fail("Exception msg [" + adminTestResponseHandler.getException().getMessage()
          + "] does contain expected substring [" + exceptionMsg + "]");
    }
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Callback encounters a processing error (induced here via a bad RestRequest).
    restRequest = new BadRestRequest();
    // there is an exception already.
    restResponseChannel = new MockRestResponseChannel();
    callback =
        new HeadForGetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler,
            router, 0);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());

    // there is no exception and the exception thrown in the callback is the primary exception.
    restResponseChannel = new MockRestResponseChannel();
    callback =
        new HeadForGetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler,
            router, 0);
    BlobInfo blobInfo = new BlobInfo(null, null);
    callback.onCompletion(blobInfo, null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
  }

  /**
   * Tests for non common case scenarios for {@link GetCallback}.
   * @throws Exception
   */
  @Test
  public void getCallbackTest()
      throws Exception {
    String exceptionMsg = new String(getRandomBytes(10));
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    adminTestResponseHandler.start();

    // the good case is tested through the postGetHeadDeleteTest() (result non-null, exception null)
    // Both arguments null
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    GetCallback callback =
        new GetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, null);
    // there should be an exception
    assertEquals("Both arguments null should have thrown exception", IllegalStateException.class,
        adminTestResponseHandler.getException().getClass());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is not null.
    adminTestResponseHandler.reset();
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new GetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    adminTestResponseHandler.reset();
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new GetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RouterException(exceptionMsg, RouterErrorCode.UnexpectedInternalError));
    assertEquals("RouterException not converted to RestServiceException", RestServiceException.class,
        adminTestResponseHandler.getException().getClass());
    if (!adminTestResponseHandler.getException().getMessage().contains(exceptionMsg)) {
      fail("Exception msg [" + adminTestResponseHandler.getException().getMessage()
          + "] does contain expected substring [" + exceptionMsg + "]");
    }
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Callback encounters a processing error (induced here via a bad RestRequest).
    restRequest = new BadRestRequest();
    // there is an exception already.
    restResponseChannel = new MockRestResponseChannel();
    callback = new GetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());

    // there is no exception and exception thrown in the callback.
    restResponseChannel = new MockRestResponseChannel();
    callback = new GetCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    ReadableStreamChannel response = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    assertTrue("Response channel is not open", response.isOpen());
    callback.onCompletion(response, null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
  }

  /**
   * Tests for non common case scenarios for {@link PostCallback}.
   * @throws Exception
   */
  @Test
  public void postCallbackTest()
      throws Exception {
    BlobProperties blobProperties = new BlobProperties(0, "test-serviceId");
    String exceptionMsg = new String(getRandomBytes(10));
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    adminTestResponseHandler.start();

    // the good case is tested through the postGetHeadDeleteTest() (result non-null, exception null)
    // Both arguments null
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    PostCallback callback =
        new PostCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler,
            blobProperties);
    callback.onCompletion(null, null);
    // there should be an exception
    assertEquals("Both arguments null should have thrown exception", IllegalStateException.class,
        adminTestResponseHandler.getException().getClass());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is not null.
    adminTestResponseHandler.reset();
    restRequest = createRestRequest(RestMethod.POST, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new PostCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler,
        blobProperties);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    adminTestResponseHandler.reset();
    restRequest = createRestRequest(RestMethod.POST, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new PostCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler,
        blobProperties);
    callback.onCompletion(null, new RouterException(exceptionMsg, RouterErrorCode.UnexpectedInternalError));
    assertEquals("RouterException not converted to RestServiceException", RestServiceException.class,
        adminTestResponseHandler.getException().getClass());
    if (!adminTestResponseHandler.getException().getMessage().contains(exceptionMsg)) {
      fail("Exception msg [" + adminTestResponseHandler.getException().getMessage()
          + "] does contain expected substring [" + exceptionMsg + "]");
    }
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // There are no tests for callback processing failure here because there is no good way of inducing a failure
    // and checking that the behavior is alright in PostCallback.
  }

  /**
   * Tests for non common case scenarios for {@link DeleteCallback}.
   * @throws Exception
   */
  @Test
  public void deleteCallbackTest()
      throws Exception {
    String exceptionMsg = new String(getRandomBytes(10));
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    adminTestResponseHandler.start();

    // the good case is tested through the postGetHeadDeleteTest() (result null, exception null)
    // Exception is not null.
    RestRequest restRequest = createRestRequest(RestMethod.DELETE, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    DeleteCallback callback =
        new DeleteCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    adminTestResponseHandler.reset();
    restRequest = createRestRequest(RestMethod.DELETE, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new DeleteCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RouterException(exceptionMsg, RouterErrorCode.UnexpectedInternalError));
    assertEquals("RouterException not converted to RestServiceException", RestServiceException.class,
        adminTestResponseHandler.getException().getClass());
    if (!adminTestResponseHandler.getException().getMessage().contains(exceptionMsg)) {
      fail("Exception msg [" + adminTestResponseHandler.getException().getMessage()
          + "] does contain expected substring [" + exceptionMsg + "]");
    }
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Callback encounters a processing error (induced here via a bad RestRequest).
    restRequest = new BadRestRequest();
    // there is an exception already.
    restResponseChannel = new MockRestResponseChannel();
    callback = new DeleteCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());

    // there is no exception and exception thrown in the callback.
    restResponseChannel = new MockRestResponseChannel();
    callback = new DeleteCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
  }

  /**
   * Tests for non common case scenarios for {@link HeadCallback}.
   * @throws Exception
   */
  @Test
  public void headCallbackTest()
      throws Exception {
    String exceptionMsg = new String(getRandomBytes(10));
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    adminTestResponseHandler.start();

    // the good case is tested through the postGetHeadDeleteTest() (result non-null, exception null)
    // Both arguments null
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    HeadCallback callback =
        new HeadCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, null);
    // there should be an exception
    assertEquals("Both arguments null should have thrown exception", IllegalStateException.class,
        adminTestResponseHandler.getException().getClass());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is not null.
    adminTestResponseHandler.reset();
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    adminTestResponseHandler.reset();
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RouterException(exceptionMsg, RouterErrorCode.UnexpectedInternalError));
    assertEquals("RouterException not converted to RestServiceException", RestServiceException.class,
        adminTestResponseHandler.getException().getClass());
    if (!adminTestResponseHandler.getException().getMessage().contains(exceptionMsg)) {
      fail("Exception msg [" + adminTestResponseHandler.getException().getMessage()
          + "] does contain expected substring [" + exceptionMsg + "]");
    }
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Callback encounters a processing error (induced here via a bad RestRequest).
    restRequest = new BadRestRequest();
    // there is an exception already.
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());

    // there is no exception and exception thrown in the callback.
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadCallback(adminBlobStorageService, restRequest, restResponseChannel, adminTestResponseHandler);
    BlobInfo blobInfo = new BlobInfo(new BlobProperties(0, "test-serviceId"), new byte[0]);
    callback.onCompletion(blobInfo, null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
  }

  // helpers
  // general

  /**
   * Method to easily create {@link RestRequest} objects containing a specific request.
   * @param restMethod the {@link RestMethod} desired.
   * @param uri string representation of the desired URI.
   * @param headers any associated headers as a {@link JSONObject}.
   * @param contents the content that accompanies the request.
   * @return A {@link RestRequest} object that defines the request required by the input.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  private RestRequest createRestRequest(RestMethod restMethod, String uri, JSONObject headers,
      List<ByteBuffer> contents)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod);
    request.put(MockRestRequest.URI_KEY, uri);
    if (headers != null) {
      request.put(MockRestRequest.HEADERS_KEY, headers);
    }
    return new MockRestRequest(request, contents);
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
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param headers the {@link JSONObject} where the headers should be set.
   * @param contentLength sets the {@link RestConstants.Headers#Blob_Size} header. Required.
   * @param ttlInSecs sets the {@link RestConstants.Headers#TTL} header. Set to {@link Utils#Infinite_Time} if no
   *                  expiry.
   * @param isPrivate sets the {@link RestConstants.Headers#Private} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestConstants.Headers#Service_Id} header. Required.
   * @param contentType sets the {@link RestConstants.Headers#Content_Type} header. Required and has to be a valid MIME
   *                    type.
   * @param ownerId sets the {@link RestConstants.Headers#Owner_Id} header. Optional - if not required, send null.
   * @throws IllegalArgumentException if any of {@code headers}, {@code serviceId}, {@code contentType} is null or if
   *                                  {@code contentLength} < 0 or if {@code ttlInSecs} < -1.
   * @throws JSONException
   */
  private void setAmbryHeaders(JSONObject headers, long contentLength, long ttlInSecs, boolean isPrivate,
      String serviceId, String contentType, String ownerId)
      throws JSONException {
    if (headers != null && contentLength >= 0 && ttlInSecs >= -1 && serviceId != null && contentType != null) {
      headers.put(RestConstants.Headers.Blob_Size, contentLength);
      headers.put(RestConstants.Headers.TTL, ttlInSecs);
      headers.put(RestConstants.Headers.Private, isPrivate);
      headers.put(RestConstants.Headers.Service_Id, serviceId);
      headers.put(RestConstants.Headers.Content_Type, contentType);
      if (ownerId != null) {
        headers.put(RestConstants.Headers.Owner_Id, ownerId);
      }
    } else {
      throw new IllegalArgumentException("Some required arguments are null. Cannot set ambry headers");
    }
  }

  /**
   * Reads the response received from the {@link AdminBlobStorageService} and decodes it into a {@link JSONObject}.
   * @param channel the {@link ReadableStreamChannel} that was received from the {@link AdminBlobStorageService}.
   * @return the response decoded into a {@link JSONObject}.
   * @throws IOException
   * @throws JSONException
   */
  private JSONObject getJsonizedResponseBody(ReadableStreamChannel channel)
      throws IOException, JSONException {
    ByteBuffer responseBuffer = ByteBuffer.allocate((int) channel.getSize());
    channel.read(new ByteBufferChannel(responseBuffer));
    return new JSONObject(new String(responseBuffer.array()));
  }

  /**
   * Does a {@link AdminBlobStorageService#handleGet(RestRequest, RestResponseChannel)} and returns
   * the result, if any. If an exception occurs during the operation, throws the exception.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @return the response as a {@link ReadableStreamChannel}.
   * @throws Exception
   */
  private ReadableStreamChannel doGet(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    AdminTestResponseHandler adminTestResponseHandler = adminResponseHandlerController.getHandler();
    adminTestResponseHandler.reset();
    adminBlobStorageService.handleGet(restRequest, restResponseChannel);
    if (adminTestResponseHandler.awaitResponseSubmission(1, TimeUnit.SECONDS)) {
      if (adminTestResponseHandler.getException() != null) {
        throw adminTestResponseHandler.getException();
      }
    } else {
      throw new IllegalStateException("handleGet() timed out");
    }
    return adminTestResponseHandler.getResponse();
  }

  /**
   * Does a {@link AdminBlobStorageService#handlePost(RestRequest, RestResponseChannel)}. If an exception occurs during
   * the operation, throws the exception.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @throws Exception
   */
  private void doPost(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    AdminTestResponseHandler adminTestResponseHandler = adminResponseHandlerController.getHandler();
    adminTestResponseHandler.reset();
    adminBlobStorageService.handlePost(restRequest, restResponseChannel);
    if (adminTestResponseHandler.awaitResponseSubmission(1, TimeUnit.SECONDS)) {
      if (adminTestResponseHandler.getException() != null) {
        throw adminTestResponseHandler.getException();
      }
    } else {
      throw new IllegalStateException("handlePost() timed out");
    }
  }

  /**
   * Does a {@link AdminBlobStorageService#handleDelete(RestRequest, RestResponseChannel)}. If an exception occurs
   * during the operation, throws the exception.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @throws Exception
   */
  private void doDelete(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    AdminTestResponseHandler adminTestResponseHandler = adminResponseHandlerController.getHandler();
    adminTestResponseHandler.reset();
    adminBlobStorageService.handleDelete(restRequest, restResponseChannel);
    if (adminTestResponseHandler.awaitResponseSubmission(1, TimeUnit.SECONDS)) {
      if (adminTestResponseHandler.getException() != null) {
        throw adminTestResponseHandler.getException();
      }
    } else {
      throw new IllegalStateException("handleDelete() timed out");
    }
  }

  /**
   * Does a {@link AdminBlobStorageService#handleHead(RestRequest, RestResponseChannel)}. If an exception occurs during
   * the operation, throws the exception.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @throws Exception
   */
  private void doHead(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    AdminTestResponseHandler adminTestResponseHandler = adminResponseHandlerController.getHandler();
    adminTestResponseHandler.reset();
    adminBlobStorageService.handleHead(restRequest, restResponseChannel);
    if (adminTestResponseHandler.awaitResponseSubmission(1, TimeUnit.SECONDS)) {
      if (adminTestResponseHandler.getException() != null) {
        throw adminTestResponseHandler.getException();
      }
    } else {
      throw new IllegalStateException("handleHead() timed out");
    }
  }

  // Constructor helpers

  /**
   * Sets up and gets an instance of {@link AdminBlobStorageService}.
   * @return an instance of {@link AdminBlobStorageService}.
   */
  private AdminBlobStorageService getAdminBlobStorageService() {
    // dud properties. pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    AdminConfig adminConfig = new AdminConfig(verifiableProperties);
    AdminMetrics adminMetrics = new AdminMetrics(new MetricRegistry());
    return new AdminBlobStorageService(adminConfig, adminMetrics, CLUSTER_MAP, adminResponseHandlerController, router);
  }

  // nullInputsForFunctionsTest() helpers

  /**
   * Checks for reaction to null input in {@code methodName} in {@link AdminBlobStorageService}.
   * @param methodName the name of the method to invoke.
   * @throws Exception
   */
  private void doNullInputsForFunctionsTest(String methodName)
      throws Exception {
    Method method =
        AdminBlobStorageService.class.getDeclaredMethod(methodName, RestRequest.class, RestResponseChannel.class);
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();

    adminResponseHandlerController.getHandler().reset();
    try {
      method.invoke(adminBlobStorageService, null, restResponseChannel);
      fail("Method [" + methodName + "] should have failed because RestRequest is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }

    adminResponseHandlerController.getHandler().reset();
    try {
      method.invoke(adminBlobStorageService, restRequest, null);
      fail("Method [" + methodName + "] should have failed because RestResponseChannel is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }
  }

  // runtimeExceptionRouterTest() helpers

  /**
   * Tests reactions of various methods of {@link AdminBlobStorageService} to a {@link Router} that throws
   * {@link RuntimeException}.
   * @param restMethod used to determine the method to invoke in {@link AdminBlobStorageService}.
   * @throws Exception
   */
  private void doRuntimeExceptionRouterTest(RestMethod restMethod)
      throws Exception {
    RestRequest restRequest = createRestRequest(restMethod, "/", null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      switch (restMethod) {
        case GET:
          doGet(restRequest, restResponseChannel);
          fail("GET should have detected a RestServiceException because of a bad router");
          break;
        case POST:
          JSONObject headers = new JSONObject();
          setAmbryHeaders(headers, 0, Utils.Infinite_Time, false, "test-serviceID", "text/plain", "test-ownerId");
          restRequest = createRestRequest(restMethod, "/", headers, null);
          doPost(restRequest, restResponseChannel);
          fail("POST should have detected a RestServiceException because of a bad router");
          break;
        case DELETE:
          doDelete(restRequest, restResponseChannel);
          fail("DELETE should have detected a RestServiceException because of a bad router");
          break;
        case HEAD:
          doHead(restRequest, restResponseChannel);
          fail("HEAD should have detected a RestServiceException because of a bad router");
          break;
        default:
          throw new IllegalArgumentException("Unrecognized RestMethod: " + restMethod);
      }
    } catch (RuntimeException e) {
      assertEquals("Unexpected error message", InMemoryRouter.OPERATION_THROW_EARLY_RUNTIME_EXCEPTION, e.getMessage());
    }
  }

  // badControllerTest() helpers.

  /**
   * Tests reactions of various methods of {@link AdminBlobStorageService} to a {@link RequestResponseHandlerController}
   * that throws {@link RuntimeException}.
   * <p/>
   * In reality, nothing can be done if this happens - the call will fail with an exception.
   * @param methodName the name of the method to invoke.
   * @throws Exception
   */
  private void doBadControllerTest(String methodName)
      throws Exception {
    Method method =
        AdminBlobStorageService.class.getDeclaredMethod(methodName, RestRequest.class, RestResponseChannel.class);
    RestRequest restRequest = createEchoGetRestRequest("echoText");
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();

    try {
      adminResponseHandlerController.exceptionToThrow = new RuntimeException();
      try {
        method.invoke(adminBlobStorageService, restRequest, restResponseChannel);
        fail("Method [" + methodName + "] should have failed because getHandler() would have thrown RuntimeException");
      } catch (InvocationTargetException e) {
        assertEquals("Unexpected exception class", RuntimeException.class, e.getTargetException().getClass());
      }
    } finally {
      adminResponseHandlerController.exceptionToThrow = null;
    }
  }

  // postGetHeadDeleteTest() helpers

  /**
   * Posts a blob with the given {@code headers} and {@code content}.
   * @param headers the headers of the new blob that get converted to blob properties.
   * @param content the content of the blob.
   * @return the blob ID of the blob.
   * @throws Exception
   */
  public String postBlobAndVerify(JSONObject headers, ByteBuffer content)
      throws Exception {
    List<ByteBuffer> contents = new LinkedList<ByteBuffer>();
    contents.add(content);
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers, contents);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doPost(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Created,
        restResponseChannel.getResponseStatus(MockRestResponseChannel.DataStatus.Flushed));
    assertTrue("No Date header",
        restResponseChannel.getHeader("Date", MockRestResponseChannel.DataStatus.Flushed) != null);
    assertTrue("No " + RestConstants.Headers.Creation_Time,
        restResponseChannel.getHeader(RestConstants.Headers.Creation_Time, MockRestResponseChannel.DataStatus.Flushed)
            != null);
    assertEquals("Content-Length is not 0", "0", restResponseChannel
        .getHeader(MockRestResponseChannel.CONTENT_LENGTH_HEADER_KEY, MockRestResponseChannel.DataStatus.Flushed));
    String blobId = restResponseChannel
        .getHeader(MockRestResponseChannel.LOCATION_HEADER_KEY, MockRestResponseChannel.DataStatus.Flushed);
    if (blobId == null) {
      fail("postBlobAndVerify did not return a blob ID");
    }
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @throws Exception
   */
  public void getBlobAndVerify(String blobId, JSONObject expectedHeaders, ByteBuffer expectedContent)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, blobId, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    ReadableStreamChannel response = doGet(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok,
        restResponseChannel.getResponseStatus(MockRestResponseChannel.DataStatus.Flushed));
    checkCommonGetHeadHeaders(restResponseChannel, expectedHeaders);
    ByteBuffer channelBuffer = ByteBuffer.allocate((int) response.getSize());
    WritableByteChannel channel = new ByteBufferChannel(channelBuffer);
    response.read(channel);
    assertArrayEquals("GET content does not match original content", expectedContent.array(), channelBuffer.array());
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getHeadAndVerify(String blobId, JSONObject expectedHeaders)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.HEAD, blobId, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doHead(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok,
        restResponseChannel.getResponseStatus(MockRestResponseChannel.DataStatus.Flushed));
    checkCommonGetHeadHeaders(restResponseChannel, expectedHeaders);
    assertEquals("Content-Length does not match blob size", expectedHeaders.getString(RestConstants.Headers.Blob_Size),
        restResponseChannel.getHeader("Content-Length", MockRestResponseChannel.DataStatus.Flushed));
    assertEquals(RestConstants.Headers.Service_Id + " does not match",
        expectedHeaders.getString(RestConstants.Headers.Service_Id),
        restResponseChannel.getHeader(RestConstants.Headers.Service_Id, MockRestResponseChannel.DataStatus.Flushed));
    assertEquals(RestConstants.Headers.Private + " does not match",
        expectedHeaders.getString(RestConstants.Headers.Private),
        restResponseChannel.getHeader(RestConstants.Headers.Private, MockRestResponseChannel.DataStatus.Flushed));
    assertEquals(RestConstants.Headers.Content_Type + " does not match",
        expectedHeaders.getString(RestConstants.Headers.Content_Type),
        restResponseChannel.getHeader(RestConstants.Headers.Content_Type, MockRestResponseChannel.DataStatus.Flushed));
    assertTrue(RestConstants.Headers.Creation_Time + " header missing",
        restResponseChannel.getHeader(RestConstants.Headers.Creation_Time, MockRestResponseChannel.DataStatus.Flushed)
            != null);
    if (expectedHeaders.getLong(RestConstants.Headers.TTL) != Utils.Infinite_Time) {
      assertEquals(RestConstants.Headers.TTL + " does not match", expectedHeaders.getString(RestConstants.Headers.TTL),
          restResponseChannel.getHeader(RestConstants.Headers.TTL, MockRestResponseChannel.DataStatus.Flushed));
    }
    if (expectedHeaders.has(RestConstants.Headers.Owner_Id)) {
      assertEquals(RestConstants.Headers.Owner_Id + " does not match",
          expectedHeaders.getString(RestConstants.Headers.Owner_Id),
          restResponseChannel.getHeader(RestConstants.Headers.Owner_Id, MockRestResponseChannel.DataStatus.Flushed));
    }
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws Exception
   */
  private void deleteBlobAndVerify(String blobId)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.DELETE, blobId, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doDelete(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Accepted,
        restResponseChannel.getResponseStatus(MockRestResponseChannel.DataStatus.Flushed));
    assertTrue("No Date header",
        restResponseChannel.getHeader("Date", MockRestResponseChannel.DataStatus.Flushed) != null);
    assertEquals("Content-Length is not 0", "0", restResponseChannel
        .getHeader(MockRestResponseChannel.CONTENT_LENGTH_HEADER_KEY, MockRestResponseChannel.DataStatus.Flushed));
  }

  /**
   * Checks headers that are common to HEAD and GET.
   * @param restResponseChannel the {@link RestResponseChannel} to check headers on.
   * @param expectedHeaders the expected headers.
   * @throws JSONException
   */
  private void checkCommonGetHeadHeaders(MockRestResponseChannel restResponseChannel, JSONObject expectedHeaders)
      throws JSONException {
    assertEquals("Content-Type does not match", expectedHeaders.getString(RestConstants.Headers.Content_Type),
        restResponseChannel.getHeader("Content-Type", MockRestResponseChannel.DataStatus.Flushed));
    assertTrue("No Date header",
        restResponseChannel.getHeader("Date", MockRestResponseChannel.DataStatus.Flushed) != null);
    assertTrue("No Last-Modified header",
        restResponseChannel.getHeader("Last-Modified", MockRestResponseChannel.DataStatus.Flushed) != null);
    assertEquals(RestConstants.Headers.Blob_Size + " does not match",
        expectedHeaders.getString(RestConstants.Headers.Blob_Size),
        restResponseChannel.getHeader(RestConstants.Headers.Blob_Size, MockRestResponseChannel.DataStatus.Flushed));
  }

  // echoTest() helpers

  /**
   * Creates a request that can be used to test {@link AdminOperationType#echo}.
   * @param echoText the text that needs to be echoed.
   * @return a {@link RestRequest} for {@link AdminOperationType#echo} with echo text as specified.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  private RestRequest createEchoGetRestRequest(String echoText)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    String uri = AdminOperationType.echo + "?" + EchoHandler.TEXT_KEY + "=" + echoText;
    return createRestRequest(RestMethod.GET, uri, null, null);
  }

  // handleGetReplicasForBlobIdWithBadInputTest() helpers

  /**
   * Creates a request that can be used to test {@link AdminOperationType#getReplicasForBlobId}.
   * @param blobId the blob ID to include in the request.
   * @return a {@link RestRequest} for {@link AdminOperationType#getReplicasForBlobId} with blob ID as specified.
   * @throws JSONException
   * @throws URISyntaxException
   * @throws UnsupportedEncodingException
   */
  private RestRequest createGetReplicasForBlobIdRestRequest(String blobId)
      throws JSONException, URISyntaxException, UnsupportedEncodingException {
    String uri = AdminOperationType.getReplicasForBlobId + "?" + GetReplicasForBlobIdHandler.BLOB_ID_KEY + "=" + blobId;
    return createRestRequest(RestMethod.GET, uri, null, null);
  }
}

/**
 * A controller for AdminTestResponseHandler that just returns the same started instance of
 * {@link AdminTestResponseHandler} on any call to {@link #getHandler()}.
 */
class AdminResponseHandlerController extends RequestResponseHandlerController {
  public RuntimeException exceptionToThrow = null;

  private final AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();

  public AdminResponseHandlerController()
      throws InstantiationException {
    super(1, new RestServerMetrics(new MetricRegistry()));
  }

  @Override
  public void start()
      throws InstantiationException {
    adminTestResponseHandler.start();
  }

  @Override
  public void shutdown() {
    adminTestResponseHandler.shutdown();
    // just in case
    super.shutdown();
  }

  @Override
  public AdminTestResponseHandler getHandler() {
    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
    return adminTestResponseHandler;
  }
}

/**
 * An extension of {@link AsyncRequestResponseHandler} that stores a submitted response/exception and signals the fact
 * that the response has been submitted. A single instance can handle only a single response at a time. To reuse, call
 * {@link #reset()}.
 */
class AdminTestResponseHandler extends AsyncRequestResponseHandler {
  private final CountDownLatch responseSubmitted = new CountDownLatch(1);
  private volatile ReadableStreamChannel response = null;
  private volatile Exception exception = null;
  private volatile boolean serviceRunning = false;

  public AdminTestResponseHandler() {
    // not starting at super, so it's ok.
    super(new RestServerMetrics(new MetricRegistry()));
  }

  @Override
  public void start() {
    serviceRunning = true;
  }

  @Override
  public void shutdown() {
    serviceRunning = false;
    // just in case.
    super.shutdown();
  }

  @Override
  public void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception)
      throws RestServiceException {
    if (serviceRunning) {
      this.response = response;
      this.exception = exception;
      restResponseChannel.onResponseComplete(exception);
      responseSubmitted.countDown();
    } else {
      throw new RestServiceException("Response handler inactive", RestServiceErrorCode.RequestResponseQueueingFailure);
    }
  }

  /**
   * Wait for response to be submitted.
   * @param timeout the length of time to wait for.
   * @param timeUnit the time unit of {@code timeout}.
   * @return {@code true} if response was submitted within {@code timeout}. {@code false} otherwise.
   * @throws InterruptedException
   */
  public boolean awaitResponseSubmission(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return responseSubmitted.await(timeout, timeUnit);
  }

  /**
   * Gets the exception that was submitted, if any. Returns null if queried before response is submitted.
   * @return exception that that was submitted, if any.
   */
  public Exception getException() {
    return exception;
  }

  /**
   * Gets the response that was submitted, if any. Returns null if queried before response is submitted.
   * @return response that that was submitted as a {@link ReadableStreamChannel}.
   */
  public ReadableStreamChannel getResponse() {
    return response;
  }

  /**
   * Resets state so that this instance can be reused.
   */
  public void reset() {
    response = null;
    exception = null;
  }
}

/**
 * A bad implementation of {@link RestRequest}. Just throws exceptions.
 */
class BadRestRequest implements RestRequest {

  @Override
  public RestMethod getRestMethod() {
    return null;
  }

  @Override
  public String getPath() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public String getUri() {
    return null;
  }

  @Override
  public Map<String, List<String>> getArgs() {
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

  @Override
  public RestRequestMetrics getMetrics() {
    return new RestRequestMetrics();
  }

  @Override
  public long getSize() {
    return -1;
  }

  @Override
  public int read(WritableByteChannel channel)
      throws IOException {
    throw new IOException("Not implemented");
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
  public int read(WritableByteChannel channel)
      throws IOException {
    throw new IOException("Not implemented");
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
