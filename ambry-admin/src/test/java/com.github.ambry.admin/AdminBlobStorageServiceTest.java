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
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestConstants;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestResponseHandler;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link AdminBlobStorageService}.
 */
public class AdminBlobStorageServiceTest {

  private static ClusterMap clusterMap;
  private static InMemoryRouter router;
  private static AdminBlobStorageService adminBlobStorageService;

  /**
   * Sets up the {@link AdminBlobStorageService} instance before all tests. Most tests use the instance started here.
   * @throws InstantiationException
   * @throws IOException
   */
  @BeforeClass
  public static void startAdminBlobStorageService()
      throws InstantiationException, IOException {
    clusterMap = new MockClusterMap();
    router = new InMemoryRouter(new VerifiableProperties(new Properties()));
    adminBlobStorageService = getAdminBlobStorageService(clusterMap, router);
    adminBlobStorageService.start();
  }

  /**
   * Shuts down the {@link AdminBlobStorageService} instance after all tests.
   * @throws IOException
   */
  @AfterClass
  public static void shutdownAdminBlobStorageService()
      throws IOException {
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
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService(clusterMap, router);
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
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService(clusterMap, router);
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
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService(clusterMap, router);
    // fine to use without start.
    doEchoTest(adminBlobStorageService);
  }

  /**
   * Checks for reactions of all methods in {@link AdminBlobStorageService} to null arguments.
   * @throws Exception
   */
  @Test
  public void nullInputsForFunctionsTest()
      throws Exception {
    doNullInputsForFunctionsTest(adminBlobStorageService, "handleGet");
    doNullInputsForFunctionsTest(adminBlobStorageService, "handlePost");
    doNullInputsForFunctionsTest(adminBlobStorageService, "handleDelete");
    doNullInputsForFunctionsTest(adminBlobStorageService, "handleHead");
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

    doRuntimeExceptionRouterTest(adminBlobStorageService, RestMethod.GET);
    doRuntimeExceptionRouterTest(adminBlobStorageService, RestMethod.POST);
    doRuntimeExceptionRouterTest(adminBlobStorageService, RestMethod.DELETE);
    doRuntimeExceptionRouterTest(adminBlobStorageService, RestMethod.HEAD);

    // make InMemoryRouter OK.
    router.setVerifiableProperties(new VerifiableProperties(new Properties()));
  }

  /**
   * Checks reactions of all methods in {@link AdminBlobStorageService} to bad {@link RestResponseHandler} and
   * {@link RestRequest} implementations.
   * @throws Exception
   */
  @Test
  public void badResponseHandlerAndRestRequestTest()
      throws Exception {
    RestRequest restRequest = new BadRestRequest();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();

    // What happens inside AdminBlobStorageService during this test?
    // 1. Since the RestRequest throws errors, AdminBlobStorageService will attempt to submit response with exception
    //      to AdminTestResponseHandler.
    // 2. The submission will fail because AdminTestResponseHandler has not been started.
    // 3. AdminBlobStorageService will directly complete the request over the RestResponseChannel with the *original*
    //      exception.
    // 4. It will then try to release resources but closing the RestRequest will also throw an exception. This exception
    //      is swallowed.
    // What the test is looking for -> No exceptions thrown when the handleX is run and the original exception arrives
    // safely.

    adminBlobStorageService.handleGet(restRequest, restResponseChannel, adminTestResponseHandler);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getCause().getClass());

    restResponseChannel = new MockRestResponseChannel();
    adminBlobStorageService.handlePost(restRequest, restResponseChannel, adminTestResponseHandler);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getCause().getClass());

    restResponseChannel = new MockRestResponseChannel();
    adminBlobStorageService.handleDelete(restRequest, restResponseChannel, adminTestResponseHandler);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getCause().getClass());

    restResponseChannel = new MockRestResponseChannel();
    adminBlobStorageService.handleHead(restRequest, restResponseChannel, adminTestResponseHandler);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getCause().getClass());
  }

  /**
   * Tests {@link AdminBlobStorageService#releaseResources(RestRequest, ReadableStreamChannel)}.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void releaseResourcesTest()
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    assertTrue("RestRequest channel not open", restRequest.isOpen());
    assertTrue("ReadableStreamChannel not open", channel.isOpen());
    AdminBlobStorageService.releaseResources(restRequest, channel);
    assertFalse("RestRequest channel is still open", restRequest.isOpen());
    assertFalse("ReadableStreamChannel is still open", channel.isOpen());

    // null ReadableStreamChannel
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel not open", restRequest.isOpen());
    AdminBlobStorageService.releaseResources(restRequest, null);
    assertFalse("RestRequest channel is still open", restRequest.isOpen());

    // bad RestRequest (close() throws IOException)
    channel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    assertTrue("ReadableStreamChannel not open", channel.isOpen());
    AdminBlobStorageService.releaseResources(new BadRestRequest(), channel);
    assertFalse("ReadableStreamChannel is still open", channel.isOpen());

    // bad ReadableStreamChannel (close() throws IOException)
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel not open", restRequest.isOpen());
    AdminBlobStorageService.releaseResources(restRequest, new BadRSC());
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

    String blobId = postBlobAndVerify(adminBlobStorageService, headers, content);
    getBlobAndVerify(adminBlobStorageService, blobId, headers, content);
    getHeadAndVerify(adminBlobStorageService, blobId, headers);
    deleteBlobAndVerify(adminBlobStorageService, blobId);
  }

  /**
   * Tests the {@link AdminOperationType#echo} admin operation. Checks to see that the echo matches input text.
   * @throws Exception
   */
  @Test
  public void echoTest()
      throws Exception {
    doEchoTest(adminBlobStorageService);
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
      doGet(adminBlobStorageService, restRequest, restResponseChannel);
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
    List<PartitionId> partitionIds = clusterMap.getWritablePartitionIds();
    for (PartitionId partitionId : partitionIds) {
      String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
      BlobId blobId = new BlobId(partitionId);
      RestRequest restRequest = createGetReplicasForBlobIdRestRequest(blobId.getID());
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = doGet(adminBlobStorageService, restRequest, restResponseChannel);
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
      doGet(adminBlobStorageService, restRequest, restResponseChannel);
      fail("Exception should have been thrown because some required parameters were missing");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.MissingArgs, e.getErrorCode());
    }

    try {
      // bad input - invalid blob id.
      RestRequest restRequest = createGetReplicasForBlobIdRestRequest("12345");
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doGet(adminBlobStorageService, restRequest, restResponseChannel);
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.NotFound, e.getErrorCode());
    }

    try {
      // bad input - invalid blob id for this cluster map.
      String blobId = "AAEAAQAAAAAAAADFAAAAJDMyYWZiOTJmLTBkNDYtNDQyNS1iYzU0LWEwMWQ1Yzg3OTJkZQ.gif";
      RestRequest restRequest = createGetReplicasForBlobIdRestRequest(blobId);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doGet(adminBlobStorageService, restRequest, restResponseChannel);
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
      doGet(adminBlobStorageService, restRequest, restResponseChannel);
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
      doGet(adminBlobStorageService, restRequest, restResponseChannel);
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
        new HeadForGetCallback(restRequest, restResponseChannel, adminTestResponseHandler, router);
    callback.onCompletion(null, null);
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is not null.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadForGetCallback(restRequest, restResponseChannel, adminTestResponseHandler, router);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadForGetCallback(restRequest, restResponseChannel, adminTestResponseHandler, router);
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

    // handleResponse of AdminTestResponseHandler throws exception
    // there is an exception already.
    adminTestResponseHandler.shutdown();
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadForGetCallback(restRequest, restResponseChannel, adminTestResponseHandler, router);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());

    // there is no exception and exception thrown in the callback.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadForGetCallback(restRequest, restResponseChannel, adminTestResponseHandler, router);
    BlobInfo blobInfo = new BlobInfo(null, null);
    callback.onCompletion(blobInfo, null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());
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
    GetCallback callback = new GetCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, null);
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is not null.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new GetCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new GetCallback(restRequest, restResponseChannel, adminTestResponseHandler);
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

    // handleResponse of AdminTestResponseHandler throws exception
    // there is an exception already.
    adminTestResponseHandler.shutdown();
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new GetCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());

    // there is no exception and exception thrown in the callback.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new GetCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    ReadableStreamChannel response = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    assertTrue("Response channel is not open", response.isOpen());
    callback.onCompletion(response, null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());
    assertFalse("Response channel is not cleaned up", response.isOpen());
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
        new PostCallback(restRequest, restResponseChannel, adminTestResponseHandler, blobProperties);
    callback.onCompletion(null, null);
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is not null.
    restRequest = createRestRequest(RestMethod.POST, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new PostCallback(restRequest, restResponseChannel, adminTestResponseHandler, blobProperties);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    restRequest = createRestRequest(RestMethod.POST, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new PostCallback(restRequest, restResponseChannel, adminTestResponseHandler, blobProperties);
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

    // handleResponse of AdminTestResponseHandler throws exception
    // there is an exception already.
    adminTestResponseHandler.shutdown();
    restRequest = createRestRequest(RestMethod.POST, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new PostCallback(restRequest, restResponseChannel, adminTestResponseHandler, blobProperties);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());

    // there is no exception and exception thrown in the callback.
    restRequest = createRestRequest(RestMethod.POST, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new PostCallback(restRequest, restResponseChannel, adminTestResponseHandler, blobProperties);
    BlobInfo blobInfo = new BlobInfo(new BlobProperties(0, "test-serviceId"), new byte[0]);
    callback.onCompletion("BlobId", null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());
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
    DeleteCallback callback = new DeleteCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    restRequest = createRestRequest(RestMethod.DELETE, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new DeleteCallback(restRequest, restResponseChannel, adminTestResponseHandler);
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

    // handleResponse of AdminTestResponseHandler throws exception
    // there is an exception already.
    adminTestResponseHandler.shutdown();
    restRequest = createRestRequest(RestMethod.DELETE, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new DeleteCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());

    // there is no exception and exception thrown in the callback.
    restRequest = createRestRequest(RestMethod.DELETE, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new DeleteCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    BlobInfo blobInfo = new BlobInfo(new BlobProperties(0, "test-serviceId"), new byte[0]);
    callback.onCompletion(null, null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());
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
    HeadCallback callback = new HeadCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, null);
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is not null.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, adminTestResponseHandler.getException().getMessage());
    // Nothing should be closed.
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restRequest.close();

    // Exception is RouterException.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadCallback(restRequest, restResponseChannel, adminTestResponseHandler);
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

    // handleResponse of AdminTestResponseHandler throws exception
    // there is an exception already.
    adminTestResponseHandler.shutdown();
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    callback.onCompletion(null, new RuntimeException(exceptionMsg));
    assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getCause().getMessage());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());

    // there is no exception and exception thrown in the callback.
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    assertTrue("RestRequest channel is not open", restRequest.isOpen());
    restResponseChannel = new MockRestResponseChannel();
    callback = new HeadCallback(restRequest, restResponseChannel, adminTestResponseHandler);
    BlobInfo blobInfo = new BlobInfo(new BlobProperties(0, "test-serviceId"), new byte[0]);
    callback.onCompletion(blobInfo, null);
    assertNotNull("There is no cause of failure", restResponseChannel.getCause());
    // resources should have been cleaned up.
    assertFalse("RestRequest channel is not cleaned up", restRequest.isOpen());
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
   * Does the {@link AdminOperationType#echo} test by creating a {@link RestRequest} specifying echo, sends it to
   * the {@link AdminBlobStorageService} instance and checks equality of response with input text.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @throws Exception
   */
  private void doEchoTest(AdminBlobStorageService adminBlobStorageService)
      throws Exception {
    String inputText = "textToBeEchoed";
    String uri = AdminOperationType.echo + "?" + EchoHandler.TEXT_KEY + "=" + inputText;
    RestRequest restRequest = createRestRequest(RestMethod.GET, uri, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    ReadableStreamChannel channel = doGet(adminBlobStorageService, restRequest, restResponseChannel);
    String echoedText = getJsonizedResponseBody(channel).getString(EchoHandler.TEXT_KEY);
    assertEquals("Unexpected Content-Type", "application/json",
        restResponseChannel.getHeader("Content-Type", MockRestResponseChannel.DataStatus.Flushed));
    assertEquals("Did not get expected response", inputText, echoedText);
  }

  /**
   * Does a {@link AdminBlobStorageService#handleGet(RestRequest, RestResponseChannel, RestResponseHandler)} and returns
   * the result, if any. If an exception occurs during the operation, throws the exception.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @return the response as a {@link ReadableStreamChannel}.
   * @throws Exception
   */
  private ReadableStreamChannel doGet(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel)
      throws Exception {
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    adminTestResponseHandler.start();
    adminBlobStorageService.handleGet(restRequest, restResponseChannel, adminTestResponseHandler);
    try {
      if (adminTestResponseHandler.awaitResponseSubmission(1, TimeUnit.SECONDS)) {
        if (adminTestResponseHandler.getException() != null) {
          throw adminTestResponseHandler.getException();
        }
      } else {
        throw new IllegalStateException("handleGet() timed out");
      }
    } finally {
      adminTestResponseHandler.shutdown();
    }
    return adminTestResponseHandler.getResponse();
  }

  /**
   * Does a {@link AdminBlobStorageService#handlePost(RestRequest, RestResponseChannel, RestResponseHandler)}. If an
   * exception occurs during the operation, throws the exception.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @throws Exception
   */
  private void doPost(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel)
      throws Exception {
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    adminTestResponseHandler.start();
    adminBlobStorageService.handlePost(restRequest, restResponseChannel, adminTestResponseHandler);
    try {
      if (adminTestResponseHandler.awaitResponseSubmission(1, TimeUnit.SECONDS)) {
        if (adminTestResponseHandler.getException() != null) {
          throw adminTestResponseHandler.getException();
        }
      } else {
        throw new IllegalStateException("handlePost() timed out");
      }
    } finally {
      adminTestResponseHandler.shutdown();
    }
  }

  /**
   * Does a {@link AdminBlobStorageService#handleDelete(RestRequest, RestResponseChannel, RestResponseHandler)}. If an
   * exception occurs during the operation, throws the exception.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @throws Exception
   */
  private void doDelete(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel)
      throws Exception {
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    adminTestResponseHandler.start();
    adminBlobStorageService.handleDelete(restRequest, restResponseChannel, adminTestResponseHandler);
    try {
      if (adminTestResponseHandler.awaitResponseSubmission(1, TimeUnit.SECONDS)) {
        if (adminTestResponseHandler.getException() != null) {
          throw adminTestResponseHandler.getException();
        }
      } else {
        throw new IllegalStateException("handleDelete() timed out");
      }
    } finally {
      adminTestResponseHandler.shutdown();
    }
  }

  /**
   * Does a {@link AdminBlobStorageService#handleHead(RestRequest, RestResponseChannel, RestResponseHandler)}. If an
   * exception occurs during the operation, throws the exception.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @throws Exception
   */
  private void doHead(AdminBlobStorageService adminBlobStorageService, RestRequest restRequest,
      RestResponseChannel restResponseChannel)
      throws Exception {
    AdminTestResponseHandler adminTestResponseHandler = new AdminTestResponseHandler();
    adminTestResponseHandler.start();
    adminBlobStorageService.handleHead(restRequest, restResponseChannel, adminTestResponseHandler);
    try {
      if (adminTestResponseHandler.awaitResponseSubmission(1, TimeUnit.SECONDS)) {
        if (adminTestResponseHandler.getException() != null) {
          throw adminTestResponseHandler.getException();
        }
      } else {
        throw new IllegalStateException("handleHead() timed out");
      }
    } finally {
      adminTestResponseHandler.shutdown();
    }
  }

  // BeforeClass helpers

  /**
   * Sets up and gets an instance of {@link AdminBlobStorageService}.
   * @param clusterMap the {@link ClusterMap} to use.
   * @param router the {@link Router} instance to use.
   * @return an instance of {@link AdminBlobStorageService}.
   */
  private static AdminBlobStorageService getAdminBlobStorageService(ClusterMap clusterMap, Router router) {
    // dud properties. pick up defaults
    Properties properties = new Properties();
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    AdminConfig adminConfig = new AdminConfig(verifiableProperties);
    AdminMetrics adminMetrics = new AdminMetrics(new MetricRegistry());
    return new AdminBlobStorageService(adminConfig, adminMetrics, clusterMap, router);
  }

  // nullInputsForFunctionsTest() helpers

  /**
   * Checks for reaction to null input in {@code methodName} in {@link AdminBlobStorageService}.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param methodName the name of the method to invoke.
   * @throws Exception
   */
  private void doNullInputsForFunctionsTest(AdminBlobStorageService adminBlobStorageService, String methodName)
      throws Exception {
    Method method = AdminBlobStorageService.class
        .getDeclaredMethod(methodName, RestRequest.class, RestResponseChannel.class, RestResponseHandler.class);
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    RestResponseHandler restResponseHandler = new AdminTestResponseHandler();

    try {
      method.invoke(adminBlobStorageService, null, restResponseChannel, restResponseHandler);
      fail("Method [" + methodName + "] should have failed because RestRequest is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }

    try {
      method.invoke(adminBlobStorageService, restRequest, null, restResponseHandler);
      fail("Method [" + methodName + "] should have failed because RestResponseChannel is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }

    try {
      method.invoke(adminBlobStorageService, restRequest, restResponseChannel, null);
      fail("Method [" + methodName + "] should have failed because RestResponseHandler is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }
  }

  // runtimeExceptionRouterTest() helpers

  /**
   * Tests reactions of various methods of {@link AdminBlobStorageService} to a {@link Router} that throws
   * {@link RuntimeException}.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use. It's router should throw
   *                                {@link RuntimeException} when the operation to deal with {@code restMethod} is
   *                                invoked.
   * @param restMethod used to determine the method to invoke in {@link AdminBlobStorageService}.
   * @throws Exception
   */
  private void doRuntimeExceptionRouterTest(AdminBlobStorageService adminBlobStorageService, RestMethod restMethod)
      throws Exception {
    RestRequest restRequest = createRestRequest(restMethod, "/", null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      switch (restMethod) {
        case GET:
          doGet(adminBlobStorageService, restRequest, restResponseChannel);
          fail("GET should have detected a RestServiceException because of a bad router");
          break;
        case POST:
          JSONObject headers = new JSONObject();
          setAmbryHeaders(headers, 0, Utils.Infinite_Time, false, "test-serviceID", "text/plain", "test-ownerId");
          restRequest = createRestRequest(restMethod, "/", headers, null);
          doPost(adminBlobStorageService, restRequest, restResponseChannel);
          fail("POST should have detected a RestServiceException because of a bad router");
          break;
        case DELETE:
          doDelete(adminBlobStorageService, restRequest, restResponseChannel);
          fail("DELETE should have detected a RestServiceException because of a bad router");
          break;
        case HEAD:
          doHead(adminBlobStorageService, restRequest, restResponseChannel);
          fail("HEAD should have detected a RestServiceException because of a bad router");
          break;
        default:
          throw new IllegalArgumentException("Unrecognized RestMethod: " + restMethod);
      }
    } catch (RuntimeException e) {
      assertEquals("Unexpected error message", InMemoryRouter.OPERATION_THROW_EARLY_RUNTIME_EXCEPTION, e.getMessage());
    }
  }

  // postGetHeadDeleteTest() helpers

  /**
   * Posts a blob with the given {@code headers} and {@code content}.
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param headers the headers of the new blob that get converted to blob properties.
   * @param content the content of the blob.
   * @return the blob ID of the blob.
   * @throws Exception
   */
  public String postBlobAndVerify(AdminBlobStorageService adminBlobStorageService, JSONObject headers,
      ByteBuffer content)
      throws Exception {
    List<ByteBuffer> contents = new LinkedList<ByteBuffer>();
    contents.add(content);
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers, contents);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doPost(adminBlobStorageService, restRequest, restResponseChannel);
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
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param blobId the blob ID of the blob to GET.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @throws Exception
   */
  public void getBlobAndVerify(AdminBlobStorageService adminBlobStorageService, String blobId,
      JSONObject expectedHeaders, ByteBuffer expectedContent)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, blobId, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    ReadableStreamChannel response = doGet(adminBlobStorageService, restRequest, restResponseChannel);
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
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param blobId the blob ID of the blob to HEAD.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getHeadAndVerify(AdminBlobStorageService adminBlobStorageService, String blobId,
      JSONObject expectedHeaders)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.HEAD, blobId, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doHead(adminBlobStorageService, restRequest, restResponseChannel);
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
   * @param adminBlobStorageService the {@link AdminBlobStorageService} instance to use.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws Exception
   */
  private void deleteBlobAndVerify(AdminBlobStorageService adminBlobStorageService, String blobId)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.DELETE, blobId, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doDelete(adminBlobStorageService, restRequest, restResponseChannel);
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
 * An implementation of {@link RestResponseHandler} that stores a submitted response/exception and signals the fact that
 * the response has been submitted. A single instance can handle only a single response.
 */
class AdminTestResponseHandler implements RestResponseHandler {
  private final CountDownLatch responseSubmitted = new CountDownLatch(1);
  private ReadableStreamChannel response = null;
  private Exception exception = null;
  private volatile boolean serviceRunning = false;

  @Override
  public void start()
      throws InstantiationException {
    serviceRunning = true;
  }

  @Override
  public void shutdown() {
    serviceRunning = false;
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

  @Override
  public boolean isRunning() {
    return serviceRunning;
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
}

/**
 * A bad implementation of {@link RestRequest}. Just throws exceptions.
 */
class BadRestRequest implements RestRequest {

  @Override
  public RestMethod getRestMethod() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public String getPath() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public String getUri() {
    throw new IllegalStateException("Not implemented");
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
  public long getSize() {
    throw new IllegalStateException("Not implemented");
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
    throw new IllegalStateException("Not implemented");
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
