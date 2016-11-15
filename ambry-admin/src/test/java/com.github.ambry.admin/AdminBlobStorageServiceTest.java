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
package com.github.ambry.admin;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.AdminConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.rest.IdConverter;
import com.github.ambry.rest.IdConverterFactory;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetricsTracker;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestResponseHandler;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.rest.SecurityServiceFactory;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
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

  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final AdminMetrics adminMetrics = new AdminMetrics(metricRegistry);
  private final AdminConfig adminConfig;
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final AdminTestResponseHandler responseHandler;
  private final InMemoryRouter router;

  private AdminBlobStorageService adminBlobStorageService;

  /**
   * Sets up the {@link AdminBlobStorageService} instance before a test.
   * @throws InstantiationException
   */
  public AdminBlobStorageServiceTest() throws InstantiationException {
    VerifiableProperties verifiableProperties = new VerifiableProperties(new Properties());
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    adminConfig = new AdminConfig(verifiableProperties);
    idConverterFactory = new AdminIdConverterFactory(verifiableProperties, metricRegistry);
    securityServiceFactory = new AdminSecurityServiceFactory(verifiableProperties, metricRegistry);
    router = new InMemoryRouter(verifiableProperties);
    responseHandler = new AdminTestResponseHandler();
    adminBlobStorageService = getAdminBlobStorageService();
    responseHandler.start();
    adminBlobStorageService.start();
  }

  /**
   * Shuts down the {@link AdminBlobStorageService} instance after all tests.
   * @throws IOException
   */
  @After
  public void shutdownAdminBlobStorageService() throws IOException {
    adminBlobStorageService.shutdown();
    responseHandler.shutdown();
    router.close();
  }

  /**
   * Tests basic startup and shutdown functionality (no exceptions).
   * @throws InstantiationException
   * @throws IOException
   */
  @Test
  public void startShutDownTest() throws InstantiationException, IOException {
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
  public void shutdownWithoutStartTest() throws IOException {
    AdminBlobStorageService adminBlobStorageService = getAdminBlobStorageService();
    adminBlobStorageService.shutdown();
  }

  /**
   * This tests for exceptions thrown when an {@link AdminBlobStorageService} instance is used without calling
   * {@link AdminBlobStorageService#start()} first.
   * @throws Exception
   */
  @Test
  public void useServiceWithoutStartTest() throws Exception {
    adminBlobStorageService = getAdminBlobStorageService();
    // not fine to use without start.
    try {
      doOperation(AdminTestUtils.createRestRequest(RestMethod.GET, "/", null, null), new MockRestResponseChannel());
      fail("Should not have been able to use AdminBlobStorageService without start");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.ServiceUnavailable, e.getErrorCode());
    }
  }

  /**
   * Checks for reactions of all methods in {@link AdminBlobStorageService} to null arguments.
   * @throws Exception
   */
  @Test
  public void nullInputsForFunctionsTest() throws Exception {
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
  public void runtimeExceptionRouterTest() throws Exception {
    // set InMemoryRouter up to throw RuntimeException
    Properties properties = new Properties();
    properties.setProperty(InMemoryRouter.OPERATION_THROW_EARLY_RUNTIME_EXCEPTION, "true");
    router.setVerifiableProperties(new VerifiableProperties(properties));

    doRuntimeExceptionRouterTest(RestMethod.GET);
    doRuntimeExceptionRouterTest(RestMethod.DELETE);
    doRuntimeExceptionRouterTest(RestMethod.HEAD);
  }

  /**
   * Checks reactions of all methods in {@link AdminBlobStorageService} to bad {@link RestResponseHandler} and
   * {@link RestRequest} implementations.
   * @throws Exception
   */
  @Test
  public void badResponseHandlerAndRestRequestTest() throws Exception {
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
    responseHandler.shutdown();

    adminBlobStorageService.handleGet(restRequest, restResponseChannel);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getException().getClass());

    responseHandler.reset();
    restResponseChannel = new MockRestResponseChannel();
    adminBlobStorageService.handleDelete(restRequest, restResponseChannel);
    // IllegalStateException or NullPointerException is thrown because of BadRestRequest.
    Exception e = restResponseChannel.getException();
    assertTrue("Unexpected exception", e instanceof IllegalStateException || e instanceof NullPointerException);

    responseHandler.reset();
    restResponseChannel = new MockRestResponseChannel();
    adminBlobStorageService.handleHead(restRequest, restResponseChannel);
    // IllegalStateException or NullPointerException is thrown because of BadRestRequest.
    e = restResponseChannel.getException();
    assertTrue("Unexpected exception", e instanceof IllegalStateException || e instanceof NullPointerException);
  }

  /**
   * Tests
   * {@link AdminBlobStorageService#submitResponse(RestRequest, RestResponseChannel, ReadableStreamChannel, Exception)}.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void submitResponseTest() throws JSONException, UnsupportedEncodingException, URISyntaxException {
    String exceptionMsg = UtilsTest.getRandomString(10);
    responseHandler.shutdown();
    // handleResponse of AdminTestResponseHandler throws exception because it has been shutdown.
    try {
      // there is an exception already.
      RestRequest restRequest = AdminTestUtils.createRestRequest(RestMethod.GET, "/", null, null);
      assertTrue("RestRequest channel is not open", restRequest.isOpen());
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      adminBlobStorageService.submitResponse(restRequest, restResponseChannel, null,
          new RuntimeException(exceptionMsg));
      assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getException().getMessage());

      // there is no exception and exception thrown when the response is submitted.
      restRequest = AdminTestUtils.createRestRequest(RestMethod.GET, "/", null, null);
      assertTrue("RestRequest channel is not open", restRequest.isOpen());
      restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel response = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
      assertTrue("Response channel is not open", response.isOpen());
      adminBlobStorageService.submitResponse(restRequest, restResponseChannel, response, null);
      assertNotNull("There is no cause of failure", restResponseChannel.getException());
      // resources should have been cleaned up.
      assertFalse("Response channel is not cleaned up", response.isOpen());
    } finally {
      responseHandler.start();
    }
  }

  /**
   * Tests releasing of resources if response submission fails.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void releaseResourcesTest() throws JSONException, UnsupportedEncodingException, URISyntaxException {
    responseHandler.shutdown();
    // handleResponse of AdminTestResponseHandler throws exception because it has been shutdown.
    try {
      RestRequest restRequest = AdminTestUtils.createRestRequest(RestMethod.GET, "/", null, null);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
      assertTrue("RestRequest channel not open", restRequest.isOpen());
      assertTrue("ReadableStreamChannel not open", channel.isOpen());
      adminBlobStorageService.submitResponse(restRequest, restResponseChannel, channel, null);
      assertFalse("ReadableStreamChannel is still open", channel.isOpen());

      // null ReadableStreamChannel
      restRequest = AdminTestUtils.createRestRequest(RestMethod.GET, "/", null, null);
      restResponseChannel = new MockRestResponseChannel();
      assertTrue("RestRequest channel not open", restRequest.isOpen());
      adminBlobStorageService.submitResponse(restRequest, restResponseChannel, null, null);

      // bad RestRequest (close() throws IOException)
      channel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
      restResponseChannel = new MockRestResponseChannel();
      assertTrue("ReadableStreamChannel not open", channel.isOpen());
      adminBlobStorageService.submitResponse(new BadRestRequest(), restResponseChannel, channel, null);

      // bad ReadableStreamChannel (close() throws IOException)
      restRequest = AdminTestUtils.createRestRequest(RestMethod.GET, "/", null, null);
      restResponseChannel = new MockRestResponseChannel();
      assertTrue("RestRequest channel not open", restRequest.isOpen());
      adminBlobStorageService.submitResponse(restRequest, restResponseChannel, new BadRSC(), null);
    } finally {
      responseHandler.start();
    }
  }

  /**
   * Tests blob GET, HEAD and DELETE operations.
   * @throws Exception
   */
  @Test
  public void getHeadDeleteTest() throws Exception {
    final int CONTENT_LENGTH = 1024;
    ByteBuffer content = ByteBuffer.wrap(RestTestUtils.getRandomBytes(CONTENT_LENGTH));
    String serviceId = "getHeadDeleteServiceID";
    String contentType = "application/octet-stream";
    String ownerId = "getHeadDeleteOwnerID";
    Map<String, Object> headers = new HashMap<>();
    setAmbryHeaders(headers, CONTENT_LENGTH, 7200, false, serviceId, contentType, ownerId);
    headers.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    headers.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
    String blobId = putBlobInRouter(headers, content);
    getBlobAndVerify(blobId, null, headers, content);
    getNotModifiedBlobAndVerify(blobId, null);
    getUserMetadataAndVerify(blobId, null, headers);
    getBlobInfoAndVerify(blobId, null, headers);
    getHeadAndVerify(blobId, null, headers);
    deleteBlobAndVerify(blobId);

    // check GET, HEAD and DELETE after delete.
    verifyOperationsAfterDelete(blobId, headers, content);
  }

  /**
   * Tests how metadata that has not been POSTed in the form of headers is returned.
   * @throws Exception
   */
  @Test
  public void oldStyleUserMetadataTest() throws Exception {
    ByteBuffer content = ByteBuffer.allocate(0);
    BlobProperties blobProperties = new BlobProperties(0, "userMetadataTestOldStyleServiceID");
    byte[] usermetadata = RestTestUtils.getRandomBytes(25);
    String blobId = router.putBlob(blobProperties, usermetadata, new ByteBufferReadableStreamChannel(content)).get();

    RestUtils.SubResource[] subResources = {RestUtils.SubResource.UserMetadata, RestUtils.SubResource.BlobInfo};
    for (RestUtils.SubResource subResource : subResources) {
      RestRequest restRequest =
          AdminTestUtils.createRestRequest(RestMethod.GET, blobId + "/" + subResource, null, null);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doOperation(restRequest, restResponseChannel);
      assertEquals("Unexpected response status for " + subResource, ResponseStatus.Ok, restResponseChannel.getStatus());
      assertEquals("Unexpected Content-Type for " + subResource, "application/octet-stream",
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
      assertEquals("Unexpected Content-Length for " + subResource, usermetadata.length,
          Integer.parseInt(restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
      assertArrayEquals("Unexpected user metadata for " + subResource, usermetadata,
          restResponseChannel.getResponseBody());
    }
  }

  /**
   * Tests {@link GetReplicasHandler#getReplicas(String, RestResponseChannel)}
   * <p/>
   * For each {@link PartitionId} in the {@link ClusterMap}, a {@link BlobId} is created. The replica list returned from
   * {@link GetReplicasHandler#getReplicas(String, RestResponseChannel)}is checked for equality against a locally
   * obtained replica list.
   * @throws Exception
   */
  @Test
  public void getReplicasTest() throws Exception {
    List<PartitionId> partitionIds = CLUSTER_MAP.getWritablePartitionIds();
    for (PartitionId partitionId : partitionIds) {
      String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
      BlobId blobId = new BlobId(partitionId);
      RestRequest restRequest =
          AdminTestUtils.createRestRequest(RestMethod.GET, blobId.getID() + "/" + RestUtils.SubResource.Replicas, null,
              null);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doOperation(restRequest, restResponseChannel);
      JSONObject response = new JSONObject(new String(restResponseChannel.getResponseBody()));
      String returnedReplicasStr = response.getString(GetReplicasHandler.REPLICAS_KEY).replace("\"", "");
      assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
          originalReplicaStr, returnedReplicasStr);
    }
  }

  /**
   * Tests reactions of the {@link GetReplicasHandler#getReplicas(String, RestResponseChannel)} operation to bad input -
   * specifically if we do not include required parameters.
   * @throws Exception
   */
  @Test
  public void getReplicasWithBadInputTest() throws Exception {
    // bad input - invalid blob id.
    RestRequest restRequest =
        AdminTestUtils.createRestRequest(RestMethod.GET, "12345/" + RestUtils.SubResource.Replicas, null, null);
    try {
      doOperation(restRequest, new MockRestResponseChannel());
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.NotFound, e.getErrorCode());
    }

    // bad input - invalid blob id for this cluster map.
    String blobId = "AAEAAQAAAAAAAADFAAAAJDMyYWZiOTJmLTBkNDYtNDQyNS1iYzU0LWEwMWQ1Yzg3OTJkZQ.gif";
    restRequest =
        AdminTestUtils.createRestRequest(RestMethod.GET, blobId + "/" + RestUtils.SubResource.Replicas, null, null);
    try {
      doOperation(restRequest, new MockRestResponseChannel());
      fail("Exception should have been thrown because the blobid is invalid");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.NotFound, e.getErrorCode());
    }
  }

  /**
   * Tests that POST fails for {@link AdminBlobStorageService}.
   * @throws Exception
   */
  @Test
  public void postFailureTest() throws Exception {
    RestRequest restRequest = AdminTestUtils.createRestRequest(RestMethod.POST, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      doOperation(restRequest, restResponseChannel);
      fail("POST should ahve failed because Admin does not support it");
    } catch (RestServiceException e) {
      assertEquals("POST is an unsupported method", RestServiceErrorCode.UnsupportedHttpMethod, e.getErrorCode());
    }
  }

  /**
   * Tests for cases where the {@link IdConverter} misbehaves and throws {@link RuntimeException}.
   * @throws InstantiationException
   * @throws JSONException
   */
  @Test
  public void misbehavingIdConverterTest() throws InstantiationException, JSONException {
    AdminTestIdConverterFactory converterFactory = new AdminTestIdConverterFactory();
    String exceptionMsg = UtilsTest.getRandomString(10);
    converterFactory.exceptionToThrow = new IllegalStateException(exceptionMsg);
    doIdConverterExceptionTest(converterFactory, exceptionMsg);
  }

  /**
   * Tests for cases where the {@link IdConverter} returns valid exceptions.
   * @throws InstantiationException
   * @throws JSONException
   */
  @Test
  public void idConverterExceptionPipelineTest() throws InstantiationException, JSONException {
    AdminTestIdConverterFactory converterFactory = new AdminTestIdConverterFactory();
    String exceptionMsg = UtilsTest.getRandomString(10);
    converterFactory.exceptionToReturn = new IllegalStateException(exceptionMsg);
    doIdConverterExceptionTest(converterFactory, exceptionMsg);
  }

  /**
   * Tests for cases where the {@link SecurityService} misbehaves and throws {@link RuntimeException}.
   * @throws InstantiationException
   * @throws JSONException
   */
  @Test
  public void misbehavingSecurityServiceTest() throws InstantiationException, JSONException {
    AdminTestSecurityServiceFactory securityFactory = new AdminTestSecurityServiceFactory();
    String exceptionMsg = UtilsTest.getRandomString(10);
    securityFactory.exceptionToThrow = new IllegalStateException(exceptionMsg);
    doSecurityServiceExceptionTest(securityFactory, exceptionMsg);
  }

  /**
   * Tests for cases where the {@link SecurityService} returns valid exceptions.
   * @throws InstantiationException
   * @throws JSONException
   */
  @Test
  public void securityServiceExceptionPipelineTest() throws InstantiationException, JSONException {
    AdminTestSecurityServiceFactory securityFactory = new AdminTestSecurityServiceFactory();
    String exceptionMsg = UtilsTest.getRandomString(10);
    securityFactory.exceptionToReturn = new IllegalStateException(exceptionMsg);
    doSecurityServiceExceptionTest(securityFactory, exceptionMsg);
  }

  /**
   * Tests for cases where the {@link Router} misbehaves and throws {@link RuntimeException}.
   * @throws Exception
   */
  @Test
  public void misbehavingRouterTest() throws Exception {
    AdminTestRouter testRouter = new AdminTestRouter();
    String exceptionMsg = UtilsTest.getRandomString(10);
    testRouter.exceptionToThrow = new IllegalStateException(exceptionMsg);
    doRouterExceptionPipelineTest(testRouter, exceptionMsg);
  }

  /**
   * Tests for cases where the {@link Router} returns valid {@link RouterException}.
   * @throws InstantiationException
   * @throws JSONException
   */
  @Test
  public void routerExceptionPipelineTest() throws Exception {
    AdminTestRouter testRouter = new AdminTestRouter();
    String exceptionMsg = UtilsTest.getRandomString(10);
    testRouter.exceptionToReturn = new RouterException(exceptionMsg, RouterErrorCode.UnexpectedInternalError);
    doRouterExceptionPipelineTest(testRouter, exceptionMsg);
  }

  // helpers
  // general

  /**
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param headers the {@link Map} where the headers should be set.
   * @param contentLength sets the {@link RestUtils.Headers#BLOB_SIZE} header. Required.
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header. Set to {@link Utils#Infinite_Time} if no
   *                  expiry.
   * @param isPrivate sets the {@link RestUtils.Headers#PRIVATE} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestUtils.Headers#SERVICE_ID} header. Required.
   * @param contentType sets the {@link RestUtils.Headers#AMBRY_CONTENT_TYPE} header. Required and has to be a valid MIME
   *                    type.
   * @param ownerId sets the {@link RestUtils.Headers#OWNER_ID} header. Optional - if not required, send null.
   * @throws IllegalArgumentException if any of {@code headers}, {@code serviceId}, {@code contentType} is null or if
   *                                  {@code contentLength} < 0 or if {@code ttlInSecs} < -1.
   */
  private static void setAmbryHeaders(Map<String, Object> headers, long contentLength, long ttlInSecs,
      boolean isPrivate, String serviceId, String contentType, String ownerId) {
    if (headers != null && contentLength >= 0 && ttlInSecs >= -1 && serviceId != null && contentType != null) {
      headers.put(RestUtils.Headers.BLOB_SIZE, contentLength);
      headers.put(RestUtils.Headers.TTL, ttlInSecs);
      headers.put(RestUtils.Headers.PRIVATE, isPrivate);
      headers.put(RestUtils.Headers.SERVICE_ID, serviceId);
      headers.put(RestUtils.Headers.AMBRY_CONTENT_TYPE, contentType);
      if (ownerId != null) {
        headers.put(RestUtils.Headers.OWNER_ID, ownerId);
      }
    } else {
      throw new IllegalArgumentException("Some required arguments are null. Cannot set ambry headers");
    }
  }

  /**
   * Does an operation in {@link AdminBlobStorageService} as dictated by the {@link RestMethod} in {@code restRequest}
   * and returns the result, if any. If an exception occurs during the operation, throws the exception.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AdminBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @throws Exception
   */
  private void doOperation(RestRequest restRequest, RestResponseChannel restResponseChannel) throws Exception {
    responseHandler.reset();
    switch (restRequest.getRestMethod()) {
      case POST:
        adminBlobStorageService.handlePost(restRequest, restResponseChannel);
        break;
      case GET:
        adminBlobStorageService.handleGet(restRequest, restResponseChannel);
        break;
      case DELETE:
        adminBlobStorageService.handleDelete(restRequest, restResponseChannel);
        break;
      case HEAD:
        adminBlobStorageService.handleHead(restRequest, restResponseChannel);
        break;
      default:
        fail("RestMethod not supported: " + restRequest.getRestMethod());
    }
    if (responseHandler.awaitResponseSubmission(1, TimeUnit.SECONDS)) {
      if (responseHandler.getException() != null) {
        throw responseHandler.getException();
      }
    } else {
      throw new IllegalStateException("doOperation() timed out");
    }
  }

  // Constructor helpers

  /**
   * Sets up and gets an instance of {@link AdminBlobStorageService}.
   * @return an instance of {@link AdminBlobStorageService}.
   */
  private AdminBlobStorageService getAdminBlobStorageService() {
    return new AdminBlobStorageService(adminConfig, adminMetrics, CLUSTER_MAP, responseHandler, router,
        idConverterFactory, securityServiceFactory);
  }

  // nullInputsForFunctionsTest() helpers

  /**
   * Checks for reaction to null input in {@code methodName} in {@link AdminBlobStorageService}.
   * @param methodName the name of the method to invoke.
   * @throws Exception
   */
  private void doNullInputsForFunctionsTest(String methodName) throws Exception {
    Method method =
        AdminBlobStorageService.class.getDeclaredMethod(methodName, RestRequest.class, RestResponseChannel.class);
    RestRequest restRequest = AdminTestUtils.createRestRequest(RestMethod.GET, "/", null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();

    responseHandler.reset();
    try {
      method.invoke(adminBlobStorageService, null, restResponseChannel);
      fail("Method [" + methodName + "] should have failed because RestRequest is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }

    responseHandler.reset();
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
  private void doRuntimeExceptionRouterTest(RestMethod restMethod) throws Exception {
    RestRequest restRequest = AdminTestUtils.createRestRequest(restMethod, "/", null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      switch (restMethod) {
        case GET:
        case DELETE:
        case HEAD:
          doOperation(restRequest, restResponseChannel);
          fail(restMethod + " should have detected a RestServiceException because of a bad router");
          break;
        default:
          throw new IllegalArgumentException("Unrecognized RestMethod: " + restMethod);
      }
    } catch (RuntimeException e) {
      assertEquals("Unexpected error message", InMemoryRouter.OPERATION_THROW_EARLY_RUNTIME_EXCEPTION, e.getMessage());
    }
  }

  // getHeadDeleteTest() helpers

  /**
   * Puts a blob directly into the {@link Router}.
   * @param headers the headers of the new blob that get converted to blob properties.
   * @param content the content of the blob.
   * @return the blob ID of the blob.
   * @throws Exception
   */
  public String putBlobInRouter(Map<String, Object> headers, ByteBuffer content) throws Exception {
    BlobProperties blobProperties = RestUtils.buildBlobProperties(headers);
    byte[] usermetadata = RestUtils.buildUsermetadata(headers);
    String blobId = router.putBlob(blobProperties, usermetadata, new ByteBufferReadableStreamChannel(content)).get();
    if (blobId == null) {
      fail("putBlobInRouter did not return a blob ID");
    }
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @throws Exception
   */
  public void getBlobAndVerify(String blobId, GetOption getOption, Map<String, Object> expectedHeaders,
      ByteBuffer expectedContent) throws Exception {
    JSONObject headers = new JSONObject();
    if (getOption != null) {
      headers.put(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    RestRequest restRequest = AdminTestUtils.createRestRequest(RestMethod.GET, blobId, headers, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    checkCommonGetHeadHeaders(restResponseChannel);
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match", expectedHeaders.get(RestUtils.Headers.BLOB_SIZE),
        Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE)));
    assertEquals("Content-Type does not match", expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertArrayEquals("GET content does not match original content", expectedContent.array(),
        restResponseChannel.getResponseBody());
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the blob is not returned as blob is not modified
   * @param blobId the blob ID of the blob to GET.
   * @param getOption the options to use while getting the blob.
   * @throws Exception
   */
  public void getNotModifiedBlobAndVerify(String blobId, GetOption getOption) throws Exception {
    JSONObject headers = new JSONObject();
    if (getOption != null) {
      headers.put(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    SimpleDateFormat dateFormat = new SimpleDateFormat(RestUtils.HTTP_DATE_FORMAT, Locale.ENGLISH);
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    Date date = new Date(System.currentTimeMillis());
    String dateStr = dateFormat.format(date);
    headers.put(RestUtils.Headers.IF_MODIFIED_SINCE, dateStr);
    RestRequest restRequest = AdminTestUtils.createRestRequest(RestMethod.GET, blobId, headers, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.NotModified, restResponseChannel.getStatus());
    assertTrue("No Date header", restResponseChannel.getHeader(RestUtils.Headers.DATE) != null);
    assertNull("No Last-Modified header expected", restResponseChannel.getHeader("Last-Modified"));
    assertNull(RestUtils.Headers.BLOB_SIZE + " should have been null ",
        restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
    assertNull("Content-Type should have been null", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertEquals("No content expected as blob is not modified", 0, restResponseChannel.getResponseBody().length);
  }

  /**
   * Gets the user metadata of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getUserMetadataAndVerify(String blobId, GetOption getOption, Map<String, Object> expectedHeaders)
      throws Exception {
    JSONObject headers = new JSONObject();
    if (getOption != null) {
      headers.put(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    RestRequest restRequest =
        AdminTestUtils.createRestRequest(RestMethod.GET, blobId + "/" + RestUtils.SubResource.UserMetadata, headers,
            null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    checkCommonGetHeadHeaders(restResponseChannel);
    assertEquals("Content-Length is not 0", "0", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    verifyUserMetadataHeaders(expectedHeaders, restResponseChannel);
  }

  /**
   * Gets the blob info of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getBlobInfoAndVerify(String blobId, GetOption getOption, Map<String, Object> expectedHeaders)
      throws Exception {
    JSONObject headers = new JSONObject();
    if (getOption != null) {
      headers.put(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    RestRequest restRequest =
        AdminTestUtils.createRestRequest(RestMethod.GET, blobId + "/" + RestUtils.SubResource.BlobInfo, headers, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    checkCommonGetHeadHeaders(restResponseChannel);
    assertEquals("Content-Length is not 0", "0", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    verifyBlobProperties(expectedHeaders, restResponseChannel);
    verifyUserMetadataHeaders(expectedHeaders, restResponseChannel);
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getHeadAndVerify(String blobId, GetOption getOption, Map<String, Object> expectedHeaders)
      throws Exception {
    JSONObject headers = new JSONObject();
    if (getOption != null) {
      headers.put(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    RestRequest restRequest = AdminTestUtils.createRestRequest(RestMethod.HEAD, blobId, headers, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    checkCommonGetHeadHeaders(restResponseChannel);
    assertEquals(RestUtils.Headers.CONTENT_LENGTH + " does not match " + RestUtils.Headers.BLOB_SIZE,
        expectedHeaders.get(RestUtils.Headers.BLOB_SIZE),
        Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
    assertEquals(RestUtils.Headers.CONTENT_TYPE + " does not match " + RestUtils.Headers.AMBRY_CONTENT_TYPE,
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    verifyBlobProperties(expectedHeaders, restResponseChannel);
  }

  /**
   * Verifies blob properties from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param restResponseChannel the {@link RestResponseChannel} which contains the response.
   */
  private void verifyBlobProperties(Map<String, Object> expectedHeaders, MockRestResponseChannel restResponseChannel) {
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match", expectedHeaders.get(RestUtils.Headers.BLOB_SIZE),
        Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE)));
    assertEquals(RestUtils.Headers.SERVICE_ID + " does not match", expectedHeaders.get(RestUtils.Headers.SERVICE_ID),
        restResponseChannel.getHeader(RestUtils.Headers.SERVICE_ID));
    assertEquals(RestUtils.Headers.PRIVATE + " does not match", expectedHeaders.get(RestUtils.Headers.PRIVATE),
        Boolean.parseBoolean(restResponseChannel.getHeader(RestUtils.Headers.PRIVATE)));
    assertEquals(RestUtils.Headers.AMBRY_CONTENT_TYPE + " does not match",
        expectedHeaders.get(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        restResponseChannel.getHeader(RestUtils.Headers.AMBRY_CONTENT_TYPE));
    assertTrue(RestUtils.Headers.CREATION_TIME + " header missing",
        restResponseChannel.getHeader(RestUtils.Headers.CREATION_TIME) != null);
    if ((long) expectedHeaders.get(RestUtils.Headers.TTL) != Utils.Infinite_Time) {
      assertEquals(RestUtils.Headers.TTL + " does not match", (long) expectedHeaders.get(RestUtils.Headers.TTL),
          Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.TTL)));
    }
    if (expectedHeaders.containsKey(RestUtils.Headers.OWNER_ID)) {
      assertEquals(RestUtils.Headers.OWNER_ID + " does not match", expectedHeaders.get(RestUtils.Headers.OWNER_ID),
          restResponseChannel.getHeader(RestUtils.Headers.OWNER_ID));
    }
  }

  /**
   * Verifies User metadata headers from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param restResponseChannel the {@link RestResponseChannel} which contains the response.
   */
  private void verifyUserMetadataHeaders(Map<String, Object> expectedHeaders,
      MockRestResponseChannel restResponseChannel) {
    for (Map.Entry<String, Object> expectedHeader : expectedHeaders.entrySet()) {
      String key = expectedHeader.getKey();
      if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
        String outValue = restResponseChannel.getHeader(key);
        assertEquals("Value for " + key + " does not match in user metadata", expectedHeaders.get(key), outValue);
      }
    }
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws Exception
   */
  private void deleteBlobAndVerify(String blobId) throws Exception {
    RestRequest restRequest = AdminTestUtils.createRestRequest(RestMethod.DELETE, blobId, null, null);
    verifyDeleteAccepted(restRequest);
  }

  /**
   * Verifies that the right {@link ResponseStatus} is returned for GET, HEAD and DELETE once a blob is deleted.
   * @param blobId the ID of the blob that was deleted.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @throws Exception
   */
  private void verifyOperationsAfterDelete(String blobId, Map<String, Object> expectedHeaders,
      ByteBuffer expectedContent) throws Exception {
    RestRequest restRequest = AdminTestUtils.createRestRequest(RestMethod.GET, blobId, null, null);
    verifyGone(restRequest);

    restRequest = AdminTestUtils.createRestRequest(RestMethod.HEAD, blobId, null, null);
    verifyGone(restRequest);

    restRequest = AdminTestUtils.createRestRequest(RestMethod.DELETE, blobId, null, null);
    verifyDeleteAccepted(restRequest);

    GetOption[] options = {GetOption.Include_Deleted_Blobs, GetOption.Include_All};
    for (GetOption option : options) {
      getBlobAndVerify(blobId, option, expectedHeaders, expectedContent);
      getNotModifiedBlobAndVerify(blobId, option);
      getUserMetadataAndVerify(blobId, option, expectedHeaders);
      getBlobInfoAndVerify(blobId, option, expectedHeaders);
      getHeadAndVerify(blobId, option, expectedHeaders);
    }
  }

  /**
   * Verifies that a blob is GONE after it is deleted.
   * @param restRequest the {@link RestRequest} to send to {@link AdminBlobStorageService}.
   */
  private void verifyGone(RestRequest restRequest) throws Exception {
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      doOperation(restRequest, restResponseChannel);
      fail("Operation should have failed because blob is deleted");
    } catch (RestServiceException e) {
      assertEquals("AdminBlobStorageService should have thrown a Deleted exception", RestServiceErrorCode.Deleted,
          e.getErrorCode());
    }
  }

  /**
   * Verifies that a request returns the right response code  once the blob has been deleted.
   * @param restRequest the {@link RestRequest} to send to {@link AdminBlobStorageService}.
   * @throws Exception
   */
  private void verifyDeleteAccepted(RestRequest restRequest) throws Exception {
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Accepted, restResponseChannel.getStatus());
    assertTrue("No Date header", restResponseChannel.getHeader(RestUtils.Headers.DATE) != null);
    assertEquals("Content-Length is not 0", "0", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
  }

  /**
   * Checks headers that are common to HEAD and GET.
   * @param restResponseChannel the {@link RestResponseChannel} to check headers on.
   */
  private void checkCommonGetHeadHeaders(MockRestResponseChannel restResponseChannel) {
    assertTrue("No Date header", restResponseChannel.getHeader(RestUtils.Headers.DATE) != null);
    assertTrue("No Last-Modified header", restResponseChannel.getHeader("Last-Modified") != null);
  }

  // IdConverter and SecurityService exception testing helpers.

  /**
   * Does the exception pipelining test for {@link IdConverter}.
   * @param converterFactory the {@link IdConverterFactory} to use to while creating {@link AdminBlobStorageService}.
   * @param expectedExceptionMsg the expected exception message.
   * @throws InstantiationException
   * @throws JSONException
   */
  private void doIdConverterExceptionTest(AdminTestIdConverterFactory converterFactory, String expectedExceptionMsg)
      throws InstantiationException, JSONException {
    adminBlobStorageService =
        new AdminBlobStorageService(adminConfig, adminMetrics, CLUSTER_MAP, responseHandler, router, converterFactory,
            securityServiceFactory);
    adminBlobStorageService.start();
    doExternalServicesBadInputTest(RestMethod.values(), expectedExceptionMsg);
  }

  /**
   * Does the exception pipelining test for {@link SecurityService}.
   * @param securityFactory the {@link SecurityServiceFactory} to use to while creating {@link AdminBlobStorageService}.
   * @param exceptionMsg the expected exception message.
   * @throws InstantiationException
   * @throws JSONException
   */
  private void doSecurityServiceExceptionTest(AdminTestSecurityServiceFactory securityFactory, String exceptionMsg)
      throws InstantiationException, JSONException {
    for (AdminTestSecurityServiceFactory.Mode mode : AdminTestSecurityServiceFactory.Mode.values()) {
      securityFactory.mode = mode;
      RestMethod[] restMethods;
      if (mode.equals(AdminTestSecurityServiceFactory.Mode.Request)) {
        restMethods = RestMethod.values();
      } else {
        restMethods = new RestMethod[2];
        restMethods[0] = RestMethod.GET;
        restMethods[1] = RestMethod.HEAD;
      }
      adminBlobStorageService =
          new AdminBlobStorageService(adminConfig, adminMetrics, CLUSTER_MAP, responseHandler, new AdminTestRouter(),
              idConverterFactory, securityFactory);
      adminBlobStorageService.start();
      doExternalServicesBadInputTest(restMethods, exceptionMsg);
    }
  }

  /**
   * Does the tests to check for exception pipelining for exceptions returned/thrown by external services.
   * @param restMethods the {@link RestMethod} types for which the test has to be run.
   * @param expectedExceptionMsg the expected exception message.
   * @throws JSONException
   */
  private void doExternalServicesBadInputTest(RestMethod[] restMethods, String expectedExceptionMsg)
      throws JSONException {
    for (RestMethod restMethod : restMethods) {
      if (restMethod.equals(RestMethod.UNKNOWN) || restMethod.equals(RestMethod.POST)) {
        continue;
      }
      try {
        doOperation(AdminTestUtils.createRestRequest(restMethod, "/", null, null), new MockRestResponseChannel());
        fail("Operation " + restMethod
            + " should have failed because an external service would have thrown an exception");
      } catch (Exception e) {
        assertEquals("Unexpected exception message", expectedExceptionMsg, e.getMessage());
      }
    }
  }

  // routerExceptionPipelineTest() helpers.

  /**
   * Does the exception pipelining test for {@link Router}.
   * @param testRouter the {@link Router} to use to while creating {@link AdminBlobStorageService}.
   * @param exceptionMsg the expected exception message.
   * @throws Exception
   */
  private void doRouterExceptionPipelineTest(AdminTestRouter testRouter, String exceptionMsg) throws Exception {
    adminBlobStorageService =
        new AdminBlobStorageService(adminConfig, adminMetrics, CLUSTER_MAP, responseHandler, testRouter,
            idConverterFactory, securityServiceFactory);
    adminBlobStorageService.start();
    for (RestMethod restMethod : RestMethod.values()) {
      switch (restMethod) {
        case HEAD:
          testRouter.exceptionOpType = AdminTestRouter.OpType.GetBlob;
          checkRouterExceptionPipeline(exceptionMsg, AdminTestUtils.createRestRequest(restMethod, "/", null, null));
          break;
        case GET:
          testRouter.exceptionOpType = AdminTestRouter.OpType.GetBlob;
          checkRouterExceptionPipeline(exceptionMsg, AdminTestUtils.createRestRequest(restMethod, "/", null, null));
          break;
        case DELETE:
          testRouter.exceptionOpType = AdminTestRouter.OpType.DeleteBlob;
          checkRouterExceptionPipeline(exceptionMsg, AdminTestUtils.createRestRequest(restMethod, "/", null, null));
          break;
        default:
          break;
      }
    }
  }

  /**
   * Checks that the exception received by submitting {@code restRequest} to {@link AdminBlobStorageService} matches
   * what was expected.
   * @param expectedExceptionMsg the expected exception message.
   * @param restRequest the {@link RestRequest} to submit to {@link AdminBlobStorageService}.
   * @throws Exception
   */
  private void checkRouterExceptionPipeline(String expectedExceptionMsg, RestRequest restRequest) throws Exception {
    try {
      doOperation(restRequest, new MockRestResponseChannel());
      fail("Operation " + restRequest.getRestMethod()
          + " should have failed because an external service would have thrown an exception");
    } catch (RestServiceException | RuntimeException e) {
      // catching RestServiceException because RouterException should have been converted.
      // RuntimeException might get bubbled up as is.
      assertEquals("Unexpected exception message", expectedExceptionMsg, Utils.getRootCause(e).getMessage());
      // Nothing should be closed.
      assertTrue("RestRequest channel is not open", restRequest.isOpen());
      restRequest.close();
    }
  }
}

/**
 * An implementation of {@link RestResponseHandler} that stores a submitted response/exception and signals the fact
 * that the response has been submitted. A single instance can handle only a single response at a time. To reuse, call
 * {@link #reset()}.
 */
class AdminTestResponseHandler implements RestResponseHandler {
  private volatile CountDownLatch responseSubmitted = new CountDownLatch(1);
  private volatile ReadableStreamChannel response = null;
  private volatile Exception exception = null;
  private volatile boolean serviceRunning = false;

  @Override
  public void start() {
    serviceRunning = true;
  }

  @Override
  public void shutdown() {
    serviceRunning = false;
  }

  @Override
  public void handleResponse(RestRequest restRequest, RestResponseChannel restResponseChannel,
      ReadableStreamChannel response, Exception exception) throws RestServiceException {
    if (serviceRunning) {
      this.response = response;
      this.exception = exception;
      if (response != null && exception == null) {
        try {
          response.readInto(restResponseChannel, null).get();
        } catch (Exception e) {
          this.exception = e;
        }
      }
      restResponseChannel.onResponseComplete(exception);
      responseSubmitted.countDown();
    } else {
      throw new RestServiceException("Response handler inactive", RestServiceErrorCode.RequestResponseQueuingFailure);
    }
  }

  /**
   * Wait for response to be submitted.
   * @param timeout the length of time to wait for.
   * @param timeUnit the time unit of {@code timeout}.
   * @return {@code true} if response was submitted within {@code timeout}. {@code false} otherwise.
   * @throws InterruptedException
   */
  public boolean awaitResponseSubmission(long timeout, TimeUnit timeUnit) throws InterruptedException {
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
    responseSubmitted = new CountDownLatch(1);
  }
}

/**
 * Implementation of {@link SecurityServiceFactory} that returns exceptions.
 */
class AdminTestSecurityServiceFactory implements SecurityServiceFactory {
  /**
   * Defines the API in which {@link #exceptionToThrow} and {@link #exceptionToReturn} will work.
   */
  protected enum Mode {
    /**
     * Works in {@link SecurityService#processRequest(RestRequest, Callback)}.
     */
    Request, /**
     * Works in {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)}.
     */
    Response
  }

  /**
   * The exception to return via future/callback.
   */
  public Exception exceptionToReturn = null;
  /**
   * The exception to throw on function invocation.
   */
  public RuntimeException exceptionToThrow = null;
  /**
   * Defines the API in which {@link #exceptionToThrow} and {@link #exceptionToReturn} will work.
   */
  public Mode mode = Mode.Request;

  @Override
  public SecurityService getSecurityService() {
    return new TestSecurityService();
  }

  private class TestSecurityService implements SecurityService {
    private boolean isOpen = true;

    @Override
    public Future<Void> processRequest(RestRequest restRequest, Callback<Void> callback) {
      if (!isOpen) {
        throw new IllegalStateException("SecurityService closed");
      }
      return completeOperation(callback, mode == null || mode == Mode.Request);
    }

    @Override
    public Future<Void> processResponse(RestRequest restRequest, RestResponseChannel responseChannel, BlobInfo blobInfo,
        Callback<Void> callback) {
      if (!isOpen) {
        throw new IllegalStateException("SecurityService closed");
      }
      return completeOperation(callback, mode == Mode.Response);
    }

    @Override
    public void close() {
      isOpen = false;
    }

    /**
     * Completes the operation by creating and invoking a {@link Future} and invoking the {@code callback} if non-null.
     * @param callback the {@link Callback} to invoke. Can be null.
     * @param misbehaveIfRequired whether to exhibit misbehavior or not.
     * @return the created {@link Future}.
     */
    private Future<Void> completeOperation(Callback<Void> callback, boolean misbehaveIfRequired) {
      if (misbehaveIfRequired && exceptionToThrow != null) {
        throw exceptionToThrow;
      }
      FutureResult<Void> futureResult = new FutureResult<Void>();
      futureResult.done(null, misbehaveIfRequired ? exceptionToReturn : null);
      if (callback != null) {
        callback.onCompletion(null, misbehaveIfRequired ? exceptionToReturn : null);
      }
      return futureResult;
    }
  }
}

/**
 * Implementation of {@link IdConverterFactory} that returns exceptions.
 */
class AdminTestIdConverterFactory implements IdConverterFactory {
  public Exception exceptionToReturn = null;
  public RuntimeException exceptionToThrow = null;

  @Override
  public IdConverter getIdConverter() {
    return new TestIdConverter();
  }

  private class TestIdConverter implements IdConverter {
    private boolean isOpen = true;

    @Override
    public Future<String> convert(RestRequest restRequest, String input, Callback<String> callback) {
      if (!isOpen) {
        throw new IllegalStateException("IdConverter closed");
      }
      return completeOperation(callback);
    }

    @Override
    public void close() {
      isOpen = false;
    }

    /**
     * Completes the operation by creating and invoking a {@link Future} and invoking the {@code callback} if non-null.
     * @param callback the {@link Callback} to invoke. Can be null.
     * @return the created {@link Future}.
     */
    private Future<String> completeOperation(Callback<String> callback) {
      if (exceptionToThrow != null) {
        throw exceptionToThrow;
      }
      FutureResult<String> futureResult = new FutureResult<String>();
      futureResult.done(null, exceptionToReturn);
      if (callback != null) {
        callback.onCompletion(null, exceptionToReturn);
      }
      return futureResult;
    }
  }
}

/**
 * A bad implementation of {@link RestRequest}. Just throws exceptions.
 */
class BadRestRequest extends BadRSC implements RestRequest {

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
  public Map<String, Object> getArgs() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public void prepare() {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public RestRequestMetricsTracker getMetricsTracker() {
    return new RestRequestMetricsTracker();
  }

  @Override
  public void setDigestAlgorithm(String digestAlgorithm) {
    throw new IllegalStateException("Not implemented");
  }

  @Override
  public byte[] getDigest() {
    throw new IllegalStateException("Not implemented");
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
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
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
}

/**
 * Implementation of {@link Router} that responds immediately or throws exceptions as required.
 */
class AdminTestRouter implements Router {
  private boolean isOpen = true;

  /**
   * Enumerates the different operation types in the router.
   */
  enum OpType {
    DeleteBlob, GetBlob, PutBlob
  }

  public OpType exceptionOpType = null;
  public Exception exceptionToReturn = null;
  public RuntimeException exceptionToThrow = null;

  @Override
  public Future<GetBlobResult> getBlob(String blobId, GetBlobOptions options) {
    return getBlob(blobId, options, null);
  }

  @Override
  public Future<GetBlobResult> getBlob(String blobId, GetBlobOptions options, Callback<GetBlobResult> callback) {
    GetBlobResult result;
    switch (options.getOperationType()) {
      case BlobInfo:
        result = new GetBlobResult(new BlobInfo(new BlobProperties(0, "FrontendTestRouter"), new byte[0]), null);
        break;
      case Data:
        result = new GetBlobResult(null, new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0)));
        break;
      default:
        result = new GetBlobResult(new BlobInfo(new BlobProperties(0, "FrontendTestRouter"), new byte[0]),
            new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0)));
        break;
    }
    return completeOperation(result, callback, OpType.GetBlob);
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel) {
    return putBlob(blobProperties, usermetadata, channel, null);
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      Callback<String> callback) {
    return completeOperation(UtilsTest.getRandomString(10), callback, OpType.PutBlob);
  }

  @Override
  public Future<Void> deleteBlob(String blobId) {
    return deleteBlob(blobId, null);
  }

  @Override
  public Future<Void> deleteBlob(String blobId, Callback<Void> callback) {
    return completeOperation(null, callback, OpType.DeleteBlob);
  }

  @Override
  public void close() {
    isOpen = false;
  }

  /**
   * Completes the operation by creating and invoking a {@link Future} and invoking the {@code callback} if non-null.
   * @param result the result to return.
   * @param callback the {@link Callback} to invoke. Can be null.
   * @param opType the type of operation calling this function.
   * @param <T> the type of future/callback.
   * @return the created {@link Future}.
   */
  private <T> Future<T> completeOperation(T result, Callback<T> callback, OpType opType) {
    if (!isOpen) {
      throw new IllegalStateException("Router not open");
    }
    Exception exception = null;
    if (opType == exceptionOpType) {
      if (exceptionToThrow != null) {
        throw new RuntimeException(exceptionToThrow);
      } else if (exceptionToReturn != null) {
        exception = exceptionToReturn;
        result = null;
      }
    }
    FutureResult<T> futureResult = new FutureResult<T>();
    futureResult.done(result, exception);
    if (callback != null) {
      callback.onCompletion(result, exception);
    }
    return futureResult;
  }
}
