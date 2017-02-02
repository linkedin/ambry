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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.IdConverter;
import com.github.ambry.rest.IdConverterFactory;
import com.github.ambry.rest.MockRestRequest;
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
import com.github.ambry.rest.RestUtilsTest;
import com.github.ambry.rest.SecurityService;
import com.github.ambry.rest.SecurityServiceFactory;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link AmbryBlobStorageService}.
 */
public class AmbryBlobStorageServiceTest {

  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final FrontendMetrics frontendMetrics = new FrontendMetrics(metricRegistry);
  private final FrontendConfig frontendConfig;
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final FrontendTestResponseHandler responseHandler;
  private final InMemoryRouter router;

  private AmbryBlobStorageService ambryBlobStorageService;

  /**
   * Sets up the {@link AmbryBlobStorageService} instance before a test.
   * @throws InstantiationException
   */
  public AmbryBlobStorageServiceTest() throws InstantiationException {
    VerifiableProperties verifiableProperties = new VerifiableProperties(new Properties());
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    frontendConfig = new FrontendConfig(verifiableProperties);
    idConverterFactory = new AmbryIdConverterFactory(verifiableProperties, metricRegistry);
    securityServiceFactory = new AmbrySecurityServiceFactory(verifiableProperties, metricRegistry);
    router = new InMemoryRouter(verifiableProperties);
    responseHandler = new FrontendTestResponseHandler();
    ambryBlobStorageService = getAmbryBlobStorageService();
    responseHandler.start();
    ambryBlobStorageService.start();
  }

  /**
   * Shuts down the {@link AmbryBlobStorageService} instance after all tests.
   * @throws IOException
   */
  @After
  public void shutdownAmbryBlobStorageService() throws IOException {
    ambryBlobStorageService.shutdown();
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
    ambryBlobStorageService.start();
    ambryBlobStorageService.shutdown();
  }

  /**
   * Tests for {@link AmbryBlobStorageService#shutdown()} when {@link AmbryBlobStorageService#start()} has not been
   * called previously.
   * <p/>
   * This test is for  cases where {@link AmbryBlobStorageService#start()} has failed and
   * {@link AmbryBlobStorageService#shutdown()} needs to be run.
   * @throws IOException
   */
  @Test
  public void shutdownWithoutStartTest() throws IOException {
    AmbryBlobStorageService ambryBlobStorageService = getAmbryBlobStorageService();
    ambryBlobStorageService.shutdown();
  }

  /**
   * This tests for exceptions thrown when an {@link AmbryBlobStorageService} instance is used without calling
   * {@link AmbryBlobStorageService#start()} first.
   * @throws Exception
   */
  @Test
  public void useServiceWithoutStartTest() throws Exception {
    ambryBlobStorageService = getAmbryBlobStorageService();
    // not fine to use without start.
    try {
      doOperation(createRestRequest(RestMethod.GET, "/", null, null), new MockRestResponseChannel());
      fail("Should not have been able to use AmbryBlobStorageService without start");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", RestServiceErrorCode.ServiceUnavailable, e.getErrorCode());
    }
  }

  /**
   * Checks for reactions of all methods in {@link AmbryBlobStorageService} to null arguments.
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
   * Checks reactions of all methods in {@link AmbryBlobStorageService} to a {@link Router} that throws
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
    doRuntimeExceptionRouterTest(RestMethod.POST);
    doRuntimeExceptionRouterTest(RestMethod.DELETE);
    doRuntimeExceptionRouterTest(RestMethod.HEAD);
  }

  /**
   * Checks reactions of all methods in {@link AmbryBlobStorageService} to bad {@link RestResponseHandler} and
   * {@link RestRequest} implementations.
   * @throws Exception
   */
  @Test
  public void badResponseHandlerAndRestRequestTest() throws Exception {
    RestRequest restRequest = new BadRestRequest();
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();

    // What happens inside AmbryBlobStorageService during this test?
    // 1. Since the RestRequest throws errors, AmbryBlobStorageService will attempt to submit response with exception
    //      to FrontendTestResponseHandler.
    // 2. The submission will fail because FrontendTestResponseHandler has been shutdown.
    // 3. AmbryBlobStorageService will directly complete the request over the RestResponseChannel with the *original*
    //      exception.
    // 4. It will then try to release resources but closing the RestRequest will also throw an exception. This exception
    //      is swallowed.
    // What the test is looking for -> No exceptions thrown when the handle is run and the original exception arrives
    // safely.
    responseHandler.shutdown();

    ambryBlobStorageService.handleGet(restRequest, restResponseChannel);
    // IllegalStateException is thrown in BadRestRequest.
    assertEquals("Unexpected exception", IllegalStateException.class, restResponseChannel.getException().getClass());

    responseHandler.reset();
    restResponseChannel = new MockRestResponseChannel();
    ambryBlobStorageService.handlePost(restRequest, restResponseChannel);
    // IllegalStateException or NullPointerException is thrown because of BadRestRequest.
    Exception e = restResponseChannel.getException();
    assertTrue("Unexpected exception", e instanceof IllegalStateException || e instanceof NullPointerException);

    responseHandler.reset();
    restResponseChannel = new MockRestResponseChannel();
    ambryBlobStorageService.handleDelete(restRequest, restResponseChannel);
    // IllegalStateException or NullPointerException is thrown because of BadRestRequest.
    e = restResponseChannel.getException();
    assertTrue("Unexpected exception", e instanceof IllegalStateException || e instanceof NullPointerException);

    responseHandler.reset();
    restResponseChannel = new MockRestResponseChannel();
    ambryBlobStorageService.handleHead(restRequest, restResponseChannel);
    // IllegalStateException or NullPointerException is thrown because of BadRestRequest.
    e = restResponseChannel.getException();
    assertTrue("Unexpected exception", e instanceof IllegalStateException || e instanceof NullPointerException);
  }

  /**
   * Tests
   * {@link AmbryBlobStorageService#submitResponse(RestRequest, RestResponseChannel, ReadableStreamChannel, Exception)}.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void submitResponseTest() throws JSONException, UnsupportedEncodingException, URISyntaxException {
    String exceptionMsg = UtilsTest.getRandomString(10);
    responseHandler.shutdown();
    // handleResponse of FrontendTestResponseHandler throws exception because it has been shutdown.
    try {
      // there is an exception already.
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      assertTrue("RestRequest channel is not open", restRequest.isOpen());
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null,
          new RuntimeException(exceptionMsg));
      assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getException().getMessage());

      // there is no exception and exception thrown when the response is submitted.
      restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      assertTrue("RestRequest channel is not open", restRequest.isOpen());
      restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel response = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
      assertTrue("Response channel is not open", response.isOpen());
      ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, response, null);
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
    // handleResponse of FrontendTestResponseHandler throws exception because it has been shutdown.
    try {
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
      assertTrue("RestRequest channel not open", restRequest.isOpen());
      assertTrue("ReadableStreamChannel not open", channel.isOpen());
      ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, channel, null);
      assertFalse("ReadableStreamChannel is still open", channel.isOpen());

      // null ReadableStreamChannel
      restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      restResponseChannel = new MockRestResponseChannel();
      assertTrue("RestRequest channel not open", restRequest.isOpen());
      ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, null, null);

      // bad RestRequest (close() throws IOException)
      channel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
      restResponseChannel = new MockRestResponseChannel();
      assertTrue("ReadableStreamChannel not open", channel.isOpen());
      ambryBlobStorageService.submitResponse(new BadRestRequest(), restResponseChannel, channel, null);

      // bad ReadableStreamChannel (close() throws IOException)
      restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      restResponseChannel = new MockRestResponseChannel();
      assertTrue("RestRequest channel not open", restRequest.isOpen());
      ambryBlobStorageService.submitResponse(restRequest, restResponseChannel, new BadRSC(), null);
    } finally {
      responseHandler.start();
    }
  }

  /**
   * Tests blob POST, GET, HEAD and DELETE operations.
   * @throws Exception
   */
  @Test
  public void postGetHeadDeleteTest() throws Exception {
    final int CONTENT_LENGTH = 1024;
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(CONTENT_LENGTH));
    String serviceId = "postGetHeadDeleteServiceID";
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    JSONObject headers = new JSONObject();
    setAmbryHeaders(headers, CONTENT_LENGTH, 7200, false, serviceId, contentType, ownerId);
    Map<String, String> userMetadata = new HashMap<String, String>();
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
    RestUtilsTest.setUserMetadataHeaders(headers, userMetadata);
    String blobId = postBlobAndVerify(headers, content);

    getBlobAndVerify(blobId, null, headers, content);
    getHeadAndVerify(blobId, null, headers);

    ByteRange range = ByteRange.fromStartOffset(ThreadLocalRandom.current().nextLong(CONTENT_LENGTH));
    getBlobAndVerify(blobId, range, headers, content);
    getHeadAndVerify(blobId, range, headers);

    range = ByteRange.fromLastNBytes(ThreadLocalRandom.current().nextLong(CONTENT_LENGTH + 1));
    getBlobAndVerify(blobId, range, headers, content);
    getHeadAndVerify(blobId, range, headers);

    long random1 = ThreadLocalRandom.current().nextLong(CONTENT_LENGTH);
    long random2 = ThreadLocalRandom.current().nextLong(CONTENT_LENGTH);
    range = ByteRange.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2));
    getBlobAndVerify(blobId, range, headers, content);
    getHeadAndVerify(blobId, range, headers);

    getNotModifiedBlobAndVerify(blobId);
    getUserMetadataAndVerify(blobId, headers);
    getBlobInfoAndVerify(blobId, headers);
    deleteBlobAndVerify(blobId);

    // check GET, HEAD and DELETE after delete.
    verifyOperationsAfterDelete(blobId);
  }

  /**
   * Tests how metadata that has not been POSTed in the form of headers is returned.
   * @throws Exception
   */
  @Test
  public void oldStyleUserMetadataTest() throws Exception {
    ByteBuffer content = ByteBuffer.allocate(0);
    BlobProperties blobProperties = new BlobProperties(0, "userMetadataTestOldStyleServiceID");
    byte[] usermetadata = TestUtils.getRandomBytes(25);
    String blobId = router.putBlob(blobProperties, usermetadata, new ByteBufferReadableStreamChannel(content)).get();

    RestUtils.SubResource[] subResources = {RestUtils.SubResource.UserMetadata, RestUtils.SubResource.BlobInfo};
    for (RestUtils.SubResource subResource : subResources) {
      RestRequest restRequest = createRestRequest(RestMethod.GET, blobId + "/" + subResource, null, null);
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
   * Tests for cases where the {@link IdConverter} misbehaves and throws {@link RuntimeException}.
   * @throws InstantiationException
   * @throws JSONException
   */
  @Test
  public void misbehavingIdConverterTest() throws InstantiationException, JSONException {
    FrontendTestIdConverterFactory converterFactory = new FrontendTestIdConverterFactory();
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
    FrontendTestIdConverterFactory converterFactory = new FrontendTestIdConverterFactory();
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
    FrontendTestSecurityServiceFactory securityFactory = new FrontendTestSecurityServiceFactory();
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
    FrontendTestSecurityServiceFactory securityFactory = new FrontendTestSecurityServiceFactory();
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
    FrontendTestRouter testRouter = new FrontendTestRouter();
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
    FrontendTestRouter testRouter = new FrontendTestRouter();
    String exceptionMsg = UtilsTest.getRandomString(10);
    testRouter.exceptionToReturn = new RouterException(exceptionMsg, RouterErrorCode.UnexpectedInternalError);
    doRouterExceptionPipelineTest(testRouter, exceptionMsg);
  }

  /**
   * Test that GET operations fail with the expected error code when a bad range header is provided.
   * @throws Exception
   */
  @Test
  public void badRangeHeaderTest() throws Exception {
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.Headers.RANGE, "adsfksakdfsdfkdaklf");
    try {
      doOperation(createRestRequest(RestMethod.GET, "/", headers, null), new MockRestResponseChannel());
      fail("GET operation should have failed because of an invalid range header");
    } catch (RestServiceException e) {
      assertEquals("Unexpected error code", RestServiceErrorCode.InvalidArgs, e.getErrorCode());
    }
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
      List<ByteBuffer> contents) throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod);
    request.put(MockRestRequest.URI_KEY, uri);
    if (headers != null) {
      request.put(MockRestRequest.HEADERS_KEY, headers);
    }
    return new MockRestRequest(request, contents);
  }

  /**
   * Sets headers that helps build {@link BlobProperties} on the server. See argument list for the headers that are set.
   * Any other headers have to be set explicitly.
   * @param headers the {@link JSONObject} where the headers should be set.
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
   * @throws JSONException
   */
  private void setAmbryHeaders(JSONObject headers, long contentLength, long ttlInSecs, boolean isPrivate,
      String serviceId, String contentType, String ownerId) throws JSONException {
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
   * Does an operation in {@link AmbryBlobStorageService} as dictated by the {@link RestMethod} in {@code restRequest}
   * and returns the result, if any. If an exception occurs during the operation, throws the exception.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link AmbryBlobStorageService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @throws Exception
   */
  private void doOperation(RestRequest restRequest, RestResponseChannel restResponseChannel) throws Exception {
    responseHandler.reset();
    switch (restRequest.getRestMethod()) {
      case POST:
        ambryBlobStorageService.handlePost(restRequest, restResponseChannel);
        break;
      case GET:
        ambryBlobStorageService.handleGet(restRequest, restResponseChannel);
        break;
      case DELETE:
        ambryBlobStorageService.handleDelete(restRequest, restResponseChannel);
        break;
      case HEAD:
        ambryBlobStorageService.handleHead(restRequest, restResponseChannel);
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
   * Sets up and gets an instance of {@link AmbryBlobStorageService}.
   * @return an instance of {@link AmbryBlobStorageService}.
   */
  private AmbryBlobStorageService getAmbryBlobStorageService() {
    return new AmbryBlobStorageService(frontendConfig, frontendMetrics, responseHandler, router, idConverterFactory,
        securityServiceFactory);
  }

  // nullInputsForFunctionsTest() helpers

  /**
   * Checks for reaction to null input in {@code methodName} in {@link AmbryBlobStorageService}.
   * @param methodName the name of the method to invoke.
   * @throws Exception
   */
  private void doNullInputsForFunctionsTest(String methodName) throws Exception {
    Method method =
        AmbryBlobStorageService.class.getDeclaredMethod(methodName, RestRequest.class, RestResponseChannel.class);
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();

    responseHandler.reset();
    try {
      method.invoke(ambryBlobStorageService, null, restResponseChannel);
      fail("Method [" + methodName + "] should have failed because RestRequest is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }

    responseHandler.reset();
    try {
      method.invoke(ambryBlobStorageService, restRequest, null);
      fail("Method [" + methodName + "] should have failed because RestResponseChannel is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }
  }

  // runtimeExceptionRouterTest() helpers

  /**
   * Tests reactions of various methods of {@link AmbryBlobStorageService} to a {@link Router} that throws
   * {@link RuntimeException}.
   * @param restMethod used to determine the method to invoke in {@link AmbryBlobStorageService}.
   * @throws Exception
   */
  private void doRuntimeExceptionRouterTest(RestMethod restMethod) throws Exception {
    RestRequest restRequest = createRestRequest(restMethod, "/", null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      switch (restMethod) {
        case GET:
        case DELETE:
        case HEAD:
          doOperation(restRequest, restResponseChannel);
          fail(restMethod + " should have detected a RestServiceException because of a bad router");
          break;
        case POST:
          JSONObject headers = new JSONObject();
          setAmbryHeaders(headers, 0, Utils.Infinite_Time, false, "test-serviceID", "text/plain", "test-ownerId");
          restRequest = createRestRequest(restMethod, "/", headers, null);
          doOperation(restRequest, restResponseChannel);
          fail("POST should have detected a RestServiceException because of a bad router");
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
   * @param headers the headers of the new blob that get converted to blob properties.
   * @param content the content of the blob.
   * @return the blob ID of the blob.
   * @throws Exception
   */
  public String postBlobAndVerify(JSONObject headers, ByteBuffer content) throws Exception {
    List<ByteBuffer> contents = new LinkedList<ByteBuffer>();
    contents.add(content);
    contents.add(null);
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers, contents);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Created, restResponseChannel.getStatus());
    assertTrue("No Date header", restResponseChannel.getHeader(RestUtils.Headers.DATE) != null);
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        restResponseChannel.getHeader(RestUtils.Headers.CREATION_TIME) != null);
    assertEquals("Content-Length is not 0", "0", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    assertNull("Content-Range header should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    String blobId = restResponseChannel.getHeader(RestUtils.Headers.LOCATION);
    if (blobId == null) {
      fail("postBlobAndVerify did not return a blob ID");
    }
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param range the optional {@link ByteRange} for the request.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @throws Exception
   */
  public void getBlobAndVerify(String blobId, ByteRange range, JSONObject expectedHeaders, ByteBuffer expectedContent)
      throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, blobId, createRequestHeaders(range), null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", range == null ? ResponseStatus.Ok : ResponseStatus.PartialContent,
        restResponseChannel.getStatus());
    checkCommonGetHeadHeaders(restResponseChannel);
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match",
        expectedHeaders.getString(RestUtils.Headers.BLOB_SIZE),
        restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
    assertEquals("Content-Type does not match", expectedHeaders.getString(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertEquals("Accept-Ranges not set correctly", "bytes",
        restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    byte[] expectedContentArray = expectedContent.array();
    if (range != null) {
      long blobSize = expectedHeaders.getLong(RestUtils.Headers.BLOB_SIZE);
      assertEquals("Content-Range does not match expected",
          RestUtils.buildContentRangeAndLength(range, blobSize).getFirst(),
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
      ByteRange resolvedRange = range.toResolvedByteRange(blobSize);
      expectedContentArray = Arrays.copyOfRange(expectedContentArray, (int) resolvedRange.getStartOffset(),
          (int) resolvedRange.getEndOffset() + 1);
    } else {
      assertNull("Content-Range header should not be set",
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    }
    assertArrayEquals("GET content does not match original content", expectedContentArray,
        restResponseChannel.getResponseBody());
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the blob is not returned as blob is not modified
   * @param blobId the blob ID of the blob to GET.
   * @throws Exception
   */
  public void getNotModifiedBlobAndVerify(String blobId) throws Exception {
    JSONObject headers = new JSONObject();
    SimpleDateFormat dateFormat = new SimpleDateFormat(RestUtils.HTTP_DATE_FORMAT, Locale.ENGLISH);
    dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    Date date = new Date(System.currentTimeMillis());
    String dateStr = dateFormat.format(date);
    headers.put(RestUtils.Headers.IF_MODIFIED_SINCE, dateStr);
    RestRequest restRequest = createRestRequest(RestMethod.GET, blobId, headers, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.NotModified, restResponseChannel.getStatus());
    assertNotNull("Date header expected", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    assertNotNull("Last-Modified header expected", restResponseChannel.getHeader(RestUtils.Headers.LAST_MODIFIED));
    assertNull(RestUtils.Headers.BLOB_SIZE + " should have been null ",
        restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
    assertNull("Content-Type should have been null", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertNull("Content-Length should have been null", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    assertEquals("No content expected as blob is not modified", 0, restResponseChannel.getResponseBody().length);
    assertNull("Accept-Ranges should not be set", restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    assertNull("Content-Range header should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
  }

  /**
   * Gets the user metadata of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getUserMetadataAndVerify(String blobId, JSONObject expectedHeaders) throws Exception {
    RestRequest restRequest =
        createRestRequest(RestMethod.GET, blobId + "/" + RestUtils.SubResource.UserMetadata, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    checkCommonGetHeadHeaders(restResponseChannel);
    assertEquals("Content-Length is not 0", "0", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    assertNull("Accept-Ranges should not be set", restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    assertNull("Content-Range header should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    verifyUserMetadataHeaders(expectedHeaders, restResponseChannel);
  }

  /**
   * Gets the blob info of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getBlobInfoAndVerify(String blobId, JSONObject expectedHeaders) throws Exception {
    RestRequest restRequest =
        createRestRequest(RestMethod.GET, blobId + "/" + RestUtils.SubResource.BlobInfo, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    checkCommonGetHeadHeaders(restResponseChannel);
    assertEquals("Content-Length is not 0", "0", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    assertNull("Accept-Ranges should not be set", restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    assertNull("Content-Range header should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    verifyBlobProperties(expectedHeaders, restResponseChannel);
    verifyUserMetadataHeaders(expectedHeaders, restResponseChannel);
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param range the optional {@link ByteRange} for the request.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getHeadAndVerify(String blobId, ByteRange range, JSONObject expectedHeaders) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.HEAD, blobId, createRequestHeaders(range), null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", range == null ? ResponseStatus.Ok : ResponseStatus.PartialContent,
        restResponseChannel.getStatus());
    checkCommonGetHeadHeaders(restResponseChannel);
    assertEquals(RestUtils.Headers.CONTENT_TYPE + " does not match " + RestUtils.Headers.AMBRY_CONTENT_TYPE,
        expectedHeaders.getString(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertEquals("Accept-Ranges not set correctly", "bytes",
        restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    long contentLength = expectedHeaders.getLong(RestUtils.Headers.BLOB_SIZE);
    if (range != null) {
      Pair<String, Long> rangeAndLength = RestUtils.buildContentRangeAndLength(range, contentLength);
      assertEquals("Content-Range does not match expected", rangeAndLength.getFirst(),
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
      contentLength = rangeAndLength.getSecond();
    } else {
      assertNull("Content-Range header should not be set",
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    }
    assertEquals(RestUtils.Headers.CONTENT_LENGTH + " does not match expected", Long.toString(contentLength),
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    verifyBlobProperties(expectedHeaders, restResponseChannel);
  }

  /**
   * Verifies blob properties from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param restResponseChannel the {@link RestResponseChannel} which contains the response.
   * @throws JSONException
   */
  private void verifyBlobProperties(JSONObject expectedHeaders, MockRestResponseChannel restResponseChannel)
      throws JSONException {
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match",
        expectedHeaders.getString(RestUtils.Headers.BLOB_SIZE),
        restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
    assertEquals(RestUtils.Headers.SERVICE_ID + " does not match",
        expectedHeaders.getString(RestUtils.Headers.SERVICE_ID),
        restResponseChannel.getHeader(RestUtils.Headers.SERVICE_ID));
    assertEquals(RestUtils.Headers.PRIVATE + " does not match", expectedHeaders.getString(RestUtils.Headers.PRIVATE),
        restResponseChannel.getHeader(RestUtils.Headers.PRIVATE));
    assertEquals(RestUtils.Headers.AMBRY_CONTENT_TYPE + " does not match",
        expectedHeaders.getString(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        restResponseChannel.getHeader(RestUtils.Headers.AMBRY_CONTENT_TYPE));
    assertTrue(RestUtils.Headers.CREATION_TIME + " header missing",
        restResponseChannel.getHeader(RestUtils.Headers.CREATION_TIME) != null);
    if (expectedHeaders.getLong(RestUtils.Headers.TTL) != Utils.Infinite_Time) {
      assertEquals(RestUtils.Headers.TTL + " does not match", expectedHeaders.getString(RestUtils.Headers.TTL),
          restResponseChannel.getHeader(RestUtils.Headers.TTL));
    }
    if (expectedHeaders.has(RestUtils.Headers.OWNER_ID)) {
      assertEquals(RestUtils.Headers.OWNER_ID + " does not match",
          expectedHeaders.getString(RestUtils.Headers.OWNER_ID),
          restResponseChannel.getHeader(RestUtils.Headers.OWNER_ID));
    }
  }

  /**
   * Verifies User metadata headers from output, to that sent in during input
   * @param expectedHeaders the expected headers in the response.
   * @param restResponseChannel the {@link RestResponseChannel} which contains the response.
   * @throws JSONException
   */
  private void verifyUserMetadataHeaders(JSONObject expectedHeaders, MockRestResponseChannel restResponseChannel)
      throws JSONException {
    Iterator itr = expectedHeaders.keys();
    while (itr.hasNext()) {
      String key = (String) itr.next();
      if (key.startsWith(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX)) {
        String outValue = restResponseChannel.getHeader(key);
        assertEquals("Value for " + key + " does not match in user metadata", expectedHeaders.getString(key), outValue);
      }
    }
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned.
   * @param blobId the blob ID of the blob to DELETE.
   * @throws Exception
   */
  private void deleteBlobAndVerify(String blobId) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.DELETE, blobId, null, null);
    verifyDeleteAccepted(restRequest);
  }

  /**
   * Verifies that the right {@link ResponseStatus} is returned for GET, HEAD and DELETE once a blob is deleted.
   * @param blobId the ID of the blob that was deleted.
   * @throws Exception
   */
  private void verifyOperationsAfterDelete(String blobId) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, blobId, null, null);
    verifyGone(restRequest);

    restRequest = createRestRequest(RestMethod.HEAD, blobId, null, null);
    verifyGone(restRequest);

    restRequest = createRestRequest(RestMethod.DELETE, blobId, null, null);
    verifyDeleteAccepted(restRequest);
  }

  /**
   * Verifies that a blob is GONE after it is deleted.
   * @param restRequest the {@link RestRequest} to send to {@link AmbryBlobStorageService}.
   */
  private void verifyGone(RestRequest restRequest) throws Exception {
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      doOperation(restRequest, restResponseChannel);
      fail("Operation should have failed because blob is deleted");
    } catch (RestServiceException e) {
      assertEquals("AmbryBlobStorageService should have thrown a Deleted exception", RestServiceErrorCode.Deleted,
          e.getErrorCode());
    }
  }

  /**
   * Verifies that a request returns the right response code  once the blob has been deleted.
   * @param restRequest the {@link RestRequest} to send to {@link AmbryBlobStorageService}.
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
   * @param converterFactory the {@link IdConverterFactory} to use to while creating {@link AmbryBlobStorageService}.
   * @param expectedExceptionMsg the expected exception message.
   * @throws InstantiationException
   * @throws JSONException
   */
  private void doIdConverterExceptionTest(FrontendTestIdConverterFactory converterFactory, String expectedExceptionMsg)
      throws InstantiationException, JSONException {
    ambryBlobStorageService =
        new AmbryBlobStorageService(frontendConfig, frontendMetrics, responseHandler, router, converterFactory,
            securityServiceFactory);
    ambryBlobStorageService.start();
    doExternalServicesBadInputTest(RestMethod.values(), expectedExceptionMsg);
  }

  /**
   * Does the exception pipelining test for {@link SecurityService}.
   * @param securityFactory the {@link SecurityServiceFactory} to use to while creating {@link AmbryBlobStorageService}.
   * @param exceptionMsg the expected exception message.
   * @throws InstantiationException
   * @throws JSONException
   */
  private void doSecurityServiceExceptionTest(FrontendTestSecurityServiceFactory securityFactory, String exceptionMsg)
      throws InstantiationException, JSONException {
    for (FrontendTestSecurityServiceFactory.Mode mode : FrontendTestSecurityServiceFactory.Mode.values()) {
      securityFactory.mode = mode;
      RestMethod[] restMethods;
      if (mode.equals(FrontendTestSecurityServiceFactory.Mode.Request)) {
        restMethods = RestMethod.values();
      } else {
        restMethods = new RestMethod[3];
        restMethods[0] = RestMethod.GET;
        restMethods[1] = RestMethod.HEAD;
        restMethods[2] = RestMethod.POST;
      }
      ambryBlobStorageService =
          new AmbryBlobStorageService(frontendConfig, frontendMetrics, responseHandler, new FrontendTestRouter(),
              idConverterFactory, securityFactory);
      ambryBlobStorageService.start();
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
      if (restMethod.equals(RestMethod.UNKNOWN)) {
        continue;
      }
      JSONObject headers = new JSONObject();
      List<ByteBuffer> contents = null;
      if (restMethod.equals(RestMethod.POST)) {
        setAmbryHeaders(headers, 0, 7200, false, "doExternalServicesBadInputTest", "application/octet-stream",
            "doExternalServicesBadInputTest");
        contents = new ArrayList<ByteBuffer>(1);
        contents.add(null);
      }
      try {
        doOperation(createRestRequest(restMethod, "/", headers, contents), new MockRestResponseChannel());
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
   * @param testRouter the {@link Router} to use to while creating {@link AmbryBlobStorageService}.
   * @param exceptionMsg the expected exception message.
   * @throws Exception
   */
  private void doRouterExceptionPipelineTest(FrontendTestRouter testRouter, String exceptionMsg) throws Exception {
    ambryBlobStorageService =
        new AmbryBlobStorageService(frontendConfig, frontendMetrics, responseHandler, testRouter, idConverterFactory,
            securityServiceFactory);
    ambryBlobStorageService.start();
    for (RestMethod restMethod : RestMethod.values()) {
      switch (restMethod) {
        case HEAD:
          testRouter.exceptionOpType = FrontendTestRouter.OpType.GetBlob;
          checkRouterExceptionPipeline(exceptionMsg, createRestRequest(restMethod, "/", null, null));
          break;
        case GET:
          testRouter.exceptionOpType = FrontendTestRouter.OpType.GetBlob;
          checkRouterExceptionPipeline(exceptionMsg, createRestRequest(restMethod, "/", null, null));
          break;
        case POST:
          testRouter.exceptionOpType = FrontendTestRouter.OpType.PutBlob;
          JSONObject headers = new JSONObject();
          setAmbryHeaders(headers, 1, 7200, false, "routerExceptionPipelineTest", "application/octet-stream",
              "routerExceptionPipelineTest");
          checkRouterExceptionPipeline(exceptionMsg, createRestRequest(restMethod, "/", headers, null));
          break;
        case DELETE:
          testRouter.exceptionOpType = FrontendTestRouter.OpType.DeleteBlob;
          checkRouterExceptionPipeline(exceptionMsg, createRestRequest(restMethod, "/", null, null));
          break;
        default:
          break;
      }
    }
  }

  /**
   * Checks that the exception received by submitting {@code restRequest} to {@link AmbryBlobStorageService} matches
   * what was expected.
   * @param expectedExceptionMsg the expected exception message.
   * @param restRequest the {@link RestRequest} to submit to {@link AmbryBlobStorageService}.
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

  /**
   * Generate a {@link JSONObject} with a range header from a {@link ByteRange}
   * @param range the {@link ByteRange} to include in the headers.
   * @return the {@link JSONObject} with a range header, or null if {@code range} is null.
   * @throws Exception
   */
  private JSONObject createRequestHeaders(ByteRange range) throws Exception {
    if (range == null) {
      return null;
    }
    JSONObject requestHeaders = new JSONObject();
    requestHeaders.put(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range));
    return requestHeaders;
  }
}

/**
 * An implementation of {@link RestResponseHandler} that stores a submitted response/exception and signals the fact
 * that the response has been submitted. A single instance can handle only a single response at a time. To reuse, call
 * {@link #reset()}.
 */
class FrontendTestResponseHandler implements RestResponseHandler {
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
class FrontendTestSecurityServiceFactory implements SecurityServiceFactory {
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
class FrontendTestIdConverterFactory implements IdConverterFactory {
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
class FrontendTestRouter implements Router {
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
