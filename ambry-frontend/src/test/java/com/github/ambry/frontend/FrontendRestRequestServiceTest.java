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
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountBuilder;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapSnapshotConstants;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.protocol.GetOption;
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
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.ByteRange;
import com.github.ambry.router.ByteRanges;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ChunkInfo;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.net.ssl.SSLSession;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Unit tests for {@link FrontendRestRequestService}. Also tests {@link AccountAndContainerInjector}.
 */
public class FrontendRestRequestServiceTest {
  private final Account refAccount;
  private final Properties configProps = new Properties();
  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final FrontendMetrics frontendMetrics = new FrontendMetrics(metricRegistry);
  private final IdConverterFactory idConverterFactory;
  private final SecurityServiceFactory securityServiceFactory;
  private final FrontendTestResponseHandler responseHandler;
  private final InMemoryRouter router;
  private final MockClusterMap clusterMap;
  private final BlobId referenceBlobId;
  private final String referenceBlobIdStr;
  private final short blobIdVersion;
  private final UrlSigningService urlSigningService;
  private final IdSigningService idSigningService;
  private final String datacenterName = "Data-Center";
  private final String hostname = "localhost";
  private final String clusterName = "ambry-test";
  private FrontendConfig frontendConfig;
  private VerifiableProperties verifiableProperties;
  private boolean shouldAllowServiceIdBasedPut = true;
  private FrontendRestRequestService frontendRestRequestService;
  private Container refContainer;
  private Container refDefaultPublicContainer;
  private Container refDefaultPrivateContainer;
  private InMemAccountService accountService = new InMemAccountServiceFactory(false, true).getAccountService();
  private AccountAndContainerInjector accountAndContainerInjector;
  private final String SECURE_PATH_PREFIX = "secure-path";
  private final int CONTENT_LENGTH = 1024;

  /**
   * Sets up the {@link FrontendRestRequestService} instance before a test.
   * @throws InstantiationException
   * @throws IOException
   */
  public FrontendRestRequestServiceTest() throws Exception {
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    configProps.setProperty("frontend.allow.service.id.based.post.request",
        String.valueOf(shouldAllowServiceIdBasedPut));
    configProps.setProperty("frontend.secure.path.prefix", SECURE_PATH_PREFIX);
    configProps.setProperty("frontend.path.prefixes.to.remove", "/media");
    verifiableProperties = new VerifiableProperties(configProps);
    clusterMap = new MockClusterMap();
    clusterMap.setPermanentMetricRegistry(metricRegistry);
    frontendConfig = new FrontendConfig(verifiableProperties);
    accountAndContainerInjector = new AccountAndContainerInjector(accountService, frontendMetrics, frontendConfig);
    String endpoint = "http://localhost:1174";
    urlSigningService = new AmbryUrlSigningService(endpoint, endpoint, frontendConfig.urlSignerDefaultUrlTtlSecs,
        frontendConfig.urlSignerDefaultMaxUploadSizeBytes, frontendConfig.urlSignerMaxUrlTtlSecs,
        frontendConfig.chunkUploadInitialChunkTtlSecs, 4 * 1024 * 1024, SystemTime.getInstance());
    idSigningService = new AmbryIdSigningService();
    idConverterFactory = new AmbryIdConverterFactory(verifiableProperties, metricRegistry, idSigningService);
    securityServiceFactory =
        new AmbrySecurityServiceFactory(verifiableProperties, clusterMap, null, urlSigningService, idSigningService,
            accountAndContainerInjector);
    accountService.clear();
    accountService.updateAccounts(Collections.singleton(InMemAccountService.UNKNOWN_ACCOUNT));
    refAccount = accountService.createAndAddRandomAccount();
    for (Container container : refAccount.getAllContainers()) {
      if (container.getId() == Container.DEFAULT_PUBLIC_CONTAINER_ID) {
        refDefaultPublicContainer = container;
      } else if (container.getId() == Container.DEFAULT_PRIVATE_CONTAINER_ID) {
        refDefaultPrivateContainer = container;
      } else {
        refContainer = container;
      }
    }
    blobIdVersion = CommonTestUtils.getCurrentBlobIdVersion();
    router = new InMemoryRouter(verifiableProperties, clusterMap);
    responseHandler = new FrontendTestResponseHandler();
    frontendRestRequestService = getFrontendRestRequestService();
    referenceBlobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID,
        Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID,
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
    referenceBlobIdStr = referenceBlobId.getID();
    responseHandler.start();
    frontendRestRequestService.start();
  }

  /**
   * Shuts down the {@link FrontendRestRequestService} instance after all tests.
   */
  @After
  public void shutdownFrontendRestRequestService() {
    frontendRestRequestService.shutdown();
    responseHandler.shutdown();
    router.close();
  }

  /**
   * Tests basic startup and shutdown functionality (no exceptions).
   * @throws InstantiationException
   */
  @Test
  public void startShutDownTest() throws InstantiationException {
    frontendRestRequestService.start();
    frontendRestRequestService.shutdown();
  }

  /**
   * Start {@link FrontendRestRequestService} without {@link RestResponseHandler} should fail.
   * @throws InstantiationException
   */
  @Test
  public void startWithoutResponseHandler() throws InstantiationException {
    FrontendRestRequestService frontendRestRequestService =
        new FrontendRestRequestService(frontendConfig, frontendMetrics, router, clusterMap, idConverterFactory,
            securityServiceFactory, urlSigningService, idSigningService, accountService, accountAndContainerInjector,
            datacenterName, hostname, clusterName);
    try {
      frontendRestRequestService.start();
      fail("Test should fail if ResponseHandler is not setup");
    } catch (InstantiationException e) {
    }
    frontendRestRequestService.setupResponseHandler(responseHandler);
    frontendRestRequestService.start();
    frontendRestRequestService.shutdown();
  }

  /**
   * Tests for {@link FrontendRestRequestService#shutdown()} when {@link FrontendRestRequestService#start()} has not been
   * called previously.
   * <p/>
   * This test is for  cases where {@link FrontendRestRequestService#start()} has failed and
   * {@link FrontendRestRequestService#shutdown()} needs to be run.
   */
  @Test
  public void shutdownWithoutStartTest() {
    FrontendRestRequestService frontendRestRequestService = getFrontendRestRequestService();
    frontendRestRequestService.shutdown();
  }

  /**
   * This tests for exceptions thrown when an {@link FrontendRestRequestService} instance is used without calling
   * {@link FrontendRestRequestService#start()} first.
   * @throws Exception
   */
  @Test
  public void useServiceWithoutStartTest() throws Exception {
    frontendRestRequestService = getFrontendRestRequestService();
    frontendRestRequestService.setupResponseHandler(responseHandler);
    // not fine to use without start.
    for (RestMethod method : RestMethod.values()) {
      if (method.equals(RestMethod.UNKNOWN)) {
        continue;
      }
      verifyOperationFailure(createRestRequest(method, "/", null, null), RestServiceErrorCode.ServiceUnavailable);
    }
  }

  /**
   * Checks for reactions of all methods in {@link FrontendRestRequestService} to null arguments.
   * @throws Exception
   */
  @Test
  public void nullInputsForFunctionsTest() throws Exception {
    doNullInputsForFunctionsTest("handleGet");
    doNullInputsForFunctionsTest("handlePost");
    doNullInputsForFunctionsTest("handleDelete");
    doNullInputsForFunctionsTest("handleHead");
    doNullInputsForFunctionsTest("handleOptions");
    doNullInputsForFunctionsTest("handlePut");
  }

  /**
   * Checks reactions of all methods in {@link FrontendRestRequestService} to a {@link Router} that throws
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
    // PUT is tested in the individual handlers
  }

  /**
   * Checks reactions of PUT methods in {@link FrontendRestRequestService} when there are bad request parameters
   * @throws Exception
   */
  @Test
  public void putFailureTest() throws Exception {
    // unrecognized operation
    RestRequest restRequest = createRestRequest(RestMethod.PUT, "/non-existent-op", null, null);
    verifyOperationFailure(restRequest, RestServiceErrorCode.BadRequest);
  }

  /**
   * Checks reactions of all methods in {@link FrontendRestRequestService} to bad {@link RestResponseHandler} and
   * {@link RestRequest} implementations.
   * @throws Exception
   */
  @Test
  public void badResponseHandlerAndRestRequestTest() throws Exception {
    // What happens inside FrontendRestRequestService during this test?
    // 1. Since the RestRequest throws errors, FrontendRestRequestService will attempt to submit response with exception
    //      to FrontendTestResponseHandler.
    // 2. The submission will fail because FrontendTestResponseHandler has been shutdown.
    // 3. FrontendRestRequestService will directly complete the request over the RestResponseChannel with the *original*
    //      exception.
    // 4. It will then try to release resources but closing the RestRequest will also throw an exception. This exception
    //      is swallowed.
    // What the test is looking for -> No exceptions thrown when the handle is run and the original exception arrives
    // safely.
    responseHandler.shutdown();
    for (String methodName : new String[]{"handleGet", "handlePost", "handleHead", "handleDelete", "handleOptions",
        "handlePut"}) {
      Method method =
          FrontendRestRequestService.class.getDeclaredMethod(methodName, RestRequest.class, RestResponseChannel.class);
      responseHandler.reset();
      RestRequest restRequest = new BadRestRequest();
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      method.invoke(frontendRestRequestService, restRequest, restResponseChannel);
      Exception e = restResponseChannel.getException();
      assertTrue("Unexpected exception", e instanceof IllegalStateException || e instanceof NullPointerException);
    }
  }

  /**
   * Tests
   * {@link FrontendRestRequestService#submitResponse(RestRequest, RestResponseChannel, ReadableStreamChannel, Exception)}.
   * @throws JSONException
   * @throws UnsupportedEncodingException
   * @throws URISyntaxException
   */
  @Test
  public void submitResponseTest() throws JSONException, UnsupportedEncodingException, URISyntaxException {
    String exceptionMsg = TestUtils.getRandomString(10);
    responseHandler.shutdown();
    // handleResponse of FrontendTestResponseHandler throws exception because it has been shutdown.
    try {
      // there is an exception already.
      RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      assertTrue("RestRequest channel is not open", restRequest.isOpen());
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      frontendRestRequestService.submitResponse(restRequest, restResponseChannel, null,
          new RuntimeException(exceptionMsg));
      assertEquals("Unexpected exception message", exceptionMsg, restResponseChannel.getException().getMessage());

      // there is no exception and exception thrown when the response is submitted.
      restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      assertTrue("RestRequest channel is not open", restRequest.isOpen());
      restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel response = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
      assertTrue("Response channel is not open", response.isOpen());
      frontendRestRequestService.submitResponse(restRequest, restResponseChannel, response, null);
      assertNotNull("There is no cause of failure", restResponseChannel.getException());
      // resources should have been cleaned up.
      assertFalse("Response channel is not cleaned up", response.isOpen());
    } finally {
      frontendRestRequestService.setupResponseHandler(responseHandler);
      responseHandler.start();
    }

    // verify tracking infos are attached accordingly.
    RestRequest restRequest;
    MockRestResponseChannel restResponseChannel;
    for (String header : RestUtils.TrackingHeaders.TRACKING_HEADERS) {
      restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      restResponseChannel = new MockRestResponseChannel();
      frontendRestRequestService.submitResponse(restRequest, restResponseChannel, null, null);
      assertTrue("Response header should not contain tracking info", restResponseChannel.getHeader(header) == null);
    }
    restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    restRequest.setArg(RestUtils.InternalKeys.SEND_TRACKING_INFO, new Boolean(true));
    restResponseChannel = new MockRestResponseChannel();
    frontendRestRequestService.submitResponse(restRequest, restResponseChannel, null, null);
    assertEquals("Unexpected or missing tracking info", datacenterName,
        restResponseChannel.getHeader(RestUtils.TrackingHeaders.DATACENTER_NAME));
    assertEquals("Unexpected or missing tracking info", hostname,
        restResponseChannel.getHeader(RestUtils.TrackingHeaders.FRONTEND_NAME));
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
      frontendRestRequestService.submitResponse(restRequest, restResponseChannel, channel, null);
      assertFalse("ReadableStreamChannel is still open", channel.isOpen());

      // null ReadableStreamChannel
      restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      restResponseChannel = new MockRestResponseChannel();
      assertTrue("RestRequest channel not open", restRequest.isOpen());
      frontendRestRequestService.submitResponse(restRequest, restResponseChannel, null, null);

      // bad RestRequest (close() throws IOException)
      channel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
      restResponseChannel = new MockRestResponseChannel();
      assertTrue("ReadableStreamChannel not open", channel.isOpen());
      frontendRestRequestService.submitResponse(new BadRestRequest(), restResponseChannel, channel, null);

      // bad ReadableStreamChannel (close() throws IOException)
      restRequest = createRestRequest(RestMethod.GET, "/", null, null);
      restResponseChannel = new MockRestResponseChannel();
      assertTrue("RestRequest channel not open", restRequest.isOpen());
      frontendRestRequestService.submitResponse(restRequest, restResponseChannel, new BadRSC(), null);
    } finally {
      frontendRestRequestService.setupResponseHandler(responseHandler);
      responseHandler.start();
    }
  }

  /**
   * Tests blob POST, GET, HEAD, TTL update and DELETE operations.
   * @throws Exception
   */
  @Test
  public void postGetHeadUpdateDeleteTest() throws Exception {
    // add another account
    accountService.createAndAddRandomAccount();
    // valid account and container names passed as part of POST
    for (Account testAccount : accountService.getAllAccounts()) {
      if (testAccount.getId() != Account.UNKNOWN_ACCOUNT_ID) {
        for (Container container : testAccount.getAllContainers()) {
          doPostGetHeadUpdateDeleteTest(testAccount, container, testAccount.getName(), !container.isCacheable(),
              testAccount, container);
          doConditionalUpdateAndDeleteTest(testAccount, container, testAccount.getName());
        }
      }
    }
    // valid account and container names but only serviceId passed as part of POST
    doPostGetHeadUpdateDeleteTest(null, null, refAccount.getName(), false, refAccount, refDefaultPublicContainer);
    doPostGetHeadUpdateDeleteTest(null, null, refAccount.getName(), true, refAccount, refDefaultPrivateContainer);
    // unrecognized serviceId
    doPostGetHeadUpdateDeleteTest(null, null, "unknown_service_id", false, InMemAccountService.UNKNOWN_ACCOUNT,
        Container.DEFAULT_PUBLIC_CONTAINER);
    doPostGetHeadUpdateDeleteTest(null, null, "unknown_service_id", true, InMemAccountService.UNKNOWN_ACCOUNT,
        Container.DEFAULT_PRIVATE_CONTAINER);
  }

  /**
   * Tests injecting target {@link Account} and {@link Container} for PUT requests. The {@link AccountService} is
   * prepopulated with a reference account and {@link InMemAccountService#UNKNOWN_ACCOUNT}. The expected behavior should be:
   *
   * <pre>
   *   accountHeader    containerHeader   serviceIdHeader     expected Error      injected account      injected container
   *    null             null              "someServiceId"     null                UNKNOWN_ACCOUNT       UNKNOWN_CONTAINER
   *    null             nonExistName      "someServiceId"     MissingArgs         null                  null
   *    null             C#UNKOWN          "someServiceId"     InvalidContainer    null                  null
   *    null             realCntName       "someServiceId"     MissingArgs         null                  null
   *    A#UNKNOWN        null              "someServiceId"     InvalidAccount      null                  null
   *    A#UNKNOWN        nonExistName      "someServiceId"     InvalidAccount      null                  null
   *    A#UNKNOWN        C#UNKOWN          "someServiceId"     InvalidAccount      null                  null
   *    A#UNKNOWN        realCntName       "someServiceId"     InvalidAccount      null                  null
   *    realAcctName     null              "someServiceId"     MissingArgs         null                  null
   *    realAcctName     nonExistName      "someServiceId"     InvalidContainer    null                  null
   *    realAcctName     C#UNKOWN          "someServiceId"     InvalidContainer    null                  null
   *    realAcctName     realCntName       "someServiceId"     null                realAccount           realContainer
   *    nonExistName     null              "someServiceId"     MissingArgs         null                  null
   *    nonExistName     nonExistName      "someServiceId"     InvalidAccount      null                  null
   *    nonExistName     C#UNKOWN          "someServiceId"     InvalidAccount      null                  null
   *    nonExistName     realCntName       "someServiceId"     InvalidAccount      null                  null
   *    null             null              A#UNKNOWN           InvalidAccount      null                  null
   *    null             nonExistName      A#UNKNOWN           InvalidAccount      null                  null
   *    null             C#UNKOWN          A#UNKNOWN           InvalidAccount      null                  null
   *    null             realCntName       A#UNKNOWN           InvalidAccount      null                  null
   *    null             null              realAcctName        InvalidContainer    null                  null     Note: The account does not have the two default containers for legacy public and private blobs.
   *    null             nonExistName      realAcctName        MissingArgs         null                  null
   *    null             C#UNKOWN          realAcctName        InvalidContainer    null                  null
   *    null             realCntName       realAcctName        MissingArgs         null                  null
   *    null             null              realAcctName        null                realAccount           default pub/private ctn     Note: The account has the two default containers for legacy public and private blobs.
   * </pre>
   * @throws Exception
   */
  @Test
  public void injectionAccountAndContainerForPostTest() throws Exception {
    injectAccountAndContainerForPostAndVerify(refDefaultPrivateContainer, true);
    injectAccountAndContainerForPostAndVerify(refDefaultPrivateContainer, false);
    injectAccountAndContainerForPostAndVerify(refDefaultPublicContainer, true);
    injectAccountAndContainerForPostAndVerify(refDefaultPublicContainer, false);
  }

  /**
   * Tests injecting target {@link Account} and {@link Container} for GET/HEAD/DELETE blobId string in {@link BlobId#BLOB_ID_V2}.
   * The {@link AccountService} is prepopulated with a reference account and {@link InMemAccountService#UNKNOWN_ACCOUNT}. The expected
   * behavior should be:
   *
   * <pre>
   *   AId in blobId    CId in blobId     expected Error      injected account      injected container
   *    realAId           realCId          NotFound            refAccount            refContainer       This can succeed if the blob exists in backend.
   *    realAId           UNKNOWN          InvalidContainer    null                  null
   *    realAId           nonExistCId      InvalidContainer    null                  null
   *    UNKNOWN           realCId          InvalidContainer    null                  null
   *    UNKNOWN           UNKNOWN          NotFound            UNKNOWN               UNKNOWN            This can succeed if the blob exists in backend.
   *    UNKNOWN           nonExistCId      InvalidContainer    null                  null
   *    nonExistAId       realCId          InvalidAccount      null                  null
   *    nonExistAId       UNKNOWN          InvalidAccount      null                  null
   *    nonExistAId       nonExistCId      InvalidAccount      null                  null
   * </pre>
   *
   * @throws Exception
   */
  @Test
  public void injectionAccountAndContainerForGetHeadDeleteBlobIdTest() throws Exception {
    List<Short> blobIdVersions = Arrays.stream(BlobId.getAllValidVersions())
        .filter(version -> version >= BlobId.BLOB_ID_V2)
        .collect(Collectors.toList());
    for (short version : blobIdVersions) {
      populateAccountService();

      // aid=refAId, cid=refCId
      String blobId =
          new BlobId(version, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, refAccount.getId(),
              refContainer.getId(), clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0),
              false, BlobId.BlobDataType.DATACHUNK).getID();
      verifyAccountAndContainerFromBlobId(blobId, refAccount, refContainer, RestServiceErrorCode.NotFound);

      // aid=refAId, cid=unknownCId
      blobId = new BlobId(version, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, refAccount.getId(),
          Container.UNKNOWN_CONTAINER_ID,
          clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
          BlobId.BlobDataType.DATACHUNK).getID();
      verifyAccountAndContainerFromBlobId(blobId, null, null, RestServiceErrorCode.InvalidContainer);

      // aid=refAId, cid=nonExistCId
      blobId = new BlobId(version, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, refAccount.getId(),
          (short) -1234, clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
          BlobId.BlobDataType.DATACHUNK).getID();
      verifyAccountAndContainerFromBlobId(blobId, null, null, RestServiceErrorCode.InvalidContainer);

      // aid=unknownAId, cid=refCId
      blobId = new BlobId(version, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID,
          Account.UNKNOWN_ACCOUNT_ID, refContainer.getId(),
          clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
          BlobId.BlobDataType.DATACHUNK).getID();
      verifyAccountAndContainerFromBlobId(blobId, null, null, RestServiceErrorCode.InvalidContainer);

      // aid=unknownAId, cid=unknownCId
      blobId = new BlobId(version, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID,
          Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID,
          clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
          BlobId.BlobDataType.DATACHUNK).getID();
      verifyAccountAndContainerFromBlobId(blobId, InMemAccountService.UNKNOWN_ACCOUNT, Container.UNKNOWN_CONTAINER,
          RestServiceErrorCode.NotFound);

      // aid=unknownAId, cid=nonExistCId
      blobId = new BlobId(version, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID,
          Account.UNKNOWN_ACCOUNT_ID, (short) -1234,
          clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
          BlobId.BlobDataType.DATACHUNK).getID();
      verifyAccountAndContainerFromBlobId(blobId, null, null, RestServiceErrorCode.InvalidContainer);

      // aid=nonExistAId, cid=refCId
      blobId = new BlobId(version, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, (short) -1234,
          refContainer.getId(), clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0),
          false, BlobId.BlobDataType.DATACHUNK).getID();
      verifyAccountAndContainerFromBlobId(blobId, null, null, RestServiceErrorCode.InvalidAccount);

      // aid=nonExistAId, cid=unknownCId
      blobId = new BlobId(version, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, (short) -1234,
          Container.UNKNOWN_CONTAINER_ID,
          clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
          BlobId.BlobDataType.DATACHUNK).getID();
      verifyAccountAndContainerFromBlobId(blobId, null, null, RestServiceErrorCode.InvalidAccount);

      // aid=nonExistAId, cid=nonExistCId
      blobId = new BlobId(version, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID, (short) -1234,
          (short) -11, clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
          BlobId.BlobDataType.DATACHUNK).getID();
      verifyAccountAndContainerFromBlobId(blobId, null, null, RestServiceErrorCode.InvalidAccount);
    }
  }

  /**
   * Tests injecting target {@link Account} and {@link Container} for GET/HEAD/DELETE blobId string in {@link BlobId#BLOB_ID_V1}.
   * The {@link AccountService} is prepopulated with a reference account and {@link InMemAccountService#UNKNOWN_ACCOUNT}. The expected
   * behavior should be:
   * <pre>
   *   AId in blobId    CId in blobId     expected Error      injected account      injected container
   *    UNKNOWN           UNKNOWN          NotFound            UNKNOWN               UNKNOWN            This can succeed if the blob exists in backend.
   * </pre>
   * @throws Exception
   */
  @Test
  public void injectionAccountAndContainerForGetHeadDeleteBlobIdV1Test() throws Exception {
    populateAccountService();
    // it does not matter what AID and CID are supplied when constructing blobId in v1.
    // expect unknown account and container for v1 blob IDs that went through request processing only.
    String blobId = new BlobId(BlobId.BLOB_ID_V1, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID,
        refAccount.getId(), refContainer.getId(),
        clusterMap.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK).getID();
    verifyAccountAndContainerFromBlobId(blobId, InMemAccountService.UNKNOWN_ACCOUNT, Container.UNKNOWN_CONTAINER,
        RestServiceErrorCode.NotFound);

    // test response path account injection for V1 blob IDs
    // public blob with service ID that does not correspond to a valid account
    verifyResponsePathAccountAndContainerInjection(refAccount.getName() + "extra", false,
        InMemAccountService.UNKNOWN_ACCOUNT, Container.DEFAULT_PUBLIC_CONTAINER);
    // private blob with service ID that does not correspond to a valid account
    verifyResponsePathAccountAndContainerInjection(refAccount.getName() + "extra", true,
        InMemAccountService.UNKNOWN_ACCOUNT, Container.DEFAULT_PRIVATE_CONTAINER);
    // public blob with service ID that corresponds to a valid account
    verifyResponsePathAccountAndContainerInjection(refAccount.getName(), false, refAccount, refDefaultPublicContainer);
    // private blob with service ID that corresponds to a valid account
    verifyResponsePathAccountAndContainerInjection(refAccount.getName(), true, refAccount, refDefaultPrivateContainer);
  }

  /**
   * Tests a corner case when {@link Account} inquired from {@link AccountService} has a name that does not match the
   * target account name set by the request.
   * @throws Exception
   */
  @Test
  public void accountNameMismatchTest() throws Exception {
    accountService = new InMemAccountServiceFactory(true, false).getAccountService();
    accountAndContainerInjector = new AccountAndContainerInjector(accountService, frontendMetrics, frontendConfig);
    frontendRestRequestService = getFrontendRestRequestService();
    frontendRestRequestService.setupResponseHandler(responseHandler);
    frontendRestRequestService.start();
    postBlobAndVerifyWithAccountAndContainer(refAccount.getName(), refContainer.getName(), "serviceId",
        !refContainer.isCacheable(), null, null, RestServiceErrorCode.InternalServerError);
  }

  /**
   * Tests how metadata that has not been POSTed in the form of headers is returned.
   * @throws Exception
   */
  @Test
  public void oldStyleUserMetadataTest() throws Exception {
    ByteBuffer content = ByteBuffer.allocate(0);
    BlobProperties blobProperties =
        new BlobProperties(0, "userMetadataTestOldStyleServiceID", Account.UNKNOWN_ACCOUNT_ID,
            Container.UNKNOWN_CONTAINER_ID, false);
    byte[] usermetadata = TestUtils.getRandomBytes(25);
    String blobId = router.putBlob(blobProperties, usermetadata, new ByteBufferReadableStreamChannel(content),
        new PutBlobOptionsBuilder().build()).get();

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
    String exceptionMsg = TestUtils.getRandomString(10);
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
    String exceptionMsg = TestUtils.getRandomString(10);
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
    String exceptionMsg = TestUtils.getRandomString(10);
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
    String exceptionMsg = TestUtils.getRandomString(10);
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
    String exceptionMsg = TestUtils.getRandomString(10);
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
    String exceptionMsg = TestUtils.getRandomString(10);
    testRouter.exceptionToReturn = new RouterException(exceptionMsg, RouterErrorCode.UnexpectedInternalError);
    doRouterExceptionPipelineTest(testRouter, exceptionMsg + " Error: " + RouterErrorCode.UnexpectedInternalError);
  }

  /**
   * Test that GET operations fail with the expected error code when a bad range header is provided.
   * @throws Exception
   */
  @Test
  public void badRangeHeaderTest() throws Exception {
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.Headers.RANGE, "adsfksakdfsdfkdaklf");
    verifyOperationFailure(createRestRequest(RestMethod.GET, "/", headers, null), RestServiceErrorCode.InvalidArgs);
  }

  /**
   * Tests put requests with prohibited headers.
   * @throws Exception
   */
  @Test
  public void badPutRequestWithProhibitedHeadersTest() throws Exception {
    putRequestWithProhibitedHeader(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY);
    putRequestWithProhibitedHeader(RestUtils.InternalKeys.TARGET_CONTAINER_KEY);
  }

  /**
   * Test that the correct service ID is sent to the router on deletes.
   * @throws Exception
   */
  @Test
  public void deleteServiceIdTest() throws Exception {
    FrontendTestRouter testRouter = new FrontendTestRouter();
    frontendRestRequestService =
        new FrontendRestRequestService(frontendConfig, frontendMetrics, testRouter, clusterMap, idConverterFactory,
            securityServiceFactory, urlSigningService, idSigningService, accountService, accountAndContainerInjector,
            datacenterName, hostname, clusterName);
    frontendRestRequestService.setupResponseHandler(responseHandler);
    frontendRestRequestService.start();
    JSONObject headers = new JSONObject();
    String serviceId = "service-id";
    headers.put(RestUtils.Headers.SERVICE_ID, serviceId);
    doOperation(createRestRequest(RestMethod.DELETE, referenceBlobIdStr, headers, null), new MockRestResponseChannel());
    assertEquals(serviceId, testRouter.deleteServiceId);
    doOperation(createRestRequest(RestMethod.DELETE, referenceBlobIdStr, null, null), new MockRestResponseChannel());
    assertNull("Service ID should not have been set for this delete", testRouter.deleteServiceId);
  }

  /**
   * Tests the handling of {@link Operations#GET_PEERS} requests.
   * @throws Exception
   */
  @Test
  public void getPeersTest() throws Exception {
    frontendRestRequestService.shutdown();
    TailoredPeersClusterMap clusterMap = new TailoredPeersClusterMap();
    frontendRestRequestService =
        new FrontendRestRequestService(frontendConfig, frontendMetrics, router, clusterMap, idConverterFactory,
            securityServiceFactory, urlSigningService, idSigningService, accountService, accountAndContainerInjector,
            datacenterName, hostname, clusterName);
    frontendRestRequestService.setupResponseHandler(responseHandler);
    frontendRestRequestService.start();
    // test good requests
    for (String datanode : TailoredPeersClusterMap.DATANODE_NAMES) {
      String[] parts = datanode.split(":");
      String baseUri = Operations.GET_PEERS + "?" + GetPeersHandler.NAME_QUERY_PARAM + "=" + parts[0] + "&"
          + GetPeersHandler.PORT_QUERY_PARAM + "=" + parts[1];
      String[] uris = {baseUri, "/" + baseUri};
      for (String uri : uris) {
        MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
        doOperation(createRestRequest(RestMethod.GET, uri, null, null), restResponseChannel);
        byte[] peerStrBytes = restResponseChannel.getResponseBody();
        Set<String> peersFromResponse =
            GetPeersHandlerTest.getPeersFromResponse(new JSONObject(new String(peerStrBytes)));
        Set<String> expectedPeers = clusterMap.getPeers(datanode);
        assertEquals("Peer list returned does not match expected for " + datanode, expectedPeers, peersFromResponse);
      }
    }
    // test one bad request
    RestRequest restRequest = createRestRequest(RestMethod.GET, Operations.GET_PEERS, null, null);
    verifyOperationFailure(restRequest, RestServiceErrorCode.MissingArgs);
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
    List<? extends PartitionId> partitionIds = clusterMap.getWritablePartitionIds(null);
    for (PartitionId partitionId : partitionIds) {
      String originalReplicaStr = partitionId.getReplicaIds().toString().replace(", ", ",");
      BlobId blobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, ClusterMapUtils.UNKNOWN_DATACENTER_ID,
          Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, partitionId, false,
          BlobId.BlobDataType.DATACHUNK);
      RestRequest restRequest =
          createRestRequest(RestMethod.GET, blobId.getID() + "/" + RestUtils.SubResource.Replicas, null, null);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      doOperation(restRequest, restResponseChannel);
      JSONObject response = new JSONObject(new String(restResponseChannel.getResponseBody()));
      String returnedReplicasStr = response.get(GetReplicasHandler.REPLICAS_KEY).toString().replace("\"", "");
      assertEquals("Replica IDs returned for the BlobId do no match with the replicas IDs of partition",
          originalReplicaStr, returnedReplicasStr);
    }
  }

  /**
   * Tests the handling of {@link Operations#GET_CLUSTER_MAP_SNAPSHOT} requests.
   * @throws Exception
   */
  @Test
  public void getClusterMapSnapshotTest() throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, Operations.GET_CLUSTER_MAP_SNAPSHOT, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    JSONObject expected = clusterMap.getSnapshot();
    JSONObject actual = new JSONObject(new String(restResponseChannel.getResponseBody()));
    // remove timestamps because they may differ
    expected.remove(ClusterMapSnapshotConstants.TIMESTAMP_MS);
    actual.remove(ClusterMapSnapshotConstants.TIMESTAMP_MS);
    assertEquals("Snapshot does not match expected", expected.toString(), actual.toString());

    // test a failure to ensure that it goes through the exception path
    String msg = TestUtils.getRandomString(10);
    clusterMap.setExceptionOnSnapshot(new RuntimeException(msg));
    restRequest = createRestRequest(RestMethod.GET, Operations.GET_CLUSTER_MAP_SNAPSHOT, null, null);
    try {
      doOperation(restRequest, new MockRestResponseChannel());
      fail("Operation should have failed");
    } catch (RuntimeException e) {
      assertEquals("Exception not as expected", msg, e.getMessage());
    }
    clusterMap.setExceptionOnSnapshot(null);
  }

  /**
   * Tests the handling of {@link Operations#ACCOUNTS} get requests.
   * @throws Exception
   */
  @Test
  public void getAccountsTest() throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, Operations.ACCOUNTS, null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    Set<Account> expected = new HashSet<>(accountService.getAllAccounts());
    Set<Account> actual = new HashSet<>(
        AccountCollectionSerde.fromJson(new JSONObject(new String(restResponseChannel.getResponseBody()))));
    assertEquals("Unexpected GET /accounts response", expected, actual);

    // test an account not found case to ensure that it goes through the exception path
    restRequest = createRestRequest(RestMethod.GET, Operations.ACCOUNTS,
        new JSONObject().put(RestUtils.Headers.TARGET_ACCOUNT_ID, accountService.generateRandomAccount().getId()),
        null);
    try {
      doOperation(restRequest, new MockRestResponseChannel());
      fail("Operation should have failed");
    } catch (RestServiceException e) {
      assertEquals("Error code not as expected", RestServiceErrorCode.NotFound, e.getErrorCode());
    }
  }

  /**
   * Tests the handling of {@link Operations#ACCOUNTS} post requests.
   * @throws Exception
   */
  @Test
  public void postAccountsTest() throws Exception {
    Account accountToAdd = accountService.generateRandomAccount();
    List<ByteBuffer> body = new LinkedList<>();
    body.add(ByteBuffer.wrap(AccountCollectionSerde.toJson(Collections.singleton(accountToAdd))
        .toString()
        .getBytes(StandardCharsets.UTF_8)));
    body.add(null);
    RestRequest restRequest = createRestRequest(RestMethod.POST, Operations.ACCOUNTS, null, body);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Account not created correctly", accountToAdd, accountService.getAccountById(accountToAdd.getId()));

    // test an invalid request case to ensure that it goes through the exception path
    body = new LinkedList<>();
    body.add(ByteBuffer.wrap("abcdefghijk".toString().getBytes(StandardCharsets.UTF_8)));
    body.add(null);
    restRequest = createRestRequest(RestMethod.POST, Operations.ACCOUNTS, null, body);
    try {
      doOperation(restRequest, new MockRestResponseChannel());
      fail("Operation should have failed");
    } catch (RestServiceException e) {
      assertEquals("Error code not as expected", RestServiceErrorCode.BadRequest, e.getErrorCode());
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
    RestRequest restRequest = createRestRequest(RestMethod.GET, "12345/" + RestUtils.SubResource.Replicas, null, null);
    verifyOperationFailure(restRequest, RestServiceErrorCode.BadRequest);

    // bad input - invalid blob id for this cluster map.
    String blobId = "AAEAAQAAAAAAAADFAAAAJDMyYWZiOTJmLTBkNDYtNDQyNS1iYzU0LWEwMWQ1Yzg3OTJkZQ.gif";
    restRequest = createRestRequest(RestMethod.GET, blobId + "/" + RestUtils.SubResource.Replicas, null, null);
    verifyOperationFailure(restRequest, RestServiceErrorCode.BadRequest);
  }

  /**
   * Tests the handling of {@link Operations#GET_SIGNED_URL} requests.
   * @throws Exception
   */
  @Test
  public void getAndUseSignedUrlTest() throws Exception {
    // setup
    int contentLength = 10;
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(contentLength));
    long blobTtl = 7200;
    String serviceId = "getAndUseSignedUrlTest";
    String contentType = "application/octet-stream";
    String ownerId = "getAndUseSignedUrlTest";
    JSONObject headers = new JSONObject();
    headers.put(RestUtils.Headers.URL_TYPE, RestMethod.POST.name());
    setAmbryHeadersForPut(headers, blobTtl, !refContainer.isCacheable(), serviceId, contentType, ownerId,
        refAccount.getName(), refContainer.getName());
    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
    RestUtilsTest.setUserMetadataHeaders(headers, userMetadata);

    // POST
    // Get signed URL
    RestRequest getSignedUrlRequest = createRestRequest(RestMethod.GET, Operations.GET_SIGNED_URL, headers, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(getSignedUrlRequest, restResponseChannel);
    assertEquals("Account not as expected", refAccount,
        getSignedUrlRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY));
    assertEquals("Container not as expected", refContainer,
        getSignedUrlRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY));
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    String signedPostUrl = restResponseChannel.getHeader(RestUtils.Headers.SIGNED_URL);
    assertNotNull("Did not get a signed POST URL", signedPostUrl);

    // Use signed URL to POST
    List<ByteBuffer> contents = new LinkedList<>();
    contents.add(content);
    contents.add(null);
    RestRequest postSignedRequest = createRestRequest(RestMethod.POST, signedPostUrl, null, contents);
    restResponseChannel = new MockRestResponseChannel();
    doOperation(postSignedRequest, restResponseChannel);
    String blobId = verifyPostAndReturnBlobId(postSignedRequest, restResponseChannel, refAccount, refContainer);

    // verify POST
    headers.put(RestUtils.Headers.BLOB_SIZE, contentLength);
    getBlobAndVerify(blobId, null, null, headers, content, refAccount, refContainer);
    getBlobInfoAndVerify(blobId, null, headers, refAccount, refContainer);

    // GET
    // Get signed URL
    JSONObject getHeaders = new JSONObject();
    getHeaders.put(RestUtils.Headers.URL_TYPE, RestMethod.GET.name());
    blobId = blobId.startsWith("/") ? blobId.substring(1) : blobId;
    getHeaders.put(RestUtils.Headers.BLOB_ID, blobId);
    getSignedUrlRequest = createRestRequest(RestMethod.GET, Operations.GET_SIGNED_URL, getHeaders, null);
    restResponseChannel = new MockRestResponseChannel();
    doOperation(getSignedUrlRequest, restResponseChannel);
    assertEquals("Account not as expected", refAccount,
        getSignedUrlRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY));
    assertEquals("Container not as expected", refContainer,
        getSignedUrlRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY));
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    String signedGetUrl = restResponseChannel.getHeader(RestUtils.Headers.SIGNED_URL);
    assertNotNull("Did not get a signed GET URL", signedGetUrl);

    // Use URL to GET blob
    RestRequest getSignedRequest = createRestRequest(RestMethod.GET, signedGetUrl, null, null);
    restResponseChannel = new MockRestResponseChannel();
    doOperation(getSignedRequest, restResponseChannel);
    verifyGetBlobResponse(getSignedRequest, restResponseChannel, null, headers, content, refAccount, refContainer);

    // one error scenario to exercise exception path
    verifyOperationFailure(createRestRequest(RestMethod.GET, Operations.GET_SIGNED_URL, null, null),
        RestServiceErrorCode.MissingArgs);
  }

  /**
   * Tests for handling of {@link RestMethod#OPTIONS}.
   * @throws Exception
   */
  @Test
  public void optionsTest() throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.OPTIONS, "/", null, null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertTrue("No Date header", restResponseChannel.getHeader(RestUtils.Headers.DATE) != null);
    assertEquals("Unexpected content length", 0,
        Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
    assertEquals("Unexpected value for " + RestUtils.Headers.ACCESS_CONTROL_ALLOW_METHODS,
        frontendConfig.optionsAllowMethods,
        restResponseChannel.getHeader(RestUtils.Headers.ACCESS_CONTROL_ALLOW_METHODS));
    assertEquals("Unexpected value for " + RestUtils.Headers.ACCESS_CONTROL_MAX_AGE,
        frontendConfig.optionsValiditySeconds,
        Long.parseLong(restResponseChannel.getHeader(RestUtils.Headers.ACCESS_CONTROL_MAX_AGE)));
  }

  /**
   * Tests the case when the TTL update is rejected
   * @throws Exception
   */
  @Test
  public void updateTtlRejectedTest() throws Exception {
    FrontendTestRouter testRouter = new FrontendTestRouter();
    String exceptionMsg = TestUtils.getRandomString(10);
    testRouter.exceptionToReturn = new RouterException(exceptionMsg, RouterErrorCode.BlobUpdateNotAllowed);
    testRouter.exceptionOpType = FrontendTestRouter.OpType.UpdateBlobTtl;
    frontendRestRequestService =
        new FrontendRestRequestService(frontendConfig, frontendMetrics, testRouter, clusterMap, idConverterFactory,
            securityServiceFactory, urlSigningService, idSigningService, accountService, accountAndContainerInjector,
            datacenterName, hostname, clusterName);
    frontendRestRequestService.setupResponseHandler(responseHandler);
    frontendRestRequestService.start();
    String blobId = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, (byte) -1, Account.UNKNOWN_ACCOUNT_ID,
        Container.UNKNOWN_CONTAINER_ID, clusterMap.getAllPartitionIds(null).get(0), false,
        BlobId.BlobDataType.DATACHUNK).getID();
    JSONObject headers = new JSONObject();
    setUpdateTtlHeaders(headers, blobId, "updateTtlRejectedTest");
    RestRequest restRequest = createRestRequest(RestMethod.PUT, Operations.UPDATE_TTL, headers, null);
    MockRestResponseChannel restResponseChannel = verifyOperationFailure(restRequest, RestServiceErrorCode.NotAllowed);
    assertEquals("Unexpected response status", ResponseStatus.MethodNotAllowed, restResponseChannel.getStatus());
    assertEquals("Unexpected value for the 'allow' header",
        FrontendRestRequestService.TTL_UPDATE_REJECTED_ALLOW_HEADER_VALUE,
        restResponseChannel.getHeader(RestUtils.Headers.ALLOW));
  }

  /**
   * Tests the injection of {@link GetOption#Include_All} and {@link GetOption#Include_Deleted_Blobs} as the default
   * {@link GetOption}
   * @throws Exception
   */
  @Test
  public void defaultGetDeletedTest() throws Exception {
    PostResults postResults =
        prepareAndPostBlob(1024, "defaultGetOptionsTest", TTL_SECS, "application/octet-stream", "defaultGetOptionsTest",
            refAccount, refContainer, null);
    // this also verifies that the blob is inaccessible
    deleteBlobAndVerify(postResults.blobId, postResults.headers, postResults.content, refAccount, refContainer, false);
    // now reload FrontendRestRequestService with a new default get option (Include_Deleted and Include_All) and the blob
    // can be retrieved
    verifyGetWithDefaultOptions(postResults.blobId, postResults.headers, postResults.content,
        EnumSet.of(GetOption.Include_Deleted_Blobs, GetOption.Include_All));
    // won't work with default GetOption.None
    restartFrontendRestRequestServiceWithDefaultGetOption(GetOption.None);
    verifyOperationsAfterDelete(postResults.blobId, postResults.headers, postResults.content, refAccount, refContainer);
  }

  /**
   * Tests the injection of {@link GetOption#Include_All} and {@link GetOption#Include_Expired_Blobs} as the default
   * {@link GetOption}
   * @throws Exception
   */
  @Test
  public void defaultGetExpiredTest() throws Exception {
    PostResults postResults =
        prepareAndPostBlob(1024, "defaultGetOptionsTest", 0, "application/octet-stream", "defaultGetOptionsTest",
            refAccount, refContainer, null);
    Thread.sleep(5);
    RestRequest restRequest = createRestRequest(RestMethod.GET, postResults.blobId, null, null);
    verifyOperationFailure(restRequest, RestServiceErrorCode.Deleted);
    // now reload FrontendRestRequestService with a new default get option (Include_Expired and Include_All) and the blob
    // can be retrieved
    verifyGetWithDefaultOptions(postResults.blobId, postResults.headers, postResults.content,
        EnumSet.of(GetOption.Include_Expired_Blobs, GetOption.Include_All));
    // won't work with default GetOption.None
    restartFrontendRestRequestServiceWithDefaultGetOption(GetOption.None);
    restRequest = createRestRequest(RestMethod.GET, postResults.blobId, null, null);
    verifyOperationFailure(restRequest, RestServiceErrorCode.Deleted);
  }

  /**
   * Test that the secure path is validated if required by {@link Container}.
   * @throws Exception
   */
  @Test
  public void validateSecurePathTest() throws Exception {
    short refAccountId = Utils.getRandomShort(TestUtils.RANDOM);
    String refAccountName = TestUtils.getRandomString(10);
    short[] refContainerIds = new short[]{2, 3};
    String[] refContainerNames = new String[]{"SecurePathValidation", "NoValidation"};
    Container signedPathRequiredContainer =
        new ContainerBuilder(refContainerIds[0], refContainerNames[0], Container.ContainerStatus.ACTIVE,
            "validate secure path", refAccountId).setSecurePathRequired(true).build();
    Container noValidationContainer =
        new ContainerBuilder(refContainerIds[1], refContainerNames[1], Container.ContainerStatus.ACTIVE,
            "no validation on secure path", refAccountId).setSecurePathRequired(false).build();
    Account account =
        new AccountBuilder(refAccountId, refAccountName, Account.AccountStatus.ACTIVE).addOrUpdateContainer(
            signedPathRequiredContainer).addOrUpdateContainer(noValidationContainer).build();
    accountService.updateAccounts(Collections.singletonList(account));

    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(CONTENT_LENGTH));
    String contentType = "application/octet-stream";
    String ownerId = "SecurePathValidationTest";
    JSONObject headers = new JSONObject();
    setAmbryHeadersForPut(headers, TTL_SECS, false, refAccountName, contentType, ownerId, refAccountName,
        refContainerNames[0]);
    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    RestUtilsTest.setUserMetadataHeaders(headers, userMetadata);
    String blobId = postBlobAndVerify(headers, content, account, signedPathRequiredContainer);
    headers.put(RestUtils.Headers.BLOB_SIZE, (long) CONTENT_LENGTH);
    // test that secure path validation succeeded
    String testUri = "/" + frontendConfig.securePathPrefix + blobId;
    getBlobAndVerify(testUri, null, null, headers, content, account, signedPathRequiredContainer);
    // test that no secure path should fail (return AccessDenied)
    try {
      getBlobAndVerify(blobId, null, null, headers, content, account, signedPathRequiredContainer);
      fail("get blob should fail because secure path is missing");
    } catch (RestServiceException e) {
      assertEquals("Mismatch in error code", RestServiceErrorCode.AccessDenied, e.getErrorCode());
    }
    // test that secure path equals other prefix should fail (return AccessDenied)
    try {
      getBlobAndVerify("/media" + blobId, null, null, headers, content, account, signedPathRequiredContainer);
      fail("get blob should fail because secure path equals other prefix and doesn't match expected one");
    } catch (RestServiceException e) {
      assertEquals("Mismatch in error code", RestServiceErrorCode.AccessDenied, e.getErrorCode());
    }
    // test that incorrect path should fail (return BadRequest)
    try {
      getBlobAndVerify("/incorrect-path" + blobId, null, null, headers, content, account, signedPathRequiredContainer);
      fail("get blob should fail because secure path is incorrect");
    } catch (RestServiceException e) {
      assertEquals("Mismatch in error code", RestServiceErrorCode.BadRequest, e.getErrorCode());
    }
    // test container with no validation
    setAmbryHeadersForPut(headers, TTL_SECS, false, refAccountName, contentType, ownerId, refAccountName,
        refContainerNames[1]);
    content = ByteBuffer.wrap(TestUtils.getRandomBytes(CONTENT_LENGTH));
    blobId = postBlobAndVerify(headers, content, account, noValidationContainer);
    // test container with no validation should fail if there is invalid path in URI
    try {
      getBlobAndVerify("/incorrect-path" + blobId, null, null, headers, content, account, noValidationContainer);
      fail("get blob should fail because there is invalid path in uri");
    } catch (RestServiceException e) {
      assertEquals("Mismatch in error code", RestServiceErrorCode.BadRequest, e.getErrorCode());
    }
    // test container with no validation should succeed if URI is correct
    getBlobAndVerify(blobId, null, null, headers, content, account, noValidationContainer);
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
  static RestRequest createRestRequest(RestMethod restMethod, String uri, JSONObject headers, List<ByteBuffer> contents)
      throws JSONException, UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
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
   * @param ttlInSecs sets the {@link RestUtils.Headers#TTL} header. Set to {@link Utils#Infinite_Time} if no
   *                  expiry.
   * @param isPrivate sets the {@link RestUtils.Headers#PRIVATE} header. Allowed values: true, false.
   * @param serviceId sets the {@link RestUtils.Headers#SERVICE_ID} header. Required.
   * @param contentType sets the {@link RestUtils.Headers#AMBRY_CONTENT_TYPE} header. Required and has to be a valid MIME
   *                    type.
   * @param ownerId sets the {@link RestUtils.Headers#OWNER_ID} header. Optional - if not required, send null.
   * @param targetAccountName sets the {@link RestUtils.Headers#TARGET_ACCOUNT_NAME} header. Can be {@code null}.
   * @param targetContainerName sets the {@link RestUtils.Headers#TARGET_CONTAINER_NAME} header. Can be {@code null}.
   * @throws IllegalArgumentException if any of {@code headers}, {@code serviceId}, {@code contentType} is null or if
   *                                  {@code contentLength} < 0 or if {@code ttlInSecs} < -1.
   * @throws JSONException
   */
  static void setAmbryHeadersForPut(JSONObject headers, long ttlInSecs, boolean isPrivate, String serviceId,
      String contentType, String ownerId, String targetAccountName, String targetContainerName) throws JSONException {
    if (headers != null && serviceId != null && contentType != null) {
      if (ttlInSecs != Utils.Infinite_Time) {
        headers.put(RestUtils.Headers.TTL, Long.toString(ttlInSecs));
      }
      headers.put(RestUtils.Headers.SERVICE_ID, serviceId);
      headers.put(RestUtils.Headers.AMBRY_CONTENT_TYPE, contentType);
      if (targetAccountName != null) {
        headers.put(RestUtils.Headers.TARGET_ACCOUNT_NAME, targetAccountName);
      }
      if (targetContainerName != null) {
        headers.put(RestUtils.Headers.TARGET_CONTAINER_NAME, targetContainerName);
      } else {
        headers.put(RestUtils.Headers.PRIVATE, Boolean.toString(isPrivate));
      }
      if (ownerId != null) {
        headers.put(RestUtils.Headers.OWNER_ID, ownerId);
      }
    } else {
      throw new IllegalArgumentException("Some required arguments are null. Cannot set ambry headers");
    }
  }

  /**
   * Sets account name and container name in headers.
   * @param headers the {@link JSONObject} where the headers should be set.
   * @param targetAccountName sets the {@link RestUtils.Headers#TARGET_ACCOUNT_NAME} header. Can be {@code null}.
   * @param targetContainerName sets the {@link RestUtils.Headers#TARGET_CONTAINER_NAME} header. Can be {@code null}.
   * @throws IllegalArgumentException if {@code headers} is null.
   * @throws JSONException
   */
  private void setAccountAndContainerHeaders(JSONObject headers, String targetAccountName, String targetContainerName)
      throws JSONException {
    if (headers != null) {
      if (targetAccountName != null) {
        headers.put(RestUtils.Headers.TARGET_ACCOUNT_NAME, targetAccountName);
      }
      if (targetContainerName != null) {
        headers.put(RestUtils.Headers.TARGET_CONTAINER_NAME, targetContainerName);
      }
    } else {
      throw new IllegalArgumentException("Some required arguments are null. Cannot set ambry headers");
    }
  }

  /**
   * Does an operation in {@link FrontendRestRequestService} as dictated by the {@link RestMethod} in {@code restRequest}
   * and returns the result, if any. If an exception occurs during the operation, throws the exception.
   * @param restRequest the {@link RestRequest} that needs to be submitted to the {@link FrontendRestRequestService}.
   * @param restResponseChannel the {@link RestResponseChannel} to use to return the response.
   * @throws Exception
   */
  private void doOperation(RestRequest restRequest, RestResponseChannel restResponseChannel) throws Exception {
    responseHandler.reset();
    switch (restRequest.getRestMethod()) {
      case POST:
        frontendRestRequestService.handlePost(restRequest, restResponseChannel);
        break;
      case PUT:
        frontendRestRequestService.handlePut(restRequest, restResponseChannel);
        break;
      case GET:
        frontendRestRequestService.handleGet(restRequest, restResponseChannel);
        break;
      case DELETE:
        frontendRestRequestService.handleDelete(restRequest, restResponseChannel);
        break;
      case HEAD:
        frontendRestRequestService.handleHead(restRequest, restResponseChannel);
        break;
      case OPTIONS:
        frontendRestRequestService.handleOptions(restRequest, restResponseChannel);
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

  /**
   * Verifies that the operation specified by {@code restRequest} fails with {@code errorCode}.
   * @param restRequest the {@link RestRequest} that should fail
   * @param errorCode the {@link RestServiceErrorCode} expected
   * @return the {@link MockRestResponseChannel} used for the operation
   * @throws Exception
   */
  private MockRestResponseChannel verifyOperationFailure(RestRequest restRequest, RestServiceErrorCode errorCode)
      throws Exception {
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      doOperation(restRequest, restResponseChannel);
      fail("Operation should have failed");
    } catch (RestServiceException e) {
      assertEquals("Op should have failed with a specific error code", errorCode, e.getErrorCode());
    }
    return restResponseChannel;
  }

  /**
   * Prepares random content, sets headers and POSTs a blob with the required parameters
   * @param contentLength the length of the content to be POSTed.
   * @param serviceId service ID for the blob
   * @param ttl TTL for the blob
   * @param contentType content type for the blob
   * @param ownerId owner id for the blob
   * @param account account that the blobs should belong to
   * @param container container that the blobs should belong to
   * @param userMetadata user metadata to associate with the blob. Can be {@code null}
   * @return
   * @throws Exception
   */
  private PostResults prepareAndPostBlob(int contentLength, String serviceId, long ttl, String contentType,
      String ownerId, Account account, Container container, Map<String, String> userMetadata) throws Exception {
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(contentLength));
    JSONObject headers = new JSONObject();
    setAmbryHeadersForPut(headers, ttl, !container.isCacheable(), serviceId, contentType, ownerId, account.getName(),
        container.getName());
    RestUtilsTest.setUserMetadataHeaders(headers, userMetadata);
    String blobId = postBlobAndVerify(headers, content, account, container);
    headers.put(RestUtils.Headers.BLOB_SIZE, (long) contentLength);
    return new PostResults(blobId, headers, content);
  }

  // Constructor helpers

  /**
   * Sets up and gets an instance of {@link FrontendRestRequestService}.
   * @return an instance of {@link FrontendRestRequestService}.
   */
  private FrontendRestRequestService getFrontendRestRequestService() {
    FrontendRestRequestService frontendRestRequestService =
        new FrontendRestRequestService(frontendConfig, frontendMetrics, router, clusterMap, idConverterFactory,
            securityServiceFactory, urlSigningService, idSigningService, accountService, accountAndContainerInjector,
            datacenterName, hostname, clusterName);
    frontendRestRequestService.setupResponseHandler(responseHandler);
    return frontendRestRequestService;
  }

  // nullInputsForFunctionsTest() helpers

  /**
   * Checks for reaction to null input in {@code methodName} in {@link FrontendRestRequestService}.
   * @param methodName the name of the method to invoke.
   * @throws Exception
   */
  private void doNullInputsForFunctionsTest(String methodName) throws Exception {
    Method method =
        FrontendRestRequestService.class.getDeclaredMethod(methodName, RestRequest.class, RestResponseChannel.class);
    RestRequest restRequest = createRestRequest(RestMethod.GET, "/", null, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();

    responseHandler.reset();
    try {
      method.invoke(frontendRestRequestService, null, restResponseChannel);
      fail("Method [" + methodName + "] should have failed because RestRequest is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }

    responseHandler.reset();
    try {
      method.invoke(frontendRestRequestService, restRequest, null);
      fail("Method [" + methodName + "] should have failed because RestResponseChannel is null");
    } catch (InvocationTargetException e) {
      assertEquals("Unexpected exception class", IllegalArgumentException.class, e.getTargetException().getClass());
    }
  }

  // runtimeExceptionRouterTest() helpers

  /**
   * Tests reactions of various methods of {@link FrontendRestRequestService} to a {@link Router} that throws
   * {@link RuntimeException}.
   * @param restMethod used to determine the method to invoke in {@link FrontendRestRequestService}.
   * @throws Exception
   */
  private void doRuntimeExceptionRouterTest(RestMethod restMethod) throws Exception {
    RestRequest restRequest = createRestRequest(restMethod, referenceBlobIdStr, null, null);
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
          setAmbryHeadersForPut(headers, Utils.Infinite_Time, !refContainer.isCacheable(), "test-serviceID",
              "text/plain", "test-ownerId", refAccount.getName(), refContainer.getName());
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

  // postGetHeadUpdateDeleteTest() helpers

  /**
   * Tests blob POST, GET, HEAD, TTL Update and DELETE operations on the given {@code container}.
   * @param toPostAccount the {@link Account} to use in post headers. Can be {@code null} if only using service ID.
   * @param toPostContainer the {@link Container} to use in post headers. Can be {@code null} if only using service ID.
   * @param serviceId the serviceId to use for the POST
   * @param isPrivate the isPrivate flag to pass as part of the POST
   * @param expectedAccount the {@link Account} details that are eventually expected to be populated.
   * @param expectedContainer the {@link Container} details that are eventually expected to be populated.
   * @throws Exception
   */
  private void doPostGetHeadUpdateDeleteTest(Account toPostAccount, Container toPostContainer, String serviceId,
      boolean isPrivate, Account expectedAccount, Container expectedContainer) throws Exception {
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(CONTENT_LENGTH));
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    JSONObject headers = new JSONObject();
    String accountNameInPost = toPostAccount != null ? toPostAccount.getName() : null;
    String containerNameInPost = toPostContainer != null ? toPostContainer.getName() : null;
    setAmbryHeadersForPut(headers, TTL_SECS, isPrivate, serviceId, contentType, ownerId, accountNameInPost,
        containerNameInPost);
    Map<String, String> userMetadata = new HashMap<>();
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key1", "value1");
    userMetadata.put(RestUtils.Headers.USER_META_DATA_HEADER_PREFIX + "key2", "value2");
    RestUtilsTest.setUserMetadataHeaders(headers, userMetadata);
    String blobId = postBlobAndVerify(headers, content, expectedAccount, expectedContainer);

    headers.put(RestUtils.Headers.BLOB_SIZE, (long) CONTENT_LENGTH);
    getBlobAndVerify(blobId, null, null, headers, content, expectedAccount, expectedContainer);
    getBlobAndVerify(blobId, null, GetOption.None, headers, content, expectedAccount, expectedContainer);
    getHeadAndVerify(blobId, null, null, headers, expectedAccount, expectedContainer);
    getHeadAndVerify(blobId, null, GetOption.None, headers, expectedAccount, expectedContainer);

    ByteRange range = ByteRanges.fromStartOffset(ThreadLocalRandom.current().nextLong(CONTENT_LENGTH));
    getBlobAndVerify(blobId, range, null, headers, content, expectedAccount, expectedContainer);
    getHeadAndVerify(blobId, range, null, headers, expectedAccount, expectedContainer);

    range = ByteRanges.fromLastNBytes(ThreadLocalRandom.current().nextLong(CONTENT_LENGTH + 1));
    getBlobAndVerify(blobId, range, null, headers, content, expectedAccount, expectedContainer);
    getHeadAndVerify(blobId, range, null, headers, expectedAccount, expectedContainer);

    long random1 = ThreadLocalRandom.current().nextLong(CONTENT_LENGTH);
    long random2 = ThreadLocalRandom.current().nextLong(CONTENT_LENGTH);
    range = ByteRanges.fromOffsetRange(Math.min(random1, random2), Math.max(random1, random2));
    getBlobAndVerify(blobId, range, null, headers, content, expectedAccount, expectedContainer);
    getHeadAndVerify(blobId, range, null, headers, expectedAccount, expectedContainer);

    getNotModifiedBlobAndVerify(blobId, null);
    getUserMetadataAndVerify(blobId, null, headers);
    getBlobInfoAndVerify(blobId, null, headers, expectedAccount, expectedContainer);
    updateBlobTtlAndVerify(blobId, headers, expectedAccount, expectedContainer, false);
    deleteBlobAndVerify(blobId, headers, content, expectedAccount, expectedContainer, false);
  }

  /**
   * Tests blob conditional TTL update and DELETE operations on the given {@code container}.
   * @param expectedAccount the {@link Account} to use in post headers.
   * @param expectedContainer the {@link Container} to use in post headers.
   * @param serviceId the serviceId to use for the POST
   * @throws Exception
   */
  private void doConditionalUpdateAndDeleteTest(Account expectedAccount, Container expectedContainer, String serviceId)
      throws Exception {
    PostResults postResults =
        prepareAndPostBlob(1024, serviceId, TTL_SECS, "application/octet-stream", "doConditionalUpdateAndDeleteTest",
            expectedAccount, expectedContainer, null);
    getBlobAndVerify(postResults.blobId, null, null, postResults.headers, postResults.content, expectedAccount,
        expectedContainer);
    getHeadAndVerify(postResults.blobId, null, null, postResults.headers, expectedAccount, expectedContainer);
    // failures
    Map<RestMethod, String> methodsToUris = new HashMap<>();
    methodsToUris.put(RestMethod.PUT, Operations.UPDATE_TTL);
    methodsToUris.put(RestMethod.DELETE, postResults.blobId);
    for (Map.Entry<RestMethod, String> methodToUri : methodsToUris.entrySet()) {
      RestMethod method = methodToUri.getKey();
      String uri = methodToUri.getValue();
      JSONObject badHeaders = new JSONObject();
      if (uri.equals(Operations.UPDATE_TTL)) {
        setUpdateTtlHeaders(badHeaders, postResults.blobId, serviceId);
      }
      // test Conditional op failure because only container name is set
      setAccountAndContainerHeaders(badHeaders, null, expectedContainer.getName());
      RestRequest restRequest = createRestRequest(method, uri, badHeaders, null);
      verifyOperationFailure(restRequest, RestServiceErrorCode.BadRequest);
      // test Conditional op failure because of incorrect account name
      setAccountAndContainerHeaders(badHeaders, "INCORRECT_ACCOUNT_NAME", expectedContainer.getName());
      restRequest = createRestRequest(method, uri, badHeaders, null);
      verifyOperationFailure(restRequest, RestServiceErrorCode.PreconditionFailed);
      // test Conditional op failure because of incorrect container name
      setAccountAndContainerHeaders(badHeaders, expectedAccount.getName(), "INCORRECT_CONTAINER_NAME");
      restRequest = createRestRequest(method, uri, badHeaders, null);
      verifyOperationFailure(restRequest, RestServiceErrorCode.PreconditionFailed);
    }
    // test success of conditional update
    updateBlobTtlAndVerify(postResults.blobId, postResults.headers, expectedAccount, expectedContainer, true);
    // test Conditional Delete succeeds
    deleteBlobAndVerify(postResults.blobId, postResults.headers, postResults.content, expectedAccount,
        expectedContainer, true);
  }

  /**
   * Posts a blob with the given {@code headers} and {@code content}.
   * @param headers the headers of the new blob that get converted to blob properties.
   * @param content the content of the blob.
   * @param expectedAccount the expected {@link Account} instance injected into the {@link RestRequest}.
   * @param expectedContainer the expected {@link Container} instance injected into the {@link RestRequest}.
   * @return the blob ID of the blob.
   * @throws Exception
   */
  private String postBlobAndVerify(JSONObject headers, ByteBuffer content, Account expectedAccount,
      Container expectedContainer) throws Exception {
    List<ByteBuffer> contents = new LinkedList<>();
    contents.add(content);
    contents.add(null);
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers, contents);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    return verifyPostAndReturnBlobId(restRequest, restResponseChannel, expectedAccount, expectedContainer);
  }

  /**
   * Verifies POST response on {@code restResponseChannel} in response to {@code restRequest}.
   * @param restRequest the {@link RestRequest} that was sent.
   * @param restResponseChannel the {@link MockRestResponseChannel} over which response was received.
   * @param expectedAccount the expected {@link Account} instance injected into the {@link RestRequest}.
   * @param expectedContainer the expected {@link Container} instance injected into the {@link RestRequest}.
   * @return the blob ID of the blob.
   */
  private String verifyPostAndReturnBlobId(RestRequest restRequest, MockRestResponseChannel restResponseChannel,
      Account expectedAccount, Container expectedContainer) {
    assertEquals("Unexpected response status", ResponseStatus.Created, restResponseChannel.getStatus());
    assertTrue("No Date header", restResponseChannel.getHeader(RestUtils.Headers.DATE) != null);
    assertTrue("No " + RestUtils.Headers.CREATION_TIME,
        restResponseChannel.getHeader(RestUtils.Headers.CREATION_TIME) != null);
    assertEquals("Content-Length is not 0", "0", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
    assertNull("Content-Range header should not be set",
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_RANGE));
    assertEquals("Wrong account object in RestRequest's args", expectedAccount,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY));
    assertEquals("Wrong container object in RestRequest's args", expectedContainer,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY));
    String blobId = restResponseChannel.getHeader(RestUtils.Headers.LOCATION);
    assertNotNull("Did not get a blobId", blobId);
    return blobId;
  }

  /**
   * Gets the blob with blob ID {@code blobId} and verifies that the headers and content match with what is expected.
   * @param blobId the blob ID of the blob to GET.
   * @param range the optional {@link ByteRange} for the request.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @param expectedAccount the expected account in the rest request.
   * @param expectedContainer the expected container in the rest request.
   * @throws Exception
   */
  private void getBlobAndVerify(String blobId, ByteRange range, GetOption getOption, JSONObject expectedHeaders,
      ByteBuffer expectedContent, Account expectedAccount, Container expectedContainer) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, blobId, createRequestHeaders(range, getOption), null);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    verifyGetBlobResponse(restRequest, restResponseChannel, range, expectedHeaders, expectedContent, expectedAccount,
        expectedContainer);
  }

  /**
   * Verifies the GET response received for {@code restRequest} on {@code restResponseChannel}.
   * @param restRequest the {@link RestRequest} that was sent.
   * @param restResponseChannel the {@link MockRestResponseChannel} over which response was received.
   * @param range the optional {@link ByteRange} for the request.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedContent the expected content of the blob.
   * @param expectedAccount the expected account in the rest request.
   * @param expectedContainer the expected container in the rest request.
   * @throws JSONException
   * @throws RestServiceException
   */
  private void verifyGetBlobResponse(RestRequest restRequest, MockRestResponseChannel restResponseChannel,
      ByteRange range, JSONObject expectedHeaders, ByteBuffer expectedContent, Account expectedAccount,
      Container expectedContainer) throws JSONException, RestServiceException {
    assertEquals("Unexpected response status", range == null ? ResponseStatus.Ok : ResponseStatus.PartialContent,
        restResponseChannel.getStatus());
    checkCommonGetHeadHeaders(restResponseChannel);
    assertEquals(RestUtils.Headers.BLOB_SIZE + " does not match",
        expectedHeaders.get(RestUtils.Headers.BLOB_SIZE).toString(),
        restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
    assertEquals("Content-Type does not match", expectedHeaders.getString(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertEquals("Accept-Ranges not set correctly", "bytes",
        restResponseChannel.getHeader(RestUtils.Headers.ACCEPT_RANGES));
    assertEquals("Wrong account object in RestRequest's args", expectedAccount,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY));
    assertEquals("Wrong container object in RestRequest's args", expectedContainer,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY));
    verifyBlobProperties(expectedHeaders, restResponseChannel);
    verifyUserMetadataHeaders(expectedHeaders, restResponseChannel);
    verifyAccountAndContainerHeaders(restResponseChannel, expectedAccount, expectedContainer);
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
   * @param getOption the options to use while getting the blob.
   * @throws Exception
   */
  private void getNotModifiedBlobAndVerify(String blobId, GetOption getOption) throws Exception {
    JSONObject headers = new JSONObject();
    if (getOption != null) {
      headers.put(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
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
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @throws Exception
   */
  private void getUserMetadataAndVerify(String blobId, GetOption getOption, JSONObject expectedHeaders)
      throws Exception {
    JSONObject headers = new JSONObject();
    if (getOption != null) {
      headers.put(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    RestRequest restRequest =
        createRestRequest(RestMethod.GET, blobId + "/" + RestUtils.SubResource.UserMetadata, headers, null);
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
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedAccount the expected account in the rest request.
   * @param expectedContainer the expected container in the rest request.
   * @throws Exception
   */
  private void getBlobInfoAndVerify(String blobId, GetOption getOption, JSONObject expectedHeaders,
      Account expectedAccount, Container expectedContainer) throws Exception {
    JSONObject headers = new JSONObject();
    if (getOption != null) {
      headers.put(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    RestRequest restRequest =
        createRestRequest(RestMethod.GET, blobId + "/" + RestUtils.SubResource.BlobInfo, headers, null);
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
    verifyAccountAndContainerHeaders(restResponseChannel, expectedAccount, expectedContainer);
  }

  /**
   * Gets the headers of the blob with blob ID {@code blobId} and verifies them against what is expected.
   * @param blobId the blob ID of the blob to HEAD.
   * @param range the optional {@link ByteRange} for the request.
   * @param getOption the options to use while getting the blob.
   * @param expectedHeaders the expected headers in the response.
   * @param expectedAccount the expected account in the rest request.
   * @param expectedContainer the expected container in the rest request.
   * @throws Exception
   */
  private void getHeadAndVerify(String blobId, ByteRange range, GetOption getOption, JSONObject expectedHeaders,
      Account expectedAccount, Container expectedContainer) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.HEAD, blobId, createRequestHeaders(range, getOption), null);
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
    verifyAccountAndContainerHeaders(restResponseChannel, expectedAccount, expectedContainer);
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
        expectedHeaders.get(RestUtils.Headers.BLOB_SIZE).toString(),
        restResponseChannel.getHeader(RestUtils.Headers.BLOB_SIZE));
    assertEquals(RestUtils.Headers.SERVICE_ID + " does not match",
        expectedHeaders.getString(RestUtils.Headers.SERVICE_ID),
        restResponseChannel.getHeader(RestUtils.Headers.SERVICE_ID));
    assertEquals(RestUtils.Headers.AMBRY_CONTENT_TYPE + " does not match",
        expectedHeaders.getString(RestUtils.Headers.AMBRY_CONTENT_TYPE),
        restResponseChannel.getHeader(RestUtils.Headers.AMBRY_CONTENT_TYPE));
    assertTrue(RestUtils.Headers.CREATION_TIME + " header missing",
        restResponseChannel.getHeader(RestUtils.Headers.CREATION_TIME) != null);
    if (expectedHeaders.has(RestUtils.Headers.TTL)
        && expectedHeaders.getLong(RestUtils.Headers.TTL) != Utils.Infinite_Time) {
      assertEquals(RestUtils.Headers.TTL + " does not match", expectedHeaders.get(RestUtils.Headers.TTL).toString(),
          restResponseChannel.getHeader(RestUtils.Headers.TTL));
    } else {
      assertNull("There should be no TTL in the response", restResponseChannel.getHeader(RestUtils.Headers.TTL));
    }
    if (expectedHeaders.has(RestUtils.Headers.OWNER_ID)) {
      assertEquals(RestUtils.Headers.OWNER_ID + " does not match",
          expectedHeaders.getString(RestUtils.Headers.OWNER_ID),
          restResponseChannel.getHeader(RestUtils.Headers.OWNER_ID));
    }
  }

  /**
   * Verify that the account and container headers in the response are correct.
   * @param restResponseChannel the {@link MockRestResponseChannel} to get headers from.
   * @param expectedAccount the expected {@link Account}.
   * @param expectedContainer the expected {@link Container}.
   */
  private void verifyAccountAndContainerHeaders(MockRestResponseChannel restResponseChannel, Account expectedAccount,
      Container expectedContainer) {
    if (expectedAccount.getId() != Account.UNKNOWN_ACCOUNT_ID) {
      Assert.assertEquals("Account name not as expected", expectedAccount.getName(),
          restResponseChannel.getHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME));
      Assert.assertEquals("Container name not as expected", expectedContainer.getName(),
          restResponseChannel.getHeader(RestUtils.Headers.TARGET_CONTAINER_NAME));
    }
    assertEquals(RestUtils.Headers.PRIVATE + " does not match", !expectedContainer.isCacheable(),
        Boolean.valueOf(restResponseChannel.getHeader(RestUtils.Headers.PRIVATE)));
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
   * Updates the TTL of the blob with blob ID {@code blobId} and verifies that the operation succeeded.
   * @param blobId the blob ID of the blob to HEAD.
   * @param expectedHeaders the expected headers in the GET response triggered for verification.
   * @param expectedAccount the expected account in the GET response triggered for verification. Also used to attach
   *                        preconditions if required
   * @param expectedContainer the expected container in the GET response triggered for verification. Also used to attach
   *                        preconditions if required
   * @param attachPreconditions if {@code true}, attaches preconditions to the request
   * @throws Exception
   */
  private void updateBlobTtlAndVerify(String blobId, JSONObject expectedHeaders, Account expectedAccount,
      Container expectedContainer, boolean attachPreconditions) throws Exception {
    JSONObject headers = new JSONObject();
    setUpdateTtlHeaders(headers, blobId, "updateBlobTtlAndVerify");
    if (attachPreconditions) {
      setAccountAndContainerHeaders(headers, expectedAccount.getName(), expectedContainer.getName());
    }
    RestRequest restRequest = createRestRequest(RestMethod.PUT, Operations.UPDATE_TTL, headers, null);
    verifyUpdateBlobTtlResponse(restRequest);
    expectedHeaders.remove(RestUtils.Headers.TTL);
    getBlobInfoAndVerify(blobId, GetOption.None, expectedHeaders, expectedAccount, expectedContainer);
  }

  /**
   * Sets headers required for TTL update
   * @param headers the headers object to set the headers in
   * @param blobId the blob ID being updated
   * @param serviceId the ID of service updating the blob
   * @throws JSONException
   */
  private void setUpdateTtlHeaders(JSONObject headers, String blobId, String serviceId) throws JSONException {
    headers.put(RestUtils.Headers.BLOB_ID, blobId);
    headers.put(RestUtils.Headers.SERVICE_ID, "updateBlobTtlAndVerify");
  }

  /**
   * Verifies that a request returns the right response code once the blob's TTL has been updated.
   * @param restRequest the {@link RestRequest} to send to {@link FrontendRestRequestService}.
   * @throws Exception
   */
  private void verifyUpdateBlobTtlResponse(RestRequest restRequest) throws Exception {
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    doOperation(restRequest, restResponseChannel);
    assertEquals("Unexpected response status", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertTrue("No Date header", restResponseChannel.getHeader(RestUtils.Headers.DATE) != null);
    assertEquals("Content-Length is not 0", "0", restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH));
  }

  /**
   * Deletes the blob with blob ID {@code blobId} and verifies the response returned. Also checks responses from
   * GET, HEAD, TTL Update and DELETE
   * @param blobId the blob ID of the blob to DELETE.
   * @param expectedHeaders the expected headers in the GET response triggered for verification (if right options are
   *                        provided).
   * @param expectedContent the expected account in the GET response triggered for verification (if right options are
   *                        provided). Also used to attach preconditions if required.
   * @param expectedAccount the expected container in the GET response triggered for verification (if right options are
   *                        provided). Also used to attach preconditions if required.
   * @param expectedContainer the {@link Container} details that are eventually expected to be populated.
   * @param attachPreconditions if {@code true}, attaches preconditions to the request
   * @throws Exception
   */
  private void deleteBlobAndVerify(String blobId, JSONObject expectedHeaders, ByteBuffer expectedContent,
      Account expectedAccount, Container expectedContainer, boolean attachPreconditions) throws Exception {
    JSONObject headers = new JSONObject();
    if (attachPreconditions) {
      setAccountAndContainerHeaders(headers, expectedAccount.getName(), expectedContainer.getName());
    }
    RestRequest restRequest = createRestRequest(RestMethod.DELETE, blobId, headers, null);
    verifyDeleteAccepted(restRequest);
    verifyOperationsAfterDelete(blobId, expectedHeaders, expectedContent, expectedAccount, expectedContainer);
  }

  /**
   * Verifies that the right {@link ResponseStatus} is returned for GET, HEAD, TTL update and DELETE once a blob is
   * deleted.
   * @param blobId the ID of the blob that was deleted.
   * @param expectedHeaders the expected headers in the response if the right options are provided.
   * @param expectedContent the expected content of the blob if the right options are provided.
   * @param expectedAccount the {@link Account} details that are eventually expected to be populated.
   * @param expectedContainer the {@link Container} details that are eventually expected to be populated.
   * @throws Exception
   */
  private void verifyOperationsAfterDelete(String blobId, JSONObject expectedHeaders, ByteBuffer expectedContent,
      Account expectedAccount, Container expectedContainer) throws Exception {
    RestRequest restRequest = createRestRequest(RestMethod.GET, blobId, null, null);
    verifyOperationFailure(restRequest, RestServiceErrorCode.Deleted);

    restRequest = createRestRequest(RestMethod.HEAD, blobId, null, null);
    verifyOperationFailure(restRequest, RestServiceErrorCode.Deleted);

    JSONObject headers = new JSONObject();
    setUpdateTtlHeaders(headers, blobId, "verifyOperationsAfterDelete");
    restRequest = createRestRequest(RestMethod.PUT, Operations.UPDATE_TTL, headers, null);
    verifyOperationFailure(restRequest, RestServiceErrorCode.Deleted);

    restRequest = createRestRequest(RestMethod.DELETE, blobId, null, null);
    verifyDeleteAccepted(restRequest);

    GetOption[] options = {GetOption.Include_Deleted_Blobs, GetOption.Include_All};
    for (GetOption option : options) {
      getBlobAndVerify(blobId, null, option, expectedHeaders, expectedContent, expectedAccount, expectedContainer);
      getNotModifiedBlobAndVerify(blobId, option);
      getUserMetadataAndVerify(blobId, option, expectedHeaders);
      getBlobInfoAndVerify(blobId, option, expectedHeaders, expectedAccount, expectedContainer);
      getHeadAndVerify(blobId, null, option, expectedHeaders, expectedAccount, expectedContainer);
    }
  }

  /**
   * Verifies that a request returns the right response code  once the blob has been deleted.
   * @param restRequest the {@link RestRequest} to send to {@link FrontendRestRequestService}.
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
   * @param converterFactory the {@link IdConverterFactory} to use to while creating {@link FrontendRestRequestService}.
   * @param expectedExceptionMsg the expected exception message.
   * @throws InstantiationException
   * @throws JSONException
   */
  private void doIdConverterExceptionTest(FrontendTestIdConverterFactory converterFactory, String expectedExceptionMsg)
      throws InstantiationException, JSONException {
    frontendRestRequestService =
        new FrontendRestRequestService(frontendConfig, frontendMetrics, router, clusterMap, converterFactory,
            securityServiceFactory, urlSigningService, idSigningService, accountService, accountAndContainerInjector,
            datacenterName, hostname, clusterName);
    frontendRestRequestService.setupResponseHandler(responseHandler);
    frontendRestRequestService.start();
    RestMethod[] restMethods = {RestMethod.POST, RestMethod.GET, RestMethod.DELETE, RestMethod.HEAD};
    doExternalServicesBadInputTest(restMethods, expectedExceptionMsg, false);
  }

  /**
   * Does the exception pipelining test for {@link SecurityService}.
   * @param securityFactory the {@link SecurityServiceFactory} to use to while creating {@link FrontendRestRequestService}.
   * @param exceptionMsg the expected exception message.
   * @throws InstantiationException
   * @throws JSONException
   */
  private void doSecurityServiceExceptionTest(FrontendTestSecurityServiceFactory securityFactory, String exceptionMsg)
      throws InstantiationException, JSONException {
    for (FrontendTestSecurityServiceFactory.Mode mode : FrontendTestSecurityServiceFactory.Mode.values()) {
      securityFactory.mode = mode;
      RestMethod[] restMethods;
      if (mode.equals(FrontendTestSecurityServiceFactory.Mode.ProcessResponse)) {
        restMethods = new RestMethod[]{RestMethod.GET, RestMethod.HEAD, RestMethod.POST, RestMethod.OPTIONS};
      } else if (mode.equals(FrontendTestSecurityServiceFactory.Mode.ProcessRequest)) {
        restMethods =
            new RestMethod[]{RestMethod.GET, RestMethod.HEAD, RestMethod.POST, RestMethod.DELETE, RestMethod.OPTIONS};
      } else if (mode.equals(FrontendTestSecurityServiceFactory.Mode.PostProcessRequest)) {
        restMethods = new RestMethod[]{RestMethod.GET, RestMethod.HEAD, RestMethod.POST, RestMethod.DELETE};
      } else {
        restMethods = RestMethod.values();
      }
      frontendRestRequestService =
          new FrontendRestRequestService(frontendConfig, frontendMetrics, new FrontendTestRouter(), clusterMap,
              idConverterFactory, securityFactory, urlSigningService, idSigningService, accountService,
              accountAndContainerInjector, datacenterName, hostname, clusterName);
      frontendRestRequestService.setupResponseHandler(responseHandler);
      frontendRestRequestService.start();
      doExternalServicesBadInputTest(restMethods, exceptionMsg,
          mode == FrontendTestSecurityServiceFactory.Mode.ProcessResponse);
    }
  }

  /**
   * Does the tests to check for exception pipelining for exceptions returned/thrown by external services.
   * @param restMethods the {@link RestMethod} types for which the test has to be run.
   * @param expectedExceptionMsg the expected exception message.
   * @param expectRouterCall if the router should have returned a value before the exception occurs.
   * @throws JSONException
   */
  private void doExternalServicesBadInputTest(RestMethod[] restMethods, String expectedExceptionMsg,
      boolean expectRouterCall) throws JSONException {
    for (RestMethod restMethod : restMethods) {
      if (restMethod.equals(RestMethod.UNKNOWN)) {
        continue;
      }
      JSONObject headers = new JSONObject();
      List<ByteBuffer> contents = null;
      if (restMethod.equals(RestMethod.POST)) {
        setAmbryHeadersForPut(headers, 7200, !refContainer.isCacheable(), "doExternalServicesBadInputTest",
            "application/octet-stream", "doExternalServicesBadInputTest", refAccount.getName(), refContainer.getName());
        contents = new ArrayList<>(1);
        contents.add(null);
      }
      String blobIdStr = new BlobId(blobIdVersion, BlobId.BlobIdType.NATIVE, (byte) -1, Account.UNKNOWN_ACCOUNT_ID,
          Container.UNKNOWN_CONTAINER_ID, clusterMap.getAllPartitionIds(null).get(0), false,
          BlobId.BlobDataType.DATACHUNK).getID();
      try {
        doOperation(createRestRequest(restMethod, blobIdStr, headers, contents), new MockRestResponseChannel());
        fail("Operation " + restMethod
            + " should have failed because an external service would have thrown an exception");
      } catch (Exception e) {
        assertEquals("Unexpected exception message", expectedExceptionMsg, e.getMessage());
        if (expectRouterCall && restMethod == RestMethod.GET) {
          assertNotNull("expected router.getBlob() call to provide a response to responseHandler",
              responseHandler.getResponse());
        }
      }
    }
  }

  // routerExceptionPipelineTest() helpers.

  /**
   * Does the exception pipelining test for {@link Router}.
   * @param testRouter the {@link Router} to use to while creating {@link FrontendRestRequestService}.
   * @param exceptionMsg the expected exception message.
   * @throws Exception
   */
  private void doRouterExceptionPipelineTest(FrontendTestRouter testRouter, String exceptionMsg) throws Exception {
    frontendRestRequestService =
        new FrontendRestRequestService(frontendConfig, frontendMetrics, testRouter, clusterMap, idConverterFactory,
            securityServiceFactory, urlSigningService, idSigningService, accountService, accountAndContainerInjector,
            datacenterName, hostname, clusterName);
    frontendRestRequestService.setupResponseHandler(responseHandler);
    frontendRestRequestService.start();
    for (RestMethod restMethod : RestMethod.values()) {
      switch (restMethod) {
        case HEAD:
          testRouter.exceptionOpType = FrontendTestRouter.OpType.GetBlob;
          checkRouterExceptionPipeline(exceptionMsg, createRestRequest(restMethod, referenceBlobIdStr, null, null));
          break;
        case GET:
          testRouter.exceptionOpType = FrontendTestRouter.OpType.GetBlob;
          checkRouterExceptionPipeline(exceptionMsg, createRestRequest(restMethod, referenceBlobIdStr, null, null));
          break;
        case POST:
          testRouter.exceptionOpType = FrontendTestRouter.OpType.PutBlob;
          JSONObject headers = new JSONObject();
          setAmbryHeadersForPut(headers, 7200, !refContainer.isCacheable(), "routerExceptionPipelineTest",
              "application/octet-stream", "routerExceptionPipelineTest", refAccount.getName(), refContainer.getName());
          checkRouterExceptionPipeline(exceptionMsg, createRestRequest(restMethod, "/", headers, null));
          break;
        case DELETE:
          testRouter.exceptionOpType = FrontendTestRouter.OpType.DeleteBlob;
          checkRouterExceptionPipeline(exceptionMsg, createRestRequest(restMethod, referenceBlobIdStr, null, null));
          break;
        default:
          break;
      }
    }
  }

  /**
   * Checks that the exception received by submitting {@code restRequest} to {@link FrontendRestRequestService} matches
   * what was expected.
   * @param expectedExceptionMsg the expected exception message.
   * @param restRequest the {@link RestRequest} to submit to {@link FrontendRestRequestService}.
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
   * @param getOption the options to use while getting the blob.
   * @return the {@link JSONObject} with range and getOption headers (if non-null). {@code null} if both args are
   * {@code null}.
   */
  private JSONObject createRequestHeaders(ByteRange range, GetOption getOption) {
    if (range == null && getOption == null) {
      return null;
    }
    JSONObject requestHeaders = new JSONObject();
    if (range != null) {
      requestHeaders.put(RestUtils.Headers.RANGE, RestTestUtils.getRangeHeaderString(range));
    }
    if (getOption != null) {
      requestHeaders.put(RestUtils.Headers.GET_OPTION, getOption.toString());
    }
    return requestHeaders;
  }

  /**
   * Verifies presence of {@link Account} and {@link Container} injected into {@link RestRequest} using a
   * blobId string, for get/head/delete operations.
   * @param blobId The blobId string to get/head/delete.
   * @param expectedAccount The expected {@link Account} to verify its presence in {@link RestRequest}.
   * @param expectedContainer The expected {@link Container} to verify its presence in {@link RestRequest}.
   * @param expectedRestErrorCode The expected {@link RestServiceErrorCode} to verify.
   * @throws Exception
   */
  private void verifyAccountAndContainerFromBlobId(String blobId, Account expectedAccount, Container expectedContainer,
      RestServiceErrorCode expectedRestErrorCode) throws Exception {
    if (blobId.startsWith("/")) {
      blobId = blobId.substring(1);
    }
    // PUT is verified in the tests of the individual handlers.
    for (RestMethod restMethod : Lists.newArrayList(RestMethod.GET, RestMethod.HEAD, RestMethod.DELETE)) {
      RestRequest restRequest = createRestRequest(restMethod, "/" + blobId, null, null);
      MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
      try {
        doOperation(restRequest, restResponseChannel);
        if (expectedRestErrorCode != null) {
          fail("Should have thrown");
        }
      } catch (RestServiceException e) {
        assertEquals("Wrong RestServiceErrorCode", expectedRestErrorCode, e.getErrorCode());
      }
      BlobId deserializedId = new BlobId(blobId, clusterMap);
      // Because BlobInfo is not fetched on deletes, V1 Blob IDs will never be reassigned to a known account/container.
      boolean alwaysExpectUnknown =
          restMethod == RestMethod.DELETE && deserializedId.getAccountId() == Account.UNKNOWN_ACCOUNT_ID
              && deserializedId.getContainerId() == Container.UNKNOWN_CONTAINER_ID;
      assertEquals("Wrong account object in RestRequest's args",
          alwaysExpectUnknown ? InMemAccountService.UNKNOWN_ACCOUNT : expectedAccount,
          restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY));
      assertEquals("Wrong container object in RestRequest's args",
          alwaysExpectUnknown ? Container.UNKNOWN_CONTAINER : expectedContainer,
          restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY));
    }
  }

  /**
   * Test response path account and container injection for V1 blob IDs.
   * @param serviceId the service ID for the blob.
   * @param isPrivate {@code true} if the blob is private.
   * @param expectedAccount the expected {@link Account} to verify its presence in {@link RestRequest}.
   * @param expectedContainer the expected {@link Container} to verify its presence in {@link RestRequest}.
   * @throws Exception
   */
  private void verifyResponsePathAccountAndContainerInjection(String serviceId, boolean isPrivate,
      Account expectedAccount, Container expectedContainer) throws Exception {
    BlobProperties blobProperties =
        new BlobProperties(0, serviceId, "owner", "image/gif", isPrivate, Utils.Infinite_Time,
            Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID, false, null);
    ReadableStreamChannel content = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    String blobId = router.putBlobWithIdVersion(blobProperties, new byte[0], content, BlobId.BLOB_ID_V1).get();
    verifyAccountAndContainerFromBlobId(blobId, expectedAccount, expectedContainer, null);
  }

  /**
   * Posts a blob and verifies the injected {@link Account} and {@link Container} into the {@link RestRequest}.
   * @param accountName The accountName to send as the header of the request.
   * @param containerName The containerName to send as the header of the request.
   * @param serviceId The serviceId to send as the header of the request.
   * @param isPrivate The isPrivate flag for the blob.
   * @param expectedAccount The expected {@link Account} that would be injected into the {@link RestRequest}.
   * @param expectedContainer The expected {@link Container} that would be injected into the {@link RestRequest}.
   * @param expectedRestErrorCode The expected {@link RestServiceErrorCode} after the put operation.
   * @return The blobId string if the put operation is successful, {@link null} otherwise.
   * @throws Exception
   */
  private String postBlobAndVerifyWithAccountAndContainer(String accountName, String containerName, String serviceId,
      boolean isPrivate, Account expectedAccount, Container expectedContainer,
      RestServiceErrorCode expectedRestErrorCode) throws Exception {
    ByteBuffer content = ByteBuffer.wrap(TestUtils.getRandomBytes(CONTENT_LENGTH));
    List<ByteBuffer> contents = new LinkedList<>();
    contents.add(content);
    contents.add(null);
    String contentType = "application/octet-stream";
    String ownerId = "postGetHeadDeleteOwnerID";
    JSONObject headers = new JSONObject();
    setAmbryHeadersForPut(headers, 7200, isPrivate, serviceId, contentType, ownerId, accountName, containerName);
    RestRequest restRequest = createRestRequest(RestMethod.POST, "/", headers, contents);
    MockRestResponseChannel restResponseChannel = new MockRestResponseChannel();
    try {
      doOperation(restRequest, restResponseChannel);
      if (expectedRestErrorCode != null) {
        fail("Should have thrown");
      }
    } catch (RestServiceException e) {
      assertEquals("Wrong RestServiceErrorCode", expectedRestErrorCode, e.getErrorCode());
    }
    assertEquals("Wrong account object in RestRequest's args", expectedAccount,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY));
    assertEquals("Wrong container object in RestRequest's args", expectedContainer,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY));
    return expectedRestErrorCode == null ? restResponseChannel.getHeader(RestUtils.Headers.LOCATION) : null;
  }

  /**
   * Prepopulates the {@link AccountService} with a reference {@link Account} and {@link InMemAccountService#UNKNOWN_ACCOUNT}.
   */
  private void populateAccountService() {
    accountService.clear();
    accountService.updateAccounts(Lists.newArrayList(refAccount, InMemAccountService.UNKNOWN_ACCOUNT));
  }

  /**
   * Put with prohibited headers.
   * @param header The header that is prohibited.
   * @throws Exception
   */
  private void putRequestWithProhibitedHeader(String header) throws Exception {
    JSONObject headers = new JSONObject();
    setAmbryHeadersForPut(headers, 7200, true, "someServiceId", "application/octet-stream", "someOwnerId",
        "someAccountName", "someContainerName");
    headers.put(header, "adsfksakdfsdfkdaklf");
    verifyOperationFailure(createRestRequest(RestMethod.POST, "/", headers, null), RestServiceErrorCode.BadRequest);
  }

  /**
   * Puts blobs and verify injected target {@link Account} and {@link Container}.
   * @param container the {@link Container} to use.
   * @param shouldAllowServiceIdBasedPut {@code true} if PUT requests with serviceId parsed as {@link Account} name is
   *                                                 allowed; {@code false} otherwise.
   * @throws Exception
   */
  private void injectAccountAndContainerForPostAndVerify(Container container, boolean shouldAllowServiceIdBasedPut)
      throws Exception {
    configProps.setProperty("frontend.allow.service.id.based.post.request",
        String.valueOf(shouldAllowServiceIdBasedPut));
    verifiableProperties = new VerifiableProperties(configProps);
    frontendConfig = new FrontendConfig(verifiableProperties);
    accountAndContainerInjector = new AccountAndContainerInjector(accountService, frontendMetrics, frontendConfig);
    frontendRestRequestService = getFrontendRestRequestService();
    frontendRestRequestService.start();
    populateAccountService();

    // should succeed when serviceId-based PUT requests are allowed.
    postBlobAndVerifyWithAccountAndContainer(null, null, "serviceId", !container.isCacheable(),
        shouldAllowServiceIdBasedPut ? InMemAccountService.UNKNOWN_ACCOUNT : null,
        shouldAllowServiceIdBasedPut ? (container.isCacheable() ? Container.DEFAULT_PUBLIC_CONTAINER
            : Container.DEFAULT_PRIVATE_CONTAINER) : null,
        shouldAllowServiceIdBasedPut ? null : RestServiceErrorCode.BadRequest);

    // should fail, because accountName needs to be specified.
    postBlobAndVerifyWithAccountAndContainer(null, "dummyContainerName", "serviceId", !container.isCacheable(), null,
        null, RestServiceErrorCode.MissingArgs);

    // should fail, because account name from serviceId could not be located in account service.
    postBlobAndVerifyWithAccountAndContainer(null, Container.UNKNOWN_CONTAINER_NAME, "serviceId",
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidContainer);

    // should fail, because accountName needs to be specified.
    postBlobAndVerifyWithAccountAndContainer(null, refContainer.getName(), "serviceId", !container.isCacheable(), null,
        null, RestServiceErrorCode.MissingArgs);

    // should fail, because accountName is not allowed.
    postBlobAndVerifyWithAccountAndContainer(Account.UNKNOWN_ACCOUNT_NAME, null, "serviceId", !container.isCacheable(),
        null, null, RestServiceErrorCode.InvalidAccount);

    // should fail, because accountName is not allowed.
    postBlobAndVerifyWithAccountAndContainer(Account.UNKNOWN_ACCOUNT_NAME, "dummyContainerName", "serviceId",
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidAccount);

    // should fail, because accountName is not allowed.
    postBlobAndVerifyWithAccountAndContainer(Account.UNKNOWN_ACCOUNT_NAME, Container.UNKNOWN_CONTAINER_NAME,
        "serviceId", !container.isCacheable(), null, null, RestServiceErrorCode.InvalidAccount);

    // should fail, because accountName is not allowed.
    postBlobAndVerifyWithAccountAndContainer(Account.UNKNOWN_ACCOUNT_NAME, refContainer.getName(), "serviceId",
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidAccount);

    // should fail, because container name needs to be specified
    postBlobAndVerifyWithAccountAndContainer(refAccount.getName(), null, "serviceId", !container.isCacheable(), null,
        null, RestServiceErrorCode.MissingArgs);

    // should fail, because containerName does not exist.
    postBlobAndVerifyWithAccountAndContainer(refAccount.getName(), "dummyContainerName", "serviceId",
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidContainer);

    // should fail, because containerName is not allowed.
    postBlobAndVerifyWithAccountAndContainer(refAccount.getName(), Container.UNKNOWN_CONTAINER_NAME, "serviceId",
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidContainer);

    // should succeed.
    String blobIdStr =
        postBlobAndVerifyWithAccountAndContainer(refAccount.getName(), refContainer.getName(), "serviceId",
            !container.isCacheable(), refAccount, refContainer, null);
    // should succeed.
    verifyAccountAndContainerFromBlobId(blobIdStr, refAccount, refContainer, null);

    // should fail, because containerName needs to be specified.
    postBlobAndVerifyWithAccountAndContainer("dummyAccountName", null, "serviceId", !container.isCacheable(), null,
        null, RestServiceErrorCode.MissingArgs);

    // should fail, because accountName does not exist.
    postBlobAndVerifyWithAccountAndContainer("dummyAccountName", "dummyContainerName", "serviceId",
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidAccount);

    // should fail, because container name is now allowed.
    postBlobAndVerifyWithAccountAndContainer("dummyAccountName", Container.UNKNOWN_CONTAINER_NAME, "serviceId",
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidContainer);

    // should fail, because accountName does not exist.
    postBlobAndVerifyWithAccountAndContainer("dummyAccountName", refContainer.getName(), "serviceId",
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidAccount);

    // should fail, because accountName implicitly set by serviceId is not allowed.
    postBlobAndVerifyWithAccountAndContainer(null, null, Account.UNKNOWN_ACCOUNT_NAME, !container.isCacheable(), null,
        null, RestServiceErrorCode.InvalidAccount);

    // should fail, because accountName implicitly set by serviceId is not allowed.
    postBlobAndVerifyWithAccountAndContainer(null, "dummyContainerName", Account.UNKNOWN_ACCOUNT_NAME,
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidAccount);

    // should fail, because accountName implicitly set by serviceId is not allowed.
    postBlobAndVerifyWithAccountAndContainer(null, Container.UNKNOWN_CONTAINER_NAME, Account.UNKNOWN_ACCOUNT_NAME,
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidAccount);

    // should fail, because accountName implicitly set by serviceId is not allowed.
    postBlobAndVerifyWithAccountAndContainer(null, refContainer.getName(), Account.UNKNOWN_ACCOUNT_NAME,
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidAccount);

    // should succeed if the serviceId-based PUT requests are allowed, but this is a special case that account is
    // created without the legacy containers for public and private put.
    postBlobAndVerifyWithAccountAndContainer(null, null, refAccount.getName(), !container.isCacheable(),
        shouldAllowServiceIdBasedPut ? refAccount : null,
        shouldAllowServiceIdBasedPut ? (container.isCacheable() ? refDefaultPublicContainer
            : refDefaultPrivateContainer) : null,
        shouldAllowServiceIdBasedPut ? null : RestServiceErrorCode.BadRequest);

    // should fail, because accountName needs to be specified.
    postBlobAndVerifyWithAccountAndContainer(null, "dummyContainerName", refAccount.getName(), !container.isCacheable(),
        null, null, RestServiceErrorCode.MissingArgs);

    // should fail, because accountName implicitly set by serviceId does not have the default container.
    postBlobAndVerifyWithAccountAndContainer(null, Container.UNKNOWN_CONTAINER_NAME, refAccount.getName(),
        !container.isCacheable(), null, null, RestServiceErrorCode.InvalidContainer);

    // should fail, because accountName needs to be specified.
    postBlobAndVerifyWithAccountAndContainer(null, refContainer.getName(), refAccount.getName(),
        !container.isCacheable(), null, null, RestServiceErrorCode.MissingArgs);

    Container legacyContainerForPublicBlob =
        new ContainerBuilder(Container.DEFAULT_PUBLIC_CONTAINER_ID, "containerForLegacyPublicPut",
            Container.ContainerStatus.ACTIVE, "This is a container for putting legacy public blob",
            refAccount.getId()).build();
    Container legacyContainerForPrivateBlob =
        new ContainerBuilder(Container.DEFAULT_PRIVATE_CONTAINER_ID, "containerForLegacyPrivatePut",
            Container.ContainerStatus.ACTIVE, "This is a container for putting legacy private blob", refAccount.getId())
            .setCacheable(false)
            .build();
    Account accountWithTwoDefaultContainers =
        new AccountBuilder(refAccount).addOrUpdateContainer(legacyContainerForPrivateBlob)
            .addOrUpdateContainer(legacyContainerForPublicBlob)
            .build();
    accountService.updateAccounts(Collections.singletonList(accountWithTwoDefaultContainers));
    if (!container.isCacheable()) {
      // should succeed if serviceId-based PUT requests are allowed.
      postBlobAndVerifyWithAccountAndContainer(null, null, accountWithTwoDefaultContainers.getName(),
          !container.isCacheable(), shouldAllowServiceIdBasedPut ? accountWithTwoDefaultContainers : null,
          shouldAllowServiceIdBasedPut ? accountWithTwoDefaultContainers.getContainerById(
              Container.DEFAULT_PRIVATE_CONTAINER_ID) : null,
          shouldAllowServiceIdBasedPut ? null : RestServiceErrorCode.BadRequest);
      // should fail, because accountName needs to be specified.
      postBlobAndVerifyWithAccountAndContainer(null, "dummyContainerName", accountWithTwoDefaultContainers.getName(),
          !container.isCacheable(), null, null, RestServiceErrorCode.MissingArgs);
    } else {
      // should succeed if serviceId-based PUT requests are allowed.
      postBlobAndVerifyWithAccountAndContainer(null, null, accountWithTwoDefaultContainers.getName(),
          !container.isCacheable(), shouldAllowServiceIdBasedPut ? accountWithTwoDefaultContainers : null,
          shouldAllowServiceIdBasedPut ? accountWithTwoDefaultContainers.getContainerById(
              Container.DEFAULT_PUBLIC_CONTAINER_ID) : null,
          shouldAllowServiceIdBasedPut ? null : RestServiceErrorCode.BadRequest);
      // should fail, because accountName needs to be specified.
      postBlobAndVerifyWithAccountAndContainer(null, "dummyContainerName", accountWithTwoDefaultContainers.getName(),
          !container.isCacheable(), null, null, RestServiceErrorCode.MissingArgs);
    }
  }

  // defaultGetDeletedTest() and defaultGetExpiredTest() helpers

  /**
   * Restarts {@link #frontendRestRequestService} with the default {@link GetOption} set to {@code option}
   * @param option the value to set for "frontend.default.router.get.option"
   * @throws InstantiationException
   */
  private void restartFrontendRestRequestServiceWithDefaultGetOption(GetOption option) throws InstantiationException {
    frontendRestRequestService.shutdown();
    configProps.setProperty("frontend.default.router.get.option", option.name());
    verifiableProperties = new VerifiableProperties(configProps);
    frontendConfig = new FrontendConfig(verifiableProperties);
    frontendRestRequestService = getFrontendRestRequestService();
    frontendRestRequestService.start();
  }

  /**
   * Verifies GET blob, blobinfo and HEAD with the given {@code defaultOptionsToTest} set as the default {
   * @link GetOption} (one by one)
   * @param blobId the id of the blob to fetch
   * @param expectedHeaders the headers expected in the responses
   * @param expectedContent the content expected for the blob
   * @param defaultOptionsToTest the {@link GetOption}s to check as defaults
   * @throws Exception
   */
  private void verifyGetWithDefaultOptions(String blobId, JSONObject expectedHeaders, ByteBuffer expectedContent,
      EnumSet<GetOption> defaultOptionsToTest) throws Exception {
    for (GetOption option : defaultOptionsToTest) {
      restartFrontendRestRequestServiceWithDefaultGetOption(option);
      getBlobInfoAndVerify(blobId, null, expectedHeaders, refAccount, refContainer);
      getHeadAndVerify(blobId, null, null, expectedHeaders, refAccount, refContainer);
      getBlobAndVerify(blobId, null, null, expectedHeaders, expectedContent, refAccount, refContainer);
    }
  }

  /**
   * Results from a POST performed against {@link FrontendRestRequestService}
   */
  private class PostResults {
    final String blobId;
    final JSONObject headers;
    final ByteBuffer content;

    PostResults(String blobId, JSONObject headers, ByteBuffer content) {
      this.blobId = blobId;
      this.headers = headers;
      this.content = content;
    }
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
  boolean awaitResponseSubmission(long timeout, TimeUnit timeUnit) throws InterruptedException {
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
  void reset() {
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
     * Works in {@link SecurityService#preProcessRequest(RestRequest, Callback)}.
     */
    PreProcessRequest,

    /**
     * Works in {@link SecurityService#processRequest(RestRequest, Callback)}.
     */
    ProcessRequest,

    /**
     * Works in {@link SecurityService#postProcessRequest(RestRequest, Callback)}
     */
    PostProcessRequest,

    /**
     * Works in {@link SecurityService#processResponse(RestRequest, RestResponseChannel, BlobInfo, Callback)}.
     */
    ProcessResponse
  }

  /**
   * The exception to return via future/callback.
   */
  Exception exceptionToReturn = null;
  /**
   * The exception to throw on function invocation.
   */
  RuntimeException exceptionToThrow = null;
  /**
   * Defines the API in which {@link #exceptionToThrow} and {@link #exceptionToReturn} will work.
   */
  Mode mode = Mode.PreProcessRequest;

  @Override
  public SecurityService getSecurityService() {
    return new TestSecurityService();
  }

  private class TestSecurityService implements SecurityService {
    private boolean isOpen = true;

    @Override
    public void preProcessRequest(RestRequest restRequest, Callback<Void> callback) {
      if (!isOpen) {
        throw new IllegalStateException("SecurityService closed");
      }
      completeOperation(callback, mode == null || mode == Mode.PreProcessRequest);
    }

    @Override
    public void processRequest(RestRequest restRequest, Callback<Void> callback) {
      if (!isOpen) {
        throw new IllegalStateException("SecurityService closed");
      }
      completeOperation(callback, mode == null || mode == Mode.ProcessRequest);
    }

    @Override
    public void postProcessRequest(RestRequest restRequest, Callback<Void> callback) {
      if (!isOpen) {
        throw new IllegalStateException("SecurityService closed");
      }
      completeOperation(callback, mode == Mode.PostProcessRequest);
    }

    @Override
    public void processResponse(RestRequest restRequest, RestResponseChannel responseChannel, BlobInfo blobInfo,
        Callback<Void> callback) {
      if (!isOpen) {
        throw new IllegalStateException("SecurityService closed");
      }
      completeOperation(callback, mode == Mode.ProcessResponse);
    }

    @Override
    public void close() {
      isOpen = false;
    }

    /**
     * Completes the operation by invoking the {@code callback}.
     * @param callback the {@link Callback} to invoke.
     * @param misbehaveIfRequired whether to exhibit misbehavior or not.
     */
    private void completeOperation(Callback<Void> callback, boolean misbehaveIfRequired) {
      if (misbehaveIfRequired && exceptionToThrow != null) {
        throw exceptionToThrow;
      }
      callback.onCompletion(null, misbehaveIfRequired ? exceptionToReturn : null);
    }
  }
}

/**
 * Implementation of {@link IdConverterFactory} that returns exceptions.
 */
class FrontendTestIdConverterFactory implements IdConverterFactory {
  Exception exceptionToReturn = null;
  RuntimeException exceptionToThrow = null;
  String translation = null;
  boolean returnInputIfTranslationNull = false;
  volatile String lastInput = null;

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
      return completeOperation(input, callback);
    }

    @Override
    public void close() {
      isOpen = false;
    }

    /**
     * Completes the operation by creating and invoking a {@link Future} and invoking the {@code callback} if non-null.
     * @param input the original input ID received
     * @param callback the {@link Callback} to invoke. Can be null.
     * @return the created {@link Future}.
     */
    private Future<String> completeOperation(String input, Callback<String> callback) {
      lastInput = input;
      if (exceptionToThrow != null) {
        throw exceptionToThrow;
      }
      FutureResult<String> futureResult = new FutureResult<String>();
      String toReturn = null;
      if (exceptionToReturn == null) {
        toReturn = translation == null ? returnInputIfTranslationNull ? input : null : translation;
      }
      futureResult.done(toReturn, exceptionToReturn);
      if (callback != null) {
        callback.onCompletion(toReturn, exceptionToReturn);
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
  public Object setArg(String key, Object value) {
    throw new IllegalStateException("Not implemented");
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

  @Override
  public long getBytesReceived() {
    return 0;
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
    DeleteBlob, GetBlob, PutBlob, StitchBlob, UpdateBlobTtl, UndeleteBlob,
  }

  OpType exceptionOpType = null;
  Exception exceptionToReturn = null;
  RuntimeException exceptionToThrow = null;
  String deleteServiceId = null;
  String ttlUpdateServiceId = null;
  String undeleteServiceId = null;

  @Override
  public Future<GetBlobResult> getBlob(String blobId, GetBlobOptions options, Callback<GetBlobResult> callback) {
    GetBlobResult result;
    switch (options.getOperationType()) {
      case BlobInfo:
        result = new GetBlobResult(new BlobInfo(
            new BlobProperties(0, "FrontendTestRouter", Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID,
                false), new byte[0]), null);
        break;
      case Data:
        result = new GetBlobResult(null, new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0)));
        break;
      default:
        result = new GetBlobResult(new BlobInfo(
            new BlobProperties(0, "FrontendTestRouter", Account.UNKNOWN_ACCOUNT_ID, Container.UNKNOWN_CONTAINER_ID,
                false), new byte[0]), new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0)));
        break;
    }
    return completeOperation(result, callback, OpType.GetBlob);
  }

  @Override
  public Future<String> putBlob(BlobProperties blobProperties, byte[] usermetadata, ReadableStreamChannel channel,
      PutBlobOptions options, Callback<String> callback) {
    return completeOperation(TestUtils.getRandomString(10), callback, OpType.PutBlob);
  }

  @Override
  public Future<String> stitchBlob(BlobProperties blobProperties, byte[] userMetadata, List<ChunkInfo> chunksToStitch,
      Callback<String> callback) {
    return completeOperation(TestUtils.getRandomString(10), callback, OpType.StitchBlob);
  }

  @Override
  public Future<Void> deleteBlob(String blobId, String serviceId, Callback<Void> callback) {
    deleteServiceId = serviceId;
    return completeOperation(null, callback, OpType.DeleteBlob);
  }

  @Override
  public Future<Void> updateBlobTtl(String blobId, String serviceId, long expiresAtMs, Callback<Void> callback) {
    ttlUpdateServiceId = serviceId;
    return completeOperation(null, callback, OpType.UpdateBlobTtl);
  }

  @Override
  public Future<Void> undeleteBlob(String blobId, String serviceId, Callback<Void> callback) {
    undeleteServiceId = serviceId;
    return completeOperation(null, callback, OpType.UndeleteBlob);
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

/**
 * Implementation of {@link UrlSigningService} for tests in frontend.
 */
class FrontendTestUrlSigningServiceFactory implements UrlSigningServiceFactory {
  String signedUrlToReturn = "";
  boolean isRequestSigned = false;
  RestServiceException getSignedUrlException = null;
  RestServiceException verifySignedRequestException = null;

  @Override
  public UrlSigningService getUrlSigningService() {
    return new UrlSigningService() {
      @Override
      public String getSignedUrl(RestRequest restRequest) throws RestServiceException {
        if (getSignedUrlException != null) {
          throw getSignedUrlException;
        }
        return signedUrlToReturn;
      }

      @Override
      public boolean isRequestSigned(RestRequest restRequest) {
        return isRequestSigned;
      }

      @Override
      public void verifySignedRequest(RestRequest restRequest) throws RestServiceException {
        if (verifySignedRequestException != null) {
          throw verifySignedRequestException;
        }
      }
    };
  }
}

