/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.Container;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.account.InMemAccountServiceFactory;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.InMemoryRouter;
import com.github.ambry.router.PutBlobOptionsBuilder;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Tests for {@link TtlUpdateHandler}.
 */
public class TtlUpdateHandlerTest {
  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();
  private static final Account REF_ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount();
  private static final Container REF_CONTAINER = REF_ACCOUNT.getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID);
  private static final ClusterMap CLUSTER_MAP;
  private static final String SERVICE_ID = "TtlUpdateHandlerTest";

  private static final BlobProperties BLOB_PROPERTIES =
      new BlobProperties(100, SERVICE_ID, null, null, false, TTL_SECS, REF_ACCOUNT.getId(), REF_CONTAINER.getId(),
          false, null);
  private static final byte[] BLOB_DATA = TestUtils.getRandomBytes(100);

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private final TtlUpdateHandler ttlUpdateHandler;
  private final String blobId;
  private final InMemoryRouter router = new InMemoryRouter(new VerifiableProperties(new Properties()), CLUSTER_MAP);
  private final FrontendTestSecurityServiceFactory securityServiceFactory = new FrontendTestSecurityServiceFactory();
  private final FrontendTestIdConverterFactory idConverterFactory = new FrontendTestIdConverterFactory();

  public TtlUpdateHandlerTest() throws Exception {
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
    FrontendConfig config = new FrontendConfig(new VerifiableProperties(new Properties()));
    AccountAndContainerInjector accountAndContainerInjector =
        new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, config);
    ttlUpdateHandler =
        new TtlUpdateHandler(router, securityServiceFactory.getSecurityService(), idConverterFactory.getIdConverter(),
            accountAndContainerInjector, metrics, CLUSTER_MAP);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(BLOB_DATA));
    blobId = router.putBlob(BLOB_PROPERTIES, new byte[0], channel, new PutBlobOptionsBuilder().build())
        .get(1, TimeUnit.SECONDS);
    idConverterFactory.translation = blobId;
  }

  /**
   * Tests the case where TTL update succeeds
   * @throws Exception
   */
  @Test
  public void handleGoodCaseTest() throws Exception {
    RestRequest restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    restRequest.setArg(RestUtils.Headers.BLOB_ID, blobId);
    restRequest.setArg(RestUtils.Headers.SERVICE_ID, SERVICE_ID);
    verifyTtlUpdate(restRequest, REF_ACCOUNT, REF_CONTAINER);
  }

  /**
   * Tests for cases when downstream services fail or return an exception
   * @throws Exception
   */
  @Test
  public void downstreamServicesFailureTest() throws Exception {
    securityServiceFailureTest();
    idConverterFailureTest();
    routerFailureTest();
    badArgsTest();
  }

  // helpers

  /**
   * Sends the given {@link RestRequest} to the {@link TtlUpdateHandler} and waits for the response and returns it.
   * @param restRequest the {@link RestRequest} to send.
   * @param restResponseChannel the {@link RestResponseChannel} where headers will be set.
   * @throws Exception
   */
  private void sendRequestGetResponse(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    ttlUpdateHandler.handle(restRequest, restResponseChannel, (result, exception) -> {
      exceptionRef.set(exception);
      latch.countDown();
    });
    assertTrue("Latch did not count down in time", latch.await(1, TimeUnit.SECONDS));
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
  }

  /**
   * Verifies that the TTL is {@code expectedTtlSecs}
   * @param expectedTtlSecs the expected TTL (in secs) of the blob
   * @throws Exception
   */
  private void assertTtl(long expectedTtlSecs) throws Exception {
    GetBlobResult result = router.getBlob(blobId, new GetBlobOptionsBuilder().build()).get(1, TimeUnit.SECONDS);
    assertEquals("TTL not as expected", expectedTtlSecs,
        result.getBlobInfo().getBlobProperties().getTimeToLiveInSeconds());
  }

  // handleGoodCaseTest()

  /**
   * Verifies that the TTL of the blob is updated
   * @param restRequest the {@link RestRequest} to get a signed URL.
   * @param expectedAccount the {@link Account} that should be populated in {@link RestRequest}.
   * @param expectedContainer the {@link Container} that should be populated in {@link RestRequest}.
   * @throws Exception
   */
  private void verifyTtlUpdate(RestRequest restRequest, Account expectedAccount, Container expectedContainer)
      throws Exception {
    assertTtl(TTL_SECS);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestGetResponse(restRequest, restResponseChannel);
    assertEquals("ResponseStatus not as expected", ResponseStatus.Ok, restResponseChannel.getStatus());
    assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    assertEquals("Content-length is not as expected", 0,
        Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
    assertEquals("Account not as expected", expectedAccount,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY));
    assertEquals("Container not as expected", expectedContainer,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY));
    assertTtl(Utils.Infinite_Time);
  }

  // downstreamServicesFailureTest()

  /**
   * Tests the case where the {@link SecurityService} denies the request.
   * @throws Exception
   */
  private void securityServiceFailureTest() throws Exception {
    String msg = "@@security-service-expected@@";
    securityServiceFactory.exceptionToReturn = new IllegalStateException(msg);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.ProcessRequest;
    verifyFailureWithMsg(msg);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.PostProcessRequest;
    verifyFailureWithMsg(msg);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.ProcessResponse;
    verifyFailureWithMsg(msg);

    securityServiceFactory.exceptionToThrow = new IllegalStateException(msg);
    securityServiceFactory.exceptionToReturn = null;
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.ProcessRequest;
    verifyFailureWithMsg(msg);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.PostProcessRequest;
    verifyFailureWithMsg(msg);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.ProcessResponse;
    verifyFailureWithMsg(msg);
    securityServiceFactory.exceptionToThrow = null;
  }

  /**
   * Tests the case where the {@link IdConverter} fails or returns an exception.
   * @throws Exception
   */
  private void idConverterFailureTest() throws Exception {
    String msg = "@@id-converter-expected@@";
    idConverterFactory.exceptionToReturn = new IllegalStateException(msg);
    verifyFailureWithMsg(msg);
    idConverterFactory.exceptionToReturn = null;
    idConverterFactory.exceptionToThrow = new IllegalStateException(msg);
    verifyFailureWithMsg(msg);
    idConverterFactory.exceptionToThrow = null;
  }

  /**
   * Tests the case where the {@link com.github.ambry.router.Router} fails or returns an exception
   * @throws Exception
   */
  private void routerFailureTest() throws Exception {
    // get the router to throw a RuntimeException
    Properties properties = new Properties();
    properties.setProperty(InMemoryRouter.OPERATION_THROW_EARLY_RUNTIME_EXCEPTION, "true");
    router.setVerifiableProperties(new VerifiableProperties(properties));
    verifyFailureWithMsg(InMemoryRouter.OPERATION_THROW_EARLY_RUNTIME_EXCEPTION);

    // get the router to return a RuntimeException
    properties = new Properties();
    properties.setProperty(InMemoryRouter.OPERATION_THROW_LATE_RUNTIME_EXCEPTION, "true");
    router.setVerifiableProperties(new VerifiableProperties(properties));
    verifyFailureWithMsg(InMemoryRouter.OPERATION_THROW_LATE_RUNTIME_EXCEPTION);

    router.setVerifiableProperties(new VerifiableProperties(new Properties()));
  }

  /**
   * Tests for expected failures with bad arguments
   * @throws Exception
   */
  private void badArgsTest() throws Exception {
    RestRequest restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    restRequest.setArg(RestUtils.Headers.BLOB_ID, blobId);
    // no service ID
    verifyFailureWithErrorCode(restRequest, RestServiceErrorCode.MissingArgs);

    restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    restRequest.setArg(RestUtils.Headers.SERVICE_ID, SERVICE_ID);
    // no blob ID
    verifyFailureWithErrorCode(restRequest, RestServiceErrorCode.MissingArgs);

    restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    // not a valid blob ID
    restRequest.setArg(RestUtils.Headers.BLOB_ID, "abcd");
    idConverterFactory.translation = "abcd";
    restRequest.setArg(RestUtils.Headers.SERVICE_ID, SERVICE_ID);
    verifyFailureWithErrorCode(restRequest, RestServiceErrorCode.BadRequest);
  }

  /**
   * Verifies that attempting to update TTL fails with the provided {@code msg}.
   * @param msg the message in the {@link Exception} that will be thrown.
   * @throws Exception
   */
  private void verifyFailureWithMsg(String msg) throws Exception {
    RestRequest restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    restRequest.setArg(RestUtils.Headers.BLOB_ID, blobId);
    restRequest.setArg(RestUtils.Headers.SERVICE_ID, SERVICE_ID);
    try {
      sendRequestGetResponse(restRequest, new MockRestResponseChannel());
      fail("Request should have failed");
    } catch (Exception e) {
      if (!msg.equals(e.getMessage())) {
        throw e;
      }
    }
  }

  /**
   * Verifies that processing {@code restRequest} fails with {@code errorCode}
   * @param restRequest the {@link RestRequest} that is expected to fail
   * @param errorCode the {@link RestServiceErrorCode} that it should fail with
   * @throws Exception
   */
  private void verifyFailureWithErrorCode(RestRequest restRequest, RestServiceErrorCode errorCode) throws Exception {
    try {
      sendRequestGetResponse(restRequest, new MockRestResponseChannel());
      fail("Request should have failed");
    } catch (RestServiceException e) {
      assertEquals("Unexpected RestServiceErrorCode", errorCode, e.getErrorCode());
    }
  }
}
