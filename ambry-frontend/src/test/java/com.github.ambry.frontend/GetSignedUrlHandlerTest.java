/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.CommonTestUtils;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.utils.TestUtils;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link GetSignedUrlHandler}.
 */
public class GetSignedUrlHandlerTest {

  private static final InMemAccountService ACCOUNT_SERVICE =
      new InMemAccountServiceFactory(false, true).getAccountService();
  private static final Account REF_ACCOUNT = ACCOUNT_SERVICE.createAndAddRandomAccount();
  private static final Container REF_CONTAINER = REF_ACCOUNT.getContainerById(Container.DEFAULT_PRIVATE_CONTAINER_ID);
  private static final ClusterMap CLUSTER_MAP;
  private final BlobId testBlobId;

  static {
    try {
      CLUSTER_MAP = new MockClusterMap();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private final FrontendTestSecurityServiceFactory securityServiceFactory = new FrontendTestSecurityServiceFactory();
  private final FrontendTestUrlSigningServiceFactory urlSigningServiceFactory =
      new FrontendTestUrlSigningServiceFactory();
  private final FrontendTestIdConverterFactory idConverterFactory = new FrontendTestIdConverterFactory();
  private final GetSignedUrlHandler getSignedUrlHandler;

  public GetSignedUrlHandlerTest() {
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
    FrontendConfig config = new FrontendConfig(new VerifiableProperties(new Properties()));
    AccountAndContainerInjector accountAndContainerInjector =
        new AccountAndContainerInjector(ACCOUNT_SERVICE, metrics, config);
    getSignedUrlHandler = new GetSignedUrlHandler(urlSigningServiceFactory.getUrlSigningService(),
        securityServiceFactory.getSecurityService(), idConverterFactory.getIdConverter(), accountAndContainerInjector,
        metrics, CLUSTER_MAP);
    testBlobId = new BlobId(CommonTestUtils.getCurrentBlobIdVersion(), BlobId.BlobIdType.NATIVE,
        ClusterMapUtils.UNKNOWN_DATACENTER_ID, REF_ACCOUNT.getId(), REF_CONTAINER.getId(),
        CLUSTER_MAP.getWritablePartitionIds(MockClusterMap.DEFAULT_PARTITION_CLASS).get(0), false,
        BlobId.BlobDataType.DATACHUNK);
  }

  /**
   * Handles the case where url signing succeeds
   * @throws Exception
   */
  @Test
  public void handleGoodCaseTest() throws Exception {
    urlSigningServiceFactory.signedUrlToReturn = TestUtils.getRandomString(10);
    // POST
    RestRequest restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    restRequest.setArg(RestUtils.Headers.URL_TYPE, RestMethod.POST.name());
    restRequest.setArg(RestUtils.Headers.TARGET_ACCOUNT_NAME, REF_ACCOUNT.getName());
    restRequest.setArg(RestUtils.Headers.TARGET_CONTAINER_NAME, REF_CONTAINER.getName());
    verifySigningUrl(restRequest, urlSigningServiceFactory.signedUrlToReturn, REF_ACCOUNT, REF_CONTAINER);

    idConverterFactory.translation = testBlobId.getID();
    // GET (also makes sure that the IDConverter is used)
    restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    restRequest.setArg(RestUtils.Headers.URL_TYPE, RestMethod.GET.name());
    // add a random string. IDConverter will convert it
    restRequest.setArg(RestUtils.Headers.BLOB_ID, TestUtils.getRandomString(10));
    verifySigningUrl(restRequest, urlSigningServiceFactory.signedUrlToReturn, REF_ACCOUNT, REF_CONTAINER);
  }

  /**
   * Tests the case where the {@link SecurityService} denies the request.
   * @throws Exception
   */
  @Test
  public void securityServiceDenialTest() throws Exception {
    String msg = "@@expected";
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
  }

  /**
   * Tests where {@link UrlSigningService} fails.
   * @throws Exception
   */
  @Test
  public void urlSigningServiceFailureTest() throws Exception {
    String msg = "@@expected";
    urlSigningServiceFactory.getSignedUrlException =
        new RestServiceException(msg, RestServiceErrorCode.InternalServerError);
    verifyFailureWithMsg(msg);
  }

  // helpers

  /**
   * Sends the given {@link RestRequest} to the {@link GetSignedUrlHandler} and waits for the response and returns it.
   * @param restRequest the {@link RestRequest} to send.
   * @param restResponseChannel the {@link RestResponseChannel} where headers will be set.
   * @throws Exception
   */
  private void sendRequestGetResponse(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    getSignedUrlHandler.handle(restRequest, restResponseChannel, (result, exception) -> {
      exceptionRef.set(exception);
      latch.countDown();
    });
    assertTrue("Latch did not count down in time", latch.await(1, TimeUnit.SECONDS));
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
  }

  /**
   * Verifies that attempting to get signed urls fails with the provided {@code msg}.
   * @param msg the message in the {@link Exception} that will be thrown.
   * @throws Exception
   */
  private void verifyFailureWithMsg(String msg) throws Exception {
    RestRequest restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    restRequest.setArg(RestUtils.Headers.URL_TYPE, RestMethod.POST.name());
    restRequest.setArg(RestUtils.Headers.TARGET_ACCOUNT_NAME, REF_ACCOUNT.getName());
    restRequest.setArg(RestUtils.Headers.TARGET_CONTAINER_NAME, REF_CONTAINER.getName());
    try {
      sendRequestGetResponse(restRequest, new MockRestResponseChannel());
      fail("Request should have failed");
    } catch (Exception e) {
      assertEquals("Unexpected Exception", msg, e.getMessage());
    }
  }

  // handleGoodCaseTest() helpers

  /**
   * Verifies that a singed URL is returned and it matches what is expected
   * @param restRequest the {@link RestRequest} to get a signed URL.
   * @param urlExpected the URL that should be returned.
   * @param expectedAccount the {@link Account} that should be populated in {@link RestRequest}.
   * @param expectedContainer the {@link Container} that should be populated in {@link RestRequest}.
   * @throws Exception
   */
  private void verifySigningUrl(RestRequest restRequest, String urlExpected, Account expectedAccount,
      Container expectedContainer) throws Exception {
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestGetResponse(restRequest, restResponseChannel);
    Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    assertEquals("Content-length is not as expected", 0,
        Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
    assertEquals("Signed URL is not as expected", urlExpected,
        restResponseChannel.getHeader(RestUtils.Headers.SIGNED_URL));
    assertEquals("Account not as expected", expectedAccount,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_ACCOUNT_KEY));
    assertEquals("Container not as expected", expectedContainer,
        restRequest.getArgs().get(RestUtils.InternalKeys.TARGET_CONTAINER_KEY));
  }
}
