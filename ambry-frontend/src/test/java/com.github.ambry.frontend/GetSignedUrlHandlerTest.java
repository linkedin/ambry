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
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.UtilsTest;
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

  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final FrontendTestUrlSigningServiceFactory urlSigningServiceFactory;
  private final GetSignedUrlHandler getSignedUrlHandler;

  public GetSignedUrlHandlerTest() {
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    urlSigningServiceFactory = new FrontendTestUrlSigningServiceFactory();
    getSignedUrlHandler = new GetSignedUrlHandler(urlSigningServiceFactory.getUrlSigningService(),
        securityServiceFactory.getSecurityService(), metrics);
  }

  /**
   * Handles the case where url signing succeeds
   * @throws Exception
   */
  @Test
  public void handleGoodCaseTest() throws Exception {
    urlSigningServiceFactory.signedUrlToReturn = UtilsTest.getRandomString(10);
    RestRequest restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    sendRequestGetResponse(restRequest, restResponseChannel);
    Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    assertEquals("Content-length is not as expected", 0,
        Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
    assertEquals("Signed URL is not as expected", urlSigningServiceFactory.signedUrlToReturn,
        restResponseChannel.getHeader(RestUtils.Headers.SIGNED_URL));
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
    securityServiceFactory.exceptionToThrow = new IllegalStateException(msg);
    securityServiceFactory.exceptionToReturn = null;
    verifyFailureWithMsg(msg);
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.ProcessRequest;
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

  /**
   * Sends the given {@link RestRequest} to the {@link GetPeersHandler} and waits for the response and returns it.
   * @param restRequest the {@link RestRequest} to send.
   * @param restResponseChannel the {@link RestResponseChannel} where headers will be set.
   * @return the response body as a {@link ReadableStreamChannel}.
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
   * Verifies that attempting to get peers of any datanode fails with the provided {@code msg}.
   * @param msg the message in the {@link Exception} that will be thrown.
   * @throws Exception
   */
  private void verifyFailureWithMsg(String msg) throws Exception {
    RestRequest restRequest = new MockRestRequest(MockRestRequest.DUMMY_DATA, null);
    try {
      sendRequestGetResponse(restRequest, new MockRestResponseChannel());
      fail("Request should have failed");
    } catch (Exception e) {
      assertEquals("Unexpected Exception", msg, e.getMessage());
    }
  }
}
