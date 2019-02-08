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
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapSnapshotConstants;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link GetClusterMapSnapshotHandler}
 */
public class GetClusterMapSnapshotHandlerTest {
  private final MockClusterMap clusterMap;
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final GetClusterMapSnapshotHandler handler;

  public GetClusterMapSnapshotHandlerTest() throws IOException {
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
    clusterMap = new MockClusterMap();
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    handler = new GetClusterMapSnapshotHandler(securityServiceFactory.getSecurityService(), metrics, clusterMap);
  }

  /**
   * Handles the case where everything works as expected
   * @throws Exception
   */
  @Test
  public void handleGoodCaseTest() throws Exception {
    RestRequest restRequest = createRestRequest();
    RestResponseChannel restResponseChannel = new MockRestResponseChannel();
    ReadableStreamChannel channel = sendRequestGetResponse(restRequest, restResponseChannel);
    assertNotNull("There should be a response", channel);
    Assert.assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
    assertEquals("Content-type is not as expected", RestUtils.JSON_CONTENT_TYPE,
        restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
    assertEquals("Content-length is not as expected", channel.getSize(),
        Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
    JSONObject expected = clusterMap.getSnapshot();
    JSONObject actual = RestTestUtils.getJsonizedResponseBody(channel);
    // remove timestamps because they may differ
    expected.remove(ClusterMapSnapshotConstants.TIMESTAMP_MS);
    actual.remove(ClusterMapSnapshotConstants.TIMESTAMP_MS);
    assertEquals("Snapshot does not match expected", expected.toString(), actual.toString());
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
    securityServiceFactory.mode = FrontendTestSecurityServiceFactory.Mode.PostProcessRequest;
    verifyFailureWithMsg(msg);
  }

  /**
   * Tests the case where the {@link ClusterMap} throws exceptions.
   * @throws Exception
   */
  @Test
  public void badClusterMapTest() throws Exception {
    String msg = "@@expected";
    clusterMap.setExceptionOnSnapshot(new RuntimeException(msg));
    verifyFailureWithMsg(msg);
    clusterMap.setExceptionOnSnapshot(null);
  }

  // helpers
  // general

  /**
   * Creates a {@link RestRequest} that requests for a snapshot of the cluster map.
   * @return a {@link RestRequest} that requests for a snapshot of the cluster map.
   * @throws Exception
   */
  private RestRequest createRestRequest() throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    data.put(MockRestRequest.URI_KEY, Operations.GET_CLUSTER_MAP_SNAPSHOT);
    return new MockRestRequest(data, null);
  }

  /**
   * Sends the given {@link RestRequest} to the {@link GetClusterMapSnapshotHandler} and waits for the response and
   * returns it.
   * @param restRequest the {@link RestRequest} to send.
   * @param restResponseChannel the {@link RestResponseChannel} where headers will be set.
   * @return the response body as a {@link ReadableStreamChannel}.
   * @throws Exception
   */
  private ReadableStreamChannel sendRequestGetResponse(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();
    final AtomicReference<ReadableStreamChannel> channelRef = new AtomicReference<>();
    handler.handle(restRequest, restResponseChannel, (result, exception) -> {
      channelRef.set(result);
      exceptionRef.set(exception);
      latch.countDown();
    });
    assertTrue("Latch did not count down in time", latch.await(1, TimeUnit.SECONDS));
    if (exceptionRef.get() != null) {
      throw exceptionRef.get();
    }
    return channelRef.get();
  }

  /**
   * Verifies that attempting to get the snapshot of the cluster map fails with {@code msg}.
   * @param msg the message in the {@link Exception} that will be thrown.
   * @throws Exception
   */
  private void verifyFailureWithMsg(String msg) throws Exception {
    RestRequest restRequest = createRestRequest();
    try {
      sendRequestGetResponse(restRequest, new MockRestResponseChannel());
      fail("Request should have failed");
    } catch (Exception e) {
      assertEquals("Unexpected Exception", msg, e.getMessage());
    }
  }
}
