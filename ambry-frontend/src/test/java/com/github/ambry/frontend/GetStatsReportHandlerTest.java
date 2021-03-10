/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingBiConsumer;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Tests for {@link GetStatsReportHandler}.
 */
public class GetStatsReportHandlerTest {
  private final String CLUSTER_NAME = "ambry-test";
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final GetStatsReportHandler handler;
  private final ObjectMapper mapper = new ObjectMapper();
  private final AccountStatsStore accountStatsStore;

  public GetStatsReportHandlerTest() {
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    accountStatsStore = mock(AccountStatsStore.class);
    handler = new GetStatsReportHandler(securityServiceFactory.getSecurityService(), metrics, accountStatsStore);
  }

  @Test
  public void handleGoodCaseTest() throws Exception {
    ThrowingBiConsumer<RestRequest, StatsSnapshot> testAction = (request, expectedStatsSnapshot) -> {
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      ReadableStreamChannel channel = sendRequestGetResponse(request, restResponseChannel);
      assertNotNull("There should be a response", channel);
      assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
      assertEquals("Content-type is not as expected", RestUtils.JSON_CONTENT_TYPE,
          restResponseChannel.getHeader(RestUtils.Headers.CONTENT_TYPE));
      assertEquals("Content-length is not as expected", channel.getSize(),
          Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
      assertEquals("StatsSnapshot mismatch", expectedStatsSnapshot,
          mapper.readValue(RestTestUtils.getResponseBody(channel), StatsSnapshot.class));
    };
    StatsSnapshot accountStatsSnapshot =
        TestUtils.makeAccountStatsSnapshotFromContainerStorageMap(TestUtils.makeStorageMap(10, 10, 10000, 1000));
    doAnswer(invocation -> {
      String clusterName = invocation.getArgument(0);
      if (clusterName.equals(CLUSTER_NAME)) {
        return accountStatsSnapshot;
      } else {
        return null;
      }
    }).when(accountStatsStore).queryAggregatedAccountStatsByClusterName(anyString());
    RestRequest restRequest = createRestRequest(CLUSTER_NAME, StatsReportType.ACCOUNT_REPORT.name());
    testAction.accept(restRequest, accountStatsSnapshot);

    StatsSnapshot partitionClassStatsSnapshot =
        TestUtils.makeAggregatedPartitionClassStats(new String[]{"PartitionClass1", "PartitionClass2"}, 10, 10);
    doAnswer(invocation -> {
      String clusterName = invocation.getArgument(0);
      if (clusterName.equals(CLUSTER_NAME)) {
        return partitionClassStatsSnapshot;
      } else {
        return null;
      }
    }).when(accountStatsStore).queryAggregatedPartitionClassStatsByClusterName(anyString());
    restRequest = createRestRequest(CLUSTER_NAME, StatsReportType.PARTITION_CLASS_REPORT.name());
    testAction.accept(restRequest, partitionClassStatsSnapshot);
  }

  @Test
  public void handleBadCaseTest() throws Exception {
    ThrowingBiConsumer<RestRequest, RestServiceErrorCode> testAction = (request, expectedErrorCode) -> {
      TestUtils.assertException(RestServiceException.class,
          () -> sendRequestGetResponse(request, new MockRestResponseChannel()),
          e -> assertEquals(expectedErrorCode, e.getErrorCode()));
    };
    RestRequest request = createRestRequest("WRONG_CLUSTER", StatsReportType.ACCOUNT_REPORT.name());
    testAction.accept(request, RestServiceErrorCode.NotFound);
    request = createRestRequest(null, StatsReportType.ACCOUNT_REPORT.name());
    testAction.accept(request, RestServiceErrorCode.MissingArgs);
    request = createRestRequest(CLUSTER_NAME, "WRONG_STATS_REPORT_TYPE");
    testAction.accept(request, RestServiceErrorCode.BadRequest);
    request = createRestRequest(CLUSTER_NAME, null);
    testAction.accept(request, RestServiceErrorCode.MissingArgs);
  }

  // Helpers

  private RestRequest createRestRequest(String clusterName, String reportType) throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.GET.name());
    data.put(MockRestRequest.URI_KEY, Operations.STATS_REPORT);
    JSONObject headers = new JSONObject();
    if (reportType != null) {
      headers.put(RestUtils.Headers.GET_STATS_REPORT_TYPE, reportType);
    }
    if (clusterName != null) {
      headers.put(RestUtils.Headers.CLUSTER_NAME, clusterName);
    }
    data.put(MockRestRequest.HEADERS_KEY, headers);
    return new MockRestRequest(data, null);
  }

  private ReadableStreamChannel sendRequestGetResponse(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    FutureResult<ReadableStreamChannel> future = new FutureResult<>();
    handler.handle(restRequest, restResponseChannel, future::done);
    try {
      return future.get(1, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : new Exception(e.getCause());
    }
  }
}
