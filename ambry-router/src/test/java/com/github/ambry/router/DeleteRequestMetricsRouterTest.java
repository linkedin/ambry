/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.frontend.IdConverter;
import com.github.ambry.frontend.IdConverterFactory;
import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.protocol.RequestOrResponseType;
import com.github.ambry.rest.DeleteRequestMetrics;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.server.ServerErrorCode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.github.ambry.router.RouterTestHelpers.AWAIT_TIMEOUT_MS;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


@RunWith(Parameterized.class)
public class DeleteRequestMetricsRouterTest extends NonBlockingRouterTestBase {
  private final String scenario;

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{"Local"}, {"OtherOrigin"}, {"Remote"}, {"Repair"}, {"RepairFailure"},
        {"RepairRetryFailure"}, {"RemoteFailure"}, {"ConversionFailure"}, {"RouterClosed"}, {"Background"}});
  }

  public DeleteRequestMetricsRouterTest(String scenario) throws Exception {
    super(false, MessageFormatRecord.Metadata_Content_Version_V3, false);
    this.scenario = scenario;
  }

  @Test
  public void testRequestCohortFromDispatchThroughFinalization() throws Exception {
    Properties props = getNonBlockingRouterProperties(localDcName);
    boolean repair = scenario.startsWith("Repair");
    props.setProperty("router.repair.with.replicate.blob.on.delete.enabled", Boolean.toString(repair));
    IdConverter converter = mock(IdConverter.class);
    IdConverterFactory factory = mock(IdConverterFactory.class);
    when(factory.getIdConverter()).thenReturn(converter);
    setRouterWithIdConverterFactory(props, mockServerLayout, new LoggingNotificationSystem(), factory);
    List<String> blobIds = new ArrayList<>();
    for (int i = 0; i < (scenario.equals("Remote") ? 2 : 1); i++) {
      setOperationParams();
      String blobId = router.putBlob(putBlobProperties, putUserMetadata, putChannel,
          new PutBlobOptionsBuilder().build()).get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (scenario.equals("OtherOrigin")) {
        BlobId original = new BlobId(blobId, mockClusterMap);
        blobId = new BlobId(original.getVersion(), BlobId.BlobIdType.NATIVE,
            (byte) (mockClusterMap.getLocalDatacenterId() + 1), original.getAccountId(), original.getContainerId(),
            original.getPartition(), false, BlobId.BlobDataType.SIMPLE).getID();
      }
      ensurePutInAllServers(blobId, mockServerLayout);
      blobIds.add(blobId);
    }
    when(converter.convert(any(), anyString(), any(), any())).thenAnswer(invocation -> {
      Callback<String> callback = invocation.getArgument(3);
      callback.onCompletion(String.join(",", blobIds),
          scenario.equals("ConversionFailure") ? new IllegalArgumentException("conversion failed") : null);
      return null;
    });
    boolean sourceSelected = false;
    for (MockServer server : mockServerLayout.getMockServers()) {
      if (scenario.startsWith("Remote") && server.getDataCenter().equals(localDcName)) {
        server.setErrorCodeForBlob(blobIds.get(blobIds.size() - 1), ServerErrorCode.ReplicaUnavailable);
      }
      if (scenario.equals("RemoteFailure")) {
        server.setServerErrorForAllRequests(ServerErrorCode.ReplicaUnavailable);
      }
      if (repair) {
        if (server.getDataCenter().equals(localDcName) && !sourceSelected) {
          sourceSelected = true;
          if (scenario.equals("RepairRetryFailure")) {
            server.setServerErrorsByType(RequestOrResponseType.DeleteRequest,
                Arrays.asList(ServerErrorCode.NoError, ServerErrorCode.ReplicaUnavailable));
          }
        } else {
          server.setServerErrorsByType(RequestOrResponseType.DeleteRequest,
              Arrays.asList(server.getDataCenter().equals(localDcName)
                  ? ServerErrorCode.ReplicaUnavailable : ServerErrorCode.BlobNotFound,
                  scenario.equals("RepairRetryFailure") ? ServerErrorCode.ReplicaUnavailable : ServerErrorCode.NoError));
        }
        if (scenario.equals("RepairFailure")) {
          server.setServerErrorsByType(RequestOrResponseType.ReplicateBlobRequest,
              Collections.singletonList(ServerErrorCode.UnknownError));
        }
      }
    }
    if (scenario.equals("RouterClosed")) {
      router.close();
    }
    MetricRegistry registry = new MetricRegistry();
    MockRestRequest request = new MockRestRequest(new JSONObject().put("restMethod", "DELETE").put("uri", "/blob"), null);
    request.setArg(RestUtils.InternalKeys.REQUEST_PATH, RequestPath.parse("/blob", Collections.emptyMap(),
        Collections.emptyList(), "test"));
    request.getMetricsTracker().injectMetrics(new RestRequestMetrics(getClass(), "DeleteBlob", registry));
    request.getMetricsTracker().setDeleteRequestTracker(
        new DeleteRequestMetrics.Tracker(new DeleteRequestMetrics(getClass(), "DeleteBlob", registry)));
    FutureResult<Void> result = new FutureResult<>();
    router.deleteBlob(scenario.equals("Background") ? null : request, blobIds.get(0), "test", result::done, null);
    try {
      result.get(AWAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertFalse(scenario.endsWith("Failure") || scenario.equals("RouterClosed"));
    } catch (ExecutionException e) {
      assertTrue(scenario, scenario.endsWith("Failure") || scenario.equals("RouterClosed"));
      request.getMetricsTracker().markFailure();
    }
    assertTrue(registry.getMeters().values().stream().allMatch(meter -> meter.getCount() == 0));
    if (!scenario.equals("Background")) {
      request.getMetricsTracker().nioMetricsTracker.markFirstByteSent();
      request.getMetricsTracker().recordMetrics();
    }
    String expected = repair ? "OnDemandRepair" : scenario.startsWith("Remote") ? "RemoteAttempt" : "NoRemoteAttempt";
    for (String path : new String[]{"NoRemoteAttempt", "RemoteAttempt", "OnDemandRepair"}) {
      String prefix = MetricRegistry.name(getClass(), "DeleteBlob" + path);
      long count = !scenario.equals("Background") && path.equals(expected) ? 1 : 0;
      assertEquals(prefix, count, registry.getMeters().get(prefix + "Rate").getCount());
      assertEquals(prefix, count, registry.getHistograms().get(prefix + "NioTimeToFirstByteInMs").getCount());
      if (count == 1) {
        assertArrayEquals(new long[]{request.getMetricsTracker().getTimeToFirstByteInMs()},
            registry.getHistograms().get(prefix + "NioTimeToFirstByteInMs").getSnapshot().getValues());
      }
    }
    long remoteDeletes = mockServerLayout.getMockServers().stream()
        .filter(server -> !server.getDataCenter().equals(localDcName))
        .mapToLong(server -> server.getCount(RequestOrResponseType.DeleteRequest)).sum();
    assertEquals(scenario, repair || scenario.startsWith("Remote"), remoteDeletes > 0);
    long replications = mockServerLayout.getMockServers().stream()
        .mapToLong(server -> server.getCount(RequestOrResponseType.ReplicateBlobRequest)).sum();
    assertEquals(scenario, repair, replications > 0);
  }
}
