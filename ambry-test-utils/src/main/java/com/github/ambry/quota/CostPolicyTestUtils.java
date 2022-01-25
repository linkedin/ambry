/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota;

import com.github.ambry.frontend.Operations;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class CostPolicyTestUtils {

  /**
   * Verify cost for read requests.
   * @param costMap {@link Map} of the costs to verify.
   * @param cUCost expected capacity unit cost.
   */
  public static void verifyReadCost(Map<String, Double> costMap, double cUCost) {
    assertEquals("incorrect number of entries in cost map", 2, costMap.size());
    assertTrue("cost for " + QuotaName.READ_CAPACITY_UNIT.name() + " should be present",
        costMap.containsKey(QuotaName.READ_CAPACITY_UNIT.name()));
    assertTrue("cost for " + QuotaName.STORAGE_IN_GB.name() + " should be present",
        costMap.containsKey(QuotaName.STORAGE_IN_GB.name()));
    assertEquals(cUCost, costMap.get(QuotaName.READ_CAPACITY_UNIT.name()), 0.000001);
    assertEquals(0, costMap.get(QuotaName.STORAGE_IN_GB.name()), 0.000001);
  }

  /**
   * Verify cost for read requests.
   * @param costMap {@link Map} of the costs to verify.
   * @param cUCost expected capacity unit cost.
   * @param storageCost expected storage cost.
   */
  public static void verifyWriteCost(Map<String, Double> costMap, double cUCost, double storageCost) {
    assertEquals("incorrect number of entries in cost map", 2, costMap.size());
    assertTrue("cost for " + QuotaName.WRITE_CAPACITY_UNIT.name() + " should be present",
        costMap.containsKey(QuotaName.WRITE_CAPACITY_UNIT.name()));
    assertTrue("cost for " + QuotaName.STORAGE_IN_GB.name() + " should be present",
        costMap.containsKey(QuotaName.STORAGE_IN_GB.name()));
    assertEquals(cUCost, costMap.get(QuotaName.WRITE_CAPACITY_UNIT.name()), 0.000001);
    assertEquals(storageCost, costMap.get(QuotaName.STORAGE_IN_GB.name()), 0.000001);
  }

  /**
   * Creates a mock {@link RestRequest} object for test.
   * @param restMethod {@link RestMethod} of the RestRequest.
   * @param bytesReceived number of bytes received in the request.
   * @return RestRequest object.
   */
  public static RestRequest createMockRequestWithMethod(RestMethod restMethod, String uri, long bytesReceived)
      throws RestServiceException {
    RestRequest restRequest = mock(RestRequest.class);
    when(restRequest.getRestMethod()).thenReturn(restMethod);
    when(restRequest.getBlobBytesReceived()).thenReturn(bytesReceived);
    when(restRequest.getBytesReceived()).thenReturn(bytesReceived);
    if (restMethod == RestMethod.POST) {
      when(restRequest.getUri()).thenReturn("/");
      when(restRequest.getPath()).thenReturn("/");
    } else {
      when(restRequest.getUri()).thenReturn(uri);
      when(restRequest.getPath()).thenReturn(uri);
    }
    RequestPath requestPath = RequestPath.parse(restRequest, null, "ambry-test");
    if (restMethod == RestMethod.PUT && bytesReceived != -1) {
      requestPath =
          RequestPath.parse("/" + Operations.NAMED_BLOB, Collections.emptyMap(), Collections.emptyList(), "ambry-test");
    }
    Map<String, Object> args = new HashMap<>();
    args.put(RestUtils.InternalKeys.REQUEST_PATH, requestPath);
    when(restRequest.getArgs()).thenReturn(args);
    return restRequest;
  }
}
