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
 * distributed under the License is distributed /on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.frontend;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


public class SimpleQuotaRequestCostPolicyTest {
  private final static String TEST_SERVICE_ID = "test-service-id";
  private final static short TEST_ACCOUNT_ID = 1;
  private final static short TEST_CONTAINER_ID = 1;
  private final static boolean DEFAULT_ENCRYPTED_FLAG = false;
  private final static short DEFAULT_LIFE_VERSION = 1;

  @Test
  public void testCalculateRequestCost() throws UnsupportedEncodingException, URISyntaxException {
    SimpleUserQuotaRequestCostPolicy quotaRequestCostPolicy = new SimpleUserQuotaRequestCostPolicy();

    // test for a 4 MB GET request.
    BlobInfo blobInfo = new BlobInfo(
        new BlobProperties(4 * 1024 * 1024, TEST_SERVICE_ID, TEST_ACCOUNT_ID, TEST_CONTAINER_ID,
            DEFAULT_ENCRYPTED_FLAG), null, DEFAULT_LIFE_VERSION);
    RestRequest restRequest = createMockRequestWithMethod(RestMethod.GET);
    Map<String, Double> costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, blobInfo);
    verifyReadCost(costMap, 1, 0);

    // test for a small GET request (fractional CU).
    blobInfo = new BlobInfo(new BlobProperties(6 * 1024 * 1024, TEST_SERVICE_ID, TEST_ACCOUNT_ID, TEST_CONTAINER_ID,
        DEFAULT_ENCRYPTED_FLAG), null, DEFAULT_LIFE_VERSION);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, blobInfo);
    verifyReadCost(costMap, 2, 0);

    // test for a GET request of blob of size 0.
    blobInfo =
        new BlobInfo(new BlobProperties(0, TEST_SERVICE_ID, TEST_ACCOUNT_ID, TEST_CONTAINER_ID, DEFAULT_ENCRYPTED_FLAG),
            null, DEFAULT_LIFE_VERSION);
    restRequest = createMockRequestWithMethod(RestMethod.GET);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, blobInfo);
    verifyReadCost(costMap, 1, 0);

    // test for a small POST request (fractional storage cost).
    blobInfo = new BlobInfo(new BlobProperties(8 * 1024 * 1024, TEST_SERVICE_ID, TEST_ACCOUNT_ID, TEST_CONTAINER_ID,
        DEFAULT_ENCRYPTED_FLAG), null, DEFAULT_LIFE_VERSION);
    restRequest = createMockRequestWithMethod(RestMethod.POST);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, blobInfo);
    verifyWriteCost(costMap, 2, .0078);

    // test for a large POST request.
    blobInfo = new BlobInfo(
        new BlobProperties(4 * 1024 * 1024 * 1024L, TEST_SERVICE_ID, TEST_ACCOUNT_ID, TEST_CONTAINER_ID,
            DEFAULT_ENCRYPTED_FLAG), null, DEFAULT_LIFE_VERSION);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, blobInfo);
    verifyWriteCost(costMap, 1024, 4);

    // test for a POST request of blob of size 0.
    blobInfo =
        new BlobInfo(new BlobProperties(0, TEST_SERVICE_ID, TEST_ACCOUNT_ID, TEST_CONTAINER_ID, DEFAULT_ENCRYPTED_FLAG),
            null, DEFAULT_LIFE_VERSION);
    restRequest = createMockRequestWithMethod(RestMethod.POST);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, blobInfo);
    verifyWriteCost(costMap, 1, 0);

    // test for a HEAD request.
    restRequest = createMockRequestWithMethod(RestMethod.HEAD);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, blobInfo);
    verifyReadCost(costMap, 1, 0);

    // test for a DELETE request.
    restRequest = createMockRequestWithMethod(RestMethod.DELETE);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, blobInfo);
    verifyWriteCost(costMap, 1, 0.0039);

    // test for a PUT request.
    restRequest = createMockRequestWithMethod(RestMethod.PUT);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, blobInfo);
    verifyWriteCost(costMap, 1, 0.0039);

    // test for null blob info.
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, null);
    assertTrue("cost map should be empty if blob info is null", costMap.isEmpty());
  }

  private void verifyReadCost(Map<String, Double> costMap, double readCUCost, double storageCost) {
    assertEquals("incorrect number of entries in cost map", 2, costMap.size());
    assertTrue("cost for " + QuotaName.READ_CAPACITY_UNIT.name() + " should be present",
        costMap.containsKey(QuotaName.READ_CAPACITY_UNIT.name()));
    assertTrue("cost for " + QuotaName.STORAGE_IN_GB.name() + " should be present",
        costMap.containsKey(QuotaName.STORAGE_IN_GB.name()));
    assertEquals(readCUCost, costMap.get(QuotaName.READ_CAPACITY_UNIT.name()), 0.0001);
    assertEquals(storageCost, costMap.get(QuotaName.STORAGE_IN_GB.name()), 0.0001);
  }

  private void verifyWriteCost(Map<String, Double> costMap, double readCUCost, double storageCost) {
    assertEquals("incorrect number of entries in cost map", 2, costMap.size());
    assertTrue("cost for " + QuotaName.WRITE_CAPACITY_UNIT.name() + " should be present",
        costMap.containsKey(QuotaName.WRITE_CAPACITY_UNIT.name()));
    assertTrue("cost for " + QuotaName.STORAGE_IN_GB.name() + " should be present",
        costMap.containsKey(QuotaName.STORAGE_IN_GB.name()));
    assertEquals(readCUCost, costMap.get(QuotaName.WRITE_CAPACITY_UNIT.name()), 0.0001);
    assertEquals(storageCost, costMap.get(QuotaName.STORAGE_IN_GB.name()), 0.0001);
  }

  private RestRequest createMockRequestWithMethod(RestMethod restMethod)
      throws UnsupportedEncodingException, URISyntaxException {
    JSONObject request = new JSONObject();
    request.put(MockRestRequest.REST_METHOD_KEY, restMethod.name());
    request.put(MockRestRequest.URI_KEY, "/");
    return new MockRestRequest(request, null);
  }
}
