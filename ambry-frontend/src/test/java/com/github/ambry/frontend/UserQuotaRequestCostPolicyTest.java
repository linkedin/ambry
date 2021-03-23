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
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link UserQuotaRequestCostPolicy}.
 */
public class UserQuotaRequestCostPolicyTest {
  private final static String TEST_SERVICE_ID = "test-service-id";
  private final static short TEST_ACCOUNT_ID = 1;
  private final static short TEST_CONTAINER_ID = 1;
  private final static boolean DEFAULT_ENCRYPTED_FLAG = false;
  private final static short DEFAULT_LIFE_VERSION = 1;
  private final static long MB = 1024 * 1024;
  private final static long GB = MB * 1024L;

  @Test
  public void testCalculateRequestCost() throws UnsupportedEncodingException, URISyntaxException {
    UserQuotaRequestCostPolicy quotaRequestCostPolicy = new UserQuotaRequestCostPolicy();

    RestResponseChannel restResponseChannel = mock(RestResponseChannel.class);
    when(restResponseChannel.getHeader(anyString())).thenReturn(0);
    // test for a 4 MB GET request.
    BlobInfo blobInfo = getBlobInfo(4 * MB);
    RestRequest restRequest = createMockRequestWithMethod(RestMethod.GET, -1);
    Map<String, Double> costMap =
        quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);

    // test for a small GET request (fractional CU).
    blobInfo = getBlobInfo(6 * MB);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 2, 0);

    // test for a GET request of blob of size 0.
    blobInfo = getBlobInfo(0);
    restRequest = createMockRequestWithMethod(RestMethod.GET, -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);

    // test for a small POST request (fractional storage cost).
    blobInfo = getBlobInfo(8 * MB);
    restRequest = createMockRequestWithMethod(RestMethod.POST, 8 * MB);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 2, .0078125);

    // test for a large POST request.
    blobInfo = getBlobInfo(4 * GB);
    restRequest = createMockRequestWithMethod(RestMethod.POST, 4 * GB);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1024, 4);

    // test for a POST request of blob of size 0.
    blobInfo = getBlobInfo(0);
    restRequest = createMockRequestWithMethod(RestMethod.POST, 0);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1, 0);

    // test for a HEAD request.
    restRequest = createMockRequestWithMethod(RestMethod.HEAD, -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);

    // test for a DELETE request.
    restRequest = createMockRequestWithMethod(RestMethod.DELETE, -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1, 0.00390625);

    // test for a PUT request.
    restRequest = createMockRequestWithMethod(RestMethod.PUT, -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1, 0.00390625);

    // test for PUT with null blob info.
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, null);
    verifyWriteCost(costMap, 1, 0.00390625);

    // test BlobInfo and UserMetadata GET requests
    String blobId = "/AAYIAQSSAAgAAQAAAAAAABpFymbGwe7sRBWYa5OPlkcNHQ.bin";
    blobInfo = getBlobInfo(40 * GB);
    restRequest = createMockRequestWithMethod(RestMethod.GET, -1);
    when(restRequest.getUri()).thenReturn(blobId + "/BlobInfo");
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);
    when(restRequest.getUri()).thenReturn(blobId + "/UserMetadata");
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);
    // Plain GET should use blob size
    when(restRequest.getUri()).thenReturn(blobId);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 10240, 0);

    // TODO add a range request case with large range
  }

  /**
   * @return BlobInfo with specified size and default metadata
   * @param blobSize blob size to use
   */
  private BlobInfo getBlobInfo(long blobSize) {
    return new BlobInfo(
        new BlobProperties(blobSize, TEST_SERVICE_ID, TEST_ACCOUNT_ID, TEST_CONTAINER_ID, DEFAULT_ENCRYPTED_FLAG), null,
        DEFAULT_LIFE_VERSION);
  }

  /**
   * Verify cost for read requests.
   * @param costMap {@link Map} of the costs to verify.
   * @param cUCost expected capacity unit cost.
   * @param storageCost expected storage cost.
   */
  private void verifyReadCost(Map<String, Double> costMap, double cUCost, double storageCost) {
    assertEquals("incorrect number of entries in cost map", 2, costMap.size());
    assertTrue("cost for " + QuotaName.READ_CAPACITY_UNIT.name() + " should be present",
        costMap.containsKey(QuotaName.READ_CAPACITY_UNIT.name()));
    assertTrue("cost for " + QuotaName.STORAGE_IN_GB.name() + " should be present",
        costMap.containsKey(QuotaName.STORAGE_IN_GB.name()));
    assertEquals(cUCost, costMap.get(QuotaName.READ_CAPACITY_UNIT.name()), 0.000001);
    assertEquals(storageCost, costMap.get(QuotaName.STORAGE_IN_GB.name()), 0.000001);
  }

  /**
   * Verify cost for read requests.
   * @param costMap {@link Map} of the costs to verify.
   * @param cUCost expected capacity unit cost.
   * @param storageCost expected storage cost.
   */
  private void verifyWriteCost(Map<String, Double> costMap, double cUCost, double storageCost) {
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
  private RestRequest createMockRequestWithMethod(RestMethod restMethod, long bytesReceived) {
    RestRequest restRequest = mock(RestRequest.class);
    when(restRequest.getRestMethod()).thenReturn(restMethod);
    when(restRequest.getBytesReceived()).thenReturn(bytesReceived);
    when(restRequest.getUri()).thenReturn("/");
    return restRequest;
  }
}
