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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.UserQuotaRequestCostPolicy;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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
  public void testCalculateRequestCost() throws Exception {
    UserQuotaRequestCostPolicy quotaRequestCostPolicy =
        new UserQuotaRequestCostPolicy(new QuotaConfig(new VerifiableProperties(new Properties())));

    RestResponseChannel restResponseChannel = mock(RestResponseChannel.class);
    when(restResponseChannel.getHeader(anyString())).thenReturn(0);
    String blobUri = "/AAYIAQSSAAgAAQAAAAAAABpFymbGwe7sRBWYa5OPlkcNHQ.bin";
    // test for a 4 MB GET request.
    BlobInfo blobInfo = getBlobInfo(4 * MB);
    RestRequest restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    Map<String, Double> costMap =
        quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);

    // test for a small GET request (fractional CU).
    blobInfo = getBlobInfo(6 * MB);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 2, 0);

    // test for a GET request of blob of size 0.
    blobInfo = getBlobInfo(0);
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);

    // test for a small POST request (fractional storage cost).
    blobInfo = getBlobInfo(8 * MB);
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, 8 * MB);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 2, 8 * 1024 * 1024 / UserQuotaRequestCostPolicy.BYTES_IN_GB);

    // test for a large POST request.
    blobInfo = getBlobInfo(4 * GB);
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, 4 * GB);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1024, 4);

    // test for a POST request of blob of size 0.
    blobInfo = getBlobInfo(0);
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, 0);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1, 0);

    // test for a HEAD request.
    restRequest = createMockRequestWithMethod(RestMethod.HEAD, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);

    // test for a DELETE request.
    restRequest = createMockRequestWithMethod(RestMethod.DELETE, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1, 0.0);

    // test for a PUT request.
    restRequest = createMockRequestWithMethod(RestMethod.PUT, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1, 0.0);

    // test for PUT with null blob info.
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, null);
    verifyWriteCost(costMap, 1, 0.0);

    // test BlobInfo and UserMetadata GET requests
    blobInfo = getBlobInfo(40 * GB);
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri + "/BlobInfo", -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri + "/UserMetadata", -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1, 0);
    // Plain GET should use blob size
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 10240, 0);

    // TODO add a range request case with large range
  }

  @Test
  public void testCalculateRequestQuotaCharge() throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(new Properties()));
    UserQuotaRequestCostPolicy quotaRequestCostPolicy = new UserQuotaRequestCostPolicy(quotaConfig);

    RestResponseChannel restResponseChannel = mock(RestResponseChannel.class);
    when(restResponseChannel.getHeader(anyString())).thenReturn(0);
    String blobUri = "/AAYIAQSSAAgAAQAAAAAAABpFymbGwe7sRBWYa5OPlkcNHQ.bin";
    // test for a 4 MB GET request.
    long blobSize = 4 * MB;
    RestRequest restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    Map<String, Double> costMap =
        quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyReadCost(costMap, Math.ceil(blobSize/quotaConfig.quotaAccountingUnit), 0);

    // test for a small GET request (fractional CU).
    blobSize = 6 * MB;
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyReadCost(costMap, Math.ceil(blobSize/quotaConfig.quotaAccountingUnit), 0);

    // test for a GET request of blob of size 0.
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, 0);
    verifyReadCost(costMap, 1, 0);

    // test for a GET request of blob of size 512.
    blobSize = 512;
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyReadCost(costMap, 1, 0);

    // test for a small POST request (fractional storage cost).
    blobSize = 8 * MB;
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, blobSize);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyWriteCost(costMap, Math.ceil(blobSize/quotaConfig.quotaAccountingUnit), 8 * 1024 * 1024 / UserQuotaRequestCostPolicy.BYTES_IN_GB);

    // test for a large POST request.
    blobSize = 4 * GB;
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, blobSize);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyWriteCost(costMap, Math.ceil(blobSize/quotaConfig.quotaAccountingUnit), 4);

    // test for a POST request of blob of size 0.
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, 0);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, 0);
    verifyWriteCost(costMap, 1, 0);

    // test for a POST request of blob of size 512.
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, 0);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, 512);
    verifyWriteCost(costMap, 1, 0);

    // test for a HEAD request.
    restRequest = createMockRequestWithMethod(RestMethod.HEAD, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, quotaConfig.quotaAccountingUnit);
    verifyReadCost(costMap, 1, 0);

    // test for a DELETE request.
    restRequest = createMockRequestWithMethod(RestMethod.DELETE, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, quotaConfig.quotaAccountingUnit);
    verifyWriteCost(costMap, 1, 0.0);

    // test for a PUT request.
    restRequest = createMockRequestWithMethod(RestMethod.PUT, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, quotaConfig.quotaAccountingUnit);
    verifyWriteCost(costMap, 1, 0.0);

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

  private RestRequest createMockRequestWithMethod(RestMethod restMethod, String uri, long bytesReceived)
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
