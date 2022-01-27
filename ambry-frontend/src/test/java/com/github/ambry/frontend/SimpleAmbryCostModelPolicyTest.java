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
package com.github.ambry.frontend;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.BlobProperties;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import java.util.Map;
import org.junit.Test;

import static com.github.ambry.quota.CostPolicyTestUtils.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link SimpleAmbryCostModelPolicy}.
 */
public class SimpleAmbryCostModelPolicyTest {
  private final static String TEST_SERVICE_ID = "test-service-id";
  private final static short TEST_ACCOUNT_ID = 1;
  private final static short TEST_CONTAINER_ID = 1;
  private final static boolean DEFAULT_ENCRYPTED_FLAG = false;
  private final static short DEFAULT_LIFE_VERSION = 1;
  private final static long MB = 1024 * 1024;
  private final static long GB = MB * 1024L;

  @Test
  public void testCalculateRequestCost() throws Exception {
    SimpleAmbryCostModelPolicy requestCostPolicy = new SimpleAmbryCostModelPolicy();

    RestResponseChannel restResponseChannel = mock(RestResponseChannel.class);
    when(restResponseChannel.getHeader(anyString())).thenReturn(0);
    String blobUri = "/AAYIAQSSAAgAAQAAAAAAABpFymbGwe7sRBWYa5OPlkcNHQ.bin";
    // test for a 4 MB GET request.
    BlobInfo blobInfo = getBlobInfo(4 * MB);
    RestRequest restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    Map<String, Double> costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1);

    // test for a small GET request (fractional CU).
    blobInfo = getBlobInfo(6 * MB);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 2);

    // test for a GET request of blob of size 0.
    blobInfo = getBlobInfo(0);
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1);

    // test for a small POST request (fractional storage cost).
    blobInfo = getBlobInfo(8 * MB);
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, 8 * MB);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 2, 8 * 1024 * 1024 / (double) QuotaUtils.BYTES_IN_GB);

    // test for a large POST request.
    blobInfo = getBlobInfo(4 * GB);
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, 4 * GB);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1024, 4);

    // test for a POST request of blob of size 0.
    blobInfo = getBlobInfo(0);
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, 0);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1, 0);

    // test for a HEAD request.
    restRequest = createMockRequestWithMethod(RestMethod.HEAD, blobUri, -1);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1);

    // test for a DELETE request.
    restRequest = createMockRequestWithMethod(RestMethod.DELETE, blobUri, -1);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1, 0.0);

    // test for a PUT request.
    restRequest = createMockRequestWithMethod(RestMethod.PUT, blobUri, -1);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyWriteCost(costMap, 1, 0.0);

    // test for PUT with null blob info.
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, null);
    verifyWriteCost(costMap, 1, 0.0);

    // test BlobInfo and UserMetadata GET requests
    blobInfo = getBlobInfo(40 * GB);
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri + "/BlobInfo", -1);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1);
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri + "/UserMetadata", -1);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 1);
    // Plain GET should use blob size
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    costMap = requestCostPolicy.calculateRequestCost(restRequest, restResponseChannel, blobInfo);
    verifyReadCost(costMap, 10240);

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
}
