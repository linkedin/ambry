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

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;

import static com.github.ambry.quota.CostPolicyTestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


/**
 * Test for {@link SimpleRequestQuotaCostPolicy}.
 */
public class SimpleRequestQuotaCostPolicyTest {
  private final static long MB = 1024 * 1024;

  @Test
  public void testCalculateRequestQuotaCharge() throws Exception {
    QuotaConfig quotaConfig = new QuotaConfig(new VerifiableProperties(new Properties()));
    SimpleRequestQuotaCostPolicy quotaRequestCostPolicy = new SimpleRequestQuotaCostPolicy(quotaConfig);

    RestResponseChannel restResponseChannel = mock(RestResponseChannel.class);
    when(restResponseChannel.getHeader(anyString())).thenReturn(0);
    String blobUri = "/AAYIAQSSAAgAAQAAAAAAABpFymbGwe7sRBWYa5OPlkcNHQ.bin";
    // test for a 4 MB GET request.
    long blobSize = 4 * MB;
    RestRequest restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    Map<String, Double> costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyReadCost(costMap, Math.ceil(blobSize / (double) quotaConfig.quotaAccountingUnit));

    // test for a small GET request (fractional CU).
    blobSize = 6 * MB;
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyReadCost(costMap, Math.ceil(blobSize / (double) quotaConfig.quotaAccountingUnit));

    // test for a GET request of blob of size 0.
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, 0);
    verifyReadCost(costMap, 1);

    // test for a GET request of blob of size 512.
    blobSize = 512;
    restRequest = createMockRequestWithMethod(RestMethod.GET, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyReadCost(costMap, 1);

    // test for a small POST request (fractional storage cost).
    blobSize = 8 * MB;
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, blobSize);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyWriteCost(costMap, Math.ceil(blobSize / (double) quotaConfig.quotaAccountingUnit),
        8 * 1024 * 1024 / (double) QuotaUtils.BYTES_IN_GB);

    // test for a large POST request.
    blobSize = 4 * QuotaUtils.BYTES_IN_GB;
    restRequest = createMockRequestWithMethod(RestMethod.POST, blobUri, blobSize);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, blobSize);
    verifyWriteCost(costMap, Math.ceil(blobSize / (double) quotaConfig.quotaAccountingUnit), 4);

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
    verifyReadCost(costMap, 1);

    // test for a DELETE request.
    restRequest = createMockRequestWithMethod(RestMethod.DELETE, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, quotaConfig.quotaAccountingUnit);
    verifyWriteCost(costMap, 1, 0.0);

    // test for a PUT request.
    restRequest = createMockRequestWithMethod(RestMethod.PUT, blobUri, -1);
    costMap = quotaRequestCostPolicy.calculateRequestQuotaCharge(restRequest, quotaConfig.quotaAccountingUnit);
    verifyWriteCost(costMap, 1, 0.0);
  }
}
