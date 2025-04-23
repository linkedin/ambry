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
package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.RouterConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.quota.QuotaAction;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaMethod;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


public class OperationQuotaChargerTest {
  private static final QuotaResource QUOTA_RESOURCE = new QuotaResource("test", QuotaResourceType.ACCOUNT);
  private final BlobId BLOBID = new BlobId((short) 1, BlobId.BlobIdType.NATIVE, (byte) 1, (short) 1, (short) 1,
      new Partition((short) 1, "", PartitionState.READ_ONLY, 1073741824L), false, BlobId.BlobDataType.DATACHUNK);
  private final RouterConfig routerConfig;

  public OperationQuotaChargerTest() {
    Properties properties = new Properties();
    properties.setProperty("router.hostname", "localhost");
    properties.setProperty("router.datacenter.name", "dc");
    routerConfig = new RouterConfig(new VerifiableProperties(properties));
  }

  @Test
  public void testCheckAndCharge() throws Exception {
    testCheckAndCharge(true);
    testCheckAndCharge(false);
  }

  @Test
  public void testGetQuotaResource() throws Exception {
    ClusterMap clusterMap = new MockClusterMap();
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(clusterMap, routerConfig);
    // getQuotaResource should return null if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger =
        new OperationQuotaCharger(null, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    Assert.assertNull("getQuotaResource should return null if quotaChargeCallback is null.",
        operationQuotaCharger.getQuotaResource());

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);

    // getQuotaResource should return what quotaChargeCallback.getQuotaResource returns.
    QuotaResource quotaResource = new QuotaResource("test", QuotaResourceType.ACCOUNT);
    Mockito.when(quotaChargeCallback.getQuotaResource()).thenReturn(quotaResource);
    Assert.assertEquals("getQuotaResource should return what quotaChargeCallback.getQuotaResource returns.",
        quotaResource, quotaChargeCallback.getQuotaResource());

    // getQuotaResource should return null if quotaChargeCallback throws quota exception.
    Mockito.when(quotaChargeCallback.getQuotaResource())
        .thenThrow(
            new QuotaException("", new RestServiceException("", RestServiceErrorCode.InternalServerError), false));
    Assert.assertNull("getQuotaResource should return null if quotaChargeCallback is null.",
        operationQuotaCharger.getQuotaResource());

    // getQuotaResource should return null if quotaChargeCallback throws exception.
    Assert.assertEquals(0, routerMetrics.unknownExceptionInChargeableRate.getCount());
    Mockito.doThrow(new RuntimeException("")).when(quotaChargeCallback).getQuotaResource();
    Assert.assertNull("getQuotaResource should return null if quotaChargeCallback is null.",
        operationQuotaCharger.getQuotaResource());
    Assert.assertEquals(1, routerMetrics.unknownExceptionInChargeableRate.getCount());
  }

  @Test
  public void testGetQuotaMethod() throws Exception {
    ClusterMap clusterMap = new MockClusterMap();
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(clusterMap, routerConfig);
    // getQuotaResource should return null if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger =
        new OperationQuotaCharger(null, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    Assert.assertNull("getQuotaResource should return null if quotaChargeCallback is null.",
        operationQuotaCharger.getQuotaMethod());

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    Mockito.when(quotaChargeCallback.getQuotaMethod()).thenReturn(QuotaMethod.READ);
    operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    Assert.assertEquals(QuotaMethod.READ, operationQuotaCharger.getQuotaMethod());

    Mockito.when(quotaChargeCallback.getQuotaMethod()).thenReturn(QuotaMethod.WRITE);
    operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    Assert.assertEquals(QuotaMethod.WRITE, operationQuotaCharger.getQuotaMethod());
  }

  private void testCheckAndCharge(boolean shouldCheckExceedAllowed) throws Exception {
    ClusterMap clusterMap = new MockClusterMap();
    NonBlockingRouterMetrics routerMetrics = new NonBlockingRouterMetrics(clusterMap, routerConfig);
    // checkAndCharge should return allow if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger =
        new OperationQuotaCharger(null, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    Assert.assertEquals("checkAndCharge should return allow if quotaChargeCallback is null.", QuotaAction.ALLOW,
        operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    Mockito.when(quotaChargeCallback.getQuotaResource()).thenReturn(QUOTA_RESOURCE);
    operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);

    // checkAndCharge should return allow if quotaChargeCallback returns ALLOW.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.ALLOW);
    Assert.assertEquals("checkAndCharge should return allow if quotaChargeCallback returns true.", QuotaAction.ALLOW,
        operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    // checkAndCharge should return delay if quotaChargeCallback returns DELAY.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.DELAY);
    Assert.assertEquals("checkAndCharge should return delay if quotaChargeCallback returns delay.", QuotaAction.DELAY,
        operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    // checkAndCharge should return allow if quotaChargeCallback returns delay but isCharged is true.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.ALLOW);
    operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed); // sets isCharged to true.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.DELAY);
    Assert.assertEquals(
        "checkAndCharge should return allow if quotaChargeCallback returns DELAY but isCharged is true.",
        QuotaAction.ALLOW, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    // checkAndCharge should return REJECT if quotaChargeCallback returns REJECT.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.REJECT);
    Assert.assertEquals(
        "checkAndCharge should return reject if quotaChargeCallback returns DELAY but isCharged is true.",
        QuotaAction.REJECT, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    // checkAndCharge should return allow if quotaChargeCallback returns REJECT but isCharged is true.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false)).thenReturn(QuotaAction.ALLOW);
    operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed); // sets isCharged to true.
    Mockito.when(quotaChargeCallback.checkAndCharge(false, false)).thenReturn(QuotaAction.REJECT);
    Assert.assertEquals(
        "checkAndCharge should return allow if quotaChargeCallback returns REJECT but isCharged is true.",
        QuotaAction.ALLOW, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));

    operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    // test if any retryable quota exception is thrown in quota charge callback.
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false))
        .thenThrow(new QuotaException("test", true));
    // exception should not propagate.
    Assert.assertEquals("checkAndCharge should return allow if there is any exception from quotachargecallback.",
        QuotaAction.ALLOW, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));
    Mockito.verify(quotaChargeCallback, Mockito.times(6)).checkAndCharge(shouldCheckExceedAllowed, false);

    Mockito.doReturn(QuotaAction.ALLOW).when(quotaChargeCallback).checkAndCharge(shouldCheckExceedAllowed, false);
    operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed); // sets isCharged to true.
    // Since quota exception was retryable above, verify that the operation is retried.
    Mockito.verify(quotaChargeCallback, Mockito.times(7)).checkAndCharge(shouldCheckExceedAllowed, false);
    Mockito.when(quotaChargeCallback.checkAndCharge(shouldCheckExceedAllowed, false))
        .thenThrow(new QuotaException("test", false));
    Assert.assertEquals("checkAndCharge should return allow if there is any exception from quotachargecallback.",
        QuotaAction.ALLOW, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));
    // Since operation was done, verify that the operation should not have happened.
    Mockito.verify(quotaChargeCallback, Mockito.times(7)).checkAndCharge(shouldCheckExceedAllowed, false);

    operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    // test if any non retryable quota exception is thrown in quota charge callback.
    Mockito.doThrow(new QuotaException("test", false))
        .when(quotaChargeCallback)
        .checkAndCharge(shouldCheckExceedAllowed, false);
    operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed);
    Mockito.verify(quotaChargeCallback, Mockito.times(8)).checkAndCharge(shouldCheckExceedAllowed, false);
    Mockito.doReturn(QuotaAction.ALLOW).when(quotaChargeCallback).checkAndCharge(shouldCheckExceedAllowed, false);
    operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed);
    // Since quota exception was non retryable above, verify that the operation is not retried.
    Mockito.verify(quotaChargeCallback, Mockito.times(8)).checkAndCharge(shouldCheckExceedAllowed, false);

    operationQuotaCharger =
        new OperationQuotaCharger(quotaChargeCallback, BLOBID, GetOperation.class.getSimpleName(), routerMetrics);
    // test if any runtime exception is thrown in quota charge callback.
    Assert.assertEquals(0, routerMetrics.unknownExceptionInChargeableRate.getCount());
    Mockito.doThrow(new RuntimeException("test"))
        .when(quotaChargeCallback)
        .checkAndCharge(shouldCheckExceedAllowed, false);
    // exception should not propagate.
    Assert.assertEquals("checkAndCharge should return allow if there is any exception from quotachargecallback.",
        QuotaAction.ALLOW, operationQuotaCharger.checkAndCharge(shouldCheckExceedAllowed));
    Mockito.verify(quotaChargeCallback, Mockito.times(9)).checkAndCharge(shouldCheckExceedAllowed, false);
    Assert.assertEquals(1, routerMetrics.unknownExceptionInChargeableRate.getCount());
  }
}
