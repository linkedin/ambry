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

import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaResourceType;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


public class OperationQuotaChargerTest {
  private final BlobId BLOBID = new BlobId((short) 1, BlobId.BlobIdType.NATIVE, (byte) 1, (short) 1, (short) 1,
      new Partition((short) 1, "", PartitionState.READ_ONLY, 1073741824L), false, BlobId.BlobDataType.DATACHUNK);

  @Test
  public void testCheck() throws Exception {
    // check should return true if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger = new OperationQuotaCharger(null, BLOBID, "GetOperation");
    Assert.assertTrue("check should return true if quotaChargeCallback is null.", operationQuotaCharger.check());

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");

    // check should return true if quotaChargeCallback returns true.
    Mockito.when(quotaChargeCallback.check()).thenReturn(true);
    Assert.assertTrue("check should return true if quotaChargeCallback returns true.", operationQuotaCharger.check());

    // check should return false if quotaChargeCallback returns false.
    Mockito.when(quotaChargeCallback.check()).thenReturn(false);
    Assert.assertFalse("check should return false if quotaChargeCallback returns false.",
        operationQuotaCharger.check());

    // check should return false even if quotaChargeCallback returns false when isCharged is true.
    Mockito.when(quotaChargeCallback.checkAndCharge()).thenReturn(true);
    operationQuotaCharger.checkAndCharge(); // sets isCharged to true.
    Mockito.when(quotaChargeCallback.check()).thenReturn(false);
    Assert.assertTrue("check should return true even if quotaChargeCallback returns false when isCharged is true.",
        operationQuotaCharger.check());
  }

  @Test
  public void testCharge() throws Exception {
    // chargeIfUsageWithinQuota should return true if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger = new OperationQuotaCharger(null, BLOBID, "GetOperation");
    Assert.assertTrue("chargeIfUsageWithinQuota should return true if quotaChargeCallback is null.",
        operationQuotaCharger.checkAndCharge());

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");

    // chargeIfUsageWithinQuota should return false if quotaChargeCallback throws exception and isCharged is false.
    Mockito.doThrow(
        new QuotaException("too many requests", new RouterException("", RouterErrorCode.TooManyRequests), true))
        .when(quotaChargeCallback)
        .checkAndCharge();
    Assert.assertFalse(
        "chargeIfUsageWithinQuota should return true if quotaChargeCallback throws exception and isCharged is false.",
        operationQuotaCharger.checkAndCharge());
    Mockito.verify(quotaChargeCallback, Mockito.times(1)).checkAndCharge();

    // chargeIfUsageWithinQuota should return true if quotaChargeCallback.chargeIfUsageWithinQuota goes through.
    Mockito.doReturn(true).when(quotaChargeCallback).checkAndCharge();
    Assert.assertTrue(
        "chargeIfUsageWithinQuota should return true if quotaChargeCallback.chargeIfUsageWithinQuota goes through.",
        operationQuotaCharger.checkAndCharge());
    Mockito.verify(quotaChargeCallback, Mockito.times(2)).checkAndCharge();

    // Once isCharged is true, chargeIfUsageWithinQuota should never call quotaChargeCallback.chargeIfUsageWithinQuota.
    Assert.assertTrue(
        "Once isCharged is true, chargeIfUsageWithinQuota should never call quotaChargeCallback.chargeIfUsageWithinQuota.",
        operationQuotaCharger.checkAndCharge());
    Mockito.verify(quotaChargeCallback, Mockito.times(2)).checkAndCharge();
  }

  @Test
  public void testQuotaExceedAllowed() throws Exception {
    // chargeIfQuotaExceedAllowed should return true if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger = new OperationQuotaCharger(null, BLOBID, "GetOperation");
    Assert.assertTrue("chargeIfQuotaExceedAllowed should return true if quotaChargeCallback is null.",
        operationQuotaCharger.chargeIfQuotaExceedAllowed());

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");

    // chargeIfQuotaExceedAllowed should return true if quotaChargeCallback.chargeIfQuotaExceedAllowed returns true.
    Mockito.when(quotaChargeCallback.chargeIfQuotaExceedAllowed()).thenReturn(true);
    Assert.assertTrue(
        "chargeIfQuotaExceedAllowed should return true if quotaChargeCallback.chargeIfQuotaExceedAllowed returns true.",
        operationQuotaCharger.chargeIfQuotaExceedAllowed());

    // chargeIfQuotaExceedAllowed should return false if quotaChargeCallback.chargeIfQuotaExceedAllowed returns false.
    Mockito.when(quotaChargeCallback.chargeIfQuotaExceedAllowed()).thenReturn(false);
    Assert.assertFalse(
        "chargeIfQuotaExceedAllowed should return false if quotaChargeCallback.chargeIfQuotaExceedAllowed returns false.",
        operationQuotaCharger.chargeIfQuotaExceedAllowed());
  }

  @Test
  public void testGetQuotaResource() throws Exception {
    // getQuotaResource should return null if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger = new OperationQuotaCharger(null, BLOBID, "GetOperation");
    Assert.assertNull("getQuotaResource should return null if quotaChargeCallback is null.",
        operationQuotaCharger.getQuotaResource());

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");

    // getQuotaResource should return what quotaChargeCallback.getQuotaResource returns.
    QuotaResource quotaResource = new QuotaResource("test", QuotaResourceType.ACCOUNT);
    Mockito.when(quotaChargeCallback.getQuotaResource()).thenReturn(quotaResource);
    Assert.assertEquals("getQuotaResource should return what quotaChargeCallback.getQuotaResource returns.",
        quotaResource, quotaChargeCallback.getQuotaResource());

    // getQuotaResource should return null if quotaChargeCallback throws exception.
    Mockito.when(quotaChargeCallback.getQuotaResource())
        .thenThrow(
            new QuotaException("", new RestServiceException("", RestServiceErrorCode.InternalServerError), false));
    Assert.assertNull("getQuotaResource should return null if quotaChargeCallback is null.",
        operationQuotaCharger.getQuotaResource());
  }
}
