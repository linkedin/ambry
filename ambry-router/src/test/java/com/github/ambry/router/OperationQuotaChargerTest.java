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
    Mockito.doNothing().when(quotaChargeCallback).charge();
    operationQuotaCharger.charge(); // sets isCharged to true.
    Mockito.when(quotaChargeCallback.check()).thenReturn(false);
    Assert.assertTrue("check should return true even if quotaChargeCallback returns false when isCharged is true.",
        operationQuotaCharger.check());
  }

  @Test
  public void testCharge() throws Exception {
    // charge should return true if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger = new OperationQuotaCharger(null, BLOBID, "GetOperation");
    Assert.assertTrue("charge should return true if quotaChargeCallback is null.", operationQuotaCharger.charge());

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");

    // charge should return false if quotaChargeCallback throws exception and isCharged is false.
    Mockito.doThrow(
        new QuotaException("too many requests", new RouterException("", RouterErrorCode.TooManyRequests), true))
        .when(quotaChargeCallback)
        .charge();
    Assert.assertFalse("charge should return true if quotaChargeCallback throws exception and isCharged is false.",
        operationQuotaCharger.charge());
    Mockito.verify(quotaChargeCallback, Mockito.times(1)).charge();

    // charge should return true if quotaChargeCallback.charge goes through.
    Mockito.doNothing().when(quotaChargeCallback).charge();
    Assert.assertTrue("charge should return true if quotaChargeCallback.charge goes through.",
        operationQuotaCharger.charge());
    Mockito.verify(quotaChargeCallback, Mockito.times(2)).charge();

    // Once isCharged is true, charge should never call quotaChargeCallback.charge.
    Mockito.doNothing().when(quotaChargeCallback).charge();
    Assert.assertTrue("Once isCharged is true, charge should never call quotaChargeCallback.charge.",
        operationQuotaCharger.charge());
    Mockito.verify(quotaChargeCallback, Mockito.times(2)).charge();
  }

  @Test
  public void testQuotaExceedAllowed() throws Exception {
    // quotaExceedAllowed should return true if quotaChargeCallback is null.
    OperationQuotaCharger operationQuotaCharger = new OperationQuotaCharger(null, BLOBID, "GetOperation");
    Assert.assertTrue("quotaExceedAllowed should return true if quotaChargeCallback is null.",
        operationQuotaCharger.quotaExceedAllowed());

    QuotaChargeCallback quotaChargeCallback = Mockito.mock(QuotaChargeCallback.class);
    operationQuotaCharger = new OperationQuotaCharger(quotaChargeCallback, BLOBID, "GetOperation");

    // quotaExceedAllowed should return true if quotaChargeCallback.quotaExceedAllowed returns true.
    Mockito.when(quotaChargeCallback.quotaExceedAllowed()).thenReturn(true);
    Assert.assertTrue("quotaExceedAllowed should return true if quotaChargeCallback.quotaExceedAllowed returns true.",
        operationQuotaCharger.quotaExceedAllowed());

    // quotaExceedAllowed should return false if quotaChargeCallback.quotaExceedAllowed returns false.
    Mockito.when(quotaChargeCallback.quotaExceedAllowed()).thenReturn(false);
    Assert.assertFalse(
        "quotaExceedAllowed should return false if quotaChargeCallback.quotaExceedAllowed returns false.",
        operationQuotaCharger.quotaExceedAllowed());
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
