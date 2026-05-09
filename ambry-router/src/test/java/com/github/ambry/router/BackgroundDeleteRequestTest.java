/*
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

import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.store.StoreKey;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link BackgroundDeleteRequest}.
 */
public class BackgroundDeleteRequestTest {

  /**
   * The constructor must reject a null storeKey at the data-class boundary so a poison-pill request
   * cannot reach the operation controller's poll loop. See QuotaAwareOperationController.pollNewRequests.
   */
  @Test(expected = NullPointerException.class)
  public void testConstructorRejectsNullStoreKey() {
    new BackgroundDeleteRequest(null, "suffix", mock(QuotaChargeCallback.class));
  }

  /**
   * A non-null storeKey constructs successfully and getBlobId / getServiceId / getQuotaChargeCallback
   * round-trip the inputs.
   */
  @Test
  public void testConstructorAcceptsNonNullStoreKey() {
    StoreKey storeKey = mock(StoreKey.class);
    when(storeKey.getID()).thenReturn("blob-1");
    QuotaChargeCallback quotaChargeCallback = mock(QuotaChargeCallback.class);

    BackgroundDeleteRequest request = new BackgroundDeleteRequest(storeKey, "suffix", quotaChargeCallback);

    assertEquals("blob-1", request.getBlobId());
    assertEquals(BackgroundDeleteRequest.SERVICE_ID_PREFIX + "suffix", request.getServiceId());
    assertSame(quotaChargeCallback, request.getQuotaChargeCallback());
  }

  /**
   * A null serviceIdSuffix is permitted by the constructor (current behavior). Documented to lock in
   * intent: only storeKey is fail-fast; serviceIdSuffix and quotaChargeCallback remain optional.
   */
  @Test
  public void testConstructorAllowsNullServiceIdSuffix() {
    StoreKey storeKey = mock(StoreKey.class);
    when(storeKey.getID()).thenReturn("blob-2");

    BackgroundDeleteRequest request = new BackgroundDeleteRequest(storeKey, null, null);

    assertEquals("blob-2", request.getBlobId());
    assertEquals(BackgroundDeleteRequest.SERVICE_ID_PREFIX + "null", request.getServiceId());
    assertNull(request.getQuotaChargeCallback());
  }
}
