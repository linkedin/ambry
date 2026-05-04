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

import com.github.ambry.network.NetworkClient;
import com.github.ambry.quota.QuotaChargeCallback;
import com.github.ambry.store.StoreKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link RouterCallback#scheduleDeletes(List, String, QuotaChargeCallback)}.
 * Null-storeKey enforcement lives at {@link BackgroundDeleteRequest}'s constructor; this class
 * verifies the happy path and that an NPE from the constructor surfaces (rather than being
 * silently skipped, which would hide an internal bug).
 */
public class RouterCallbackTest {

  /**
   * Happy path: every entry is enqueued in order with the correct service id.
   */
  @Test
  public void testScheduleDeletesAllValid() {
    List<BackgroundDeleteRequest> queue = new ArrayList<>();
    RouterCallback routerCallback = new RouterCallback(mock(NetworkClient.class), queue);

    routerCallback.scheduleDeletes(Arrays.asList(mockKey("a"), mockKey("b"), mockKey("c")), "svc",
        mock(QuotaChargeCallback.class));

    assertEquals(3, queue.size());
    assertEquals("a", queue.get(0).getBlobId());
    assertEquals("b", queue.get(1).getBlobId());
    assertEquals("c", queue.get(2).getBlobId());
    assertEquals(BackgroundDeleteRequest.SERVICE_ID_PREFIX + "svc", queue.get(0).getServiceId());
  }

  /**
   * Empty list is a no-op.
   */
  @Test
  public void testScheduleDeletesEmptyList() {
    List<BackgroundDeleteRequest> queue = new ArrayList<>();
    RouterCallback routerCallback = new RouterCallback(mock(NetworkClient.class), queue);

    routerCallback.scheduleDeletes(Collections.emptyList(), "suffix", mock(QuotaChargeCallback.class));

    assertTrue(queue.isEmpty());
  }

  /**
   * A null entry surfaces as NPE from BackgroundDeleteRequest's constructor — the data-class
   * boundary is the single enforcement point for the non-null invariant. Production callers source
   * IDs from local PutOperation state, so a null here is an internal bug we want to fail loudly on
   * rather than silently skip.
   */
  @Test(expected = NullPointerException.class)
  public void testScheduleDeletesNullStoreKeyThrows() {
    List<BackgroundDeleteRequest> queue = new ArrayList<>();
    RouterCallback routerCallback = new RouterCallback(mock(NetworkClient.class), queue);

    routerCallback.scheduleDeletes(Arrays.asList(mockKey("a"), null, mockKey("c")), "suffix",
        mock(QuotaChargeCallback.class));
  }

  private static StoreKey mockKey(String id) {
    StoreKey key = mock(StoreKey.class);
    when(key.getID()).thenReturn(id);
    return key;
  }
}
