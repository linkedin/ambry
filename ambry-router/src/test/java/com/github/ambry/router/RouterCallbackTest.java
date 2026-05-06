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
 *
 * scheduleDeletes runs from {@code PutManager.onComplete} on every put completion (success and
 * failure). The next line after this method is {@code nonBlockingRouter.completeOperation}, so
 * an NPE escaping here would skip the customer's PUT callback and hang the request until timeout.
 * The skip-and-count pattern below preserves operational availability and surfaces the internal
 * bug via metric for investigation.
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
   * A null entry must be skipped without throwing, so the put-completion path in
   * {@code PutManager.onComplete} can reach {@code completeOperation} and fire the customer
   * callback. Valid sibling entries must still be enqueued. Internal-bug nulls are surfaced via
   * the error log; no metric is added because production callers source IDs from local
   * PutOperation state — null here is a deterministic code bug, not a rate-driven external signal.
   */
  @Test
  public void testScheduleDeletesNullStoreKeyIsSkipped() {
    List<BackgroundDeleteRequest> queue = new ArrayList<>();
    RouterCallback routerCallback = new RouterCallback(mock(NetworkClient.class), queue);

    routerCallback.scheduleDeletes(Arrays.asList(mockKey("a"), null, mockKey("c")), "suffix",
        mock(QuotaChargeCallback.class));

    assertEquals("Two valid entries should be enqueued; null skipped", 2, queue.size());
    assertEquals("a", queue.get(0).getBlobId());
    assertEquals("c", queue.get(1).getBlobId());
  }

  /**
   * Multiple null entries are each skipped without throwing.
   */
  @Test
  public void testScheduleDeletesMultipleNullsSkipped() {
    List<BackgroundDeleteRequest> queue = new ArrayList<>();
    RouterCallback routerCallback = new RouterCallback(mock(NetworkClient.class), queue);

    routerCallback.scheduleDeletes(Arrays.asList(null, mockKey("a"), null, null, mockKey("b")), "suffix",
        mock(QuotaChargeCallback.class));

    assertEquals(2, queue.size());
    assertEquals("a", queue.get(0).getBlobId());
    assertEquals("b", queue.get(1).getBlobId());
  }

  private static StoreKey mockKey(String id) {
    StoreKey key = mock(StoreKey.class);
    when(key.getID()).thenReturn(id);
    return key;
  }
}
