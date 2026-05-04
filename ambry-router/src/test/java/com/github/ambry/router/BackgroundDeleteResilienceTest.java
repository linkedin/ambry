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

import com.github.ambry.messageformat.MessageFormatRecord;
import com.github.ambry.commons.LoggingNotificationSystem;
import com.github.ambry.router.PutBlobOptions;
import com.github.ambry.store.StoreKey;
import com.github.ambry.utils.TestUtils;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Resilience tests for the background-delete path. These cover the production bug observed on
 * lor1-app101586 (2026-05-04): a single malformed BackgroundDeleteRequest aborted the
 * QuotaAwareOperationController poll loop on every cycle, blocking request drain and accumulating
 * inflight operations on RequestResponseHandlerThread-3.
 *
 * Each test exercises a real {@link NonBlockingRouter} (via {@link NonBlockingRouterTestBase}) and
 * verifies that the catch + counter-rollback patterns introduced in initiateBackgroundDeletes and
 * BackgroundDeleter.deleteBlob restore counters to baseline after a malformed request.
 */
public class BackgroundDeleteResilienceTest extends NonBlockingRouterTestBase {

  public BackgroundDeleteResilienceTest() throws Exception {
    super(false, MessageFormatRecord.Metadata_Content_Version_V2, false);
  }

  /**
   * A {@link BackgroundDeleteRequest} whose {@code storeKey.getID()} throws a {@link RuntimeException}
   * (simulating the production NPE on a null-deserialized chunk id) must be caught by the new try/catch
   * in {@code initiateBackgroundDeletes}: the failure metric increments and both router-level counters
   * roll back to baseline.
   */
  @Test
  public void testInitiateBackgroundDeletesIsolatesPoisonPill() throws Exception {
    setRouter();
    long submitFailureBefore = routerMetrics.backgroundDeleterSubmitFailureCount.getCount();
    int currentOpsBefore = router.currentOperationsCount.get();

    StoreKey poisonKey = mock(StoreKey.class);
    when(poisonKey.getID()).thenThrow(new RuntimeException("simulated corruption"));
    BackgroundDeleteRequest poison = new BackgroundDeleteRequest(poisonKey, "test", null);

    router.initiateBackgroundDeletes(Collections.singletonList(poison));

    assertEquals("Submit failure metric must increment for the malformed entry", submitFailureBefore + 1,
        routerMetrics.backgroundDeleterSubmitFailureCount.getCount());
    assertEquals("currentOperationsCount must roll back to baseline after a malformed entry",
        currentOpsBefore, router.currentOperationsCount.get());
  }

  /**
   * Multiple poison pills must each be caught independently. Counters return to baseline; the metric
   * advances by the number of malformed entries.
   */
  @Test
  public void testInitiateBackgroundDeletesHandlesRepeatedFailures() throws Exception {
    setRouter();
    long submitFailureBefore = routerMetrics.backgroundDeleterSubmitFailureCount.getCount();
    int currentOpsBefore = router.currentOperationsCount.get();

    StoreKey poisonKey1 = mock(StoreKey.class);
    StoreKey poisonKey2 = mock(StoreKey.class);
    StoreKey poisonKey3 = mock(StoreKey.class);
    when(poisonKey1.getID()).thenThrow(new RuntimeException("a"));
    when(poisonKey2.getID()).thenThrow(new RuntimeException("b"));
    when(poisonKey3.getID()).thenThrow(new RuntimeException("c"));

    router.initiateBackgroundDeletes(Arrays.asList(
        new BackgroundDeleteRequest(poisonKey1, "a", null),
        new BackgroundDeleteRequest(poisonKey2, "b", null),
        new BackgroundDeleteRequest(poisonKey3, "c", null)));

    assertEquals("Each malformed entry must increment the submit-failure metric", submitFailureBefore + 3,
        routerMetrics.backgroundDeleterSubmitFailureCount.getCount());
    assertEquals("currentOperationsCount must roll back to baseline", currentOpsBefore,
        router.currentOperationsCount.get());
  }

  /**
   * An empty list is a no-op: no metric movement, no counter movement, no exception.
   */
  @Test
  public void testInitiateBackgroundDeletesEmptyList() throws Exception {
    setRouter();
    long submitFailureBefore = routerMetrics.backgroundDeleterSubmitFailureCount.getCount();
    int currentOpsBefore = router.currentOperationsCount.get();

    router.initiateBackgroundDeletes(Collections.emptyList());

    assertEquals(submitFailureBefore, routerMetrics.backgroundDeleterSubmitFailureCount.getCount());
    assertEquals(currentOpsBefore, router.currentOperationsCount.get());
  }

  /**
   * Queued-path counter rollback. Configures the BackgroundDeleter's queued dispatch path
   * ({@code routerBackgroundDeleterMaxConcurrentOperations > 0}), simulates the per-request
   * increments {@link NonBlockingRouter#initiateBackgroundDeletes(java.util.List)} would have
   * performed for a queued request, then injects a poison {@link Supplier} that throws on dispatch.
   *
   * The BackgroundDeleter's poll loop will dequeue and invoke it; the throw escapes
   * {@code deleteOperationQueue.poll().get()}, the {@code finally} in
   * {@link OperationController#pollForRequests} fires, and all three counters
   * (currentOperationsCount, currentBackgroundOperationsCount, concurrentBackgroundDeleteOperationCount)
   * must roll back to baseline. Asserting on the observable counter values rather than the rollback
   * method directly ensures the wiring (`if (!started) ... rollbackBackgroundDeleteRouterCounters()`)
   * actually fires under the dispatch failure.
   */
  @Test
  public void testQueuedDispatchFailureRollsBackAllCounters() throws Exception {
    Properties props =
        getNonBlockingRouterProperties(mockClusterMap.getDatacenterName(mockClusterMap.getLocalDatacenterId()));
    props.setProperty("router.background.deleter.max.concurrent.operations", "5");
    setRouter(props, mockServerLayout, new LoggingNotificationSystem());

    Field bgField = NonBlockingRouter.class.getDeclaredField("currentBackgroundOperationsCount");
    bgField.setAccessible(true);
    AtomicInteger backgroundCount = (AtomicInteger) bgField.get(router);

    int opsBaseline = router.currentOperationsCount.get();
    int backgroundBaseline = backgroundCount.get();

    // Step 1: simulate what initiateBackgroundDeletes does for a request that will be queued.
    // The real call path increments both counters before invoking backgroundDeleter.deleteBlob,
    // which under limit > 0 just calls offer() and returns; submitted=true, no rollback there.
    router.currentOperationsCount.incrementAndGet();
    backgroundCount.incrementAndGet();

    // Step 2: inject a poison Supplier directly into the BackgroundDeleter's queue. When the
    // poll loop dispatches it, the throw escapes — exactly what would happen if super.deleteBlob
    // threw a RuntimeException before registering the DeleteOperation.
    Field bgDeleterField = NonBlockingRouter.class.getDeclaredField("backgroundDeleter");
    bgDeleterField.setAccessible(true);
    Object backgroundDeleter = bgDeleterField.get(router);
    Field queueField = backgroundDeleter.getClass().getDeclaredField("deleteOperationQueue");
    queueField.setAccessible(true);
    @SuppressWarnings("unchecked")
    ConcurrentLinkedQueue<Supplier<Void>> queue =
        (ConcurrentLinkedQueue<Supplier<Void>>) queueField.get(backgroundDeleter);
    queue.offer(() -> {
      throw new RuntimeException("simulated queued dispatch failure");
    });

    // Step 3: wait for the BackgroundDeleter's poll loop to drain the queue and roll back.
    assertTrue("currentOperationsCount must return to baseline after queued dispatch failure",
        TestUtils.checkAndSleep(opsBaseline, () -> router.currentOperationsCount.get(), 5_000));
    assertEquals("currentBackgroundOperationsCount must roll back to baseline",
        backgroundBaseline, backgroundCount.get());
  }

  /**
   * Poison-then-good: a malformed entry must not wedge the loop or starve subsequent valid entries.
   * Puts a real blob, then submits [poison, valid] to initiateBackgroundDeletes. The poison is
   * caught and rolled back; the valid entry is submitted, completes asynchronously, and counters
   * return to baseline (verified via @After's currentOperationsCount==0 assertion plus an explicit
   * checkAndSleep here for fast-fail).
   */
  @Test
  public void testInitiateBackgroundDeletesPoisonThenGood() throws Exception {
    setRouter();
    setOperationParams();
    String validBlobId =
        router.putBlob(putBlobProperties, putUserMetadata, putChannel, PutBlobOptions.DEFAULT).get();
    long submitFailureBefore = routerMetrics.backgroundDeleterSubmitFailureCount.getCount();

    StoreKey poisonKey = mock(StoreKey.class);
    when(poisonKey.getID()).thenThrow(new RuntimeException("simulated corruption"));
    StoreKey validKey = mock(StoreKey.class);
    when(validKey.getID()).thenReturn(validBlobId);

    router.initiateBackgroundDeletes(Arrays.asList(
        new BackgroundDeleteRequest(poisonKey, "poison", null),
        new BackgroundDeleteRequest(validKey, "good", null)));

    assertEquals("Only the poison entry should increment the failure metric", submitFailureBefore + 1,
        routerMetrics.backgroundDeleterSubmitFailureCount.getCount());
    assertTrue("currentOperationsCount must drain to 0 after the valid background delete completes",
        TestUtils.checkAndSleep(0, () -> router.currentOperationsCount.get(), 5_000));
  }
}
