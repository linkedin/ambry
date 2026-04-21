/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link StoreMetrics}.
 */
public class StoreMetricsTest {

  /**
   * Tests that all 5 compactor gauges registered by {@link StoreMetrics#initializeCompactorGauges} are removed
   * when {@link StoreMetrics#deregisterMetrics} is called. This guards against a memory leak where orphaned
   * gauge lambdas accumulate across store open/close cycles (e.g. after BlobStore restart).
   *
   * The two gauges that were historically missing from deregistration are CompactedLogCount and LogSegmentCount.
   */
  @Test
  public void testDeregisterCompactorGaugesRemovesAllFive() {
    MetricRegistry registry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(registry);

    String storeId = "testPartition_0";
    AtomicBoolean compactionInProgress = new AtomicBoolean(false);
    AtomicReference<CompactionDetails> compactionDetails = new AtomicReference<>(null);
    AtomicInteger compactedLogCount = new AtomicInteger(0);
    AtomicInteger logSegmentCount = new AtomicInteger(0);

    metrics.initializeCompactorGauges(storeId, compactionInProgress, compactionDetails, compactedLogCount,
        logSegmentCount);

    // Verify all 5 per-store gauges are registered
    String prefix = storeId + ".";
    assertTrue("CompactionInProgress gauge should be registered",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionInProgress")));
    assertTrue("CompactionCost gauge should be registered",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionCost")));
    assertTrue("CompactionBenefit gauge should be registered",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionBenefit")));
    assertTrue("CompactedLogCount gauge should be registered",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactedLogCount")));
    assertTrue("LogSegmentCount gauge should be registered",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "LogSegmentCount")));

    // Deregister and verify all 5 gauges are removed
    metrics.deregisterMetrics(storeId);

    assertFalse("CompactionInProgress gauge should be removed after deregister",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionInProgress")));
    assertFalse("CompactionCost gauge should be removed after deregister",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionCost")));
    assertFalse("CompactionBenefit gauge should be removed after deregister",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactionBenefit")));
    assertFalse("CompactedLogCount gauge should be removed after deregister",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "CompactedLogCount")));
    assertFalse("LogSegmentCount gauge should be removed after deregister",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix + "LogSegmentCount")));
  }

  /**
   * Tests that deregistering gauges for one store does not affect gauges registered for another store.
   * This verifies the per-store prefix isolation.
   */
  @Test
  public void testDeregisterCompactorGaugesOnlyAffectsTargetStore() {
    MetricRegistry registry = new MetricRegistry();
    StoreMetrics metrics = new StoreMetrics(registry);

    String storeId1 = "partition_1";
    String storeId2 = "partition_2";

    metrics.initializeCompactorGauges(storeId1, new AtomicBoolean(), new AtomicReference<>(), new AtomicInteger(),
        new AtomicInteger());
    metrics.initializeCompactorGauges(storeId2, new AtomicBoolean(), new AtomicReference<>(), new AtomicInteger(),
        new AtomicInteger());

    metrics.deregisterMetrics(storeId1);

    // store 2 gauges should still be present
    String prefix2 = storeId2 + ".";
    assertTrue("partition_2 CompactedLogCount should still be registered after partition_1 deregister",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix2 + "CompactedLogCount")));
    assertTrue("partition_2 LogSegmentCount should still be registered after partition_1 deregister",
        registry.getGauges().containsKey(MetricRegistry.name(BlobStoreCompactor.class, prefix2 + "LogSegmentCount")));
  }
}
