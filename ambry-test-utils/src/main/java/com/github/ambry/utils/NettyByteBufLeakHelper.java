/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.utils;

import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.util.internal.SystemPropertyUtil;
import java.util.List;
import org.junit.Assert;


/**
 * A helper class to detect Netty ByteBuf leak. To use this class, create a {@link NettyByteBufLeakHelper} in your test
 * class and call {@link NettyByteBufLeakHelper#beforeTest()} at any method that has {@link org.junit.Before} tag and
 * call {@link NettyByteBufLeakHelper#afterTest()} at any method that has {@link org.junit.After} tag.
 */
public class NettyByteBufLeakHelper {
  private long activeHeapAllocations;
  private long activeDirectAllocations;

  private long heapAllocations;
  private long heapDeallocations;

  private long directAllocations;
  private long directDeallocations;

  // when the cache is enable, just don't check the allocation and deallocation.
  private boolean cachedEnabled;

  private boolean disabled;

  /**
   * Constructor to create a {@link NettyByteBufLeakHelper}.
   */
  public NettyByteBufLeakHelper() {
    // The allocator cache is enabled by default if this property is not set. The default value of 32k comes from
    // PooledByteBufAllocator.DEFAULT_MAX_CACHED_BUFFER_CAPACITY (private constant)
    cachedEnabled = SystemPropertyUtil.getInt("io.netty.allocator.maxCachedBufferCapacity", 32 * 1024) != 0;
  }

  /**
   * Method to call at any method that is tagged {@link org.junit.Before} to collect some allocation stats.
   */
  public void beforeTest() {
    if (cachedEnabled) {
      return;
    }
    PooledByteBufAllocatorMetric metric = PooledByteBufAllocator.DEFAULT.metric();
    List<PoolArenaMetric> heaps = metric.heapArenas();
    activeHeapAllocations = heaps.stream().mapToLong(PoolArenaMetric::numActiveAllocations).sum();
    heapAllocations = heaps.stream().mapToLong(PoolArenaMetric::numAllocations).sum();
    heapDeallocations = heaps.stream().mapToLong(PoolArenaMetric::numDeallocations).sum();

    List<PoolArenaMetric> directs = metric.directArenas();
    activeDirectAllocations = directs.stream().mapToLong(PoolArenaMetric::numActiveAllocations).sum();
    directAllocations = directs.stream().mapToLong(PoolArenaMetric::numAllocations).sum();
    directDeallocations = directs.stream().mapToLong(PoolArenaMetric::numDeallocations).sum();
  }

  /**
   * Method to call at any method that is tagged {@link org.junit.After} to verify there is no leak within this test case.
   */
  public void afterTest() {
    if (cachedEnabled || disabled) {
      return;
    }

    PooledByteBufAllocatorMetric metric = PooledByteBufAllocator.DEFAULT.metric();
    List<PoolArenaMetric> heaps = metric.heapArenas();
    long currentActiveHeapAllocations = heaps.stream().mapToLong(PoolArenaMetric::numActiveAllocations).sum();
    long currentHeapAllocations = heaps.stream().mapToLong(PoolArenaMetric::numAllocations).sum();
    long currentHeapDeallocations = heaps.stream().mapToLong(PoolArenaMetric::numDeallocations).sum();

    List<PoolArenaMetric> directs = metric.directArenas();
    long currentActiveDirectAllocations = directs.stream().mapToLong(PoolArenaMetric::numActiveAllocations).sum();
    long currentDirectAllocations = directs.stream().mapToLong(PoolArenaMetric::numAllocations).sum();
    long currentDirectDeallocations = directs.stream().mapToLong(PoolArenaMetric::numDeallocations).sum();

    String message = String.format("DirectMemoryLeak: [allocation|deallocation] before test[%d|%d], after test[%d|%d]",
        directAllocations, directDeallocations, currentDirectAllocations, currentDirectDeallocations);
    Assert.assertEquals(message, activeDirectAllocations, currentActiveDirectAllocations);
    message = String.format("HeapMemoryLeak: [allocation|deallocation] before test[%d|%d], after test[%d|%d]",
        heapAllocations, heapDeallocations, currentHeapAllocations, currentHeapDeallocations);
    Assert.assertEquals(message, activeHeapAllocations, currentActiveHeapAllocations);
  }

  /**
   * Disable this leak detector by passing true to this function. Some test cases would perform some usual behaviors, like
   * kill thread, forcefully close some service. In those cases, there might be some leaks that can't be fixed.
   * @param disabled true to disable the leak detector.
   */
  public void setDisabled(boolean disabled) {
    this.disabled = disabled;
  }
}
