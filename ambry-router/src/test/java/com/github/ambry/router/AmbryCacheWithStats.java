/**
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

package com.github.ambry.router;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.AmbryCache;
import com.github.ambry.commons.AmbryCacheEntry;
import java.util.concurrent.CountDownLatch;


public class AmbryCacheWithStats extends AmbryCache {

  final CountDownLatch cacheHitCountDown;
  final CountDownLatch cacheMissCountDown;
  final CountDownLatch putObjectCountDown;
  final CountDownLatch deleteObjectCountDown;

  /**
   * Constructs an instance of AmbryCache
   * @param cacheId String identifier for this cache
   * @param cacheEnabled Toggles cache. If true, cache is enabled. Else, cache is disabled.
   * @param maxNumCacheEntries Maximum number of cache entries
   * @param metricRegistry Instance of metrics registry to record stats
   */
  public AmbryCacheWithStats(String cacheId, boolean cacheEnabled, int maxNumCacheEntries,
      MetricRegistry metricRegistry, AmbryCacheStats ambryCacheStats) {
    super(cacheId, cacheEnabled, maxNumCacheEntries, metricRegistry);
    cacheHitCountDown = new CountDownLatch(ambryCacheStats.getNumCacheHit());
    cacheMissCountDown = new CountDownLatch(ambryCacheStats.getNumCacheMiss());
    putObjectCountDown = new CountDownLatch(ambryCacheStats.getNumPut());
    deleteObjectCountDown = new CountDownLatch(ambryCacheStats.getNumDelete());
  }

  @Override
  public boolean putObject(String key, AmbryCacheEntry value) {
    if (super.putObject(key, value)) {
      putObjectCountDown.countDown();
      return true;
    }
    return false;
  }

  @Override
  public AmbryCacheEntry getObject(String key) {
    AmbryCacheEntry ambryCacheEntry = super.getObject(key);
    if (ambryCacheEntry != null) {
      cacheHitCountDown.countDown();
    } else {
      cacheMissCountDown.countDown();
    }
    return ambryCacheEntry;
  }

  @Override
  public boolean deleteObject(String key) {
    if (super.deleteObject(key)) {
      deleteObjectCountDown.countDown();
      return true;
    }
    return false;
  }

  public CountDownLatch getCacheHitCountDown() {
    return cacheHitCountDown;
  }

  public CountDownLatch getCacheMissCountDown() {
    return cacheMissCountDown;
  }

  public CountDownLatch getPutObjectCountDown() {
    return putObjectCountDown;
  }

  public CountDownLatch getDeleteObjectCountDown() {
    return deleteObjectCountDown;
  }
}
