/*
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
package com.github.ambry.quota;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class to provide synchronization constructs (lock and unlock) for all {@link QuotaResource} objects based on their value.
 * This class can be used to enforce mutual exclusion between multiple {@link QuotaResource} objects that represent the
 * same quota resource (as determined by {@link QuotaResource#equals}) method.
 *
 * To ensure the synchronization, this class internally creates a unique {@link ReentrantLock} for each quota resource,
 * and maintains a {@link ConcurrentHashMap} to cache the mapping and reuse same {@link ReentrantLock} across multiple
 * {@link QuotaResource} instances that represent the same quota resource.
 *
 * In order to prevent {@link ReentrantLock}s from being inadvertently reused outside this scope, this class doesn't
 * leak the {@link ReentrantLock}s outside. It provides only lock and unlock constructs to the callers.
 */
public class QuotaResourceSynchronizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuotaResourceMutexCache.class);
  protected final QuotaResourceMutexCache quotaResourceMutexCache = new QuotaResourceMutexCache();
  private final QuotaMetrics quotaMetrics;

  /**
   * Constructor for {@link QuotaResourceSynchronizer}.
   * @param quotaMetrics {@link QuotaMetrics} object.
   */
  public QuotaResourceSynchronizer(QuotaMetrics quotaMetrics) {
    this.quotaMetrics = quotaMetrics;
  }

  /**
   * Acquires the lock for the quota resource represented by the specified {@link QuotaResource}.
   * @param quotaResource {@link QuotaResource} object representing the quota resource for which lock needs to be
   *                      acquired.
   */
  public void lock(QuotaResource quotaResource) {
    quotaResourceMutexCache.get(quotaResource).lock();
  }

  /**
   * Releases the lock for the quota resource represented by the specified {@link QuotaResource}.
   * @param quotaResource {@link QuotaResource} object representing the quota resource for which lock needs to be
   *                      released.
   */
  public void unlock(QuotaResource quotaResource) {
    ReentrantLock lock = quotaResourceMutexCache.mutexCache.getOrDefault(quotaResource, null);
    if (lock != null) {
      lock.unlock();
    } else {
      LOGGER.warn("Attempted unlock for {} when lock wasn't held.", quotaResource);
      quotaMetrics.quotaResourceInvalidUnlockAttemptCount.inc();
    }
  }

  /**
   * A cache of unique mutexes for all {@link QuotaResource} objects that represent the same quota resource.
   */
  static class QuotaResourceMutexCache {
    /*
     * There are multiple optimization opportunities here.
     * 1. If we never clean the cache, we don't really need all the complexity, and hence latency, of a ConcurrentHashMap.
     *    We could just use an array of mutexes to synchronize. But doing that will add extra complexity (extra code,
     *    extra tests etc). So unless there is a performance issue, using ConcurrentHashMap should be just fine.
     * 2. This cache never removes entries until the service (Ambry frontend) is restarted. In the worst case the size
     *    of the cache might grow upto the total number of QuotaResources (accounts + containers + services) for which quota
     *    is being enforced. Periodically cleaning up cache is one way to reduce the memory pressure, but will add more
     *    complexity to maintain thread safety (and maybe more latency to access the cache too). Ambry's frontend keeps
     *    the accounts and containers in memory at multiple places currently (e.g, AccountService implementation), with the
     *    assumption that the total number of accounts and containers are small enough to keep in memory. As long as that
     *    assumption holds, not cleaning up this cache is fine. If that assumption doesn't hold, a lot of code might need
     *    restructuring, and this cache cleanup should be taken up as part of that effort.
     */
    protected final ConcurrentHashMap<QuotaResource, ReentrantLock> mutexCache = new ConcurrentHashMap<>();

    /**
     * Returns the mutex in the form of {@link ReentrantLock} object for the specified {@link QuotaResource}.
     * If the mutex is not already present in the cache, it creates a new mapping.
     * @param quotaResource {@link QuotaResource} object.
     * @return ReentrantLock object as mutex for the specified quotaResource.
     */
    ReentrantLock get(QuotaResource quotaResource) {
      return mutexCache.computeIfAbsent(quotaResource, k -> new ReentrantLock());
    }
  }
}
