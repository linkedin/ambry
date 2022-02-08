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

import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Test for {@link QuotaResourceSynchronizer}.
 */
public class QuotaResourceSynchronizerTest {
  private TestQuotaResourceSynchronizer quotaResourceSychronizer;
  private QuotaResourceSynchronizer.QuotaResourceMutexCache quotaResourceMutexCache;

  @Before
  public void setup() {
    quotaResourceSychronizer = new TestQuotaResourceSynchronizer(new QuotaMetrics(new MetricRegistry()));
    quotaResourceMutexCache = quotaResourceSychronizer.quotaResourceMutexCache;
  }

  @Test
  public void testQuotaResourceMutexes() {
    // test that same quotaresource objects get the same mutex
    QuotaResource quotaResource1 = new QuotaResource(Integer.toString(1), QuotaResourceType.ACCOUNT);
    QuotaResource quotaResource2 = new QuotaResource(Integer.toString(1), QuotaResourceType.ACCOUNT);
    Assert.assertNotSame(quotaResource1, quotaResource2);
    Assert.assertEquals(quotaResource1, quotaResource2);
    ReentrantLock mutex1 = quotaResourceMutexCache.get(quotaResource1);
    ReentrantLock mutex2 = quotaResourceMutexCache.get(quotaResource2);
    ReentrantLock mutex = mutex2;
    Assert.assertSame(mutex1, mutex2);
    Assert.assertEquals(1, quotaResourceSychronizer.getMutexMap().size());

    // test that different quota resource objects get different mutex
    quotaResource1 = new QuotaResource(Integer.toString(1), QuotaResourceType.ACCOUNT);
    quotaResource2 = new QuotaResource(Integer.toString(2), QuotaResourceType.ACCOUNT);
    Assert.assertFalse(quotaResource1.equals(quotaResource2));
    mutex1 = quotaResourceMutexCache.get(quotaResource1);
    mutex2 = quotaResourceMutexCache.get(quotaResource2);
    Assert.assertNotSame(mutex1, mutex2);
    Assert.assertSame(mutex, mutex1);
    Assert.assertEquals(2, quotaResourceSychronizer.getMutexMap().size());

    // test that different quota resource objects get different mutex
    quotaResource1 = new QuotaResource(Integer.toString(1), QuotaResourceType.ACCOUNT);
    quotaResource2 = new QuotaResource(Integer.toString(2), QuotaResourceType.CONTAINER);
    Assert.assertFalse(quotaResource1.equals(quotaResource2));
    mutex1 = quotaResourceMutexCache.get(quotaResource1);
    mutex2 = quotaResourceMutexCache.get(quotaResource2);
    Assert.assertNotSame(mutex1, mutex2);
    Assert.assertSame(mutex, mutex1);
    Assert.assertEquals(3, quotaResourceSychronizer.getMutexMap().size());
  }

  @Test
  public void testLockUnlock() throws InterruptedException {
    // 1. acquire lock for a quota resource (quotaResource1)
    QuotaResource quotaResource1 = new QuotaResource(Integer.toString(1), QuotaResourceType.ACCOUNT);
    quotaResourceSychronizer.lock(quotaResource1);
    Assert.assertEquals(1,
        quotaResourceSychronizer.lockAttemptCountMap.getOrDefault(quotaResource1, new AtomicInteger(0)).get());
    Assert.assertEquals(0,
        quotaResourceSychronizer.unlockAttemptCountMap.getOrDefault(quotaResource1, new AtomicInteger(0)).get());

    // 2. acquire lock for a different quota resource (quotaResource2). This should just work.
    QuotaResource quotaResource2 = new QuotaResource(Integer.toString(2), QuotaResourceType.ACCOUNT);
    quotaResourceSychronizer.lock(quotaResource2);
    quotaResourceSychronizer.unlock(quotaResource2);
    Assert.assertEquals(1,
        quotaResourceSychronizer.lockAttemptCountMap.getOrDefault(quotaResource2, new AtomicInteger(0)).get());
    Assert.assertEquals(1,
        quotaResourceSychronizer.unlockAttemptCountMap.getOrDefault(quotaResource2, new AtomicInteger(0)).get());

    // 3. acquire lock for quotaResource1 in a different thread. the thread should block until current thread releases the lock.
    ReentrantLock lockForSignalling = new ReentrantLock();
    Condition acquired = lockForSignalling.newCondition();
    Condition released = lockForSignalling.newCondition();
    LockUnlockSignalHelper lockUnlockSignalHelper =
        new LockUnlockSignalHelper(quotaResourceSychronizer, lockForSignalling, quotaResource1, acquired, released);
    Thread thread = new Thread(lockUnlockSignalHelper);
    thread.start();
    // sleeping below allow the new thread to execute.
    // Note that there is still no guarantee that the thread would have executed in 5 seconds, it only highly likely.
    Thread.sleep(5000);
    quotaResource1 = new QuotaResource(Integer.toString(1), QuotaResourceType.ACCOUNT);

    // ensure that the other thread was not able to lock
    Assert.assertEquals(1,
        quotaResourceSychronizer.lockAttemptCountMap.getOrDefault(quotaResource1, new AtomicInteger(0)).get());
    Assert.assertEquals(0,
        quotaResourceSychronizer.unlockAttemptCountMap.getOrDefault(quotaResource1, new AtomicInteger(0)).get());

    quotaResourceSychronizer.unlock(quotaResource1);
    // ensure that the other thread was able to lock.
    try {
      lockForSignalling.lock();
      while (quotaResourceSychronizer.lockAttemptCountMap.getOrDefault(quotaResource1, new AtomicInteger(0)).get()
          != 2) {
        acquired.await();
      }
    } finally {
      lockForSignalling.unlock();
    }

    // ensure that the other thread was able to unlock.
    try {
      lockForSignalling.lock();
      while (quotaResourceSychronizer.unlockAttemptCountMap.getOrDefault(quotaResource1, new AtomicInteger(0)).get()
          != 2) {
        released.await();
      }
    } finally {
      lockForSignalling.unlock();
    }

    Assert.assertEquals(2,
        quotaResourceSychronizer.lockAttemptCountMap.getOrDefault(quotaResource1, new AtomicInteger(0)).get());
    Assert.assertEquals(2,
        quotaResourceSychronizer.unlockAttemptCountMap.getOrDefault(quotaResource1, new AtomicInteger(0)).get());

    // ensure thread exited
    thread.join();
  }

  /**
   * Helper {@link Runnable} implementation that attempts to lock and unlock for a {@link QuotaResource} and signal for
   * every such successful attempt.
   */
  private static class LockUnlockSignalHelper implements Runnable {
    private final QuotaResourceSynchronizer quotaResourceSynchronizer;
    private final ReentrantLock lock;
    private final QuotaResource quotaResource;
    private final Condition acquired;
    private final Condition released;

    /**
     * Constructor for {@link LockUnlockSignalHelper}.
     * @param quotaResourceSynchronizer {@link QuotaResourceSynchronizer} object.
     * @param lock {@link ReentrantLock} object for signalling.
     * @param quotaResource {@link QuotaResource} object.
     * @param acquired {@link Condition} to signal when quota resource lock is acquired.
     * @param released {@link Condition} to signal when quota resource lock is released.
     */
    LockUnlockSignalHelper(QuotaResourceSynchronizer quotaResourceSynchronizer, ReentrantLock lock,
        QuotaResource quotaResource, Condition acquired, Condition released) {
      this.lock = lock;
      this.quotaResourceSynchronizer = quotaResourceSynchronizer;
      this.quotaResource = quotaResource;
      this.acquired = acquired;
      this.released = released;
    }

    @Override
    public void run() {
      try {
        lock.lock();
        quotaResourceSynchronizer.lock(quotaResource);
        acquired.signalAll();
      } finally {
        lock.unlock();
      }

      try {
        lock.lock();
        quotaResourceSynchronizer.unlock(quotaResource);
        released.signalAll();
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Test class that extends {@link QuotaResourceSynchronizer} to expose some of its protected members for tests.
   */
  private static class TestQuotaResourceSynchronizer extends QuotaResourceSynchronizer {
    ConcurrentHashMap<QuotaResource, AtomicInteger> lockAttemptCountMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<QuotaResource, AtomicInteger> unlockAttemptCountMap = new ConcurrentHashMap<>();

    /**
     * Constructor for {@link TestQuotaResourceSynchronizer}.
     * @param quotaMetrics {@link QuotaMetrics} object.
     */
    public TestQuotaResourceSynchronizer(QuotaMetrics quotaMetrics) {
      super(quotaMetrics);
    }

    @Override
    public void lock(QuotaResource quotaResource) {
      super.lock(quotaResource);
      lockAttemptCountMap.putIfAbsent(quotaResource, new AtomicInteger(0));
      lockAttemptCountMap.get(quotaResource).incrementAndGet();
    }

    @Override
    public void unlock(QuotaResource quotaResource) {
      super.unlock(quotaResource);
      unlockAttemptCountMap.putIfAbsent(quotaResource, new AtomicInteger(0));
      unlockAttemptCountMap.get(quotaResource).incrementAndGet();
    }

    /**
     * Method to return {@link QuotaResourceSynchronizer}'s mutex cache for tests.
     * @return ConcurrentMap of {@link QuotaResource} id and its mutex {@link ReentrantLock}.
     */
    public ConcurrentMap<QuotaResource, ReentrantLock> getMutexMap() {
      return quotaResourceMutexCache.mutexCache;
    }
  }
}
