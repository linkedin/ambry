/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.TestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link AmbryStorageQuotaEnforcer}.
 */
public class AmbryStorageQuotaEnforcerTest {

  /**
   * Test to initialize the storage usage with empty map.
   */
  @Test
  public void testInitEmptyStorageUsage() {
    AmbryStorageQuotaEnforcer enforcer = new AmbryStorageQuotaEnforcer();
    enforcer.initStorageUsage(Collections.EMPTY_MAP);
    TestUtils.assertContainerMap(Collections.EMPTY_MAP, enforcer.getStorageUsage());
  }

  /**
   * Test to initialize the storage usage with non-empty map.
   */
  @Test
  public void testInitStorageUsage() {
    AmbryStorageQuotaEnforcer enforcer = new AmbryStorageQuotaEnforcer();
    Map<String, Map<String, Long>> containerUsage = TestUtils.makeStorageMap(10, 10, 1000, 100);
    enforcer.initStorageUsage(containerUsage);
    TestUtils.assertContainerMap(containerUsage, enforcer.getStorageUsage());
  }

  /**
   * Test to initialize the storage quota with empty map.
   */
  @Test
  public void testInitEmptyStorageQuota() {
    AmbryStorageQuotaEnforcer enforcer = new AmbryStorageQuotaEnforcer();
    enforcer.initStorageQuota(Collections.EMPTY_MAP);
    TestUtils.assertContainerMap(Collections.EMPTY_MAP, enforcer.getStorageQuota());
  }

  /**
   * Test to initialize the storage quota with non-empty map.
   */
  @Test
  public void testInitStorageQuota() {
    AmbryStorageQuotaEnforcer enforcer = new AmbryStorageQuotaEnforcer();
    Map<String, Map<String, Long>> containerQuota = TestUtils.makeStorageMap(10, 10, 1000, 100);
    enforcer.initStorageQuota(containerQuota);
    TestUtils.assertContainerMap(containerQuota, enforcer.getStorageQuota());
  }

  /**
   * Test on storage quota updates.
   */
  @Test
  public void testStorageQuotaSourceListener() {
    AmbryStorageQuotaEnforcer enforcer = new AmbryStorageQuotaEnforcer();
    int initNumAccounts = 10;
    Map<String, Map<String, Long>> expectedQuota = TestUtils.makeStorageMap(initNumAccounts, 10, 10000, 1000);
    enforcer.initStorageQuota(expectedQuota);
    TestUtils.assertContainerMap(expectedQuota, enforcer.getStorageQuota());

    StorageQuotaSource.Listener listener = enforcer.getQuotaSourceListener();
    int numUpdates = 10;
    for (int i = 1; i <= numUpdates; i++) {
      Map<String, Map<String, Long>> additionalUsage = TestUtils.makeStorageMap(1, 10, 10000, 1000);
      expectedQuota.put(String.valueOf(initNumAccounts + i), additionalUsage.remove("1"));
      listener.onNewContainerStorageQuota(expectedQuota);
      TestUtils.assertContainerMap(expectedQuota, enforcer.getStorageQuota());
    }
  }

  /**
   * Test when storage usage updates.
   */
  @Test
  public void testStorageUsageRefresherListener() {
    AmbryStorageQuotaEnforcer enforcer = new AmbryStorageQuotaEnforcer();
    int initNumAccounts = 10;
    Map<String, Map<String, Long>> expectedUsage = TestUtils.makeStorageMap(initNumAccounts, 10, 10000, 1000);
    enforcer.initStorageUsage(expectedUsage);
    TestUtils.assertContainerMap(expectedUsage, enforcer.getStorageUsage());

    // Adding extra account and container usage
    enforcer.getStorageUsage().put("1000", new ConcurrentHashMap<String, Long>());
    enforcer.getStorageUsage().get("1000").put("1", new Long(10000));

    StorageUsageRefresher.Listener listener = enforcer.getUsageRefresherListener();
    int numUpdates = 10;
    for (int i = 1; i <= numUpdates; i++) {
      if (i % 2 == 0) {
        // add new storage usage
        Map<String, Map<String, Long>> additionalUsage = TestUtils.makeStorageMap(1, 10, 10000, 1000);
        expectedUsage.put(String.valueOf(initNumAccounts + i), additionalUsage.remove("1"));
      } else {
        // change existing storage usage
        Random random = new Random();
        int accountId = random.nextInt(initNumAccounts) + 1;
        int containerId = random.nextInt(10) + 1;
        long newValue = random.nextLong();
        expectedUsage.get(String.valueOf(accountId)).put(String.valueOf(containerId), newValue);
      }
      listener.onNewContainerStorageUsage(expectedUsage);
      expectedUsage.put("1000", new HashMap<String, Long>());
      expectedUsage.get("1000").put("1", new Long(10000));
      TestUtils.assertContainerMap(expectedUsage, enforcer.getStorageUsage());
      expectedUsage.remove("1000");
    }
  }

  /**
   * Test on {@link StorageQuotaEnforcer#shouldThrottle} when account and contaienr doesn't have a quota specified
   */
  @Test
  public void testThrottleNoQuota() {
    AmbryStorageQuotaEnforcer enforcer = new AmbryStorageQuotaEnforcer();
    int initNumAccounts = 10;
    Map<String, Map<String, Long>> expectedQuota = TestUtils.makeStorageMap(initNumAccounts, 10, 10000, 1000);
    enforcer.initStorageQuota(expectedQuota);
    enforcer.initStorageUsage(Collections.EMPTY_MAP);
    enforcer.setQuotaMode(QuotaMode.Throttling);

    // Make sure there is no quota for this account and container
    Random random = new Random();
    Map<String, Map<String, Long>> expectedUsage = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      short accountId = (short) (random.nextInt(10) + initNumAccounts + 1);
      short containerId = (short) (random.nextInt(10));
      long size = random.nextLong() % 10000 + 100;
      expectedUsage.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
          .compute(String.valueOf(containerId), (k, v) -> {
            if (v == null) {
              return size;
            } else {
              return v.longValue() + size;
            }
          });
      assertFalse(enforcer.shouldThrottle(accountId, containerId, QuotaOperation.Post, size));
    }

    TestUtils.assertContainerMap(expectedUsage, enforcer.getStorageUsage());
  }

  /**
   *  Test on {@link StorageQuotaEnforcer#shouldThrottle} when the mode is {@link QuotaMode#Tracking}.
   */
  @Test
  public void testThrottleTracking() {
    AmbryStorageQuotaEnforcer enforcer = new AmbryStorageQuotaEnforcer();
    int initNumAccounts = 10;
    Map<String, Map<String, Long>> expectedQuota = TestUtils.makeStorageMap(initNumAccounts, 10, 10000, 1000);
    enforcer.initStorageQuota(expectedQuota);
    enforcer.initStorageUsage(Collections.EMPTY_MAP);
    enforcer.setQuotaMode(QuotaMode.Tracking);

    // Make sure there is no quota for this account and container
    Random random = new Random();
    Map<String, Map<String, Long>> expectedUsage = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      short accountId = (short) (random.nextInt(10) + initNumAccounts + 1);
      short containerId = (short) (random.nextInt(10));
      long size = random.nextLong() % 10000 + 100;
      expectedUsage.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
          .compute(String.valueOf(containerId), (k, v) -> {
            if (v == null) {
              return size;
            } else {
              return v.longValue() + size;
            }
          });
      assertFalse(enforcer.shouldThrottle(accountId, containerId, QuotaOperation.Post, size));
    }
    // Make sure the container with quota will not be throttled.
    for (int i = 0; i < 100; i++) {
      short accountId = (short) (random.nextInt(10) + 1);
      short containerId = (short) (random.nextInt(10) + 1);
      long size = random.nextLong() % 10000 + 1000; // this might be greater than the quota
      long quota = expectedQuota.get(String.valueOf(accountId)).get(String.valueOf(containerId));
      expectedUsage.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
          .compute(String.valueOf(containerId), (k, v) -> {
            if (v == null) {
              return size;
            } else if (v.longValue() + size < quota) {
              return v.longValue() + size;
            } else {
              return v.longValue();
            }
          });
      assertFalse(enforcer.shouldThrottle(accountId, containerId, QuotaOperation.Post, size));
    }
    TestUtils.assertContainerMap(expectedUsage, enforcer.getStorageUsage());
  }

  /**
   * Test on {@link StorageQuotaEnforcer#shouldThrottle} when the quota is exceeded.
   */
  @Test
  public void testThrottleExceedsQuota() {
    AmbryStorageQuotaEnforcer enforcer = new AmbryStorageQuotaEnforcer();
    int initNumAccounts = 10;
    Map<String, Map<String, Long>> expectedQuota = TestUtils.makeStorageMap(initNumAccounts, 10, 10000, 1000);
    enforcer.initStorageQuota(expectedQuota);
    enforcer.initStorageUsage(Collections.EMPTY_MAP);
    enforcer.setQuotaMode(QuotaMode.Throttling);

    for (Map.Entry<String, Map<String, Long>> accountQuota : expectedQuota.entrySet()) {
      short accountId = Short.parseShort(accountQuota.getKey());
      for (Map.Entry<String, Long> containerQuota : accountQuota.getValue().entrySet()) {
        short containerId = Short.parseShort(containerQuota.getKey());
        long quota = containerQuota.getValue();
        // Delete should return false
        assertFalse(enforcer.shouldThrottle(accountId, containerId, QuotaOperation.Delete, quota));
        // First upload should return false
        assertFalse(enforcer.shouldThrottle(accountId, containerId, QuotaOperation.Post, quota));
        // Exceeds quota should return true
        assertTrue(enforcer.shouldThrottle(accountId, containerId, QuotaOperation.Post, 1));
      }
    }
  }
}
