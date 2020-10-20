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

import com.github.ambry.clustermap.HelixStoreOperator;
import com.github.ambry.clustermap.MockHelixPropertyStore;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.utils.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * The unit test class for {@link HelixStorageUsageRefresher}.
 */
public class HelixStorageUsageRefresherTest {
  private static final String ZK_CLIENT_CONNECT_ADDRESS = "localhost:localport";
  private static final long MAX_CONTAINER_USAGE = 1000000000;
  private static final long MIN_CONTAINER_USAGE = 100;
  private static final int UPDATE_TIMEOUT_IN_SECOND = 10;

  private static ScheduledExecutorService scheduler = Utils.newScheduler(1, "storage-usage-refresher", false);
  private HelixPropertyStore<ZNRecord> mockHelixStore;
  private StorageQuotaConfig storageQuotaConfig;
  private HelixStorageUsageRefresher storageUsageRefresher;

  /**
   * Constructor to create a {@link HelixStorageUsageRefresherTest}.
   */
  public HelixStorageUsageRefresherTest() {
    mockHelixStore = new MockHelixPropertyStore<>();
    Properties properties = new Properties();
    properties.setProperty(StorageQuotaConfig.ZK_CLIENT_CONNECT_ADDRESS, ZK_CLIENT_CONNECT_ADDRESS);
    properties.setProperty(StorageQuotaConfig.HELIX_PROPERTY_ROOT_PATH, "");
    storageQuotaConfig = new StorageQuotaConfig(new VerifiableProperties(properties));
  }

  @AfterClass
  public static void cleanupTestSuite() {
    scheduler.shutdown();
  }

  /**
   * Test when starting {@link HelixStorageUsageRefresher} with empty storage usage.
   */
  @Test
  public void testStartRefresherWithEmptyStorageUsage() {
    storageUsageRefresher = new HelixStorageUsageRefresher(mockHelixStore, null, storageQuotaConfig);
    Map<String, Map<String, Long>> emptyMap = storageUsageRefresher.getContainerStorageUsage();
    assertTrue(emptyMap.equals(Collections.EMPTY_MAP));
  }

  /**
   * Test when starting {@link HelixStorageUsageRefresher} with storage usage for some container.
   */
  @Test
  public void testStartRefresherWithStorageUsage() throws Exception {
    Map<String, Map<String, Long>> expectedUsage = makeStorageUsageAndWriteToHelix(10, 10);
    storageUsageRefresher = new HelixStorageUsageRefresher(mockHelixStore, null, storageQuotaConfig);
    Map<String, Map<String, Long>> obtainedUsage = storageUsageRefresher.getContainerStorageUsage();
    TestUtils.assertContainerMap(expectedUsage, obtainedUsage);
  }

  /**
   * Test when starting {@link HelixStorageUsageRefresher} with bad {@link ZNRecord}.
   */
  @Test
  public void testStartRefresherWithBadZNRecord() throws Exception {
    writeToHelix("BAD ZNRECORD");
    storageUsageRefresher = new HelixStorageUsageRefresher(mockHelixStore, null, storageQuotaConfig);
    Map<String, Map<String, Long>> emptyMap = storageUsageRefresher.getContainerStorageUsage();
    assertTrue(emptyMap.equals(Collections.EMPTY_MAP));
  }

  /**
   * Test if {@link HelixStorageUsageRefresher} can successfully subscribe to the change of storage usage.
   * @throws Exception
   */
  @Test
  public void testRefresherWithMultipleUpdates() throws Exception {
    int initialNumAccounts = 10;
    Map<String, Map<String, Long>> expectedUsage = makeStorageUsageAndWriteToHelix(initialNumAccounts, 10);
    storageUsageRefresher = new HelixStorageUsageRefresher(mockHelixStore, null, storageQuotaConfig);
    Map<String, Map<String, Long>> obtainedUsage = storageUsageRefresher.getContainerStorageUsage();
    TestUtils.assertContainerMap(expectedUsage, obtainedUsage);

    // Use an AtomicReference to hold a CountDownLatch since we can only register listener once.
    AtomicReference<CountDownLatch> latchHolder = new AtomicReference<>();
    storageUsageRefresher.registerListener(new StorageUsageRefresher.Listener() {
      @Override
      public void onNewContainerStorageUsage(Map<String, Map<String, Long>> containerStorageUsage) {
        latchHolder.get().countDown();
      }
    });

    int numUpdates = 10;
    for (int i = 1; i <= numUpdates; i++) {
      Map<String, Map<String, Long>> additionalUsage =
          TestUtils.makeStorageMap(1, 10, MAX_CONTAINER_USAGE, MIN_CONTAINER_USAGE);
      expectedUsage.put(String.valueOf(initialNumAccounts + i), additionalUsage.remove("1"));
      CountDownLatch latch = new CountDownLatch(1);
      latchHolder.set(latch);
      writeStorageUsageToHelix(expectedUsage);
      latch.await(UPDATE_TIMEOUT_IN_SECOND, TimeUnit.SECONDS);

      obtainedUsage = storageUsageRefresher.getContainerStorageUsage();
      TestUtils.assertContainerMap(expectedUsage, obtainedUsage);
    }
  }

  /**
   * Test when {@link HelixStorageUsageRefresher} sees a bad update to the storage usage.
   * @throws Exception
   */
  @Test
  public void testRefresherUpdateWithBadRecord() throws Exception {
    Map<String, Map<String, Long>> expectedUsage = makeStorageUsageAndWriteToHelix(10, 10);
    storageUsageRefresher = new HelixStorageUsageRefresher(mockHelixStore, null, storageQuotaConfig);
    Map<String, Map<String, Long>> obtainedUsage = storageUsageRefresher.getContainerStorageUsage();
    TestUtils.assertContainerMap(expectedUsage, obtainedUsage);

    // write a bad znrecord to helix
    writeToHelix("BAD ZNRECORD");
    // wait for a while
    Thread.sleep(1000);
    // Make sure it doesn't ruin the in-memory cache
    obtainedUsage = storageUsageRefresher.getContainerStorageUsage();
    TestUtils.assertContainerMap(expectedUsage, obtainedUsage);
  }

  /**
   * Test when {@link HelixStorageUsageRefresher} receive a good update after a bad one.
   * @throws Exception
   */
  @Test
  public void testRefresherGoodUpdateAfterBadRecord() throws Exception {
    testRefresherUpdateWithBadRecord();
    // Now place a good
    CountDownLatch latch = new CountDownLatch(1);
    storageUsageRefresher.registerListener(new StorageUsageRefresher.Listener() {
      @Override
      public void onNewContainerStorageUsage(Map<String, Map<String, Long>> containerStorageUsage) {
        latch.countDown();
      }
    });
    Map<String, Map<String, Long>> expectedUsage = makeStorageUsageAndWriteToHelix(10, 10);
    latch.await(UPDATE_TIMEOUT_IN_SECOND, TimeUnit.SECONDS);
    Map<String, Map<String, Long>> obtainedUsage = storageUsageRefresher.getContainerStorageUsage();
    TestUtils.assertContainerMap(expectedUsage, obtainedUsage);
  }

  /**
   * Test starting {@link HelixStorageUsageRefresher} with a scheduler.
   * @throws Exception
   */
  @Test
  public void testStartRefresherWithScheduler() throws Exception {
    Map<String, Map<String, Long>> expectedUsage = makeStorageUsageAndWriteToHelix(10, 10);
    storageUsageRefresher = new HelixStorageUsageRefresher(mockHelixStore, scheduler, storageQuotaConfig);
    Map<String, Map<String, Long>> obtainedUsage = storageUsageRefresher.getContainerStorageUsage();
    TestUtils.assertContainerMap(expectedUsage, obtainedUsage);
  }

  /**
   * Make several accounts and containers' storage usage and write them to {@link HelixPropertyStore}.
   * @param numAccounts The number of accounts to make.
   * @param numContainerPerAccount The number of containers per account.
   * @return The map representing storage usage for each container.
   * @throws Exception
   */
  private Map<String, Map<String, Long>> makeStorageUsageAndWriteToHelix(int numAccounts, int numContainerPerAccount)
      throws Exception {
    Map<String, Map<String, Long>> accountUsage =
        TestUtils.makeStorageMap(numAccounts, numContainerPerAccount, MAX_CONTAINER_USAGE, MIN_CONTAINER_USAGE);
    writeStorageUsageToHelix(accountUsage);
    return accountUsage;
  }

  /**
   * Persist given storage usage in {@link HelixPropertyStore}.
   * @param accountStorageUsage The map representing storage usage.
   * @throws Exception
   */
  private void writeStorageUsageToHelix(Map<String, Map<String, Long>> accountStorageUsage) throws Exception {
    Map<String, StatsSnapshot> accountSnapshots = new HashMap<>();
    long sumOfAccountUsage = 0;
    for (Map.Entry<String, Map<String, Long>> accountStorageUsageEntry : accountStorageUsage.entrySet()) {
      String accountId = accountStorageUsageEntry.getKey();
      Map<String, StatsSnapshot> containerSnapshots = new HashMap<>();
      long sumOfContainerUsage = 0;
      for (Map.Entry<String, Long> containerStorageUsageEntry : accountStorageUsageEntry.getValue().entrySet()) {
        String containerId = containerStorageUsageEntry.getKey();
        long usage = containerStorageUsageEntry.getValue();
        sumOfContainerUsage += usage;
        containerSnapshots.put("C[" + containerId + "]", new StatsSnapshot(usage, null));
      }
      accountSnapshots.put("A[" + accountId + "]", new StatsSnapshot(sumOfContainerUsage, containerSnapshots));
      sumOfAccountUsage += sumOfContainerUsage;
    }
    StatsSnapshot topSnapshot = new StatsSnapshot(sumOfAccountUsage, accountSnapshots);
    writeToHelix(new ObjectMapper().writeValueAsString(topSnapshot));
  }

  /**
   * Write any value in string to {@link HelixPropertyStore}.
   * @param value The string value to write.
   * @throws Exception
   */
  private void writeToHelix(String value) throws Exception {
    ZNRecord znRecord = new ZNRecord(String.valueOf(System.currentTimeMillis()));
    znRecord.setSimpleField(HelixStorageUsageRefresher.VALID_SIZE_FILED_NAME, value);
    HelixStoreOperator storeOperator = new HelixStoreOperator(mockHelixStore);
    storeOperator.write(HelixStorageUsageRefresher.AGGREGATED_CONTAINER_STORAGE_USAGE_PATH, znRecord);
  }
}
