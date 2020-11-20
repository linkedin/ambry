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
package com.github.ambry.server.mysql;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link AccountStatsMySqlStore}.
 */
public class AccountStatsMySqlStoreTest {
  private final Connection mockConnection;
  private final MySqlDataAccessor dataAccessor;
  private final MySqlMetrics metrics;
  private static final String clusterName = "Ambry-test";
  private static final String hostname = "test1.ambry_1300";
  private static final long MAX_CONTAINER_USAGE = 100000;
  private static final long MIN_CONTAINER_USAGE = 1000;
  private static final long DEFAULT_CONTAINER_USAGE = 5000;
  private static final int BASE_PARTITION_ID = 100;
  private static final int BASE_ACCOUNT_ID = 1000;
  private static final int BASE_CONTAINER_ID = 1;

  public AccountStatsMySqlStoreTest() throws SQLException {
    mockConnection = mock(Connection.class);
    PreparedStatement mockInsertStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("INSERT"))).thenReturn(mockInsertStatement);
    when(mockInsertStatement.executeUpdate()).thenReturn(1);

    metrics = new MySqlMetrics(AccountStatsMySqlStore.class, new MetricRegistry());
    dataAccessor = getDataAccessor(mockConnection, metrics);
  }

  /**
   * Utility to get a {@link MySqlDataAccessor}.
   * @param mockConnection the connection to use.
   * @return the {@link MySqlDataAccessor}.
   * @throws SQLException
   */
  static MySqlDataAccessor getDataAccessor(Connection mockConnection, MySqlMetrics metrics) throws SQLException {
    Driver mockDriver = mock(Driver.class);
    when(mockDriver.connect(anyString(), any(Properties.class))).thenReturn(mockConnection);
    MySqlUtils.DbEndpoint dbEndpoint =
        new MySqlUtils.DbEndpoint("jdbc:mysql://localhost/ambry_container_storage_stats", "dc1", true, "ambry",
            "ambry");
    return new MySqlDataAccessor(Collections.singletonList(dbEndpoint), mockDriver, metrics);
  }

  @Test
  public void testLocalBackupFile() throws IOException {
    // First, make sure there is no local backup file.
    Path tempDir = Files.createTempDirectory("AccountStatsMySqlStoreTest");
    Path localBackupFilePath = tempDir.resolve("localbackup");
    AccountStatsMySqlStore store =
        new AccountStatsMySqlStore(dataAccessor, clusterName, hostname, localBackupFilePath.toString(), null,
            new MetricRegistry());
    assertNull(store.getPreviousStats());
    // Second, save a backup file.
    StatsSnapshot snapshot = createStatsSnapshot(10, 10, 10, false);
    StatsHeader header =
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, System.currentTimeMillis(), 10, 10, null);
    StatsWrapper statsWrapper = new StatsWrapper(header, snapshot);
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(localBackupFilePath.toFile(), statsWrapper);
    store = new AccountStatsMySqlStore(dataAccessor, clusterName, hostname, localBackupFilePath.toString(), null,
        new MetricRegistry());

    StatsWrapper backupWrapper = store.getPreviousStats();
    assertNotNull(backupWrapper);
    assertStatsHeader(backupWrapper.getHeader(), 10, 10);
    assertStatsSnapshot(backupWrapper.getSnapshot(), 10, 10, 10);
  }

  @Test
  public void testLocalBackupAndPublish() throws Exception {
    Path tempDir = Files.createTempDirectory("AccountStatsMySqlStoreTest");
    Path localBackupFilePath = tempDir.resolve("localbackup");
    AccountStatsMySqlStore store =
        new AccountStatsMySqlStore(dataAccessor, clusterName, hostname, localBackupFilePath.toString(), null,
            new MetricRegistry());
    StatsSnapshot snapshot = createStatsSnapshot(10, 10, 1, false);
    StatsHeader header =
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, System.currentTimeMillis(), 10, 10, null);
    StatsWrapper statsWrapper = new StatsWrapper(header, snapshot);
    store.publish(statsWrapper);
    assertEquals("Write success count should be " + (10 * 10), 10 * 10, metrics.writeSuccessCount.getCount());

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(localBackupFilePath.toFile(), statsWrapper);

    store = new AccountStatsMySqlStore(dataAccessor, clusterName, hostname, localBackupFilePath.toString(), null,
        new MetricRegistry());

    // We have a local backup to start with.
    // Write the statsWrapper from local backup
    store.publish(statsWrapper);
    assertEquals("Write success count should be " + (10 * 10), (10 * 10), metrics.writeSuccessCount.getCount());
  }

  @Test
  public void testMultiPublish() throws Exception {
    Path tempDir = Files.createTempDirectory("AccountStatsMySqlStoreTest");
    Path localBackupFilePath = tempDir.resolve("localbackup");

    StatsSnapshot snapshot = createStatsSnapshot(10, 10, 1, true);
    StatsHeader header =
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, System.currentTimeMillis(), 10, 10, null);
    StatsWrapper statsWrapper = new StatsWrapper(header, snapshot);
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(localBackupFilePath.toFile(), statsWrapper);

    // create a store with a local backup file and try to publish it again.
    AccountStatsMySqlStore store =
        new AccountStatsMySqlStore(dataAccessor, clusterName, hostname, localBackupFilePath.toString(), null,
            new MetricRegistry());
    store.publish(statsWrapper);
    assertEquals("Write success count should be 0", 0, metrics.writeSuccessCount.getCount());

    // Since all container storage usages are default values, second Snapshot should be the same as the first snapshot
    StatsSnapshot secondSnapshot = createStatsSnapshot(10, 10, 1, true);
    StatsWrapper secondStatsWrapper = new StatsWrapper(header, secondSnapshot);
    store.publish(secondStatsWrapper);
    assertEquals("Write success count should be 0", 0, metrics.writeSuccessCount.getCount());
    // Now the previous stats wrapper should be the second statsWrapper
    assertTrue(secondStatsWrapper == store.getPreviousStats());

    // Change one of the value in statsWrapper
    int partitionId = BASE_PARTITION_ID + 1;
    int accountId = BASE_ACCOUNT_ID + 1;
    statsWrapper.getSnapshot()
        .getSubMap()
        .get("Partition[" + partitionId + "]")
        .getSubMap()
        .get("A[" + accountId + "]")
        .getSubMap()
        .put("C[" + BASE_CONTAINER_ID + "]", new StatsSnapshot((long) (DEFAULT_CONTAINER_USAGE + 1), null));
    store.publish(statsWrapper);
    assertEquals("Write success count should be 1", 1, metrics.writeSuccessCount.getCount());
    assertTrue(statsWrapper == store.getPreviousStats());

    // Add a partition
    StatsSnapshot additionalSnapshot = createStatsSnapshot(1, 10, 1, true);
    secondStatsWrapper.getSnapshot()
        .getSubMap()
        .put("Partition[" + (BASE_PARTITION_ID + 10) + "]",
            additionalSnapshot.getSubMap().get("Partition[" + BASE_PARTITION_ID + "]"));
    // With the change from the previous one, we should see 10 + 1 writes
    store.publish(secondStatsWrapper);
    assertEquals("Write success count should be 12", 12, metrics.writeSuccessCount.getCount());
    assertTrue(secondStatsWrapper == store.getPreviousStats());
  }

  private StatsSnapshot createStatsSnapshot(int numPartitions, int numAccounts, int numContainers,
      boolean defaultValue) {
    Random random = new Random();
    Map<String, StatsSnapshot> partitionSubMap = new HashMap<>();
    long allPartitionValue = 0;
    for (int ip = 0; ip < numPartitions; ip++) {
      Map<String, StatsSnapshot> accountSubMap = new HashMap<>();
      long allAccountValue = 0;
      for (int ia = 0; ia < numAccounts; ia++) {
        Map<String, StatsSnapshot> containerSubMap = new HashMap<>();
        long allContainerValue = 0;
        for (int ic = 0; ic < numContainers; ic++) {
          long containerUsage = defaultValue ? DEFAULT_CONTAINER_USAGE
              : Math.abs(random.nextLong()) % (MAX_CONTAINER_USAGE - MIN_CONTAINER_USAGE) + MIN_CONTAINER_USAGE;
          StatsSnapshot containerStatsSnapshot = new StatsSnapshot(containerUsage, null);
          containerSubMap.put("C[" + (BASE_CONTAINER_ID + ic) + "]", containerStatsSnapshot);
          allContainerValue += containerUsage;
        }
        StatsSnapshot accountStatsSnapshot = new StatsSnapshot(allContainerValue, containerSubMap);
        accountSubMap.put("A[" + (BASE_ACCOUNT_ID + ia) + "]", accountStatsSnapshot);
        allAccountValue += allContainerValue;
      }
      StatsSnapshot partitionStatsSnapshot = new StatsSnapshot(allAccountValue, accountSubMap);
      partitionSubMap.put("Partition[" + (BASE_PARTITION_ID + ip) + "]", partitionStatsSnapshot);
      allPartitionValue += allAccountValue;
    }
    return new StatsSnapshot(allPartitionValue, partitionSubMap);
  }

  private void assertStatsHeader(StatsHeader statsHeader, int storesContactedCount, int storesRespondedCount) {
    assertEquals(statsHeader.getStoresContactedCount(), storesContactedCount);
    assertEquals(statsHeader.getStoresRespondedCount(), storesRespondedCount);
  }

  private void assertStatsSnapshot(StatsSnapshot snapshot, int numPartitions, int numAccounts, int numContainers) {
    Map<String, StatsSnapshot> partitionSubMap = snapshot.getSubMap();
    assertEquals(partitionSubMap.size(), numPartitions);
    for (String partitionIdKey : partitionSubMap.keySet()) {
      Map<String, StatsSnapshot> accountSubMap = partitionSubMap.get(partitionIdKey).getSubMap();
      assertEquals(accountSubMap.size(), numAccounts);
      for (String accountIdKey : accountSubMap.keySet()) {
        Map<String, StatsSnapshot> containerSubMap = accountSubMap.get(accountIdKey).getSubMap();
        assertEquals(containerSubMap.size(), numContainers);
        for (String containerIdKey : containerSubMap.keySet()) {
          long storageUsage = containerSubMap.get(containerIdKey).getValue();
          assertTrue("Storage usage is " + storageUsage + " at " + partitionIdKey + accountIdKey + containerIdKey
              + " it should be greater than " + MIN_CONTAINER_USAGE, storageUsage >= MIN_CONTAINER_USAGE);
          assertTrue("Storage usage is " + storageUsage + " it should be less than " + MAX_CONTAINER_USAGE,
              storageUsage <= MAX_CONTAINER_USAGE);
        }
      }
    }
  }
}
