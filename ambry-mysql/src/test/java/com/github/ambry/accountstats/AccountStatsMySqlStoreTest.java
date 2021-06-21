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
package com.github.ambry.accountstats;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import javax.sql.DataSource;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link AccountStatsMySqlStore}.
 */
public class AccountStatsMySqlStoreTest {
  private final Connection mockConnection;
  private final DataSource mockDataSource;
  private final MySqlMetrics metrics;
  private static final String clusterName = "Ambry-test";
  private static final String hostname = "test1.ambry_1300";
  private static final long MAX_CONTAINER_USAGE = 100000;
  private static final long MIN_CONTAINER_USAGE = 1000;
  private static final long DEFAULT_CONTAINER_USAGE = 5000;
  private static final int BASE_PARTITION_ID = 100;
  private static final int BASE_ACCOUNT_ID = 1000;
  private static final int BASE_CONTAINER_ID = 1;
  private static AccountStatsMySqlConfig config =
      new AccountStatsMySqlConfig(new VerifiableProperties(new Properties()));

  public AccountStatsMySqlStoreTest() throws SQLException {
    mockConnection = mock(Connection.class);
    PreparedStatement mockInsertStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("INSERT"))).thenReturn(mockInsertStatement);
    when(mockInsertStatement.executeUpdate()).thenReturn(1);

    metrics = new MySqlMetrics(AccountStatsMySqlStore.class, new MetricRegistry());
    mockDataSource = getDataSource(mockConnection);
  }

  /**
   * Utility to get a {@link DataSource}.
   * @param mockConnection the connection to use.
   * @return the {@link DataSource}.
   * @throws SQLException
   */
  static DataSource getDataSource(Connection mockConnection) throws SQLException {
    DataSource mockDataSource = mock(DataSource.class);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    return mockDataSource;
  }

  @Test
  public void testLocalBackupFile() throws IOException {
    // First, make sure there is no local backup file.
    Path tempDir = Files.createTempDirectory("AccountStatsMySqlStoreTest");
    Path localBackupFilePath = tempDir.resolve("localbackup");
    AccountStatsMySqlStore store =
        new AccountStatsMySqlStore(config, mockDataSource, clusterName, hostname, localBackupFilePath.toString(), null,
            new MetricRegistry());
    assertNull(store.getPreviousStats());
    // Second, save a backup file.
    StatsSnapshot snapshot = createStatsSnapshot(10, 10, 10, false);
    StatsHeader header =
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, System.currentTimeMillis(), 10, 10, null);
    StatsWrapper statsWrapper = new StatsWrapper(header, snapshot);
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(localBackupFilePath.toFile(), statsWrapper);
    store =
        new AccountStatsMySqlStore(config, mockDataSource, clusterName, hostname, localBackupFilePath.toString(), null,
            new MetricRegistry());

    StatsWrapper backupWrapper = store.getPreviousStats();
    assertNotNull(backupWrapper);
    assertStatsHeader(backupWrapper.getHeader(), 10, 10);
    assertStatsSnapshot(backupWrapper.getSnapshot(), 10, 10, 10);
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
          containerSubMap.put(Utils.statsContainerKey((short) (BASE_CONTAINER_ID + ic)), containerStatsSnapshot);
          allContainerValue += containerUsage;
        }
        StatsSnapshot accountStatsSnapshot = new StatsSnapshot(allContainerValue, containerSubMap);
        accountSubMap.put(Utils.statsAccountKey((short) (BASE_ACCOUNT_ID + ia)), accountStatsSnapshot);
        allAccountValue += allContainerValue;
      }
      StatsSnapshot partitionStatsSnapshot = new StatsSnapshot(allAccountValue, accountSubMap);
      partitionSubMap.put(Utils.statsPartitionKey((short) (BASE_PARTITION_ID + ip)), partitionStatsSnapshot);
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
