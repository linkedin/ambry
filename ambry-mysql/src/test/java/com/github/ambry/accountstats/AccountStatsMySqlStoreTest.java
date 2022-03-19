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
import com.github.ambry.server.HostAccountStorageStatsWrapper;
import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.StorageStatsUtilTest;
import com.github.ambry.server.storagestats.HostAccountStorageStats;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import junit.framework.Assert;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit test for {@link AccountStatsMySqlStore}.
 */
public class AccountStatsMySqlStoreTest {
  private final DataSource mockDataSource;
  private static final String clusterName = "Ambry-test";
  private static final String hostname = "test1.ambry_1300";

  public AccountStatsMySqlStoreTest() throws SQLException {
    Connection mockConnection = mock(Connection.class);
    PreparedStatement mockInsertStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(contains("INSERT"))).thenReturn(mockInsertStatement);
    when(mockInsertStatement.executeUpdate()).thenReturn(1);

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
    Properties prop = new Properties();
    prop.setProperty(AccountStatsMySqlConfig.LOCAL_BACKUP_FILE_PATH, localBackupFilePath.toString());
    AccountStatsMySqlConfig accountStatsMySqlConfig = new AccountStatsMySqlConfig(new VerifiableProperties(prop));
    AccountStatsMySqlStore store =
        new AccountStatsMySqlStore(accountStatsMySqlConfig, mockDataSource, clusterName, hostname, null,
            new MetricRegistry());
    assertNull(store.getPreviousHostAccountStorageStatsWrapper());
    // Second, save a backup file.
    HostAccountStorageStats hostAccountStorageStats = new HostAccountStorageStats(
        StorageStatsUtilTest.generateRandomHostAccountStorageStats(10, 10, 10, 10000L, 2, 10));
    StatsHeader header =
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, System.currentTimeMillis(), 10, 10, null);
    HostAccountStorageStatsWrapper statsWrapper = new HostAccountStorageStatsWrapper(header, hostAccountStorageStats);
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(localBackupFilePath.toFile(), statsWrapper);
    store = new AccountStatsMySqlStore(accountStatsMySqlConfig, mockDataSource, clusterName, hostname, null,
        new MetricRegistry());

    HostAccountStorageStatsWrapper backupWrapper = store.getPreviousHostAccountStorageStatsWrapper();
    assertNotNull(backupWrapper);
    assertStatsHeader(backupWrapper.getHeader(), 10, 10);
    Assert.assertEquals(hostAccountStorageStats.getStorageStats(), backupWrapper.getStats().getStorageStats());
  }

  /**
   * Assert given {@link StatsHeader} has proper fields.
   * @param statsHeader
   * @param storesContactedCount
   * @param storesRespondedCount
   */
  private void assertStatsHeader(StatsHeader statsHeader, int storesContactedCount, int storesRespondedCount) {
    assertEquals(statsHeader.getStoresContactedCount(), storesContactedCount);
    assertEquals(statsHeader.getStoresRespondedCount(), storesRespondedCount);
  }
}
