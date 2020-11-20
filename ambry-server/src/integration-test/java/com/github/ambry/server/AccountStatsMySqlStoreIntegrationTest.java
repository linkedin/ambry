/**
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
package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.server.mysql.AccountReportsDao;
import com.github.ambry.server.mysql.AccountStatsMySqlStore;
import com.github.ambry.server.mysql.AccountStatsMySqlStoreFactory;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Integration tests for {@link AccountStatsMySqlStore}.
 */
public class AccountStatsMySqlStoreIntegrationTest {
  private static final String clusterName1 = "Ambry-test";
  private static final String clusterName2 = "Ambry-random";
  private static final String hostname1 = "ambry1.test.github.com";
  private static final String hostname2 = "ambry2.test.github.com";
  private static final String hostname3 = "ambry3.test.github.com";
  private static final int port = 12345;

  public AccountStatsMySqlStoreIntegrationTest() throws Exception {
    AccountStatsMySqlStore mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, false);
    cleanup(mySqlStore.getMySqlDataAccessor());
  }

  /**
   * Tests to publish multiple stats and recover stats from database.
   * @throws Exception
   */
  @Test
  public void testMultiStorePublish() throws Exception {
    AccountStatsMySqlStore mySqlStore1 = createAccountStatsMySqlStore(clusterName1, hostname1, false);
    AccountStatsMySqlStore mySqlStore2 = createAccountStatsMySqlStore(clusterName1, hostname2, false);
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, false);

    StatsWrapper stats1 = generateStatsWrapper(10, 10, 1);
    StatsWrapper stats2 = generateStatsWrapper(10, 10, 1);
    StatsWrapper stats3 = generateStatsWrapper(10, 10, 1);
    mySqlStore1.publish(stats1);
    mySqlStore2.publish(stats2);
    mySqlStore3.publish(stats3);

    assertTableSize(mySqlStore1.getMySqlDataAccessor(), 3 * 10 * 10);

    StatsSnapshot obtainedStats1 = mySqlStore1.queryStatsSnapshotOf(clusterName1, hostname1);
    StatsSnapshot obtainedStats2 = mySqlStore2.queryStatsSnapshotOf(clusterName1, hostname2);
    StatsSnapshot obtainedStats3 = mySqlStore3.queryStatsSnapshotOf(clusterName2, hostname3);
    assertTwoStatsSnapshots(obtainedStats1, stats1.getSnapshot());
    assertTwoStatsSnapshots(obtainedStats2, stats2.getSnapshot());
    assertTwoStatsSnapshots(obtainedStats3, stats3.getSnapshot());
  }

  private AccountStatsMySqlStore createAccountStatsMySqlStore(String clustername, String hostname,
      boolean withLocalbackup) throws Exception {
    Path localBackupFilePath = withLocalbackup ? createLocalBackup(10, 10, 1) : createTemporaryFile();
    Properties configProps = Utils.loadPropsFromResource("mysql.properties");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, clustername);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, hostname);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_PORT, String.valueOf(port));
    configProps.setProperty(AccountStatsMySqlConfig.DOMAIN_NAMES_TO_REMOVE, ".github.com");
    configProps.setProperty(StatsManagerConfig.STATS_OUTPUT_FILE_PATH, localBackupFilePath.toString());
    VerifiableProperties verifiableProperties = new VerifiableProperties(configProps);
    return new AccountStatsMySqlStoreFactory(verifiableProperties, new ClusterMapConfig(verifiableProperties),
        new StatsManagerConfig(verifiableProperties), new MetricRegistry()).getAccountStatsMySqlStore();
  }

  private static Path createLocalBackup(int numPartitions, int numAccounts, int numContainers) throws IOException {
    Path localBackupFilePath = createTemporaryFile();
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(localBackupFilePath.toFile(),
        generateStatsWrapper(numPartitions, numAccounts, numContainers));
    return localBackupFilePath;
  }

  private static Path createTemporaryFile() throws IOException {
    Path tempDir = Files.createTempDirectory("AccountStatsMySqlStoreTest");
    return tempDir.resolve("localbackup");
  }

  private static StatsWrapper generateStatsWrapper(int numPartitions, int numAccounts, int numContainers) {
    Random random = new Random();
    List<StatsSnapshot> storeSnapshots = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      storeSnapshots.add(
          TestUtils.generateStoreStats(numAccounts, numContainers, random, StatsReportType.ACCOUNT_REPORT));
    }
    return TestUtils.generateNodeStats(storeSnapshots, 1000, StatsReportType.ACCOUNT_REPORT);
  }

  private void cleanup(MySqlDataAccessor dataAccessor) throws SQLException {
    Connection dbConnection = dataAccessor.getDatabaseConnection(true);
    Statement statement = dbConnection.createStatement();
    statement.executeUpdate("DELETE FROM " + AccountReportsDao.ACCOUNT_REPORTS_TABLE);
  }

  private void assertTableSize(MySqlDataAccessor dataAccessor, int expectedNumRows) throws SQLException {
    Connection dbConnection = dataAccessor.getDatabaseConnection(true);
    Statement statement = dbConnection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM " + AccountReportsDao.ACCOUNT_REPORTS_TABLE);
    int numRows = 0;
    if (resultSet != null) {
      while (resultSet.next()) {
        numRows++;
      }
      resultSet.close();
    }
    assertEquals(expectedNumRows, numRows);
  }

  private void assertTwoStatsSnapshots(StatsSnapshot snapshot1, StatsSnapshot snapshot2) {
    assertEquals("Snapshot values are not equal", snapshot1.getValue(), snapshot2.getValue());
    if (snapshot1.getSubMap() == null) {
      assertNull(snapshot2.getSubMap());
    } else {
      assertEquals("Snapshot Submap size mismatch", snapshot1.getSubMap().size(), snapshot2.getSubMap().size());
      for (String key : snapshot1.getSubMap().keySet()) {
        assertTrue(snapshot2.getSubMap().containsKey(key));
        assertTwoStatsSnapshots(snapshot1.getSubMap().get(key), snapshot2.getSubMap().get(key));
      }
    }
  }
}
