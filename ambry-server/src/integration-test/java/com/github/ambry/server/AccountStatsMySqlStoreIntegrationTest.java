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
import com.github.ambry.server.mysql.AggregatedAccountReportsDao;
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
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
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
  private final AccountStatsMySqlStore mySqlStore;

  public AccountStatsMySqlStoreIntegrationTest() throws Exception {
    mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, false);
  }

  @Before
  public void before() throws Exception {
    cleanup(mySqlStore.getMySqlDataAccessor());
  }

  /**
   * Tests to store multiple stats for multiple hosts and recover stats from database.
   * @throws Exception
   */
  @Test
  public void testMultiStoreStats() throws Exception {
    AccountStatsMySqlStore mySqlStore1 = createAccountStatsMySqlStore(clusterName1, hostname1, false);
    AccountStatsMySqlStore mySqlStore2 = createAccountStatsMySqlStore(clusterName1, hostname2, false);
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, false);

    StatsWrapper stats1 = generateStatsWrapper(10, 10, 1);
    StatsWrapper stats2 = generateStatsWrapper(10, 10, 1);
    StatsWrapper stats3 = generateStatsWrapper(10, 10, 1);
    mySqlStore1.storeStats(stats1);
    mySqlStore2.storeStats(stats2);
    mySqlStore3.storeStats(stats3);

    assertTableSize(mySqlStore1.getMySqlDataAccessor(), 3 * 10 * 10);

    StatsWrapper obtainedStats1 = mySqlStore1.queryStatsOf(clusterName1, hostname1);
    StatsWrapper obtainedStats2 = mySqlStore2.queryStatsOf(clusterName1, hostname2);
    StatsWrapper obtainedStats3 = mySqlStore3.queryStatsOf(clusterName2, hostname3);
    assertTwoStatsSnapshots(obtainedStats1.getSnapshot(), stats1.getSnapshot());
    assertTwoStatsSnapshots(obtainedStats2.getSnapshot(), stats2.getSnapshot());
    assertTwoStatsSnapshots(obtainedStats3.getSnapshot(), stats3.getSnapshot());
  }

  /**
   * Tests to store multiple stats for one hosts and recover stats from database.
   * @throws Exception
   */
  @Test
  public void testStoreMulitpleWrites() throws Exception {
    AccountStatsMySqlStore mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, false);
    StatsWrapper stats1 = generateStatsWrapper(10, 10, 1);
    mySqlStore.storeStats(stats1);
    StatsWrapper stats2 =
        new StatsWrapper(new StatsHeader(stats1.getHeader()), new StatsSnapshot(stats1.getSnapshot()));
    // change one value, and store it to mysql database again
    stats2.getSnapshot().getSubMap().get("Partition[0]").getSubMap().get("A[0]").getSubMap().get("C[0]").setValue(1);
    stats2.getSnapshot().updateValue();
    mySqlStore.storeStats(stats2);
    StatsWrapper obtainedStats = mySqlStore.queryStatsOf(clusterName1, hostname1);
    assertTwoStatsSnapshots(obtainedStats.getSnapshot(), stats2.getSnapshot());
  }

  @Test
  public void testAggregatedStats() throws Exception {
    Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    StatsSnapshot snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages);
    mySqlStore.storeAggregatedStats(snapshot);
    Map<String, Map<String, Long>> obtainedContainerStorageUsages = mySqlStore.queryAggregatedStats(clusterName1);
    TestUtils.assertContainerMap(containerStorageUsages, obtainedContainerStorageUsages);

    // Change one value and store it to mysql database again
    StatsSnapshot newSnapshot = new StatsSnapshot(snapshot);
    newSnapshot.getSubMap().get("A[1]").getSubMap().get("C[1]").setValue(1);
    newSnapshot.updateValue();
    containerStorageUsages.get("1").put("1", 1L);
    mySqlStore.storeAggregatedStats(newSnapshot);
    obtainedContainerStorageUsages = mySqlStore.queryAggregatedStats(clusterName1);
    TestUtils.assertContainerMap(containerStorageUsages, obtainedContainerStorageUsages);
  }

  @Test
  public void testMonthlyAggregatedStats() throws Exception {
    String monthValue = "2020-01";
    Map<String, Map<String, Long>> currentContainerStorageUsages = mySqlStore.queryAggregatedStats(clusterName1);
    if (currentContainerStorageUsages.size() == 0) {
      Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
      StatsSnapshot snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages);
      mySqlStore.storeAggregatedStats(snapshot);
      currentContainerStorageUsages = mySqlStore.queryAggregatedStats(clusterName1);
    }
    mySqlStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(clusterName1, monthValue);
    Map<String, Map<String, Long>> monthlyContainerStorageUsages = mySqlStore.queryMonthlyAggregatedStats(clusterName1);
    TestUtils.assertContainerMap(currentContainerStorageUsages, monthlyContainerStorageUsages);
    String obtainedMonthValue = mySqlStore.queryRecordedMonth(clusterName1);
    assertTrue(obtainedMonthValue.equals(monthValue));

    // Change the value and store it back to mysql database
    monthValue = "2020-02";
    currentContainerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    StatsSnapshot snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(currentContainerStorageUsages);
    mySqlStore.storeAggregatedStats(snapshot);
    mySqlStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(clusterName1, monthValue);
    monthlyContainerStorageUsages = mySqlStore.queryMonthlyAggregatedStats(clusterName1);
    TestUtils.assertContainerMap(currentContainerStorageUsages, monthlyContainerStorageUsages);
    obtainedMonthValue = mySqlStore.queryRecordedMonth(clusterName1);
    assertTrue(obtainedMonthValue.equals(monthValue));
  }

  private AccountStatsMySqlStore createAccountStatsMySqlStore(String clusterName, String hostname,
      boolean withLocalbackup) throws Exception {
    Path localBackupFilePath = withLocalbackup ? createLocalBackup(10, 10, 1) : createTemporaryFile();
    Properties configProps = Utils.loadPropsFromResource("mysql.properties");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, clusterName);
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
    statement.executeUpdate("DELETE FROM " + AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_TABLE);
    statement.executeUpdate("DELETE FROM " + AggregatedAccountReportsDao.MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE);
    statement.executeUpdate("DELETE FROM " + AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE);
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
