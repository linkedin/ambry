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
package com.github.ambry.accountstats;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.Pair;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Integration tests for {@link AccountStatsMySqlStore}.
 */
@RunWith(Parameterized.class)
public class AccountStatsMySqlStoreIntegrationTest {
  private static final String clusterName1 = "Ambry-test";
  private static final String clusterName2 = "Ambry-random";
  private static final String hostname1 = "ambry1.test.github.com";
  private static final String hostname2 = "ambry2.test.github.com";
  private static final String hostname3 = "ambry3.test.github.com";
  private static final int port = 12345;
  private final int batchSize;
  private final AccountStatsMySqlStore mySqlStore;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{0}, {17}});
  }

  public AccountStatsMySqlStoreIntegrationTest(int batchSize) throws Exception {
    this.batchSize = batchSize;
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

    StatsWrapper stats1 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    StatsWrapper stats2 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    StatsWrapper stats3 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    mySqlStore1.storeAccountStats(stats1);
    mySqlStore2.storeAccountStats(stats2);
    mySqlStore3.storeAccountStats(stats3);

    assertTableSize(mySqlStore1.getMySqlDataAccessor(), 3 * 10 * 10);

    StatsWrapper obtainedStats1 = mySqlStore1.queryAccountStatsOf(hostname1);
    StatsWrapper obtainedStats2 = mySqlStore2.queryAccountStatsOf(hostname2);
    StatsWrapper obtainedStats3 = mySqlStore3.queryAccountStatsOf(hostname3);
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
    StatsWrapper stats1 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    mySqlStore.storeAccountStats(stats1);
    StatsWrapper stats2 =
        new StatsWrapper(new StatsHeader(stats1.getHeader()), new StatsSnapshot(stats1.getSnapshot()));
    // change one value, and store it to mysql database again
    stats2.getSnapshot().getSubMap().get("Partition[0]").getSubMap().get("A[0]").getSubMap().get("C[0]").setValue(1);
    stats2.getSnapshot().updateValue();
    mySqlStore.storeAccountStats(stats2);
    StatsWrapper obtainedStats = mySqlStore.queryAccountStatsOf(hostname1);
    assertTwoStatsSnapshots(obtainedStats.getSnapshot(), stats2.getSnapshot());
  }

  @Test
  public void testAggregatedStats() throws Exception {
    Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    StatsSnapshot snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages);
    mySqlStore.storeAggregatedAccountStats(snapshot);
    Map<String, Map<String, Long>> obtainedContainerStorageUsages = mySqlStore.queryAggregatedAccountStats();
    TestUtils.assertContainerMap(containerStorageUsages, obtainedContainerStorageUsages);

    // Change one value and store it to mysql database again
    StatsSnapshot newSnapshot = new StatsSnapshot(snapshot);
    newSnapshot.getSubMap().get("A[1]").getSubMap().get("C[1]").setValue(1);
    newSnapshot.updateValue();
    containerStorageUsages.get("1").put("1", 1L);
    mySqlStore.storeAggregatedAccountStats(newSnapshot);
    obtainedContainerStorageUsages = mySqlStore.queryAggregatedAccountStats();
    TestUtils.assertContainerMap(containerStorageUsages, obtainedContainerStorageUsages);
  }

  @Test
  public void testMonthlyAggregatedStats() throws Exception {
    String monthValue = "2020-01";
    Map<String, Map<String, Long>> currentContainerStorageUsages = mySqlStore.queryAggregatedAccountStats();
    if (currentContainerStorageUsages.size() == 0) {
      Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
      StatsSnapshot snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(containerStorageUsages);
      mySqlStore.storeAggregatedAccountStats(snapshot);
      currentContainerStorageUsages = mySqlStore.queryAggregatedAccountStats();
    }
    // fetch the month and it should return emtpy string
    Assert.assertEquals("", mySqlStore.queryRecordedMonth());
    mySqlStore.takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(monthValue);
    Map<String, Map<String, Long>> monthlyContainerStorageUsages = mySqlStore.queryMonthlyAggregatedAccountStats();
    TestUtils.assertContainerMap(currentContainerStorageUsages, monthlyContainerStorageUsages);
    String obtainedMonthValue = mySqlStore.queryRecordedMonth();
    assertTrue(obtainedMonthValue.equals(monthValue));

    // Change the value and store it back to mysql database
    monthValue = "2020-02";
    currentContainerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    StatsSnapshot snapshot = TestUtils.makeStatsSnapshotFromContainerStorageMap(currentContainerStorageUsages);
    mySqlStore.storeAggregatedAccountStats(snapshot);
    mySqlStore.takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(monthValue);
    monthlyContainerStorageUsages = mySqlStore.queryMonthlyAggregatedAccountStats();
    TestUtils.assertContainerMap(currentContainerStorageUsages, monthlyContainerStorageUsages);
    obtainedMonthValue = mySqlStore.queryRecordedMonth();
    assertTrue(obtainedMonthValue.equals(monthValue));
  }

  @Test
  public void testHostPartitionClassStats() throws Exception {
    // First write some stats to account reports
    testMultiStoreStats();
    StatsWrapper accountStats1 = mySqlStore.queryAccountStatsOf(hostname1);
    StatsWrapper accountStats2 = mySqlStore.queryAccountStatsOf(hostname2);
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, false);
    StatsWrapper accountStats3 = mySqlStore3.queryAccountStatsOf(hostname3);

    // From this account stats, create partition class stats;
    Set<String> allPartitionKeys = new HashSet<String>() {
      {
        addAll(accountStats1.getSnapshot().getSubMap().keySet());
        addAll(accountStats2.getSnapshot().getSubMap().keySet());
        addAll(accountStats3.getSnapshot().getSubMap().keySet());
      }
    };
    List<String> partitionClassNames = Arrays.asList("default", "new");
    Map<String, String> partitionKeyToClassName = new HashMap<>();
    int ind = 0;
    for (String partitionKey : allPartitionKeys) {
      partitionKeyToClassName.put(partitionKey, partitionClassNames.get(ind % partitionClassNames.size()));
      ind++;
    }
    StatsWrapper partitionClassStats1 =
        convertAccountStatsToPartitionClassStats(accountStats1, partitionKeyToClassName);
    StatsWrapper partitionClassStats2 =
        convertAccountStatsToPartitionClassStats(accountStats2, partitionKeyToClassName);
    StatsWrapper partitionClassStats3 =
        convertAccountStatsToPartitionClassStats(accountStats3, partitionKeyToClassName);
    mySqlStore.storePartitionClassStats(partitionClassStats1);
    mySqlStore.storePartitionClassStats(partitionClassStats2);
    mySqlStore3.storePartitionClassStats(partitionClassStats3);

    Map<String, Set<Short>> partitionNameAndIds = mySqlStore.queryPartitionNameAndIds();
    assertEquals(new HashSet<>(partitionClassNames), partitionNameAndIds.keySet());
    Map<String, String> dbPartitionKeyToClassName = partitionNameAndIds.entrySet()
        .stream()
        .flatMap(
            ent -> ent.getValue().stream().map(pid -> new Pair<String, String>(ent.getKey(), "Partition[" + pid + "]")))
        .collect(Collectors.toMap(Pair::getSecond, Pair::getFirst));
    assertEquals(partitionKeyToClassName, dbPartitionKeyToClassName);

    StatsWrapper obtainedStats1 = mySqlStore.queryPartitionClassStatsOf(hostname1, partitionNameAndIds);
    assertEquals(partitionClassStats1.getSnapshot(), obtainedStats1.getSnapshot());
    StatsWrapper obtainedStats2 = mySqlStore.queryPartitionClassStatsOf(hostname2, partitionNameAndIds);
    assertEquals(partitionClassStats2.getSnapshot(), obtainedStats2.getSnapshot());
    StatsWrapper obtainedStats3 = mySqlStore3.queryPartitionClassStatsOf(hostname3, partitionNameAndIds);
    assertEquals(partitionClassStats3.getSnapshot(), obtainedStats3.getSnapshot());
  }

  @Test
  public void testAggregatedPartitionClassStats() throws Exception {
    testHostPartitionClassStats();
    Map<String, Set<Short>> partitionNameAndIds = mySqlStore.queryPartitionNameAndIds();
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, false);

    // Now we should have partition class names and partition ids in database
    // Construct an aggregated partition class report
    StatsSnapshot aggregated =
        generateAggregatedPartitionClassStats(partitionNameAndIds.keySet().toArray(new String[0]), 10, 10);
    mySqlStore.storeAggregatedPartitionClassStats(aggregated);

    partitionNameAndIds = mySqlStore3.queryPartitionNameAndIds();
    StatsSnapshot aggregated3 =
        generateAggregatedPartitionClassStats(partitionNameAndIds.keySet().toArray(new String[0]), 10, 10);
    mySqlStore3.storeAggregatedPartitionClassStats(aggregated3);

    StatsSnapshot obtained = mySqlStore.queryAggregatedPartitionClassStatsOf();
    assertEquals(aggregated, obtained);

    StatsSnapshot obtained3 = mySqlStore3.queryAggregatedPartitionClassStatsOf();
    assertEquals(aggregated3, obtained3);

    // Change one value and store it to mysql database again
    StatsSnapshot newSnapshot = new StatsSnapshot(aggregated);
    newSnapshot.getSubMap().get("default").getSubMap().get("A[1]___C[1]").setValue(1);
    newSnapshot.updateValue();
    mySqlStore.storeAggregatedPartitionClassStats(aggregated);
    obtained = mySqlStore.queryAggregatedPartitionClassStatsOf();
    assertEquals(aggregated, obtained);
  }

  private AccountStatsMySqlStore createAccountStatsMySqlStore(String clusterName, String hostname,
      boolean withLocalbackup) throws Exception {
    Path localBackupFilePath = withLocalbackup ? createLocalBackup(10, 10, 1) : createTemporaryFile();
    Properties configProps = Utils.loadPropsFromResource("accountstats_mysql.properties");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, clusterName);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, hostname);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_PORT, String.valueOf(port));
    configProps.setProperty(AccountStatsMySqlConfig.DOMAIN_NAMES_TO_REMOVE, ".github.com");
    configProps.setProperty(AccountStatsMySqlConfig.UPDATE_BATCH_SIZE, String.valueOf(batchSize));
    configProps.setProperty(StatsManagerConfig.STATS_OUTPUT_FILE_PATH, localBackupFilePath.toString());
    VerifiableProperties verifiableProperties = new VerifiableProperties(configProps);
    return new AccountStatsMySqlStoreFactory(verifiableProperties, new ClusterMapConfig(verifiableProperties),
        new StatsManagerConfig(verifiableProperties), new MetricRegistry()).getAccountStatsMySqlStore();
  }

  private static Path createLocalBackup(int numPartitions, int numAccounts, int numContainers) throws IOException {
    Path localBackupFilePath = createTemporaryFile();
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(localBackupFilePath.toFile(),
        generateStatsWrapper(numPartitions, numAccounts, numContainers, StatsReportType.ACCOUNT_REPORT));
    return localBackupFilePath;
  }

  private static Path createTemporaryFile() throws IOException {
    Path tempDir = Files.createTempDirectory("AccountStatsMySqlStoreTest");
    return tempDir.resolve("localbackup");
  }

  private static StatsWrapper generateStatsWrapper(int numPartitions, int numAccounts, int numContainers,
      StatsReportType reportType) {
    Random random = new Random();
    List<StatsSnapshot> storeSnapshots = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      storeSnapshots.add(TestUtils.generateStoreStats(numAccounts, numContainers, random, reportType));
    }
    return TestUtils.generateNodeStats(storeSnapshots, 1000, reportType);
  }

  private void cleanup(MySqlDataAccessor dataAccessor) throws SQLException {
    Connection dbConnection = dataAccessor.getDatabaseConnection(true);
    Statement statement = dbConnection.createStatement();
    for (String table : AccountStatsMySqlStore.TABLES) {
      statement.executeUpdate("DELETE FROM " + table);
    }
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

  private StatsWrapper convertAccountStatsToPartitionClassStats(StatsWrapper accountStats,
      Map<String, String> partitionKeyToClassName) {
    Map<String, StatsSnapshot> partitionClassSubMap = new HashMap<>();
    StatsSnapshot originHostStats = accountStats.getSnapshot();
    for (String partitionKey : originHostStats.getSubMap().keySet()) {
      StatsSnapshot originPartitionStats = originHostStats.getSubMap().get(partitionKey);
      String currentClassName = partitionKeyToClassName.get(partitionKey);
      StatsSnapshot partitionClassStats =
          partitionClassSubMap.computeIfAbsent(currentClassName, k -> new StatsSnapshot(0L, new HashMap<>()));
      Map<String, StatsSnapshot> accountContainerSubMap = new HashMap<>();
      for (String accountKey : originPartitionStats.getSubMap().keySet()) {
        for (Map.Entry<String, StatsSnapshot> containerEntry : originPartitionStats.getSubMap()
            .get(accountKey)
            .getSubMap()
            .entrySet()) {
          String containerKey = containerEntry.getKey();
          StatsSnapshot containerStats = new StatsSnapshot(containerEntry.getValue());
          accountContainerSubMap.put(accountKey + Utils.ACCOUNT_CONTAINER_SEPARATOR + containerKey, containerStats);
        }
      }
      long accountContainerValue = accountContainerSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
      StatsSnapshot partitionStats = new StatsSnapshot(accountContainerValue, accountContainerSubMap);
      partitionClassStats.getSubMap().put(partitionKey, partitionStats);
      partitionClassStats.setValue(partitionClassStats.getValue() + accountContainerValue);
    }
    return new StatsWrapper(new StatsHeader(accountStats.getHeader()),
        new StatsSnapshot(originHostStats.getValue(), partitionClassSubMap));
  }

  private StatsSnapshot generateAggregatedPartitionClassStats(String[] partitionClassNames, int numAccount,
      int numContainer) {
    Random random = new Random();
    long maxValue = 10000;
    StatsSnapshot finalStats = new StatsSnapshot(0L, new HashMap<>());
    for (String className : partitionClassNames) {
      StatsSnapshot classNameStats = new StatsSnapshot(0L, new HashMap<>());
      finalStats.getSubMap().put(className, classNameStats);
      for (int ia = 0; ia < numAccount; ia++) {
        for (int ic = 0; ic < numContainer; ic++) {
          String key = "A[" + ia + "]" + Utils.ACCOUNT_CONTAINER_SEPARATOR + "C[" + ic + "]";
          classNameStats.getSubMap().put(key, new StatsSnapshot(random.nextLong() % maxValue, null));
        }
      }
      long classNameValue = classNameStats.getSubMap().values().stream().mapToLong(StatsSnapshot::getValue).sum();
      classNameStats.setValue(classNameValue);
    }
    finalStats.setValue(finalStats.getSubMap().values().stream().mapToLong(StatsSnapshot::getValue).sum());
    return finalStats;
  }
}
