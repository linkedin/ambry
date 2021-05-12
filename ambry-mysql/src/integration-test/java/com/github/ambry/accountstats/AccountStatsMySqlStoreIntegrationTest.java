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
  // hostname1 and hostname2 are the same, but with different port numbers
  private static final String hostname1 = "ambry1.test.github.com";
  private static final String hostname2 = "ambry1.test.github.com";
  private static final String hostname3 = "ambry3.test.github.com";
  private static final int port1 = 12345;
  private static final int port2 = 12346;
  private static final int port3 = 12347;
  private final int batchSize;
  private final AccountStatsMySqlStore mySqlStore;

  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{0}, {17}});
  }

  public AccountStatsMySqlStoreIntegrationTest(int batchSize) throws Exception {
    this.batchSize = batchSize;
    mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
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
    AccountStatsMySqlStore mySqlStore1 = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    AccountStatsMySqlStore mySqlStore2 = createAccountStatsMySqlStore(clusterName1, hostname2, port2);
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, port3);

    StatsWrapper stats1 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    StatsWrapper stats2 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    StatsWrapper stats3 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    mySqlStore1.storeAccountStats(stats1);
    mySqlStore2.storeAccountStats(stats2);
    mySqlStore3.storeAccountStats(stats3);

    assertTableSize(mySqlStore1.getMySqlDataAccessor(), 3 * 10 * 10);

    StatsWrapper obtainedStats1 = mySqlStore1.queryAccountStatsByHost(hostname1, port1);
    StatsWrapper obtainedStats2 = mySqlStore2.queryAccountStatsByHost(hostname2, port2);
    StatsWrapper obtainedStats3 = mySqlStore3.queryAccountStatsByHost(hostname3, port3);
    assertTwoStatsSnapshots(obtainedStats1.getSnapshot(), stats1.getSnapshot());
    assertTwoStatsSnapshots(obtainedStats2.getSnapshot(), stats2.getSnapshot());
    assertTwoStatsSnapshots(obtainedStats3.getSnapshot(), stats3.getSnapshot());
  }

  @Test
  public void testEmptyStats() throws Exception {
    AccountStatsMySqlStore mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    StatsWrapper stats = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    stats.getSnapshot().getSubMap().put(Utils.statsPartitionKey((short) 10), new StatsSnapshot(0L, null));
    mySqlStore.storeAccountStats(stats);

    StatsWrapper obtainedStats = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertFalse(obtainedStats.getSnapshot().getSubMap().containsKey(Utils.statsPartitionKey((short) 10)));

    // Write a new stats with partition 10 still empty
    StatsWrapper stats2 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    stats2.getSnapshot().getSubMap().put(Utils.statsPartitionKey((short) 10), new StatsSnapshot(0L, null));
    mySqlStore.storeAccountStats(stats2);

    StatsWrapper obtainedStats2 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertFalse(obtainedStats2.getSnapshot().getSubMap().containsKey(Utils.statsPartitionKey((short) 10)));

    // Write a new stats with partition 10 not empty
    StatsWrapper stats3 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    stats3.getSnapshot()
        .getSubMap()
        .put(Utils.statsPartitionKey((short) 10),
            stats.getSnapshot().getSubMap().get(Utils.statsPartitionKey((short) 1)));
    mySqlStore.storeAccountStats(stats3);

    StatsWrapper obtainedStats3 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertTrue(obtainedStats3.getSnapshot().getSubMap().containsKey(Utils.statsPartitionKey((short) 10)));

    // Write a new stats with empty submap
    StatsWrapper stats4 = generateStatsWrapper(1, 1, 1, StatsReportType.ACCOUNT_REPORT);
    stats4.getSnapshot().setSubMap(null);
    mySqlStore.storeAccountStats(stats4);

    // empty stats should remove all the data in the database
    StatsWrapper obtainedStats4 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertTrue(obtainedStats4.getSnapshot().getSubMap().isEmpty());

    // Write a new stats with empty submap again
    StatsWrapper stats5 = generateStatsWrapper(1, 1, 1, StatsReportType.ACCOUNT_REPORT);
    stats5.getSnapshot().setSubMap(null);
    mySqlStore.storeAccountStats(stats5);

    StatsWrapper obtainedStats5 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertTrue(obtainedStats5.getSnapshot().getSubMap().isEmpty());

    StatsWrapper stats6 = generateStatsWrapper(20, 20, 20, StatsReportType.ACCOUNT_REPORT);
    mySqlStore.storeAccountStats(stats6);
    StatsWrapper obtainedStats6 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertEquals(obtainedStats6.getSnapshot(), stats6.getSnapshot());
  }

  /**
   * Test to delete partition, account and container data from database
   * @throws Exception
   */
  @Test
  public void testStatsDeletePartitionAccountContainer() throws Exception {
    AccountStatsMySqlStore mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    StatsWrapper stats = generateStatsWrapper(10, 10, 10, StatsReportType.ACCOUNT_REPORT);
    mySqlStore.storeAccountStats(stats);

    // Now remove one partition from stats
    StatsWrapper stats2 = new StatsWrapper(new StatsHeader(stats.getHeader()), new StatsSnapshot(stats.getSnapshot()));
    stats2.getSnapshot().getSubMap().remove(Utils.statsPartitionKey((short) 1));
    stats2.getSnapshot().updateValue();
    mySqlStore.storeAccountStats(stats2);
    StatsWrapper obtainedStats2 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertEquals(obtainedStats2.getSnapshot(), stats2.getSnapshot());

    // Now remove one partition's submap
    StatsWrapper stats3 =
        new StatsWrapper(new StatsHeader(stats2.getHeader()), new StatsSnapshot(stats2.getSnapshot()));
    stats3.getSnapshot().getSubMap().get(Utils.statsPartitionKey((short) 2)).setSubMap(null);
    stats3.getSnapshot().getSubMap().get(Utils.statsPartitionKey((short) 2)).setValue(0L);
    stats3.getSnapshot().updateValue();
    mySqlStore.storeAccountStats(stats3);
    stats3.getSnapshot().getSubMap().remove(Utils.statsPartitionKey((short) 2));
    StatsWrapper obtainedStats3 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertEquals(obtainedStats3.getSnapshot(), stats3.getSnapshot());

    // Now remove one account from stats
    StatsWrapper stats4 =
        new StatsWrapper(new StatsHeader(stats3.getHeader()), new StatsSnapshot(stats3.getSnapshot()));
    stats4.getSnapshot()
        .getSubMap()
        .get(Utils.statsPartitionKey((short) 3))
        .getSubMap()
        .remove(Utils.statsAccountKey((short) 1));
    stats4.getSnapshot().updateValue();
    mySqlStore.storeAccountStats(stats4);
    StatsWrapper obtainedStats4 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertEquals(obtainedStats4.getSnapshot(), stats4.getSnapshot());

    // Now remove one account's submap
    StatsWrapper stats5 =
        new StatsWrapper(new StatsHeader(stats4.getHeader()), new StatsSnapshot(stats4.getSnapshot()));
    stats5.getSnapshot()
        .getSubMap()
        .get(Utils.statsPartitionKey((short) 3))
        .getSubMap()
        .get(Utils.statsAccountKey((short) 2))
        .setSubMap(null);
    stats5.getSnapshot()
        .getSubMap()
        .get(Utils.statsPartitionKey((short) 3))
        .getSubMap()
        .get(Utils.statsAccountKey((short) 2))
        .setValue(0L);
    stats5.getSnapshot().updateValue();
    mySqlStore.storeAccountStats(stats5);
    stats5.getSnapshot()
        .getSubMap()
        .get(Utils.statsPartitionKey((short) 3))
        .getSubMap()
        .remove(Utils.statsAccountKey((short) 2));
    StatsWrapper obtainedStats5 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertEquals(obtainedStats5.getSnapshot(), stats5.getSnapshot());

    // Now remove some containers
    StatsWrapper stats6 =
        new StatsWrapper(new StatsHeader(stats5.getHeader()), new StatsSnapshot(stats5.getSnapshot()));
    for (short containerId : new short[]{0, 1, 2}) {
      stats6.getSnapshot()
          .getSubMap()
          .get(Utils.statsPartitionKey((short) 3))
          .getSubMap()
          .get(Utils.statsAccountKey((short) 3))
          .getSubMap()
          .remove(Utils.statsContainerKey(containerId));
    }
    stats6.getSnapshot().updateValue();
    mySqlStore.storeAccountStats(stats6);
    StatsWrapper obtainedStats6 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertEquals(obtainedStats6.getSnapshot(), stats6.getSnapshot());

    // Now write the stats back
    mySqlStore.storeAccountStats(stats);
    StatsWrapper obtainedStats = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertEquals(obtainedStats.getSnapshot(), stats.getSnapshot());
  }

  /**
   * Tests to store multiple stats for one hosts and recover stats from database.
   * @throws Exception
   */
  @Test
  public void testStoreMulitpleWrites() throws Exception {
    AccountStatsMySqlStore mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    StatsWrapper stats1 = generateStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    mySqlStore.storeAccountStats(stats1);
    StatsWrapper stats2 =
        new StatsWrapper(new StatsHeader(stats1.getHeader()), new StatsSnapshot(stats1.getSnapshot()));
    // change one value, and store it to mysql database again
    stats2.getSnapshot()
        .getSubMap()
        .get(Utils.statsPartitionKey((short) 0))
        .getSubMap()
        .get(Utils.statsAccountKey((short) 0))
        .getSubMap()
        .get(Utils.statsContainerKey((short) 0))
        .setValue(1);
    stats2.getSnapshot().updateValue();
    mySqlStore.storeAccountStats(stats2);
    StatsWrapper obtainedStats = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    assertTwoStatsSnapshots(obtainedStats.getSnapshot(), stats2.getSnapshot());
  }

  @Test
  public void testAggregatedStats() throws Exception {
    Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    StatsSnapshot snapshot = TestUtils.makeAccountStatsSnapshotFromContainerStorageMap(containerStorageUsages);
    mySqlStore.storeAggregatedAccountStats(snapshot);
    Map<String, Map<String, Long>> obtainedContainerStorageUsages = mySqlStore.queryAggregatedAccountStats();
    assertEquals(containerStorageUsages, obtainedContainerStorageUsages);
    StatsSnapshot obtainedSnapshot = mySqlStore.queryAggregatedAccountStatsByClusterName(clusterName1);
    assertEquals(snapshot, obtainedSnapshot);

    // Fetching aggregated account stats for clustername2 should result in a null;
    assertNull(mySqlStore.queryAggregatedAccountStatsByClusterName(clusterName2));

    // Change one value and store it to mysql database again
    StatsSnapshot newSnapshot = new StatsSnapshot(snapshot);
    newSnapshot.getSubMap()
        .get(Utils.statsAccountKey((short) 1))
        .getSubMap()
        .get(Utils.statsContainerKey((short) 1))
        .setValue(1);
    newSnapshot.updateValue();
    containerStorageUsages.get("1").put("1", 1L);
    mySqlStore.storeAggregatedAccountStats(newSnapshot);
    obtainedContainerStorageUsages = mySqlStore.queryAggregatedAccountStats();
    assertEquals(containerStorageUsages, obtainedContainerStorageUsages);
  }

  @Test
  public void testMonthlyAggregatedStats() throws Exception {
    String monthValue = "2020-01";
    Map<String, Map<String, Long>> currentContainerStorageUsages = mySqlStore.queryAggregatedAccountStats();
    if (currentContainerStorageUsages.size() == 0) {
      Map<String, Map<String, Long>> containerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
      StatsSnapshot snapshot = TestUtils.makeAccountStatsSnapshotFromContainerStorageMap(containerStorageUsages);
      mySqlStore.storeAggregatedAccountStats(snapshot);
      currentContainerStorageUsages = mySqlStore.queryAggregatedAccountStats();
    }
    // fetch the month and it should return emtpy string
    Assert.assertEquals("", mySqlStore.queryRecordedMonth());
    mySqlStore.takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(monthValue);
    Map<String, Map<String, Long>> monthlyContainerStorageUsages = mySqlStore.queryMonthlyAggregatedAccountStats();
    assertEquals(currentContainerStorageUsages, monthlyContainerStorageUsages);
    String obtainedMonthValue = mySqlStore.queryRecordedMonth();
    assertTrue(obtainedMonthValue.equals(monthValue));

    // Change the value and store it back to mysql database
    monthValue = "2020-02";
    currentContainerStorageUsages = TestUtils.makeStorageMap(10, 10, 100000, 1000);
    StatsSnapshot snapshot = TestUtils.makeAccountStatsSnapshotFromContainerStorageMap(currentContainerStorageUsages);
    mySqlStore.storeAggregatedAccountStats(snapshot);
    mySqlStore.takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(monthValue);
    monthlyContainerStorageUsages = mySqlStore.queryMonthlyAggregatedAccountStats();
    assertEquals(currentContainerStorageUsages, monthlyContainerStorageUsages);
    obtainedMonthValue = mySqlStore.queryRecordedMonth();
    assertTrue(obtainedMonthValue.equals(monthValue));
  }

  @Test
  public void testHostPartitionClassStats() throws Exception {
    // First write some stats to account reports
    testMultiStoreStats();
    StatsWrapper accountStats1 = mySqlStore.queryAccountStatsByHost(hostname1, port1);
    StatsWrapper accountStats2 = mySqlStore.queryAccountStatsByHost(hostname2, port2);
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, port3);
    StatsWrapper accountStats3 = mySqlStore3.queryAccountStatsByHost(hostname3, port3);

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

    Map<String, Set<Integer>> partitionNameAndIds = mySqlStore.queryPartitionNameAndIds();
    assertEquals(new HashSet<>(partitionClassNames), partitionNameAndIds.keySet());
    Map<String, String> dbPartitionKeyToClassName = partitionNameAndIds.entrySet()
        .stream()
        .flatMap(
            ent -> ent.getValue().stream().map(pid -> new Pair<String, String>(ent.getKey(), "Partition[" + pid + "]")))
        .collect(Collectors.toMap(Pair::getSecond, Pair::getFirst));
    assertEquals(partitionKeyToClassName, dbPartitionKeyToClassName);

    StatsWrapper obtainedStats1 = mySqlStore.queryPartitionClassStatsByHost(hostname1, port1, partitionNameAndIds);
    assertEquals(partitionClassStats1.getSnapshot(), obtainedStats1.getSnapshot());
    StatsWrapper obtainedStats2 = mySqlStore.queryPartitionClassStatsByHost(hostname2, port2, partitionNameAndIds);
    assertEquals(partitionClassStats2.getSnapshot(), obtainedStats2.getSnapshot());
    StatsWrapper obtainedStats3 = mySqlStore3.queryPartitionClassStatsByHost(hostname3, port3, partitionNameAndIds);
    assertEquals(partitionClassStats3.getSnapshot(), obtainedStats3.getSnapshot());
  }

  @Test
  public void testAggregatedPartitionClassStats() throws Exception {
    testHostPartitionClassStats();
    Map<String, Set<Integer>> partitionNameAndIds = mySqlStore.queryPartitionNameAndIds();
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, port3);

    // Now we should have partition class names and partition ids in database
    // Construct an aggregated partition class report
    StatsSnapshot aggregated =
        TestUtils.makeAggregatedPartitionClassStats(partitionNameAndIds.keySet().toArray(new String[0]), 10, 10);
    mySqlStore.storeAggregatedPartitionClassStats(aggregated);

    partitionNameAndIds = mySqlStore3.queryPartitionNameAndIds();
    StatsSnapshot aggregated3 =
        TestUtils.makeAggregatedPartitionClassStats(partitionNameAndIds.keySet().toArray(new String[0]), 10, 10);
    mySqlStore3.storeAggregatedPartitionClassStats(aggregated3);

    StatsSnapshot obtained = mySqlStore.queryAggregatedPartitionClassStats();
    assertEquals(aggregated, obtained);
    assertNull(mySqlStore.queryAggregatedPartitionClassStatsByClusterName("random-cluster"));

    StatsSnapshot obtained3 = mySqlStore3.queryAggregatedPartitionClassStats();
    assertEquals(aggregated3, obtained3);

    // Change one value and store it to mysql database again
    StatsSnapshot newSnapshot = new StatsSnapshot(aggregated);
    newSnapshot.getSubMap()
        .get("default")
        .getSubMap()
        .get(Utils.partitionClassStatsAccountContainerKey((short) 1, (short) 1))
        .setValue(1);
    newSnapshot.updateValue();
    mySqlStore.storeAggregatedPartitionClassStats(aggregated);
    obtained = mySqlStore.queryAggregatedPartitionClassStats();
    assertEquals(aggregated, obtained);
  }

  private AccountStatsMySqlStore createAccountStatsMySqlStore(String clusterName, String hostname, int port)
      throws Exception {
    Path localBackupFilePath = createTemporaryFile();
    Properties configProps = Utils.loadPropsFromResource("accountstats_mysql.properties");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, clusterName);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, hostname);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_PORT, String.valueOf(port));
    configProps.setProperty(AccountStatsMySqlConfig.DOMAIN_NAMES_TO_REMOVE, ".github.com");
    configProps.setProperty(AccountStatsMySqlConfig.UPDATE_BATCH_SIZE, String.valueOf(batchSize));
    configProps.setProperty(StatsManagerConfig.STATS_OUTPUT_FILE_PATH, localBackupFilePath.toString());
    VerifiableProperties verifiableProperties = new VerifiableProperties(configProps);
    return (AccountStatsMySqlStore) new AccountStatsMySqlStoreFactory(verifiableProperties,
        new ClusterMapConfig(verifiableProperties), new StatsManagerConfig(verifiableProperties),
        new MetricRegistry()).getAccountStatsStore();
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
      assertEquals("Snapshot submap size mismatch", snapshot1.getSubMap().size(), snapshot2.getSubMap().size());
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
          String accountContainerKey =
              Utils.partitionClassStatsAccountContainerKey(Utils.accountIdFromStatsAccountKey(accountKey),
                  Utils.containerIdFromStatsContainerKey(containerKey));
          accountContainerSubMap.put(accountContainerKey, containerStats);
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
}
