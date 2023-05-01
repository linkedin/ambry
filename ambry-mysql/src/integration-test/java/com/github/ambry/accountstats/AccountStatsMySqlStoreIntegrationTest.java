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
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.HostAccountStorageStatsWrapper;
import com.github.ambry.server.HostPartitionClassStorageStatsWrapper;
import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.StorageStatsUtilTest;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.AggregatedPartitionClassStorageStats;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.server.storagestats.HostAccountStorageStats;
import com.github.ambry.server.storagestats.HostPartitionClassStorageStats;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.After;
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
    mySqlStore.cleanupTables();
  }

  @After
  public void after() {
    mySqlStore.shutdown();
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

    // Generating HostAccountStorageStatsWrappers, store and retrieve them
    HostAccountStorageStatsWrapper hostStats1 =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    HostAccountStorageStatsWrapper hostStats2 =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    HostAccountStorageStatsWrapper hostStats3 =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    mySqlStore1.storeHostAccountStorageStats(hostStats1);
    mySqlStore2.storeHostAccountStorageStats(hostStats2);
    mySqlStore3.storeHostAccountStorageStats(hostStats3);

    HostAccountStorageStatsWrapper obtainedHostStats1 =
        mySqlStore1.queryHostAccountStorageStatsByHost(hostname1, port1);
    HostAccountStorageStatsWrapper obtainedHostStats2 =
        mySqlStore2.queryHostAccountStorageStatsByHost(hostname2, port2);
    HostAccountStorageStatsWrapper obtainedHostStats3 =
        mySqlStore3.queryHostAccountStorageStatsByHost(hostname3, port3);
    assertEquals(hostStats1.getStats().getStorageStats(), obtainedHostStats1.getStats().getStorageStats());
    assertEquals(hostStats2.getStats().getStorageStats(), obtainedHostStats2.getStats().getStorageStats());
    assertEquals(hostStats3.getStats().getStorageStats(), obtainedHostStats3.getStats().getStorageStats());

    mySqlStore1.shutdown();
    mySqlStore2.shutdown();
    mySqlStore3.shutdown();
  }

  @Test
  public void testEmptyStatsWhenReadingPreviousStatsFromMysqlDb() throws Exception {
    //write a new stats into database.
    AccountStatsMySqlStore mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    HostAccountStorageStatsWrapper stats =
        generateHostAccountStorageStatsWrapper(1, 1, 1, StatsReportType.ACCOUNT_REPORT);
    mySqlStore.storeHostAccountStorageStats(stats);

    HostAccountStorageStatsWrapper obtainedStats = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertTrue(obtainedStats.getStats().getStorageStats().containsKey((long) 0));

    //initialized the mySqlStore and write a new stats with the same partition.
    mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    assertTrue(
        mySqlStore.getPreviousHostAccountStorageStatsWrapper().getStats().getStorageStats().containsKey((long) 0));

    HostAccountStorageStatsWrapper stats2 =
        generateHostAccountStorageStatsWrapper(0, 0, 0, StatsReportType.ACCOUNT_REPORT);
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> newStorageStats =
        new HashMap<>(stats2.getStats().getStorageStats());
    newStorageStats.put((long) 0,
        new HashMap<>()); // Remove partition 0's storage stats data, this would remove entire partition from database
    mySqlStore.storeHostAccountStorageStats(
        new HostAccountStorageStatsWrapper(stats2.getHeader(), new HostAccountStorageStats(newStorageStats)));

    // empty stats should remove all the data in the database
    obtainedStats = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertFalse(obtainedStats.getStats().getStorageStats().containsKey((long) 0));
  }

  @Test
  public void testEmptyStatsWhenReadingPreviousStatsFromLocalBackUpFile() throws Exception {
    AccountStatsMySqlStore mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    HostAccountStorageStatsWrapper stats =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> newStorageStats =
        new HashMap<>(stats.getStats().getStorageStats());
    newStorageStats.put((long) 10, new HashMap<>());
    mySqlStore.storeHostAccountStorageStats(
        new HostAccountStorageStatsWrapper(stats.getHeader(), new HostAccountStorageStats(newStorageStats)));

    HostAccountStorageStatsWrapper obtainedStats = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertFalse(obtainedStats.getStats().getStorageStats().containsKey((long) 10));

    // Write a new stats with partition 10 still empty
    HostAccountStorageStatsWrapper stats2 =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    newStorageStats = new HashMap<>(stats.getStats().getStorageStats());
    newStorageStats.put((long) 10, new HashMap<>());
    mySqlStore.storeHostAccountStorageStats(
        new HostAccountStorageStatsWrapper(stats2.getHeader(), new HostAccountStorageStats(newStorageStats)));

    HostAccountStorageStatsWrapper obtainedStats2 = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertFalse(obtainedStats2.getStats().getStorageStats().containsKey((long) 10));

    // Write a new stats with partition 10 not empty
    HostAccountStorageStatsWrapper stats3 =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    newStorageStats = new HashMap<>(stats.getStats().getStorageStats());
    newStorageStats.put((long) 10, stats.getStats().getStorageStats().get((long) 1));
    mySqlStore.storeHostAccountStorageStats(
        new HostAccountStorageStatsWrapper(stats3.getHeader(), new HostAccountStorageStats(newStorageStats)));

    HostAccountStorageStatsWrapper obtainedStats3 = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertTrue(obtainedStats3.getStats().getStorageStats().containsKey((long) 10));

    // Write an empty HostAccountStorageStats
    mySqlStore.storeHostAccountStorageStats(
        new HostAccountStorageStatsWrapper(stats3.getHeader(), new HostAccountStorageStats()));

    // Empty storage stats should remove all the data in the database
    HostAccountStorageStatsWrapper obtainedStats4 = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertTrue(obtainedStats4.getStats().getStorageStats().isEmpty());

    // Write an empty HostAccountStorageStats again
    mySqlStore.storeHostAccountStorageStats(
        new HostAccountStorageStatsWrapper(stats3.getHeader(), new HostAccountStorageStats()));

    HostAccountStorageStatsWrapper obtainedStats5 = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertTrue(obtainedStats5.getStats().getStorageStats().isEmpty());

    HostAccountStorageStatsWrapper stats6 =
        generateHostAccountStorageStatsWrapper(20, 20, 20, StatsReportType.ACCOUNT_REPORT);
    mySqlStore.storeHostAccountStorageStats(stats6);
    HostAccountStorageStatsWrapper obtainedStats6 = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(obtainedStats6.getStats().getStorageStats(), stats6.getStats().getStorageStats());
    mySqlStore.shutdown();
  }

  /**
   * Test to delete partition, account and container data from database
   * @throws Exception
   */
  @Test
  public void testStatsDeletePartitionAccountContainer() throws Exception {
    AccountStatsMySqlStore mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    HostAccountStorageStatsWrapper stats =
        generateHostAccountStorageStatsWrapper(10, 10, 10, StatsReportType.ACCOUNT_REPORT);
    mySqlStore.storeHostAccountStorageStats(stats);

    // Now remove one partition from stats
    HostAccountStorageStats storageStatsCopy = new HostAccountStorageStats(stats.getStats());
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> newStorageStatsMap =
        new HashMap<>(storageStatsCopy.getStorageStats());
    newStorageStatsMap.remove((long) 1);
    HostAccountStorageStatsWrapper stats2 = new HostAccountStorageStatsWrapper(new StatsHeader(stats.getHeader()),
        new HostAccountStorageStats(newStorageStatsMap));
    mySqlStore.storeHostAccountStorageStats(stats2);
    HostAccountStorageStatsWrapper obtainedStats2 = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(obtainedStats2.getStats().getStorageStats(), stats2.getStats().getStorageStats());

    // Now remove one account from stats
    storageStatsCopy = new HostAccountStorageStats(stats2.getStats());
    newStorageStatsMap = new HashMap<>(storageStatsCopy.getStorageStats());
    newStorageStatsMap.get((long) 3).remove((short) 1);
    HostAccountStorageStatsWrapper stats3 = new HostAccountStorageStatsWrapper(new StatsHeader(stats2.getHeader()),
        new HostAccountStorageStats(newStorageStatsMap));
    mySqlStore.storeHostAccountStorageStats(stats3);
    HostAccountStorageStatsWrapper obtainedStats3 = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(obtainedStats3.getStats().getStorageStats(), stats3.getStats().getStorageStats());

    // Now remove some containers
    storageStatsCopy = new HostAccountStorageStats(stats3.getStats());
    newStorageStatsMap = new HashMap<>(storageStatsCopy.getStorageStats());
    for (short containerId : new short[]{0, 1, 2}) {
      newStorageStatsMap.get((long) 3).get((short) 3).remove(containerId);
    }
    HostAccountStorageStatsWrapper stats4 = new HostAccountStorageStatsWrapper(new StatsHeader(stats3.getHeader()),
        new HostAccountStorageStats(newStorageStatsMap));
    mySqlStore.storeHostAccountStorageStats(stats4);
    HostAccountStorageStatsWrapper obtainedStats4 = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(obtainedStats4.getStats().getStorageStats(), stats4.getStats().getStorageStats());

    // Now write the stats back
    stats = generateHostAccountStorageStatsWrapper(10, 10, 10, StatsReportType.ACCOUNT_REPORT);
    mySqlStore.storeHostAccountStorageStats(stats);
    HostAccountStorageStatsWrapper obtainedStats = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(stats.getStats().getStorageStats(), obtainedStats.getStats().getStorageStats());
    mySqlStore.shutdown();
  }

  /**
   * Tests to store multiple stats for one hosts and recover stats from database.
   * @throws Exception
   */
  @Test
  public void testStoreMultilpleWrites() throws Exception {
    AccountStatsMySqlStore mySqlStore = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    HostAccountStorageStatsWrapper stats1 =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    mySqlStore.storeHostAccountStorageStats(stats1);

    HostAccountStorageStats hostAccountStorageStatsCopy = new HostAccountStorageStats(stats1.getStats());
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> newStorageStats =
        new HashMap<>(hostAccountStorageStatsCopy.getStorageStats());
    ContainerStorageStats origin = newStorageStats.get((long) 0).get((short) 0).get((short) 0);
    newStorageStats.get((long) 0)
        .get((short) 0)
        .put((short) 0,
            new ContainerStorageStats.Builder(origin).logicalStorageUsage(origin.getLogicalStorageUsage() + 1).build());

    HostAccountStorageStatsWrapper stats2 = new HostAccountStorageStatsWrapper(new StatsHeader(stats1.getHeader()),
        new HostAccountStorageStats(newStorageStats));
    mySqlStore.storeHostAccountStorageStats(stats2);
    HostAccountStorageStatsWrapper obtainedStats = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(stats2.getStats().getStorageStats(), obtainedStats.getStats().getStorageStats());

    hostAccountStorageStatsCopy = new HostAccountStorageStats(stats1.getStats());
    newStorageStats = new HashMap<>(hostAccountStorageStatsCopy.getStorageStats());
    origin = newStorageStats.get((long) 0).get((short) 0).get((short) 0);
    newStorageStats.get((long) 0)
        .get((short) 0)
        .put((short) 0,
            new ContainerStorageStats.Builder(origin).physicalStorageUsage(origin.getPhysicalStorageUsage() + 1)
                .build());

    HostAccountStorageStatsWrapper stats3 = new HostAccountStorageStatsWrapper(new StatsHeader(stats1.getHeader()),
        new HostAccountStorageStats(newStorageStats));
    mySqlStore.storeHostAccountStorageStats(stats3);
    obtainedStats = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(stats3.getStats().getStorageStats(), obtainedStats.getStats().getStorageStats());

    hostAccountStorageStatsCopy = new HostAccountStorageStats(stats1.getStats());
    newStorageStats = new HashMap<>(hostAccountStorageStatsCopy.getStorageStats());
    origin = newStorageStats.get((long) 0).get((short) 0).get((short) 0);
    newStorageStats.get((long) 0)
        .get((short) 0)
        .put((short) 0, new ContainerStorageStats.Builder(origin).numberOfBlobs(origin.getNumberOfBlobs() + 1).build());

    HostAccountStorageStatsWrapper stats4 = new HostAccountStorageStatsWrapper(new StatsHeader(stats1.getHeader()),
        new HostAccountStorageStats(newStorageStats));
    mySqlStore.storeHostAccountStorageStats(stats4);
    obtainedStats = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(stats4.getStats().getStorageStats(), obtainedStats.getStats().getStorageStats());

    mySqlStore.shutdown();
  }

  /**
   * Test method {@link AccountStatsStore#deleteHostAccountStorageStatsForHost}
   * @throws Exception
   */
  @Test
  public void testDeleteHostAccountStorageStatsForHost() throws Exception {
    AccountStatsMySqlStore mySqlStore1 = createAccountStatsMySqlStore(clusterName1, hostname1, port1);
    AccountStatsMySqlStore mySqlStore2 = createAccountStatsMySqlStore(clusterName1, hostname2, port2);
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, port3);

    // Generating HostAccountStorageStatsWrappers, store and retrieve them
    HostAccountStorageStatsWrapper hostStats1 =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    HostAccountStorageStatsWrapper hostStats2 =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    HostAccountStorageStatsWrapper hostStats3 =
        generateHostAccountStorageStatsWrapper(10, 10, 1, StatsReportType.ACCOUNT_REPORT);
    mySqlStore1.storeHostAccountStorageStats(hostStats1);
    mySqlStore2.storeHostAccountStorageStats(hostStats2);
    mySqlStore3.storeHostAccountStorageStats(hostStats3);

    // Delete a non-existing host
    mySqlStore1.deleteHostAccountStorageStatsForHost(hostname1, port3);
    HostAccountStorageStatsWrapper obtainedHostStats1 =
        mySqlStore1.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(hostStats1.getStats().getStorageStats(), obtainedHostStats1.getStats().getStorageStats());

    // Delete an existing host
    mySqlStore1.deleteHostAccountStorageStatsForHost(hostname1, port1);
    obtainedHostStats1 = mySqlStore1.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(0, obtainedHostStats1.getStats().getStorageStats().size());

    // Delete this host again
    mySqlStore1.deleteHostAccountStorageStatsForHost(hostname1, port1);
    obtainedHostStats1 = mySqlStore1.queryHostAccountStorageStatsByHost(hostname1, port1);
    assertEquals(0, obtainedHostStats1.getStats().getStorageStats().size());

    // Delete another host
    mySqlStore3.deleteHostAccountStorageStatsForHost(hostname3, port3);
    HostAccountStorageStatsWrapper obtainedHostStats3 =
        mySqlStore3.queryHostAccountStorageStatsByHost(hostname3, port3);
    assertEquals(0, obtainedHostStats3.getStats().getStorageStats().size());

    mySqlStore1.shutdown();
    mySqlStore2.shutdown();
    mySqlStore3.shutdown();
  }

  /**
   * Test the methods for storing, deleting and fetch aggregated account storage stats.
   * @throws Exception
   */
  @Test
  public void testAggregatedAccountStorageStats() throws Exception {
    AggregatedAccountStorageStats aggregatedAccountStorageStats = new AggregatedAccountStorageStats(
        StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, 10, 10, 10000L, 2, 10));
    mySqlStore.storeAggregatedAccountStorageStats(aggregatedAccountStorageStats);

    // Compare AggregatedAccountStorageStats
    AggregatedAccountStorageStats obtainedStats = mySqlStore.queryAggregatedAccountStorageStats();
    assertEquals(aggregatedAccountStorageStats.getStorageStats(), obtainedStats.getStorageStats());

    obtainedStats = mySqlStore.queryAggregatedAccountStorageStatsByClusterName(clusterName1);
    assertEquals(aggregatedAccountStorageStats.getStorageStats(), obtainedStats.getStorageStats());

    // Fetching aggregated account stats for clustername2 should result in a null;
    assertEquals(mySqlStore.queryAggregatedAccountStorageStatsByClusterName(clusterName2).getStorageStats().size(), 0);

    // Change one value and store it to mysql database again
    Map<Short, Map<Short, ContainerStorageStats>> newStorageStatsMap =
        new HashMap<>(aggregatedAccountStorageStats.getStorageStats());
    ContainerStorageStats origin = newStorageStatsMap.get((short) 1).get((short) 1);
    newStorageStatsMap.get((short) 1)
        .put((short) 1,
            new ContainerStorageStats.Builder(origin).logicalStorageUsage(origin.getLogicalStorageUsage() + 1).build());
    aggregatedAccountStorageStats = new AggregatedAccountStorageStats(newStorageStatsMap);
    mySqlStore.storeAggregatedAccountStorageStats(aggregatedAccountStorageStats);
    obtainedStats = mySqlStore.queryAggregatedAccountStorageStats();
    assertEquals(newStorageStatsMap, obtainedStats.getStorageStats());

    // Delete account and container
    newStorageStatsMap = new HashMap<>(aggregatedAccountStorageStats.getStorageStats());
    newStorageStatsMap.remove((short) 1);
    newStorageStatsMap.get((short) 2).remove((short) 1);
    // Now remove all containers for account 1 and container 1 of account 2
    for (short containerId : aggregatedAccountStorageStats.getStorageStats().get((short) 1).keySet()) {
      mySqlStore.deleteAggregatedAccountStatsForContainer((short) 1, containerId);
    }
    mySqlStore.deleteAggregatedAccountStatsForContainer((short) 2, (short) 1);
    obtainedStats = mySqlStore.queryAggregatedAccountStorageStatsByClusterName(clusterName1);
    assertEquals(newStorageStatsMap, obtainedStats.getStorageStats());
    mySqlStore.shutdown();
  }

  /**
   * Test methods to store, delete and fetch monthly aggregated stats
   * @throws Exception
   */
  @Test
  public void testMonthlyAggregatedStats() throws Exception {
    String monthValue = "2020-01";
    AggregatedAccountStorageStats currentAggregatedStats = mySqlStore.queryAggregatedAccountStorageStats();
    if (currentAggregatedStats.getStorageStats().size() == 0) {
      AggregatedAccountStorageStats aggregatedAccountStorageStats = new AggregatedAccountStorageStats(
          StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, 10, 10, 10000L, 2, 10));
      mySqlStore.storeAggregatedAccountStorageStats(aggregatedAccountStorageStats);
      currentAggregatedStats = mySqlStore.queryAggregatedAccountStorageStats();
    }

    // fetch the month and it should return empty string
    Assert.assertEquals("", mySqlStore.queryRecordedMonth());
    mySqlStore.takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(monthValue);
    AggregatedAccountStorageStats monthlyAggregatedAccountStorageStats =
        mySqlStore.queryMonthlyAggregatedAccountStorageStats();
    assertEquals(currentAggregatedStats.getStorageStats(), monthlyAggregatedAccountStorageStats.getStorageStats());
    String obtainedMonthValue = mySqlStore.queryRecordedMonth();
    assertTrue(obtainedMonthValue.equals(monthValue));

    // Change the value and store it back to mysql database
    monthValue = "2020-02";
    currentAggregatedStats = new AggregatedAccountStorageStats(
        StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, 10, 10, 10000L, 2, 10));
    mySqlStore.storeAggregatedAccountStorageStats(currentAggregatedStats);
    mySqlStore.takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(monthValue);
    monthlyAggregatedAccountStorageStats = mySqlStore.queryMonthlyAggregatedAccountStorageStats();
    assertEquals(currentAggregatedStats.getStorageStats(), monthlyAggregatedAccountStorageStats.getStorageStats());
    obtainedMonthValue = mySqlStore.queryRecordedMonth();
    assertTrue(obtainedMonthValue.equals(monthValue));

    // Delete the snapshots
    mySqlStore.deleteSnapshotOfAggregatedAccountStats();
    assertEquals(0, mySqlStore.queryMonthlyAggregatedAccountStorageStats().getStorageStats().size());
  }

  /**
   * Test methods to store and fetch partition class, partition name partition id and partition class storage stats.
   * @throws Exception
   */
  @Test
  public void testHostPartitionClassStorageStats() throws Exception {
    // First write some stats to account reports
    testMultiStoreStats();
    HostAccountStorageStatsWrapper accountStats1 = mySqlStore.queryHostAccountStorageStatsByHost(hostname1, port1);
    HostAccountStorageStatsWrapper accountStats2 = mySqlStore.queryHostAccountStorageStatsByHost(hostname2, port2);
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, port3);
    HostAccountStorageStatsWrapper accountStats3 = mySqlStore3.queryHostAccountStorageStatsByHost(hostname3, port3);
// From this account stats, create partition class stats;
    Set<Long> allPartitionKeys = new HashSet<Long>() {
      {
        addAll(accountStats1.getStats().getStorageStats().keySet());
        addAll(accountStats2.getStats().getStorageStats().keySet());
        addAll(accountStats3.getStats().getStorageStats().keySet());
      }
    };
    List<String> partitionClassNames = Arrays.asList("default", "new");
    Map<Long, String> partitionIdToClassName = new HashMap<>();
    int ind = 0;
    for (long partitionId : allPartitionKeys) {
      partitionIdToClassName.put(partitionId, partitionClassNames.get(ind % partitionClassNames.size()));
      ind++;
    }
    HostPartitionClassStorageStatsWrapper partitionClassStats1 =
        convertHostAccountStorageStatsToHostPartitionClassStorageStats(accountStats1, partitionIdToClassName);
    HostPartitionClassStorageStatsWrapper partitionClassStats2 =
        convertHostAccountStorageStatsToHostPartitionClassStorageStats(accountStats2, partitionIdToClassName);
    HostPartitionClassStorageStatsWrapper partitionClassStats3 =
        convertHostAccountStorageStatsToHostPartitionClassStorageStats(accountStats3, partitionIdToClassName);
    mySqlStore.storeHostPartitionClassStorageStats(partitionClassStats1);
    mySqlStore.storeHostPartitionClassStorageStats(partitionClassStats2);
    mySqlStore3.storeHostPartitionClassStorageStats(partitionClassStats3);

    Map<String, Set<Integer>> partitionNameAndIds = mySqlStore.queryPartitionNameAndIds();
    assertEquals(new HashSet<>(partitionClassNames), partitionNameAndIds.keySet());
    Map<Long, String> dbPartitionKeyToClassName = partitionNameAndIds.entrySet()
        .stream()
        .flatMap(ent -> ent.getValue().stream().map(pid -> new Pair<>(ent.getKey(), (long) pid)))
        .collect(Collectors.toMap(Pair::getSecond, Pair::getFirst));
    assertEquals(partitionIdToClassName, dbPartitionKeyToClassName);

    // Fetch HostPartitionClassStorageStats
    HostPartitionClassStorageStatsWrapper obtainedStats1 =
        mySqlStore.queryHostPartitionClassStorageStatsByHost(hostname1, port1, partitionNameAndIds);
    assertEquals(partitionClassStats1.getStats().getStorageStats(), obtainedStats1.getStats().getStorageStats());
    HostPartitionClassStorageStatsWrapper obtainedStats2 =
        mySqlStore.queryHostPartitionClassStorageStatsByHost(hostname2, port2, partitionNameAndIds);
    assertEquals(partitionClassStats2.getStats().getStorageStats(), obtainedStats2.getStats().getStorageStats());
    HostPartitionClassStorageStatsWrapper obtainedStats3 =
        mySqlStore3.queryHostPartitionClassStorageStatsByHost(hostname3, port3, partitionNameAndIds);
    assertEquals(partitionClassStats3.getStats().getStorageStats(), obtainedStats3.getStats().getStorageStats());

    mySqlStore3.shutdown();
  }

  @Test
  public void testAggregatedPartitionClassStorageStats() throws Exception {
    testHostPartitionClassStorageStats();
    Map<String, Set<Integer>> partitionNameAndIds = mySqlStore.queryPartitionNameAndIds();
    AccountStatsMySqlStore mySqlStore3 = createAccountStatsMySqlStore(clusterName2, hostname3, port3);

    // Now we should have partition class names and partition ids in database
    // Construct an aggregated partition class report
    AggregatedPartitionClassStorageStats aggregatedStats = new AggregatedPartitionClassStorageStats(
        StorageStatsUtilTest.generateRandomAggregatedPartitionClassStorageStats(
            partitionNameAndIds.keySet().toArray(new String[0]), (short) 0, 10, 10, 10000L, 2, 10));
    mySqlStore.storeAggregatedPartitionClassStorageStats(aggregatedStats);

    partitionNameAndIds = mySqlStore3.queryPartitionNameAndIds();
    AggregatedPartitionClassStorageStats aggregatedStats3 = new AggregatedPartitionClassStorageStats(
        StorageStatsUtilTest.generateRandomAggregatedPartitionClassStorageStats(
            partitionNameAndIds.keySet().toArray(new String[0]), (short) 0, 10, 10, 10000L, 2, 10));
    mySqlStore3.storeAggregatedPartitionClassStorageStats(aggregatedStats3);

    AggregatedPartitionClassStorageStats obtained = mySqlStore.queryAggregatedPartitionClassStorageStats();
    assertEquals(aggregatedStats.getStorageStats(), obtained.getStorageStats());
    assertEquals(
        mySqlStore.queryAggregatedPartitionClassStorageStatsByClusterName("random-cluster").getStorageStats().size(),
        0);

    AggregatedPartitionClassStorageStats obtained3 = mySqlStore3.queryAggregatedPartitionClassStorageStats();
    assertEquals(aggregatedStats3.getStorageStats(), obtained3.getStorageStats());

    // Change one value and store it to mysql database again
    Map<String, Map<Short, Map<Short, ContainerStorageStats>>> newStorageStatsMap =
        new HashMap<>(aggregatedStats.getStorageStats());
    ContainerStorageStats origin = newStorageStatsMap.get("default").get((short) 1).get((short) 1);
    newStorageStatsMap.get("default")
        .get((short) 1)
        .put((short) 1,
            new ContainerStorageStats.Builder(origin).logicalStorageUsage(origin.getLogicalStorageUsage() + 1).build());
    mySqlStore.storeAggregatedPartitionClassStorageStats(new AggregatedPartitionClassStorageStats(newStorageStatsMap));
    obtained = mySqlStore.queryAggregatedPartitionClassStorageStats();
    assertEquals(newStorageStatsMap, obtained.getStorageStats());

    // Delete some account and container
    short accountId = (short) 1;
    short containerId = (short) 1;
    for (String partitionClassName : partitionNameAndIds.keySet()) {
      mySqlStore.deleteAggregatedPartitionClassStatsForAccountContainer(partitionClassName, accountId, containerId);
      newStorageStatsMap.get(partitionClassName).get(accountId).remove(containerId);
    }
    obtained = mySqlStore.queryAggregatedPartitionClassStorageStats();
    assertEquals(newStorageStatsMap, obtained.getStorageStats());
    mySqlStore3.shutdown();
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
    configProps.setProperty(AccountStatsMySqlConfig.POOL_SIZE, String.valueOf(5));
    configProps.setProperty(AccountStatsMySqlConfig.LOCAL_BACKUP_FILE_PATH, localBackupFilePath.toString());
    VerifiableProperties verifiableProperties = new VerifiableProperties(configProps);
    return (AccountStatsMySqlStore) new AccountStatsMySqlStoreFactory(verifiableProperties,
        new ClusterMapConfig(verifiableProperties), new MetricRegistry()).getAccountStatsStore();
  }

  private static Path createTemporaryFile() throws IOException {
    Path tempDir = Files.createTempDirectory("AccountStatsMySqlStoreTest");
    return tempDir.resolve("localbackup");
  }

  private static HostAccountStorageStatsWrapper generateHostAccountStorageStatsWrapper(int numPartitions,
      int numAccounts, int numContainersPerAccount, StatsReportType reportType) {
    HostAccountStorageStats hostAccountStorageStats = new HostAccountStorageStats(
        StorageStatsUtilTest.generateRandomHostAccountStorageStats(numPartitions, numAccounts, numContainersPerAccount,
            100000L, 2, 10));
    StatsHeader statsHeader =
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, 1000, numPartitions, numPartitions,
            Collections.emptyList());
    return new HostAccountStorageStatsWrapper(statsHeader, hostAccountStorageStats);
  }

  private HostPartitionClassStorageStatsWrapper convertHostAccountStorageStatsToHostPartitionClassStorageStats(
      HostAccountStorageStatsWrapper accountStatsWrapper, Map<Long, String> partitionIdToClassName) {
    HostPartitionClassStorageStats hostPartitionClassStorageStats = new HostPartitionClassStorageStats();
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> storageStats =
        accountStatsWrapper.getStats().getStorageStats();
    for (long partitionId : storageStats.keySet()) {
      Map<Short, Map<Short, ContainerStorageStats>> accountStorageStatsMap = storageStats.get(partitionId);
      String partitionClassName = partitionIdToClassName.get(partitionId);
      for (short accountId : accountStorageStatsMap.keySet()) {
        accountStorageStatsMap.get(accountId)
            .values()
            .forEach(containerStats -> hostPartitionClassStorageStats.addContainerStorageStats(partitionClassName,
                partitionId, accountId, containerStats));
      }
    }
    return new HostPartitionClassStorageStatsWrapper(new StatsHeader(accountStatsWrapper.getHeader()),
        hostPartitionClassStorageStats);
  }
}
