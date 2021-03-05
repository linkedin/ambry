/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.Account;
import com.github.ambry.accountstats.AccountStatsMySqlStore;
import com.github.ambry.accountstats.AccountStatsMySqlStoreFactory;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreStats;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Utils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Integration tests of {@link StatsManager} to publish account stats and partition class stats to mysql database.
 */
public class StatsManagerIntegrationTest {
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String HOSTNAME = "localhost";
  private static final int PORT = 12345;
  private static final int BATCH_SIZE = 0;

  private final Path tempDir;
  private final String accountStatsOutputFileString;
  private final AccountStatsMySqlStore accountStatsMySqlStore;
  private final StatsManager statsManager;
  private final List<ReplicaId> replicas = new ArrayList<>();
  private final Map<PartitionId, Store> storeMap = new HashMap<>();
  private StatsSnapshot aggregatedAccountStatsSnapshot = new StatsSnapshot(0L, new HashMap<>());
  private StatsSnapshot aggregatedPartitionClassStatsSnapshot = new StatsSnapshot(0L, new HashMap<>());

  public StatsManagerIntegrationTest() throws Exception {
    tempDir = Files.createTempDirectory("StatsManagerIntegrationTest");
    tempDir.toFile().deleteOnExit();
    accountStatsOutputFileString = tempDir.resolve("stats_output.json").toAbsolutePath().toString();
    Properties properties = initProperties();
    accountStatsMySqlStore = createAccountStatsMySqlStore(properties);
    DataNodeId dataNodeId = new MockDataNodeId(Collections.singletonList(new Port(6667, PortType.PLAINTEXT)),
        Collections.singletonList("/tmp"), "DC1");

    for (int i = 0; i < 6; i++) {
      String partitionClassName =
          i % 2 == 0 ? MockClusterMap.DEFAULT_PARTITION_CLASS : MockClusterMap.SPECIAL_PARTITION_CLASS;
      PartitionId partitionId =
          new MockPartitionId(i, partitionClassName, Collections.singletonList((MockDataNodeId) dataNodeId), 0);
      Map<StatsReportType, StatsSnapshot> snapshotsByType = StatsManagerTest.generateRandomSnapshot();
      StoreStats storeStats = new StatsManagerTest.MockStoreStats(snapshotsByType, false, null);
      storeMap.put(partitionId, new StatsManagerTest.MockStore(storeStats));
      replicas.add(partitionId.getReplicaIds().get(0));
      aggregatedAccountStatsSnapshot.getSubMap()
          .put(Utils.statsPartitionKey(i), snapshotsByType.get(StatsReportType.ACCOUNT_REPORT));
      aggregatedPartitionClassStatsSnapshot.getSubMap()
          .computeIfAbsent(partitionClassName, k -> new StatsSnapshot(0L, new HashMap<>()))
          .getSubMap()
          .put(Utils.statsPartitionKey(i), snapshotsByType.get(StatsReportType.PARTITION_CLASS_REPORT));
    }
    aggregatedAccountStatsSnapshot.updateValue();
    aggregatedPartitionClassStatsSnapshot.updateValue();
    StorageManager storageManager = new MockStorageManager(storeMap, dataNodeId);
    statsManager = new StatsManager(storageManager, replicas, new MetricRegistry(),
        new StatsManagerConfig(new VerifiableProperties(properties)), new MockTime(), null, null, null);
  }

  /**
   * Clean up the database by removing all the data in all tables.
   * @throws Exception
   */
  @Before
  public void before() throws Exception {
    Connection dbConnection = accountStatsMySqlStore.getMySqlDataAccessor().getDatabaseConnection(true);
    Statement statement = dbConnection.createStatement();
    for (String table : AccountStatsMySqlStore.TABLES) {
      statement.executeUpdate("DELETE FROM " + table);
    }
  }

  /**
   * Test account stats publisher
   * @throws Exception
   */
  @Test
  public void testAccountStatsPublisher() throws Exception {
    StatsManager.AccountStatsPublisher publisher = statsManager.new AccountStatsPublisher(accountStatsMySqlStore);
    publisher.run();

    StatsWrapper statsWrapper = accountStatsMySqlStore.queryAccountStatsByHost(HOSTNAME, PORT);
    assertEquals(aggregatedAccountStatsSnapshot, statsWrapper.getSnapshot());
  }

  /**
   * Test partition class stats publisher
   * @throws Exception
   */
  @Test
  public void testPartitionClassStatsPublisher() throws Exception {
    // Before publishing partition class stats, we have to publish account stats since they use the same data
    StatsManager.AccountStatsPublisher accountStatsPublisher =
        statsManager.new AccountStatsPublisher(accountStatsMySqlStore);
    accountStatsPublisher.run();

    StatsManager.PartitionClassStatsPublisher publisher =
        statsManager.new PartitionClassStatsPublisher(accountStatsMySqlStore);
    publisher.run();

    Map<String, Set<Integer>> partitionNameAndIds = accountStatsMySqlStore.queryPartitionNameAndIds();
    StatsWrapper statsWrapper =
        accountStatsMySqlStore.queryPartitionClassStatsByHost(HOSTNAME, PORT, partitionNameAndIds);
    assertEquals(aggregatedPartitionClassStatsSnapshot, statsWrapper.getSnapshot());
  }

  private Properties initProperties() throws Exception {
    Properties configProps = Utils.loadPropsFromResource("accountstats_mysql.properties");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, CLUSTER_NAME);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, HOSTNAME);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_PORT, String.valueOf(PORT));
    configProps.setProperty(AccountStatsMySqlConfig.DOMAIN_NAMES_TO_REMOVE, ".github.com");
    configProps.setProperty(AccountStatsMySqlConfig.UPDATE_BATCH_SIZE, String.valueOf(BATCH_SIZE));
    configProps.setProperty(StatsManagerConfig.STATS_OUTPUT_FILE_PATH, accountStatsOutputFileString);
    configProps.setProperty(StatsManagerConfig.STATS_ENABLE_MYSQL_REPORT, Boolean.TRUE.toString());
    return configProps;
  }

  private AccountStatsMySqlStore createAccountStatsMySqlStore(Properties configProps) throws Exception {
    VerifiableProperties verifiableProperties = new VerifiableProperties(configProps);
    return (AccountStatsMySqlStore) new AccountStatsMySqlStoreFactory(verifiableProperties,
        new ClusterMapConfig(verifiableProperties), new StatsManagerConfig(verifiableProperties),
        new MetricRegistry()).getAccountStatsStore();
  }
}
