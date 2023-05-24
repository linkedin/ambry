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
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.accountstats.AccountStatsMySqlStore;
import com.github.ambry.accountstats.AccountStatsMySqlStoreFactory;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockHelixParticipant;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.server.storagestats.HostAccountStorageStats;
import com.github.ambry.server.storagestats.HostPartitionClassStorageStats;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreStats;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Utils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.clustermap.TestUtils.*;
import static org.junit.Assert.*;


/**
 * Integration tests of {@link StatsManager} to publish account stats and partition class stats to mysql database.
 */
public class StatsManagerIntegrationTest {
  private static final String CLUSTER_NAME = "ambry-test";
  private static final String DC_NAME = "DC1";
  private static final int BATCH_SIZE = 0;

  private final Path tempDir;
  private final String accountStatsOutputFileString;
  private final AccountStatsMySqlStore accountStatsMySqlStore;
  private final HostAccountStorageStats hostAccountStorageStats;
  private final HostPartitionClassStorageStats hostPartitionClassStorageStats;
  private final MockClusterMap mockClusterMap;
  private final MockHelixParticipant mockClusterParticipant;
  private final StatsManager statsManager;
  private final List<ReplicaId> replicas = new ArrayList<>();
  private final Map<PartitionId, Store> storeMap = new HashMap<>();
  private final MockDataNodeId currentNode;

  public StatsManagerIntegrationTest() throws Exception {
    tempDir = Files.createTempDirectory("StatsManagerIntegrationTest");
    tempDir.toFile().deleteOnExit();
    accountStatsOutputFileString = tempDir.resolve("stats_output.json").toAbsolutePath().toString();
    mockClusterMap = new MockClusterMap();
    currentNode =
        mockClusterMap.getDataNodes().stream().filter(dn -> dn.getDatacenterName().equals(DC_NAME)).findFirst().get();
    Properties properties =
        initProperties(currentNode.getHostname(), currentNode.getPort(), currentNode.getDatacenterName());
    accountStatsMySqlStore = createAccountStatsMySqlStore(properties);

    hostAccountStorageStats = new HostAccountStorageStats(
        StorageStatsUtilTest.generateRandomHostAccountStorageStats(6, 10, 6, 10000L, 2, 10));
    Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> hostPartitionClassStorageStatsMap =
        new HashMap<>();
    for (int i = 0; i < 6; i++) {
      String partitionClassName =
          i % 2 == 0 ? MockClusterMap.DEFAULT_PARTITION_CLASS : MockClusterMap.SPECIAL_PARTITION_CLASS;
      PartitionId partitionId =
          new MockPartitionId(i, partitionClassName, Collections.singletonList((MockDataNodeId) currentNode), 0);
      StoreStats storeStats =
          new StatsManagerTest.MockStoreStats(hostAccountStorageStats.getStorageStats().get((long) i), false, null);
      storeMap.put(partitionId, new StatsManagerTest.MockStore(storeStats));
      replicas.add(partitionId.getReplicaIds().get(0));
      hostPartitionClassStorageStatsMap.computeIfAbsent(partitionClassName, k -> new HashMap<>())
          .put((long) i, hostAccountStorageStats.getStorageStats().get((long) i));
    }
    hostPartitionClassStorageStats = new HostPartitionClassStorageStats(hostPartitionClassStorageStatsMap);
    StorageManager storageManager = new MockStorageManager(storeMap, currentNode);
    mockClusterParticipant = new MockHelixParticipant(new ClusterMapConfig(new VerifiableProperties(properties)));
    statsManager = new StatsManager(storageManager, mockClusterMap, replicas, new MetricRegistry(),
        new StatsManagerConfig(new VerifiableProperties(properties)), new MockTime(), mockClusterParticipant,
        accountStatsMySqlStore, new InMemAccountService(false, false), currentNode);
  }

  /**
   * Clean up the database by removing all the data in all tables.
   * @throws Exception
   */
  @Before
  public void before() throws Exception {
    accountStatsMySqlStore.cleanupTables();
  }

  @After
  public void after() {
    accountStatsMySqlStore.shutdown();
  }

  /**
   * Test account stats publisher
   * @throws Exception
   */
  @Test
  public void testAccountStatsPublisher() throws Exception {
    StatsManager.AccountStatsPublisher publisher = statsManager.new AccountStatsPublisher(accountStatsMySqlStore);
    publisher.run();

    HostAccountStorageStatsWrapper statsWrapper =
        accountStatsMySqlStore.queryHostAccountStorageStatsByHost(currentNode.getHostname(), currentNode.getPort());
    assertEquals(hostAccountStorageStats.getStorageStats(), statsWrapper.getStats().getStorageStats());
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
    HostPartitionClassStorageStatsWrapper statsWrapper =
        accountStatsMySqlStore.queryHostPartitionClassStorageStatsByHost(currentNode.getHostname(),
            currentNode.getPort(), partitionNameAndIds);
    assertEquals(hostPartitionClassStorageStats.getStorageStats(), statsWrapper.getStats().getStorageStats());
  }

  @Test
  public void testHostRemoval() throws Exception {
    // Publish some data to account stats store
    StatsManager.AccountStatsPublisher publisher = statsManager.new AccountStatsPublisher(accountStatsMySqlStore);
    publisher.run();

    mockClusterMap.invokeListenerForDataNodeRemoval(currentNode);
    // There should be a task scheduled to delete this current from the account stats store
    // Sleep for a while so the task can be executed
    Thread.sleep(2000);
    HostAccountStorageStatsWrapper statsWrapper =
        accountStatsMySqlStore.queryHostAccountStorageStatsByHost(currentNode.getHostname(), currentNode.getPort());
    assertEquals(0, statsWrapper.getStats().getStorageStats().size());
  }

  private Properties initProperties(String hostname, int port, String dc) throws Exception {
    Properties configProps = Utils.loadPropsFromResource("accountstats_mysql.properties");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, CLUSTER_NAME);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, hostname);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, dc);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_PORT, String.valueOf(port));
    configProps.setProperty(AccountStatsMySqlConfig.DOMAIN_NAMES_TO_REMOVE, ".github.com");
    configProps.setProperty(AccountStatsMySqlConfig.UPDATE_BATCH_SIZE, String.valueOf(BATCH_SIZE));
    configProps.setProperty(AccountStatsMySqlConfig.LOCAL_BACKUP_FILE_PATH, accountStatsOutputFileString);
    configProps.setProperty(StatsManagerConfig.STATS_ENABLE_MYSQL_REPORT, Boolean.TRUE.toString());
    configProps.setProperty(StatsManagerConfig.STATS_ENABLE_REMOVE_HOST_FROM_ACCOUNT_STATS_STORE,
        Boolean.TRUE.toString());

    List<com.github.ambry.utils.TestUtils.ZkInfo> zkInfoList = new ArrayList<>();
    zkInfoList.add(new com.github.ambry.utils.TestUtils.ZkInfo(null, dc, (byte) 0, 2199, false));
    JSONObject zkJson = constructZkLayoutJSON(zkInfoList);
    configProps.setProperty("clustermap.dcs.zk.connect.strings", zkJson.toString(2));
    return configProps;
  }

  private AccountStatsMySqlStore createAccountStatsMySqlStore(Properties configProps) throws Exception {
    VerifiableProperties verifiableProperties = new VerifiableProperties(configProps);
    return (AccountStatsMySqlStore) new AccountStatsMySqlStoreFactory(verifiableProperties,
        new ClusterMapConfig(verifiableProperties), new MetricRegistry()).getAccountStatsStore();
  }
}
