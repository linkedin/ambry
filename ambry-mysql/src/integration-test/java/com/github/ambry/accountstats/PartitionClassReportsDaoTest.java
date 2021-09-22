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
package com.github.ambry.accountstats;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.utils.Utils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit test for {@link PartitionClassReportsDao}.
 */
public class PartitionClassReportsDaoTest {
  private final AccountStatsMySqlStore mySqlStore;
  private final PartitionClassReportsDao dao;

  public PartitionClassReportsDaoTest() throws Exception {
    mySqlStore = createAccountStatsMySqlStore("clusterName", "hostName", 12345, 100);
    dao = new PartitionClassReportsDao(mySqlStore.getDataSource(),
        new MySqlMetrics(PartitionClassReportsDao.class, new MetricRegistry()));
  }

  @Before
  public void before() throws Exception {
    mySqlStore.cleanupTables();
  }

  @After
  public void after() {
    mySqlStore.shutdown();
  }

  @Test
  public void testPartitionClassName() throws Exception {
    // There is no partition class name rows in the table now, query should return empty result
    String clusterName = "ambry-test";
    Map<String, Short> partitionClassNames = dao.queryPartitionClassNames(clusterName);
    assertTrue(partitionClassNames.isEmpty());

    String partitionClassName1 = "default";
    String partitionClassName2 = "new-partition";

    dao.insertPartitionClassName(clusterName, partitionClassName1);
    dao.insertPartitionClassName(clusterName, partitionClassName2);
    // Add partition class 2 again, it shouldn't fail
    dao.insertPartitionClassName(clusterName, partitionClassName2);

    // Now query partition class names, it should return two result;
    partitionClassNames = dao.queryPartitionClassNames(clusterName);
    assertEquals(2, partitionClassNames.size());
    assertTrue(partitionClassNames.containsKey(partitionClassName1));
    assertTrue(partitionClassNames.containsKey(partitionClassName2));
    assertEquals(2, partitionClassNames.values().stream().distinct().collect(Collectors.toList()).size());

    // Add same partition class name with different clustername
    String clusterName2 = "ambry-git";
    dao.insertPartitionClassName(clusterName2, partitionClassName1);
    dao.insertPartitionClassName(clusterName2, partitionClassName1);

    // It should equal to the first result
    Map<String, Short> partitionClassNamesAgain = dao.queryPartitionClassNames(clusterName);
    assertEquals(partitionClassNames, partitionClassNamesAgain);

    // get partition class names for clusterName2
    Map<String, Short> partitionClassNames2 = dao.queryPartitionClassNames(clusterName2);
    assertEquals(1, partitionClassNames2.size());
    assertTrue(partitionClassNames2.containsKey(partitionClassName1));
    assertFalse(partitionClassNames.values().contains(partitionClassNames2.get(partitionClassName1)));
  }

  @Test
  public void testPartitionId() throws Exception {
    String clusterName = "ambry-test";
    // Before adding any partition ids, query should return empty result
    Set<Integer> partitionIds = dao.queryPartitionIds(clusterName);
    assertTrue(partitionIds.isEmpty());
    Map<String, Set<Integer>> partitionNameAndIds = dao.queryPartitionNameAndIds(clusterName);
    assertTrue(partitionNameAndIds.isEmpty());

    String partitionClassName1 = "default";
    String partitionClassName2 = "new-partition";
    dao.insertPartitionClassName(clusterName, partitionClassName1);
    dao.insertPartitionClassName(clusterName, partitionClassName2);
    Map<String, Short> partitionClassNames = dao.queryPartitionClassNames(clusterName);
    short partitionClassId1 = partitionClassNames.get(partitionClassName1);
    short partitionClassId2 = partitionClassNames.get(partitionClassName2);

    // insert some partition ids and query them later
    Map<String, Set<Integer>> expectedPartitionIds = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      short classId = i % 2 == 0 ? partitionClassId1 : partitionClassId2;
      String className = i % 2 == 0 ? partitionClassName1 : partitionClassName2;
      dao.insertPartitionId(clusterName, i, classId);
      // reinsert the same partition id should result in conflict, but conflict is handled by method
      dao.insertPartitionId(clusterName, i, classId);
      expectedPartitionIds.computeIfAbsent(className, k -> new HashSet<Integer>()).add(i);
    }

    // Add partition ids for other clusters
    dao.insertPartitionClassName("other-cluster", partitionClassName1);
    partitionClassNames = dao.queryPartitionClassNames("other-cluster");
    short partitionClassIdForOtherCluster = partitionClassNames.get(partitionClassName1);
    for (int i = 0; i < 100; i++) {
      dao.insertPartitionId(clusterName, i, partitionClassIdForOtherCluster);
    }

    // Get all partition ids for cluster
    partitionIds = dao.queryPartitionIds(clusterName);
    assertEquals(100, partitionIds.size());
    for (int i = 0; i < 100; i++) {
      assertTrue(partitionIds.contains(i));
    }

    // Get all partition ids under corresponding class name
    partitionNameAndIds = dao.queryPartitionNameAndIds(clusterName);
    assertEquals(expectedPartitionIds, partitionNameAndIds);
  }

  @Test
  public void testAggregatedPartitionClassReports() throws Exception {
    // Prepare the partition class name and partition id
    String clusterName = "ambry-test";
    String clusterName1 = "ambry-another-cluster";
    String className1 = "default";
    String className2 = "new-class";

    dao.insertPartitionClassName(clusterName, className1);
    dao.insertPartitionClassName(clusterName, className2);
    dao.insertPartitionClassName(clusterName1, className1);

    final Map<String, Map<Short, Map<Short, ContainerStorageStats>>> usagesInDB = new HashMap<>();
    dao.queryAggregatedPartitionClassReport(clusterName, (partitionClassName, accountId, containerStats, updatedAt) -> {
      usagesInDB.computeIfAbsent(partitionClassName, k -> new HashMap<>())
          .computeIfAbsent(accountId, k -> new HashMap<>())
          .put(containerStats.getContainerId(), containerStats);
    });
    assertTrue(usagesInDB.isEmpty());

    final int numAccount = 100;
    final int numContainer = 10;
    Map<String, Map<Short, Map<Short, ContainerStorageStats>>> classNameAccountContainerUsages = new HashMap<>();
    Random random = new Random();
    final long maxUsage = 10000;

    for (int i = 0; i < 3; i++) {
      classNameAccountContainerUsages.clear();
      usagesInDB.clear();
      for (String className : new String[]{className1, className2}) {
        classNameAccountContainerUsages.put(className, IntStream.range(0, numAccount)
            .boxed()
            .collect(Collectors.toMap(Integer::shortValue, a -> IntStream.range(0, numContainer)
                .boxed()
                .collect(Collectors.toMap(Integer::shortValue,
                    c -> new ContainerStorageStats.Builder(c.shortValue()).logicalStorageUsage(
                        random.nextLong() % maxUsage)
                        .physicalStorageUsage(random.nextLong() % maxUsage)
                        .numberOfBlobs(10)
                        .build())))));
      }

      PartitionClassReportsDao.StorageBatchUpdater batch = dao.new StorageBatchUpdater(17);
      for (String className : classNameAccountContainerUsages.keySet()) {
        for (short accountId : classNameAccountContainerUsages.get(className).keySet()) {
          for (short containerId : classNameAccountContainerUsages.get(className).get(accountId).keySet()) {
            ContainerStorageStats containerStats =
                classNameAccountContainerUsages.get(className).get(accountId).get(containerId);
            batch.addUpdateToBatch(clusterName, className, accountId, containerStats);
          }
        }
      }
      batch.flush();
      dao.queryAggregatedPartitionClassReport(clusterName,
          (partitionClassName, accountId, containerStats, updatedAt) -> {
            usagesInDB.computeIfAbsent(partitionClassName, k -> new HashMap<>())
                .computeIfAbsent(accountId, k -> new HashMap<>())
                .put(containerStats.getContainerId(), containerStats);
          });

      assertEquals(classNameAccountContainerUsages, usagesInDB);
    }
  }

  private static AccountStatsMySqlStore createAccountStatsMySqlStore(String clusterName, String hostname, int port,
      int batchSize) throws Exception {
    Path tempDir = Files.createTempDirectory("PartitionClassReportsDaoTest");
    Properties configProps = Utils.loadPropsFromResource("accountstats_mysql.properties");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_CLUSTER_NAME, clusterName);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_HOST_NAME, hostname);
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_DATACENTER_NAME, "dc1");
    configProps.setProperty(ClusterMapConfig.CLUSTERMAP_PORT, String.valueOf(port));
    configProps.setProperty(AccountStatsMySqlConfig.DOMAIN_NAMES_TO_REMOVE, ".github.com");
    configProps.setProperty(AccountStatsMySqlConfig.UPDATE_BATCH_SIZE, String.valueOf(batchSize));
    configProps.setProperty(AccountStatsMySqlConfig.LOCAL_BACKUP_FILE_PATH, tempDir.toString());
    VerifiableProperties verifiableProperties = new VerifiableProperties(configProps);
    return (AccountStatsMySqlStore) new AccountStatsMySqlStoreFactory(verifiableProperties,
        new ClusterMapConfig(verifiableProperties), new MetricRegistry()).getAccountStatsStore();
  }
}
