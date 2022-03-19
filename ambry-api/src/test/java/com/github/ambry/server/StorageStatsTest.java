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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.AggregatedPartitionClassStorageStats;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.server.storagestats.HostAccountStorageStats;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Unit tests for storage stats classes
 */
public class StorageStatsTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Random random = new Random();

  /**
   * Test methods in {@link ContainerStorageStats}.
   * @throws Exception
   */
  @Test
  public void testContainerStorageStats() throws Exception {
    short containerId = 10;
    long logicalStorageUsage = 10000;
    long physicalStorageUsage = 20000;
    long numberOfBlobs = 100;
    ContainerStorageStats stats =
        new ContainerStorageStats.Builder(containerId).logicalStorageUsage(logicalStorageUsage)
            .physicalStorageUsage(physicalStorageUsage)
            .numberOfBlobs(numberOfBlobs)
            .build();
    assertContainerStorageStats(stats, containerId, logicalStorageUsage, physicalStorageUsage, numberOfBlobs);

    String serialized = objectMapper.writeValueAsString(stats);
    Map<String, Object> tempMap = objectMapper.readValue(serialized, new TypeReference<Map<String, Object>>() {
    });
    // We are only expecting "containerId", "logicalStorageUsage", "physicalStorageUsage" and "numberOfBlobs" in the serialized string
    Assert.assertEquals(4, tempMap.size());
    for (String key : new String[]{"containerId", "logicalStorageUsage", "physicalStorageUsage", "numberOfBlobs"}) {
      Assert.assertTrue(tempMap.containsKey(key));
    }
    ContainerStorageStats deserialized = objectMapper.readValue(serialized, ContainerStorageStats.class);
    Assert.assertEquals(stats, deserialized);

    ContainerStorageStats newStats = stats.add(deserialized);
    assertContainerStorageStats(stats, containerId, logicalStorageUsage, physicalStorageUsage, numberOfBlobs);
    assertContainerStorageStats(newStats, containerId, 2 * logicalStorageUsage, 2 * physicalStorageUsage,
        2 * numberOfBlobs);

    serialized = "{`logicalStorageUsage`:1234, `physicalStorageUsage`:2345, `numberOfBlobs`: 12}".replace("`", "\"");
    try {
      objectMapper.readValue(serialized, ContainerStorageStats.class);
      Assert.fail("Missing container is should fail deserialization");
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof IllegalStateException);
    }
  }

  /**
   * Test methods in {@link HostAccountStorageStats}.
   * @throws Exception
   */
  @Test
  public void testHostAccountStorageStats() throws Exception {
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> storageStats = new HashMap<>();
    long partitionId = 10000L;
    short accountId = 0;
    short containerId = 100;

    int numberOfPartitions = 2;
    int numberOfAccounts = 2;
    int numberOfContainers = 2;
    for (int i = 0; i < numberOfPartitions; i++) {
      partitionId++;
      if (!storageStats.containsKey(partitionId)) {
        storageStats.put(partitionId, new HashMap<>());
      }
      for (int j = 0; j < numberOfAccounts; j++) {
        accountId++;
        if (!storageStats.get(partitionId).containsKey(accountId)) {
          storageStats.get(partitionId).put(accountId, new HashMap<>());
        }
        for (int k = 0; k < numberOfContainers; k++) {
          containerId++;
          if (!storageStats.get(partitionId).get(accountId).containsKey(containerId)) {
            storageStats.get(partitionId)
                .get(accountId)
                .put(containerId, generateRandomContainerStorageStats(containerId));
          }
        }
      }
    }

    HostAccountStorageStats host1 = new HostAccountStorageStats(storageStats);
    HostAccountStorageStats host2 = new HostAccountStorageStats();
    for (Map.Entry<Long, Map<Short, Map<Short, ContainerStorageStats>>> partitionEntry : storageStats.entrySet()) {
      long pid = partitionEntry.getKey();
      for (Map.Entry<Short, Map<Short, ContainerStorageStats>> accountEntry : partitionEntry.getValue().entrySet()) {
        short aid = accountEntry.getKey();
        for (Map.Entry<Short, ContainerStorageStats> containerEntry : accountEntry.getValue().entrySet()) {
          host2.addContainerStorageStats(pid, aid, containerEntry.getValue());
        }
      }
    }

    Assert.assertEquals(host1.getStorageStats(), host2.getStorageStats());

    // Serialize the host account storage stats
    String serialized = objectMapper.writeValueAsString(host1);
    HostAccountStorageStats deserialized = objectMapper.readValue(serialized, HostAccountStorageStats.class);
    Assert.assertEquals(host1.getStorageStats(), deserialized.getStorageStats());
  }

  @Test
  public void testAggregatedAccountStorageStats() throws Exception {
    Map<Short, Map<Short, ContainerStorageStats>> storageStatsMap =
        StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 10, 10, 5, 10000L, 2, 10);
    String serialized = objectMapper.writeValueAsString(storageStatsMap);
    AggregatedAccountStorageStats deserialized =
        objectMapper.readValue(serialized, AggregatedAccountStorageStats.class);
    Assert.assertEquals(storageStatsMap, deserialized.getStorageStats());

    serialized = objectMapper.writeValueAsString(deserialized);
    deserialized = objectMapper.readValue(serialized, AggregatedAccountStorageStats.class);
    Assert.assertEquals(storageStatsMap, deserialized.getStorageStats());
  }

  @Test
  public void testAggregatedPartitionClassStorageStats() throws Exception {
    Map<String, Map<Short, Map<Short, ContainerStorageStats>>> storageStatsMap =
        StorageStatsUtilTest.generateRandomAggregatedPartitionClassStorageStats(new String[]{"default", "newClass"},
            (short) 10, 10, 5, 10000L, 2, 100);
    String serialized = objectMapper.writeValueAsString(storageStatsMap);
    AggregatedPartitionClassStorageStats deserialized =
        objectMapper.readValue(serialized, AggregatedPartitionClassStorageStats.class);
    Assert.assertEquals(storageStatsMap, deserialized.getStorageStats());

    serialized = objectMapper.writeValueAsString(deserialized);
    deserialized = objectMapper.readValue(serialized, AggregatedPartitionClassStorageStats.class);
    Assert.assertEquals(storageStatsMap, deserialized.getStorageStats());
  }

  /**
   * Helper method to compare {@link ContainerStorageStats}.
   * @param stats The {@link ContainerStorageStats}.
   * @param containerId the container id
   * @param logicalStorageUsage the logical storage usage
   * @param physicalStorageUsage the physical storage usage
   * @param numberOfBlobs the number of blobs
   */
  private void assertContainerStorageStats(ContainerStorageStats stats, short containerId, long logicalStorageUsage,
      long physicalStorageUsage, long numberOfBlobs) {
    Assert.assertEquals(containerId, stats.getContainerId());
    Assert.assertEquals(logicalStorageUsage, stats.getLogicalStorageUsage());
    Assert.assertEquals(physicalStorageUsage, stats.getPhysicalStorageUsage());
    Assert.assertEquals(numberOfBlobs, stats.getNumberOfBlobs());
  }

  /**
   * Helper method to generate a random {@link ContainerStorageStats} for given containerId.
   * @param containerId The container id.
   * @return The generated {@link ContainerStorageStats}.
   */
  private ContainerStorageStats generateRandomContainerStorageStats(short containerId) {
    long logicalStorageUsage = Math.abs(random.nextLong() % 100000L);
    return new ContainerStorageStats.Builder(containerId).logicalStorageUsage(logicalStorageUsage)
        .physicalStorageUsage(logicalStorageUsage * 2)
        .numberOfBlobs(10)
        .build();
  }
}
