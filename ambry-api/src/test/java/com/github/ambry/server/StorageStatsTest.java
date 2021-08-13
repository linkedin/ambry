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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Unit tests for storage stats classes
 */
public class StorageStatsTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

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
}
