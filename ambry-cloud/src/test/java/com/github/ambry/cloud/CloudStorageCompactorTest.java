/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.PartitionInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


@RunWith(MockitoJUnitRunner.class)
public class CloudStorageCompactorTest {

  private final CloudDestination mockDest = mock(CloudDestination.class);
  private final CloudStorageCompactor compactor;
  private final Map<PartitionId, PartitionInfo> partitionMap = new HashMap<>();
  private final VcrMetrics vcrMetrics = new VcrMetrics(new MetricRegistry());
  private final int pageSize = 10;

  public CloudStorageCompactorTest() {
    Properties properties = new Properties();
    properties.setProperty(CloudConfig.CLOUD_BLOB_COMPACTION_QUERY_LIMIT, String.valueOf(pageSize));
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_QUERY_BUCKET_DAYS, "7");
    properties.setProperty(CloudConfig.CLOUD_COMPACTION_LOOKBACK_DAYS, "28");
    CloudConfig cloudConfig = new CloudConfig(new VerifiableProperties(properties));
    compactor = new CloudStorageCompactor(mockDest, cloudConfig, partitionMap.keySet(), vcrMetrics);
  }

  /**
   * Test the compactPartitions method.
   */
  @Test
  public void testCompactPartitions() throws Exception {
    // start with empty map
    assertEquals(0, compactor.compactPartitions());

    int numPartitions = 40;
    // add partitions to map
    String defaultClass = MockClusterMap.DEFAULT_PARTITION_CLASS;
    for (int i = 0; i < numPartitions; i++) {
      partitionMap.put(new MockPartitionId(i, defaultClass), null);
      when(mockDest.compactPartition(eq(Integer.toString(i)))).thenReturn(pageSize);
    }

    assertEquals(pageSize * numPartitions, compactor.compactPartitions());
    assertEquals(0, vcrMetrics.compactionFailureCount.getCount());

    // remove a partition from map
    partitionMap.remove(new MockPartitionId(0, defaultClass));
    assertEquals(pageSize * (numPartitions - 1), compactor.compactPartitions());
    assertEquals(0, vcrMetrics.compactionFailureCount.getCount());

    // Make compaction fail for some partitions
    CloudStorageException csex = new CloudStorageException("failure", new RuntimeException("Don't hurt me!"));
    when(mockDest.compactPartition(eq("2"))).thenThrow(csex);
    when(mockDest.compactPartition(eq("20"))).thenThrow(csex);
    assertEquals(pageSize * (numPartitions - 3), compactor.compactPartitions());
    assertEquals(2, vcrMetrics.compactionFailureCount.getCount());

    // Test shutdown
    assertFalse("Should not be shutting down yet", compactor.isShutDown());
    compactor.shutdown();
    assertTrue("Should be shutting down now", compactor.isShutDown());
    // TODO: test shutting down with compaction still in progress (more involved)
  }
}
