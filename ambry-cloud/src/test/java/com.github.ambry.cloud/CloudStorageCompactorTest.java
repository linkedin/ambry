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
import com.github.ambry.replication.PartitionInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


@RunWith(Parameterized.class)
public class CloudStorageCompactorTest {

  /**
   * Run in both test and production mode.
   * @return an array with both {@code false} and {@code true}.
   */
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return Arrays.asList(new Object[][]{{false}, {true}});
  }

  private final boolean testMode;
  private final CloudDestination mockDest = mock(CloudDestination.class);
  private final CloudStorageCompactor compactor;
  private final Map<PartitionId, PartitionInfo> partitionMap = new HashMap<>();
  private final VcrMetrics vcrMetrics = new VcrMetrics(new MetricRegistry());

  public CloudStorageCompactorTest(boolean testMode) {
    this.testMode = testMode;
    compactor = new CloudStorageCompactor(mockDest, partitionMap.keySet(), vcrMetrics, testMode);
  }

  /**
   * Test the compactPartitions method.
   */
  @Test
  public void testCompactPartitions() throws Exception {
    // start with empty map
    assertEquals(0, compactor.compactPartitions());
    verify(mockDest, times(0)).getDeadBlobs(anyString());
    verify(mockDest, times(0)).purgeBlobs(any());

    // add 2 partitions to map
    int partition1 = 101, partition2 = 102;
    String partitionPath1 = String.valueOf(partition1), partitionPath2 = String.valueOf(partition2);
    String defaultClass = MockClusterMap.DEFAULT_PARTITION_CLASS;
    partitionMap.put(new MockPartitionId(partition1, defaultClass), null);
    partitionMap.put(new MockPartitionId(partition2, defaultClass), null);
    List<CloudBlobMetadata> deadBlobsPartition1 = getMetadataList(partitionPath1, 10);
    when(mockDest.getDeadBlobs(eq(partitionPath1))).thenReturn(deadBlobsPartition1);
    when(mockDest.purgeBlobs(eq(deadBlobsPartition1))).thenReturn(deadBlobsPartition1.size());
    List<CloudBlobMetadata> deadBlobsPartition2 = getMetadataList(partitionPath2, 20);
    when(mockDest.getDeadBlobs(eq(partitionPath2))).thenReturn(deadBlobsPartition2);
    when(mockDest.purgeBlobs(eq(deadBlobsPartition2))).thenReturn(deadBlobsPartition2.size());
    assertEquals(deadBlobsPartition1.size() + deadBlobsPartition2.size(), compactor.compactPartitions());
    verify(mockDest, times(1)).getDeadBlobs(eq(partitionPath1));
    verify(mockDest, times(1)).getDeadBlobs(eq(partitionPath2));
    verify(mockDest, times(testMode ? 0 : 1)).purgeBlobs(eq(deadBlobsPartition1));
    verify(mockDest, times(testMode ? 0 : 1)).purgeBlobs(eq(deadBlobsPartition2));

    // remove 1 partition from map
    partitionMap.remove(new MockPartitionId(partition2, defaultClass));
    assertEquals(deadBlobsPartition1.size(), compactor.compactPartitions());
    verify(mockDest, times(2)).getDeadBlobs(eq(partitionPath1));
    verify(mockDest, times(1)).getDeadBlobs(eq(partitionPath2));
    verify(mockDest, times(testMode ? 0 : 2)).purgeBlobs(eq(deadBlobsPartition1));
    verify(mockDest, times(testMode ? 0 : 1)).purgeBlobs(eq(deadBlobsPartition2));
  }

  /**
   * Utility method to create a list of CloudBlobMetadata to be returned by getDeadBlobs call.
   * @param partitionId the partitionId string.
   * @param numBlobs the number of dead blobs to return.
   * @return
   */
  private List<CloudBlobMetadata> getMetadataList(String partitionId, int numBlobs) {
    List<CloudBlobMetadata> metadataList = new ArrayList<>();
    for (int j = 0; j < numBlobs; j++) {
      metadataList.add(new CloudBlobMetadata().setPartitionId(partitionId).setCloudBlobName("blob_" + j));
    }
    return metadataList;
  }
}
