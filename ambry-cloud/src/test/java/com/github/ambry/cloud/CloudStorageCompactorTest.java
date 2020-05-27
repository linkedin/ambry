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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
    verify(mockDest, times(0)).getDeletedBlobs(anyString(), anyLong(), anyLong(), anyInt());
    verify(mockDest, times(0)).getExpiredBlobs(anyString(), anyLong(), anyLong(), anyInt());
    verify(mockDest, times(0)).purgeBlobs(any());

    // add 2 partitions to map
    int partition1 = 101, partition2 = 102;
    long compactionEndTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(CloudConfig.DEFAULT_RETENTION_DAYS);
    String partitionPath1 = String.valueOf(partition1), partitionPath2 = String.valueOf(partition2);
    String defaultClass = MockClusterMap.DEFAULT_PARTITION_CLASS;
    partitionMap.put(new MockPartitionId(partition1, defaultClass), null);
    partitionMap.put(new MockPartitionId(partition2, defaultClass), null);
    List<CloudBlobMetadata> noBlobs = Collections.emptyList();
    List<CloudBlobMetadata> deadBlobsPartition1 = getDeadBlobsList(partitionPath1, pageSize, compactionEndTime, true);
    when(mockDest.getDeletedBlobs(eq(partitionPath1), anyLong(), anyLong(), anyInt())).thenReturn(deadBlobsPartition1)
        .thenReturn(noBlobs);
    when(mockDest.getExpiredBlobs(eq(partitionPath1), anyLong(), anyLong(), anyInt())).thenReturn(noBlobs);
    when(mockDest.purgeBlobs(eq(deadBlobsPartition1))).thenReturn(pageSize);
    List<CloudBlobMetadata> deadBlobsPartition2 =
        getDeadBlobsList(partitionPath2, pageSize * 2, compactionEndTime, false);
    List<CloudBlobMetadata> deadBlobsPartition2Page1 = deadBlobsPartition2.subList(0, pageSize);
    List<CloudBlobMetadata> deadBlobsPartition2Page2 = deadBlobsPartition2.subList(pageSize, pageSize * 2);
    when(mockDest.getExpiredBlobs(eq(partitionPath2), anyLong(), anyLong(), anyInt())).thenReturn(
        deadBlobsPartition2Page1).thenReturn(deadBlobsPartition2Page2).thenReturn(noBlobs);
    when(mockDest.getDeletedBlobs(eq(partitionPath2), anyLong(), anyLong(), anyInt())).thenReturn(noBlobs);
    when(mockDest.purgeBlobs(eq(deadBlobsPartition2Page1))).thenReturn(pageSize);
    when(mockDest.purgeBlobs(eq(deadBlobsPartition2Page2))).thenReturn(pageSize);
    assertEquals(deadBlobsPartition1.size() + deadBlobsPartition2.size(), compactor.compactPartitions());
    verify(mockDest, times(1)).purgeBlobs(eq(deadBlobsPartition1));
    verify(mockDest, times(1)).purgeBlobs(eq(deadBlobsPartition2Page1));
    verify(mockDest, times(1)).purgeBlobs(eq(deadBlobsPartition2Page2));
    reset(mockDest);

    // remove partition2 from map
    partitionMap.remove(new MockPartitionId(partition2, defaultClass));
    deadBlobsPartition1 = getDeadBlobsList(partitionPath1, pageSize / 2, compactionEndTime, true);
    when(mockDest.getDeletedBlobs(eq(partitionPath1), anyLong(), anyLong(), anyInt())).thenReturn(deadBlobsPartition1)
        .thenReturn(noBlobs);
    when(mockDest.purgeBlobs(eq(deadBlobsPartition1))).thenReturn(deadBlobsPartition1.size());
    assertEquals(deadBlobsPartition1.size(), compactor.compactPartitions());
    // Expect one more call for partition1, no more for partition2
    verify(mockDest, times(1)).purgeBlobs(eq(deadBlobsPartition1));
    verify(mockDest, times(1)).purgeBlobs(any());
    assertNull(compactor.getOldestExpiredBlob(partitionPath1));
    assertNull(compactor.getOldestDeletedBlob(partitionPath2));

    // Test shutdown
    assertFalse("Should not be shutting down yet", compactor.isShuttingDown());
    compactor.shutdown();
    assertTrue("Should be shutting down now", compactor.isShuttingDown());
    // TODO: test shutting down with compaction still in progress (more involved)
  }

  /**
   * Utility method to create a list of CloudBlobMetadata to be returned by getDeadBlobs call.
   * @param partitionId the partitionId string.
   * @param numBlobs the number of dead blobs to return.
   * @param endTime the compaction end time to use.
   * @param isDeleted true for deleted blobs, false for expired.
   * @return
   */
  private List<CloudBlobMetadata> getDeadBlobsList(String partitionId, int numBlobs, long endTime, boolean isDeleted) {
    List<CloudBlobMetadata> metadataList = new ArrayList<>();
    for (int j = 0; j < numBlobs; j++) {
      CloudBlobMetadata metadata = new CloudBlobMetadata().setPartitionId(partitionId).setId("blob_" + j);
      if (isDeleted) {
        metadata.setDeletionTime(endTime - j);
      } else {
        metadata.setExpirationTime(endTime - j);
      }
      metadataList.add(metadata);
    }
    return metadataList;
  }
}
