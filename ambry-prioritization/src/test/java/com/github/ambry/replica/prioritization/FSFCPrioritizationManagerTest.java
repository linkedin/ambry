/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateTransitionException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;
import static org.junit.Assert.*;


public class FSFCPrioritizationManagerTest {
  private MockClusterMap clusterMap;
  FCFSPrioritizationManager prioritizationManager = new FCFSPrioritizationManager();

  @Before
  public void initializeCluster() throws IOException {
    clusterMap = new MockClusterMap(false, true, 1,
        10, 3, false,
        false, null);
  }
   @Test
   public void testAddAndRemoveReplica() {
     int partitionId1 = 1;
     int partitionId2 = 2;
     int partitionId3 = 3;
     int mountPathIndex1 = 1;
     int mountPathIndex2 = 2;
     int mountPathIndex3 = 3;
     PartitionId partition1 =
         new MockPartitionId(partitionId1, MockClusterMap.DEFAULT_PARTITION_CLASS,
             clusterMap.getDataNodes(), mountPathIndex1);
     PartitionId partition2 =
         new MockPartitionId(partitionId2, MockClusterMap.DEFAULT_PARTITION_CLASS,
             clusterMap.getDataNodes(), mountPathIndex2);
     PartitionId partition3 =
          new MockPartitionId(partitionId3, MockClusterMap.DEFAULT_PARTITION_CLASS,
              clusterMap.getDataNodes(), mountPathIndex3);

     /**
      * Adding replicas to the prioritization manager before starting it should fail.
      */
     try {
       assertFalse(prioritizationManager.addReplica(partition1.getReplicaIds().get(0)));
     } catch (StateTransitionException e){
       assertEquals("Error code doesn't match", PrioritizationManagerRunningFailure, e.getErrorCode());
     }

     prioritizationManager.start();
     assertTrue(prioritizationManager.addReplica(partition1.getReplicaIds().get(0)));
     assertTrue(prioritizationManager.addReplica(partition2.getReplicaIds().get(0)));
     assertTrue(prioritizationManager.addReplica(partition3.getReplicaIds().get(0)));

     assertTrue(prioritizationManager.removeReplica(partition1.getReplicaIds().get(0).getDiskId(), partition1.getReplicaIds().get(0)));
     assertTrue(prioritizationManager.removeReplica(partition2.getReplicaIds().get(0).getDiskId(), partition2.getReplicaIds().get(0)));
   }

   @Test
  public void testAddRemoveAndGetPartitionsOnADisk(){
     Map<Integer, List<PartitionId>> diskToPartitionMap = new ConcurrentHashMap<>();

     prioritizationManager.start();
     int partitionCounter = 1;

     // Add 5 Replicas On Each Disk
     for(int diskId=1; diskId <= 10; diskId++){
       for(int numPartitions=0 ; numPartitions < 5; numPartitions++){
          PartitionId partition =
              new MockPartitionId(partitionCounter++, MockClusterMap.DEFAULT_PARTITION_CLASS,
                  clusterMap.getDataNodes(), diskId-1);
          diskToPartitionMap.putIfAbsent(diskId, new ArrayList<>());
          diskToPartitionMap.get(diskId).add(partition);
          assertTrue(prioritizationManager.addReplica(partition.getReplicaIds().get(0)));
       }
     }

     // Remove replicas and test number of replicas returned by getPartitionListForDisk.
     for(int diskId=1; diskId <= 10; diskId++){
       for(int numPartitions=0 ; numPartitions < 5; numPartitions++){
         PartitionId partition = diskToPartitionMap.get(diskId).get(numPartitions);
         assertTrue(prioritizationManager.removeReplica(partition.getReplicaIds().get(0).getDiskId(),
             partition.getReplicaIds().get(0)));
       }
     }
   }

  @Test
  public void testOrderOfPartitionsAddedAndRemoved() {
    prioritizationManager.start();

    // Create and add partitions in a specific order
    int partitionId1 = 1;
    int partitionId2 = 2;
    int partitionId3 = 3;
    PartitionId partition1 = new MockPartitionId(partitionId1, MockClusterMap.DEFAULT_PARTITION_CLASS,
        clusterMap.getDataNodes(), 0);
    PartitionId partition2 = new MockPartitionId(partitionId2, MockClusterMap.DEFAULT_PARTITION_CLASS,
        clusterMap.getDataNodes(), 0);
    PartitionId partition3 = new MockPartitionId(partitionId3, MockClusterMap.DEFAULT_PARTITION_CLASS,
        clusterMap.getDataNodes(), 0);

    assertTrue(prioritizationManager.addReplica(partition1.getReplicaIds().get(0)));
    assertTrue(prioritizationManager.addReplica(partition2.getReplicaIds().get(0)));
    assertTrue(prioritizationManager.addReplica(partition3.getReplicaIds().get(0)));

    // Retrieve and remove partitions in the same order
    List<ReplicaId> replicas = prioritizationManager.getPartitionListForDisk(
        partition1.getReplicaIds().get(0).getDiskId(), 3);

    assertEquals("First partition should match", partition1.getReplicaIds().get(0), replicas.get(0));
    assertEquals("Second partition should match", partition2.getReplicaIds().get(0), replicas.get(1));
    assertEquals("Third partition should match", partition3.getReplicaIds().get(0), replicas.get(2));
    prioritizationManager.reset();
  }

  @Test
  public void testRemainingReplicasInPrioritizationManager() {
    prioritizationManager.start();

    // Create and add replicas
    int partitionId1 = 1;
    int partitionId2 = 2;
    int partitionId3 = 3;
    PartitionId partition1 = new MockPartitionId(partitionId1, MockClusterMap.DEFAULT_PARTITION_CLASS,
        clusterMap.getDataNodes(), 0);
    PartitionId partition2 = new MockPartitionId(partitionId2, MockClusterMap.DEFAULT_PARTITION_CLASS,
        clusterMap.getDataNodes(), 0);
    PartitionId partition3 = new MockPartitionId(partitionId3, MockClusterMap.DEFAULT_PARTITION_CLASS,
        clusterMap.getDataNodes(), 0);

    assertTrue(prioritizationManager.addReplica(partition1.getReplicaIds().get(0)));
    assertTrue(prioritizationManager.addReplica(partition2.getReplicaIds().get(0)));
    assertTrue(prioritizationManager.addReplica(partition3.getReplicaIds().get(0)));

    // Remove one replica
    assertTrue(prioritizationManager.removeReplica(partition1.getReplicaIds().get(0).getDiskId(),
        partition1.getReplicaIds().get(0)));

    // Check remaining replicas
    List<ReplicaId> remainingReplicas = prioritizationManager.getPartitionListForDisk(
        partition2.getReplicaIds().get(0).getDiskId(), 10);

    assertEquals("Remaining replicas count should match", 2, remainingReplicas.size());
    assertTrue("Remaining replicas should contain partition2", remainingReplicas.contains(partition2.getReplicaIds().get(0)));
    assertTrue("Remaining replicas should contain partition3", remainingReplicas.contains(partition3.getReplicaIds().get(0)));
    prioritizationManager.reset();
  }

  @Test
  public void testInProgressReplicaAssertion() {
    prioritizationManager.start();

    // Create and add replicas
    int partitionId1 = 1;
    int partitionId2 = 2;
    PartitionId partition1 = new MockPartitionId(partitionId1, MockClusterMap.DEFAULT_PARTITION_CLASS,
        clusterMap.getDataNodes(), 0);
    PartitionId partition2 = new MockPartitionId(partitionId2, MockClusterMap.DEFAULT_PARTITION_CLASS,
        clusterMap.getDataNodes(), 0);

    assertTrue(prioritizationManager.addReplica(partition1.getReplicaIds().get(0)));
    assertTrue(prioritizationManager.addReplica(partition2.getReplicaIds().get(0)));

    // Move replicas to in-progress
    List<ReplicaId> inProgressReplicas = prioritizationManager.getPartitionListForDisk(
        partition1.getReplicaIds().get(0).getDiskId(), 2);

    // Assert in-progress replicas
    List<ReplicaId> retrievedInProgressReplicas = prioritizationManager.getInProgressReplicaIdsForDisk(
        partition1.getReplicaIds().get(0).getDiskId());

    assertEquals("In-progress replicas count should match", 2, retrievedInProgressReplicas.size());
    assertTrue("In-progress replicas should contain partition1",
        retrievedInProgressReplicas.contains(partition1.getReplicaIds().get(0)));
    assertTrue("In-progress replicas should contain partition2",
        retrievedInProgressReplicas.contains(partition2.getReplicaIds().get(0)));

    // Remove in-progress replicas and verify
    assertTrue(prioritizationManager.removeInProgressReplica(partition1.getReplicaIds().get(0).getDiskId(),
        partition1.getReplicaIds().get(0)));
    assertTrue(prioritizationManager.removeInProgressReplica(partition2.getReplicaIds().get(0).getDiskId(),
        partition2.getReplicaIds().get(0)));

    List<ReplicaId> remainingInProgressReplicas = prioritizationManager.getInProgressReplicaIdsForDisk(
        partition1.getReplicaIds().get(0).getDiskId());
    assertTrue("No in-progress replicas should remain", remainingInProgressReplicas.isEmpty());
  }

}
