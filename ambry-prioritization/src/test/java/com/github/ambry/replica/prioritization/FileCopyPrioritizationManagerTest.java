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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.github.ambry.clustermap.*;
import com.github.ambry.replica.prioritization.disruption.DefaultDisruptionService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.util.List;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class FileCopyPrioritizationManagerTest {

  @Mock
  private FileCopyPrioritizationManager prioritizationManager;

  @Mock
  private ClusterManagerQueryHelper<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode> clusterManagerQueryHelper;

  @Mock
  private DefaultDisruptionService disruptionService;

  private MockClusterMap clusterMap;

  DataNodeId currentDataNodeId;

  @Before
  public void setUp() throws Exception {
    clusterMap = new MockClusterMap(false, true, 3, 4, 1, true, false, "DC1");
    currentDataNodeId = clusterMap.getDataNodeIds().get(0);
    clusterMap.createNewPartition(clusterMap.getDataNodes(), 0);
    prioritizationManager = new FileCopyPrioritizationManager(disruptionService, "DC1", clusterManagerQueryHelper);
    when(clusterManagerQueryHelper.getMinActiveReplicas(any())).thenReturn(0);
    prioritizationManager.start();
  }

  @After
  public void cleanUp() throws Exception {
    prioritizationManager.shutdown();
  }

  @Test
  public void testAddReplica() {
    List<ReplicaId> replicaIds = clusterMap.getReplicaIds(currentDataNodeId);
    ReplicaId replica = replicaIds.get(0);
    boolean added = prioritizationManager.addReplica(replica);
    assertTrue("Replica should be added successfully", added);

    boolean duplicateAdd = prioritizationManager.addReplica(replica);
    assertFalse("Duplicate replica should not be added", duplicateAdd);
  }

  @Test
  public void testRemoveReplica() {
    List<ReplicaId> replicaIds = clusterMap.getReplicaIds(currentDataNodeId);
    ReplicaId replica = replicaIds.get(0);
    DiskId disk = replica.getDiskId();

    prioritizationManager.addReplica(replica);
    boolean removed = prioritizationManager.removeReplica(disk, replica);
    assertTrue("Replica should be removed successfully", removed);

    boolean removeNonExistent = prioritizationManager.removeReplica(disk, replica);
    assertFalse("Non-existent replica should not be removed", removeNonExistent);
  }

  @Test
  public void testGetPartitionListForDisk() {
    List<ReplicaId> replicaIds = clusterMap.getReplicaIds(currentDataNodeId);
    ReplicaId replica1 = replicaIds.get(0);
    DiskId disk = replica1.getDiskId();
    ReplicaId replica2 = replicaIds.get(1);

    prioritizationManager.addReplica(replica1);
    prioritizationManager.addReplica(replica2);

    List<ReplicaId> replicas = prioritizationManager.getPartitionListForDisk(disk, 1);
    assertEquals("Should return one replica", 1, replicas.size());
    assertTrue("Returned replica should be in the list", replicas.contains(replica1) || replicas.contains(replica2));
  }

  @Test
  public void testShutdown() {
    assertTrue("Manager should be running initially", prioritizationManager.isRunning());
    prioritizationManager.shutdown();
    assertFalse("Manager should not be running after shutdown", prioritizationManager.isRunning());
  }

  @Test
  public void testRemoveInProgressReplica() {
    List<ReplicaId> replicaIds = clusterMap.getReplicaIds(currentDataNodeId);
    ReplicaId replica = replicaIds.get(0);
    DiskId disk = replica.getDiskId();

    prioritizationManager.addReplica(replica);
    List<ReplicaId> returnedReplicas = prioritizationManager.getPartitionListForDisk(disk, 1); // Mark as in-progress
    assertEquals("Replica that was submitted should be returned", replica, returnedReplicas.get(0));

    List<ReplicaId> inProgressReplicas = prioritizationManager.getInProgressReplicaIdsForDisk(disk);
    assertEquals("There should be 1 in progress replica", 1, inProgressReplicas.size());
    assertEquals("After fetching replica should be in progress", replica, inProgressReplicas.get(0));

    boolean removed = prioritizationManager.removeInProgressReplica(disk, replica);
    assertTrue("In-progress replica should be removed successfully", removed);

    inProgressReplicas = prioritizationManager.getInProgressReplicaIdsForDisk(disk);
    assertTrue("There should be no in progress replicas", inProgressReplicas.isEmpty());

    boolean removeNonExistent = prioritizationManager.removeInProgressReplica(disk, replica);
    assertFalse("Non-existent in-progress replica should not be removed", removeNonExistent);
  }

  @Test
  public void testReplicaOrderingBasedOnDisruptionService() throws InterruptedException {
    DataNodeId dataNodeId = clusterMap.getDataNodeIds().get(0);

    clusterMap.createNewPartition(Collections.singletonList((MockDataNodeId) dataNodeId), 0);
    clusterMap.createNewPartition(Collections.singletonList((MockDataNodeId) dataNodeId), 0);
    clusterMap.createNewPartition(Collections.singletonList((MockDataNodeId) dataNodeId), 0);

    // Mock replicas and their partitions
    List<ReplicaId> replicaIds = clusterMap.getReplicaIds(dataNodeId);

    Map<DiskId, Set<ReplicaId>> diskToReplica = new HashMap<>();
    for (ReplicaId replicaId : replicaIds) {
      diskToReplica.putIfAbsent(replicaId.getDiskId(), new HashSet<>());
      diskToReplica.get(replicaId.getDiskId()).add(replicaId);
    }

    List<ReplicaId> allReplicaIds = new ArrayList<>();

    Set<DiskId> diskIds = diskToReplica.keySet();
    for (DiskId diskId : diskIds) {
      if (diskToReplica.get(diskId).size() >= 3) {
        allReplicaIds = new ArrayList<>(diskToReplica.get(diskId));
      }
    }

    ReplicaId replica1 = allReplicaIds.get(0);
    ReplicaId replica2 = allReplicaIds.get(1);
    ReplicaId replica3 = allReplicaIds.get(2);
    DiskId disk = replica1.getDiskId();

    // Mock the disruptionService to return a specific order of partitions
    PartitionId partition1 = replica1.getPartitionId();
    PartitionId partition2 = replica2.getPartitionId();
    PartitionId partition3 = replica3.getPartitionId();
    prioritizationManager.addReplica(replica1);
    prioritizationManager.addReplica(replica2);
    prioritizationManager.addReplica(replica3);

    when(disruptionService.sortByDisruptions(anyList())).thenAnswer(invocation -> {
      List<PartitionId> partitions = invocation.getArgument(0);

      List<PartitionId> result = new ArrayList<>();
      for (PartitionId partitionId : Arrays.asList(partition2, partition1, partition3)) {
        partitions.remove(partitionId);
        result.add(partitionId);
      }
      result.addAll(partitions);
      return result;
    });

    // Add replicas to the prioritization manager
    prioritizationManager.runPrioritizationCycle();

    // Retrieve replicas for the disk
    List<ReplicaId> sortedReplicas = prioritizationManager.getPartitionListForDisk(disk, 3);

    // Verify the order of replicas matches the order provided by disruptionService
    assertEquals("First replica should match the order from disruptionService", replica2, sortedReplicas.get(0));
    assertEquals("Second replica should match the order from disruptionService", replica1, sortedReplicas.get(1));
    assertEquals("Third replica should match the order from disruptionService", replica3, sortedReplicas.get(2));

    sortedReplicas = prioritizationManager.getPartitionListForDisk(disk, 3);
    assertEquals("No replicas should be polled now", 0, sortedReplicas.size());
  }
}
