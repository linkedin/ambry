/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Loop through all the host's disks and check if the replicas are placed correctly
 * (as compared with the latest Helix state). Sometimes, disks can get re-mounted in the wrong
 * order. This class checks for such cases and reshuffles the replicas if necessary, creating
 * a map of the broken disks and the disks they should be reshuffled to.
 */
public class ReplicaPlacementValidator {
  private final Map<DiskId, Set<String>> foundDiskToPartitionMap;
  private Map<DiskId, List<ReplicaId>> expectedDiskToReplicaMap;
  private Map<DiskId, List<ReplicaId>> newDiskToReplicaMap;
  private List<DiskId> brokenDisks = new ArrayList<>();
  public ReplicaPlacementValidator(Map<DiskId, List<ReplicaId>> expectedDiskToReplicaMap,
      Map<DiskId, Set<String>> actualDiskToPartitionMap) {
    foundDiskToPartitionMap = actualDiskToPartitionMap;
    newDiskToReplicaMap = new HashMap<>();
    this.expectedDiskToReplicaMap = expectedDiskToReplicaMap;

    for (Map.Entry<DiskId, List<ReplicaId>> entry : expectedDiskToReplicaMap.entrySet()) {
      DiskId currentDisk = entry.getKey();
      List<ReplicaId> replicas = entry.getValue();
      for (ReplicaId replica : replicas) {
        String partitionID = replica.getPartitionId().toString();
        if(!foundDiskToPartitionMap.get(currentDisk).contains(partitionID)) {
          brokenDisks.add(currentDisk);
        }
      }
    }
  }

  /**
   * Check the placement of replicas on the disks and reshuffle them if necessary.
   * @return A map of the broken disks and the disks they should be reshuffled to.
   *         An empty map if no reshuffling is necessary or if the reshuffling failed.
   */
  public Map<DiskId, DiskId> reshuffleDisks() {
    Map<DiskId, DiskId> shuffledDisks = new HashMap<>();

    // Sanity checks: - Abort if we did not find the same number of disks on the
    //                  host as we got from Helix.
    //                - Abort if we did not find any broken disks.
    if ((expectedDiskToReplicaMap.size() != foundDiskToPartitionMap.size()) ||
        brokenDisks.size() == 0) {
      return Collections.emptyMap();
    }

    for (DiskId currentDisk : brokenDisks) {
      List<ReplicaId> expectedReplicas = expectedDiskToReplicaMap.get(currentDisk);
      DiskId foundDisk = findDiskWithReplicas(expectedReplicas);
      if(foundDisk == null) {
        return Collections.emptyMap();
      } else {
        shuffledDisks.put(currentDisk, foundDisk);
      }
    }
    return shuffledDisks;
  }

  /**
   * Find the disk that contains the expected replicas.
   * @param expectedReplicas A list of replicas that should be on the disk.
   * @return The disk that contains the expected replicas, or null if no such disk was found.
   */
  private DiskId findDiskWithReplicas(List<ReplicaId> expectedReplicas) {
    for (Map.Entry<DiskId, Set<String>> entry : foundDiskToPartitionMap.entrySet()) {
      DiskId currentDisk = entry.getKey();
      Set<String> partitions = entry.getValue();
      boolean found = true;
      for (ReplicaId replica : expectedReplicas) {
        String partitionID = replica.getPartitionId().toPathString();
        if(!partitions.contains(partitionID)) {
          found = false;
          break;
        }
      }
      if(found) {
        return currentDisk;
      }
    }
    return null;
  }
}
