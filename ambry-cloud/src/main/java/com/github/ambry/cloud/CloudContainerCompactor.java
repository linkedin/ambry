/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.VirtualReplicatorCluster;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that runs scheduled or on-demand compaction of blobs of deleted containers.
 */
public class CloudContainerCompactor {
  private static final Logger logger = LoggerFactory.getLogger(CloudContainerCompactor.class);

  private final CloudDestination cloudDestination;
  private final VirtualReplicatorCluster virtualReplicatorCluster;
  private ContainerDeletionEntryCache containerDeletionEntryCache;

  /**
   * Constructor for {@link CloudContainerCompactor}.
   * @param cloudDestination {@link CloudDestination} object.
   * @param virtualReplicatorCluster {@link VirtualReplicatorCluster} object.
   */
  CloudContainerCompactor(CloudDestination cloudDestination, VirtualReplicatorCluster virtualReplicatorCluster) {
    this.cloudDestination = cloudDestination;
    this.virtualReplicatorCluster = virtualReplicatorCluster;
  }

  public void run() {
    compactDeletedContainers();
  }

  /**
   * Compact blobs of the deleted container from cloud.
   */
  private void compactDeletedContainers() {
    while (!containerDeletionEntryCache.isEmpty()) {
      fetchContainerDeletionEntries();
      if (containerDeletionEntryCache.isEmpty()) {
        // this means there are no more blobs of container to be deleted in partitions assigned to current node
        break;
      }
      ContainerDeletionEntry containerDeletionEntry = containerDeletionEntryCache.pop();
      for (String partitionId : containerDeletionEntry.getDeletePendingPartitions()) {
        try {
          int blobCompactedCount = cloudDestination.compactContainer(containerDeletionEntry.getContainerId(),
              containerDeletionEntry.getAccountId(), partitionId);
        } catch (CloudStorageException csEx) {
          logger.error("Container compaction failed for account {} container {} in partition {}",
              containerDeletionEntry.getAccountId(), containerDeletionEntry.getContainerId(), partitionId);
        }
      }
    }
  }

  /**
   * Fetch the {@link ContainerDeletionEntry} from cloud and create a cache with entries that have atleast one partition
   * assigned to current node.
   */
  private void fetchContainerDeletionEntries() {
    Set<ContainerDeletionEntry> containerDeletionEntrySet = cloudDestination.getContainersToDelete();
    Set<ContainerDeletionEntry> assignedPartitionContainerDeletionEntries = new HashSet<>();
    Set<String> assignedParitions = virtualReplicatorCluster.getAssignedPartitionIds()
        .stream()
        .map(partition -> partition.toPathString())
        .collect(Collectors.toSet());
    for (ContainerDeletionEntry containerDeletionEntry : containerDeletionEntrySet) {
      Set<String> assignedDeletePendingPartitions = containerDeletionEntry.getDeletePendingPartitions()
          .stream()
          .filter(partitionId -> assignedParitions.contains(partitionId))
          .collect(Collectors.toSet());
      if (assignedDeletePendingPartitions.size() > 0) {
        assignedPartitionContainerDeletionEntries.add(
            new ContainerDeletionEntry(containerDeletionEntry.getContainerId(), containerDeletionEntry.getAccountId(),
                containerDeletionEntry.getDeleteTriggerTimestamp(), false, assignedDeletePendingPartitions));
      }
    }
    containerDeletionEntryCache = new ContainerDeletionEntryCache(assignedPartitionContainerDeletionEntries);
  }

  class ContainerDeletionEntryCache {
    private final SortedSet<ContainerDeletionEntry> containerDeletionEntries;

    public ContainerDeletionEntryCache(Collection<ContainerDeletionEntry> containerDeletionEntryList) {
      this.containerDeletionEntries = Collections.unmodifiableSortedSet(new TreeSet<>(containerDeletionEntryList));
    }

    public ContainerDeletionEntry pop() {
      ContainerDeletionEntry containerDeletionEntry = containerDeletionEntries.first();
      containerDeletionEntries.remove(containerDeletionEntry);
      return containerDeletionEntry;
    }

    public boolean isEmpty() {
      return containerDeletionEntries.isEmpty();
    }
  }
}
