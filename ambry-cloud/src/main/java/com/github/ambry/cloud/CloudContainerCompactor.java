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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * Class that runs scheduled or on-demand compaction of blobs of deleted containers.
 */
public class CloudContainerCompactor {

  private final CloudDestination cloudDestination;
  private ContainerDeletionEntryCache containerDeletionEntryCache;
  private final VirtualReplicatorCluster virtualReplicatorCluster;

  CloudContainerCompactor(CloudDestination cloudDestination, VirtualReplicatorCluster virtualReplicatorCluster) {
    this.cloudDestination = cloudDestination;
    this.virtualReplicatorCluster = virtualReplicatorCluster;
  }

  public void run() {
    compactDeletedContainers();
  }

  public void compactDeletedContainers() {
    while (!containerDeletionEntryCache.isEmpty()) {
      fetchContainerDeletionEntries();
      if (containerDeletionEntryCache.isEmpty()) {
        // this means there are no more blobs of container to be deleted in partitions assigned to current node
        break;
      }

    }
  }

  private void fetchContainerDeletionEntries() {
    cloudDestination.getContainersToDelete();
  }

  class ContainerDeletionEntryCache {
    private final SortedSet<ContainerDeletionEntry> containerDeletionEntries;

    public ContainerDeletionEntryCache(Collection<ContainerDeletionEntry> containerDeletionEntryList) {
      this.containerDeletionEntries = Collections.unmodifiableSortedSet(new TreeSet<>(containerDeletionEntryList));
    }

    public boolean isEmpty() {
      return containerDeletionEntries.isEmpty();
    }
  }
}
