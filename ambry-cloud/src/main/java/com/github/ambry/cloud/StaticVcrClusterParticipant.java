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

import com.github.ambry.clustermap.CloudDataNode;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.clustermap.VcrClusterParticipantListener;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * VCR Cluster based on static partition assignment.
 */
public class StaticVcrClusterParticipant implements VcrClusterParticipant {

  private final DataNodeId currentDataNode;
  private final Map<String, PartitionId> partitionIdMap;
  private final Set<PartitionId> assignedPartitionIds;
  private final List<VcrClusterParticipantListener> listeners = new ArrayList<>();

  /**
   * Construct the static VCR cluster.
   * @param cloudConfig The cloud configuration to use.
   * @param clusterMapConfig The clustermap configuration to use.
   * @param clusterMap The clustermap to use.
   */
  public StaticVcrClusterParticipant(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig,
      ClusterMap clusterMap) {
    currentDataNode = new CloudDataNode(cloudConfig, clusterMapConfig);
    if (Utils.isNullOrEmpty(cloudConfig.vcrAssignedPartitions)) {
      throw new IllegalArgumentException("Missing value for " + CloudConfig.VCR_ASSIGNED_PARTITIONS);
    }
    Set<String> assignedPartitionSet =
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList(cloudConfig.vcrAssignedPartitions.split(","))));
    List<? extends PartitionId> allPartitions = clusterMap.getAllPartitionIds(null);
    // map partitions by path
    partitionIdMap = allPartitions.stream().collect(Collectors.toMap(PartitionId::toPathString, Function.identity()));

    assignedPartitionIds = new HashSet<>();
    for (String id : assignedPartitionSet) {
      if (!partitionIdMap.containsKey(id)) {
        throw new IllegalArgumentException("Invalid partition specified: " + id);
      }
      assignedPartitionIds.add(partitionIdMap.get(id));
    }
  }

  @Override
  public List<? extends DataNodeId> getAllDataNodeIds() {
    return Collections.singletonList(currentDataNode);
  }

  @Override
  public DataNodeId getCurrentDataNodeId() {
    return currentDataNode;
  }

  @Override
  public void participate() throws Exception {
    for (VcrClusterParticipantListener listener : listeners) {
      for (PartitionId partitionId : assignedPartitionIds) {
        listener.onPartitionAdded(partitionId);
      }
    }
    // TODO Add task for container deletion sync service here.
  }

  @Override
  public Collection<? extends PartitionId> getAssignedPartitionIds() {
    return Collections.unmodifiableCollection(assignedPartitionIds);
  }

  @Override
  public void addListener(VcrClusterParticipantListener listener) {
    listeners.add(listener);
  }

  @Override
  public void close() throws Exception {
  }
}
